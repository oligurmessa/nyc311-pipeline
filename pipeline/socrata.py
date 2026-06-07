"""
Socrata (SODA 2.1) client with retries and keyset pagination.

Why keyset instead of $offset: bulk updates give thousands of rows the identical `:updated_at`
(observed: >50k rows sharing one timestamp), and large offsets get slow on this 40M-row dataset.
Paging on (`:updated_at`, `:id`) is stable and O(1) per page regardless of depth.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Iterator

import requests

from . import config

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Cursor:
    updated_at: str
    row_id: str


class SocrataClient:
    def __init__(self, base_url: str = config.BASE_URL, app_token: str | None = config.APP_TOKEN,
                 timeout: int = config.REQUEST_TIMEOUT, max_retries: int = config.MAX_RETRIES, session=None):
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = session or requests.Session()
        self.session.headers["User-Agent"] = "nyc311-pipeline/0.1 (portfolio project)"
        if app_token:
            self.session.headers["X-App-Token"] = app_token

    # ---------------------------------------------------------------------------------
    def get(self, params: dict) -> list[dict]:
        """GET with exponential backoff on 429 / 5xx / connection errors."""
        delay = 2.0
        for attempt in range(1, self.max_retries + 1):
            try:
                r = self.session.get(self.base_url, params=params, timeout=self.timeout)
                if r.status_code == 200:
                    return r.json()
                if r.status_code in (429, 500, 502, 503, 504):
                    log.warning("HTTP %s on attempt %d/%d: %s", r.status_code, attempt, self.max_retries, r.text[:200])
                else:
                    r.raise_for_status()
            except (requests.ConnectionError, requests.Timeout, ValueError) as e:
                log.warning("request failed on attempt %d/%d: %s", attempt, self.max_retries, e)
            if attempt == self.max_retries:
                raise RuntimeError(f"Socrata request failed after {self.max_retries} attempts: {params}")
            time.sleep(delay)
            delay = min(delay * 2, 60)
        raise AssertionError("unreachable")

    def count(self, where: str) -> int:
        return int(self.get({"$select": "count(*)", "$where": where})[0]["count"])

    # ---------------------------------------------------------------------------------
    @staticmethod
    def window_where(updated_after: str, updated_through: str) -> str:
        return f":updated_at > '{updated_after}' AND :updated_at <= '{updated_through}'"


