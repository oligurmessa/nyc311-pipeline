"""Unit tests for the ingestion layer: paging, normalisation, watermark semantics, failure handling."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import duckdb
import pytest

from pipeline import config, ingest, state
from pipeline.socrata import Cursor, SocrataClient


# ---------------------------------------------------------------- fakes -------------------
def _ts(s: str) -> datetime:
    """Socrata compares timestamps, not strings; mirror that in the fake."""
    return datetime.fromisoformat(s.replace("Z", "")).replace(tzinfo=None)


class _FakeSession:
    headers: dict = {}


class FakeClient(SocrataClient):
    """Serves an in-memory dataset with the same ordering/keyset semantics as Socrata."""

    def __init__(self, rows, fail_after_pages=None):
        super().__init__(session=_FakeSession())
        self.rows = sorted(rows, key=lambda r: (r[":updated_at"], r[":id"]))
        self.calls = []
        self.fail_after_pages = fail_after_pages

    def get(self, params):
        self.calls.append(params)
        if params.get("$select") == "count(*)":
            return [{"count": str(len(self._filter(params["$where"])))}]
        if self.fail_after_pages is not None and len([c for c in self.calls if "$limit" in c]) > self.fail_after_pages:
            raise RuntimeError("boom")
        return self._filter(params["$where"])[: params["$limit"]]

    def _filter(self, where):
        # parse the pieces our client generates
        import re
        lo = re.search(r":updated_at > '([^']+)' AND :updated_at <= '([^']+)'", where)
        after, through = _ts(lo.group(1)), _ts(lo.group(2))
        rows = [r for r in self.rows if after < _ts(r[":updated_at"]) <= through]
        ks = re.search(r"\(:updated_at > '([^']+)'\) OR \(:updated_at = '[^']+' AND :id > '([^']+)'\)", where)
        if ks:
            cu, cid = ks.group(1), ks.group(2)
            rows = [r for r in rows if (r[":updated_at"], r[":id"]) > (cu, cid)]
        return rows


def mk(i, updated, **kw):
    r = {":id": f"row-{i:04d}", ":updated_at": updated, ":created_at": "2026-09-01T00:00:00.000Z", ":version": "v",
         "unique_key": str(10_000 + i), "created_date": "2026-09-01T10:00:00.000", "agency": "NYPD",
         "complaint_type": "Noise", "status": "Open"}
    r.update(kw)
    return r


@pytest.fixture
def env(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    db = tmp_path / "wh.duckdb"
    monkeypatch.setattr(config, "RAW_DIR", raw)
    monkeypatch.setattr(config, "DB_PATH", db)
    return raw, db


# ---------------------------------------------------------------- tests -------------------
def test_keyset_where_builds_tuple_comparison():
    base = SocrataClient.window_where("2026-09-01T00:00:00", "2026-09-02T00:00:00")
    w = SocrataClient.keyset_where(base, Cursor("2026-09-01T12:00:00.000Z", "row-abc"))
    assert ":updated_at > '2026-09-01T00:00:00'" in w
    assert "(:updated_at > '2026-09-01T12:00:00.000Z') OR (:updated_at = '2026-09-01T12:00:00.000Z' AND :id > 'row-abc')" in w


def test_paging_handles_identical_timestamps_without_skips_or_dups():
    # 25 rows, 20 of which share one :updated_at -> must survive page boundaries inside the tie
    rows = [mk(i, "2026-09-05T01:00:00.000Z") for i in range(20)] + [mk(i, f"2026-09-05T02:00:0{i-20}.000Z") for i in range(20, 25)]
    c = FakeClient(rows)
    pages = list(c.iter_pages("2026-09-01T00:00:00", "2026-09-30T00:00:00", page_size=7))
    got = [r[":id"] for p in pages for r in p]
    assert got == sorted(got) and len(got) == 25 and len(set(got)) == 25
    assert [len(p) for p in pages] == [7, 7, 7, 4]


def test_normalise_rows_fixed_schema_and_extra_capture():
    rows = [mk(1, "2026-09-05T01:00:00.000Z", location={"type": "Point", "coordinates": [-73.9, 40.7]}, brand_new_col="x")]
    t, unexpected = ingest.normalise_rows(rows, "run", "batch", "2026-09-06T00:00:00")
    assert t.column_names == config.RAW_COLUMNS
    assert t.num_rows == 1
    d = t.to_pylist()[0]
    assert d["sys_id"] == "row-0001" and d["closed_date"] is None
    assert json.loads(d["location"])["type"] == "Point"
    assert unexpected == {"brand_new_col"} and json.loads(d["_extra"]) == {"brand_new_col": "x"}










