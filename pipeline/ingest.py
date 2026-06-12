"""
Incremental ingestion of NYC 311 service requests into an append-only parquet landing zone.

Run semantics
-------------
* window      = (watermark_from - OVERLAP, run_start_utc]   on Socrata's `:updated_at`
* every page  -> one parquet file under data/raw/311/ingest_date=YYYY-MM-DD/
* raw is append-only: the same request appears once per state change (and once more inside the
  overlap window). Deduplication to "latest version per unique_key" happens in dbt staging.
* the watermark advances to run_start_utc only when the run succeeds. A failed run leaves its
  partial files in place (harmless: duplicates) and the next run re-fetches the same window.

CLI
---
    python -m pipeline.ingest                 # incremental from the last successful run
    python -m pipeline.ingest --start 2026-09-01T00:00:00   # first run / explicit backfill start
    python -m pipeline.ingest --dry-run       # print the window and row count, fetch nothing
"""
from __future__ import annotations

import argparse
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from . import config, state
from .socrata import SocrataClient

log = logging.getLogger("pipeline.ingest")


# --------------------------------------------------------------------------------------
def normalise_rows(rows: list[dict], run_id: str, batch_id: str, ingested_at: str) -> tuple[pa.Table, set[str]]:
    """Map API records onto the fixed raw schema. Values stay as strings (typing is dbt's job)."""
    known = set(config.DATA_COLUMNS) | set(config.SYSTEM_FIELDS)
    unexpected: set[str] = set()
    columns: dict[str, list] = {c: [] for c in config.RAW_COLUMNS}
    for r in rows:
        extra = {k: v for k, v in r.items() if k not in known}
        unexpected.update(extra)
        for api, name in config.SYSTEM_FIELDS.items():
            columns[name].append(r.get(api))
        for c in config.DATA_COLUMNS:
            v = r.get(c)
            if isinstance(v, (dict, list)):
                v = json.dumps(v, separators=(",", ":"))
            elif v is not None:
                v = str(v)
            columns[config.COLUMN_RENAMES.get(c, c)].append(v)
        columns["_run_id"].append(run_id)
        columns["_batch_id"].append(batch_id)
        columns["_ingested_at"].append(ingested_at)
        columns["_extra"].append(json.dumps(extra, separators=(",", ":")) if extra else None)
    schema = pa.schema([(c, pa.string()) for c in config.RAW_COLUMNS])
    return pa.table(columns, schema=schema), unexpected


def write_batch(table: pa.Table, raw_dir: Path, ingest_date: str, batch_id: str) -> Path:
    d = raw_dir / f"ingest_date={ingest_date}"
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{batch_id}.parquet"
    tmp = path.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp, compression="zstd")
    tmp.rename(path)  # atomic: readers never see a half-written file
    return path


# --------------------------------------------------------------------------------------






