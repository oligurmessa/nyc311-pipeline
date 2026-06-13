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
def resolve_window(con, start: str | None, now: datetime, raw_dir: Path = config.RAW_DIR) -> tuple[str, str]:
    """Return (updated_after, updated_through) for this run."""
    wm = state.last_successful_watermark(con)
    if wm is None and not state.has_any_runs(con):
        wm = state.watermark_from_raw(raw_dir)  # warehouse lost: rebuild the watermark from the landing zone
        if wm:
            log.warning("no run log found; watermark %s rebuilt from raw parquet", wm)
    if start and wm:
        raise SystemExit(f"--start given but a watermark already exists ({wm}); use --force-start to override")
    if not wm:
        if not start:
            start = config.BACKFILL_START
            log.info("no watermark found; backfilling from config.BACKFILL_START=%s", start)
        lower = start
    else:
        lower = (datetime.fromisoformat(wm.replace("Z", "")) - timedelta(minutes=config.OVERLAP_MINUTES)).strftime("%Y-%m-%dT%H:%M:%S")
    upper = now.strftime("%Y-%m-%dT%H:%M:%S")
    return lower, upper


def run_ingest(client: SocrataClient | None = None, start: str | None = None, force_start: bool = False,
               dry_run: bool = False, raw_dir: Path = config.RAW_DIR, db_path: Path = config.DB_PATH,
               page_size: int = config.PAGE_SIZE) -> dict:
    client = client or SocrataClient()
    con = state.connect(db_path)
    now = datetime.now(timezone.utc)
    if force_start:
        lower, upper = start, now.strftime("%Y-%m-%dT%H:%M:%S")
    else:
        lower, upper = resolve_window(con, start, now, raw_dir)
    log.info("window: :updated_at in (%s, %s]", lower, upper)

    if dry_run:
        n = client.count(client.window_where(lower, upper))
        log.info("dry run: %d rows would be fetched", n)
        return {"window": (lower, upper), "rows": n, "dry_run": True}

    run_id = now.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    ingest_date = now.strftime("%Y-%m-%d")
    state.start_run(con, run_id, now.strftime("%Y-%m-%dT%H:%M:%S"), lower, upper)

    rows_total, files, max_seen, unexpected = 0, 0, None, set()
    try:
        for i, rows in enumerate(client.iter_pages(lower, upper, page_size=page_size)):
            batch_id = f"{run_id}-{i:04d}"
            table, extra = normalise_rows(rows, run_id, batch_id, state.utc_now_iso())
            write_batch(table, raw_dir, ingest_date, batch_id)
            rows_total += len(rows)
            files += 1
            unexpected |= extra
            max_seen = max(max_seen or "", rows[-1][":updated_at"])
        state.finish_run(con, run_id, "succeeded", rows_total, files, max_seen, unexpected)
        state.ensure_raw_view(con, raw_dir)
    except Exception as e:  # noqa: BLE001 - we want the run log to capture anything
        state.finish_run(con, run_id, "failed", rows_total, files, max_seen, unexpected, error=repr(e))
        log.exception("run %s failed after %d rows / %d files", run_id, rows_total, files)
        raise
    finally:
        con.close()

    if unexpected:
        log.warning("schema drift: unexpected keys %s (stored in _extra)", sorted(unexpected))
    log.info("run %s succeeded: %d rows, %d files, max :updated_at seen %s", run_id, rows_total, files, max_seen)
    return {"run_id": run_id, "window": (lower, upper), "rows": rows_total, "files": files,
            "max_updated_seen": max_seen, "unexpected_keys": sorted(unexpected)}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", help="ISO timestamp; lower bound on :updated_at for a first run / backfill")
    ap.add_argument("--force-start", action="store_true", help="use --start even if a watermark exists")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--page-size", type=int, default=config.PAGE_SIZE)
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if a.force_start and not a.start:
        ap.error("--force-start requires --start")
    result = run_ingest(start=a.start, force_start=a.force_start, dry_run=a.dry_run, page_size=a.page_size)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
