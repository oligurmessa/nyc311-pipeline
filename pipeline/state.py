"""
Ingestion state: a run log in DuckDB (`meta.ingest_runs`) from which the watermark is derived.

The watermark is NOT a separately stored number that could drift from reality: it is
max(watermark_to) over *successful* runs. If the database is lost (no run log at all), it is rebuilt
from the raw parquet files (max sys_updated_at), so raw remains the source of truth.
The raw fallback is deliberately NOT used when the run log exists but holds only failed runs: their
partial files end mid-window, and a watermark derived from them would skip the rest of that window.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from . import config

DDL = """
CREATE SCHEMA IF NOT EXISTS meta;
CREATE TABLE IF NOT EXISTS meta.ingest_runs (
    run_id            VARCHAR PRIMARY KEY,
    started_at        TIMESTAMP NOT NULL,
    finished_at       TIMESTAMP,
    status            VARCHAR NOT NULL,            -- running | succeeded | failed
    watermark_from    VARCHAR NOT NULL,            -- :updated_at lower bound (exclusive)
    watermark_to      VARCHAR NOT NULL,            -- :updated_at upper bound (inclusive) = run start (UTC)
    rows_fetched      BIGINT DEFAULT 0,
    files_written     INTEGER DEFAULT 0,
    max_updated_seen  VARCHAR,
    unexpected_keys   VARCHAR,                     -- JSON list of columns not in config.DATA_COLUMNS
    error             VARCHAR
);
"""


def connect(db_path: Path = config.DB_PATH) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute(DDL)
    return con


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def has_any_runs(con: duckdb.DuckDBPyConnection) -> bool:
    return con.execute("SELECT count(*) FROM meta.ingest_runs").fetchone()[0] > 0


def last_successful_watermark(con: duckdb.DuckDBPyConnection) -> str | None:
    row = con.execute("SELECT max(watermark_to) FROM meta.ingest_runs WHERE status = 'succeeded'").fetchone()
    return row[0] if row and row[0] else None


def watermark_from_raw(raw_dir: Path = config.RAW_DIR) -> str | None:
    files = list(raw_dir.glob("ingest_date=*/*.parquet"))
    if not files:
        return None
    con = duckdb.connect()
    row = con.execute(f"SELECT max(sys_updated_at) FROM read_parquet('{raw_dir}/ingest_date=*/*.parquet', union_by_name=true)").fetchone()
    return row[0]


def start_run(con, run_id: str, started_at: str, wm_from: str, wm_to: str) -> None:
    con.execute("INSERT INTO meta.ingest_runs (run_id, started_at, status, watermark_from, watermark_to) VALUES (?, ?, 'running', ?, ?)",
                [run_id, started_at, wm_from, wm_to])


def finish_run(con, run_id: str, status: str, rows: int, files: int, max_seen: str | None,
               unexpected: set[str], error: str | None = None) -> None:
    con.execute("""UPDATE meta.ingest_runs SET finished_at = ?, status = ?, rows_fetched = ?, files_written = ?,
                   max_updated_seen = ?, unexpected_keys = ?, error = ? WHERE run_id = ?""",
                [utc_now_iso(), status, rows, files, max_seen, json.dumps(sorted(unexpected)), error, run_id])


def ensure_raw_view(con, raw_dir: Path = config.RAW_DIR) -> None:
    """Expose the parquet landing zone as raw.service_requests (no copy; schema unions across files)."""
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    glob = f"{raw_dir}/ingest_date=*/*.parquet"
    if list(raw_dir.glob("ingest_date=*/*.parquet")):
        con.execute(f"CREATE OR REPLACE VIEW raw.service_requests AS SELECT * FROM read_parquet('{glob}', union_by_name=true, hive_partitioning=true)")


# --------------------------------------------------------------------------------------
# Portable state: the run log travels with the raw parquet between stateless CI runs.
# --------------------------------------------------------------------------------------
def export_runs(con, path: Path) -> int:
    rows = con.execute("SELECT * FROM meta.ingest_runs ORDER BY started_at").fetchall()
    cols = [d[0] for d in con.description]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([dict(zip(cols, r)) for r in rows], indent=1, default=str))
    return len(rows)


def import_runs(con, path: Path) -> int:
    if not path.exists():
        return 0
    rows = json.loads(path.read_text())
    cols = ["run_id", "started_at", "finished_at", "status", "watermark_from", "watermark_to", "rows_fetched",
            "files_written", "max_updated_seen", "unexpected_keys", "error"]
    con.execute("DELETE FROM meta.ingest_runs")
    con.executemany(f"INSERT INTO meta.ingest_runs ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
                    [[r.get(c) for c in cols] for r in rows])
    return len(rows)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Export/import the ingest run log (meta.ingest_runs) as JSON.")
    ap.add_argument("action", choices=["export", "import", "show"])
    ap.add_argument("--path", default=str(config.DATA_DIR / "state" / "ingest_runs.json"))
    a = ap.parse_args()
    c = connect(config.DB_PATH)
    if a.action == "export":
        print(f"exported {export_runs(c, Path(a.path))} runs -> {a.path}")
    elif a.action == "import":
        print(f"imported {import_runs(c, Path(a.path))} runs <- {a.path}")
        ensure_raw_view(c, config.RAW_DIR)
    else:
        print(c.execute("SELECT run_id, status, watermark_from, watermark_to, rows_fetched, files_written FROM meta.ingest_runs ORDER BY started_at").df().to_string())
    c.close()
