"""
Integration test: raw parquet -> dbt build -> late-arriving update -> dbt build again.

Runs the real dbt project against a throwaway DuckDB + raw directory and asserts that
  * the first build loads every request once,
  * a second build with a newer version of an existing request updates the fact row in place,
    adds a second row to the status history, and scans only the new batch.
Marked slow; runs in ~15s.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import duckdb
import pytest

from pipeline import ingest, state

ROOT = Path(__file__).resolve().parents[1]
DBT = ROOT / ".venv" / "bin" / "dbt"
if not DBT.exists():
    DBT = Path("dbt")


def mk(i, updated, created="2026-09-10T10:00:00.000", **kw):
    r = {":id": f"row-{i:04d}", ":updated_at": updated, ":created_at": "2026-09-01T00:00:00.000Z", ":version": "v",
         "unique_key": str(50_000 + i), "created_date": created, "agency": "DOT", "agency_name": "Department of Transportation",
         "complaint_type": "Street Condition", "descriptor": "Pothole", "status": "Open", "incident_zip": "10001",
         "borough": "MANHATTAN", "latitude": "40.75", "longitude": "-73.99", "open_data_channel_type": "ONLINE"}
    r.update(kw)
    return r


def dbt_build(db_path: Path, select: str | None = None) -> str:
    env = {**os.environ, "NYC311_DB_PATH": str(db_path), "DBT_PROFILES_DIR": str(ROOT / "dbt")}
    cmd = [str(DBT), "build", "--quiet", "--no-partial-parse", "--target-path", str(db_path.parent / "target"),
           "--vars", "{max_hours_since_source_update: 1000000}"]   # synthetic data is 'stale' by design
    if select:
        cmd += ["--select", select]
    p = subprocess.run(cmd, cwd=ROOT / "dbt", env=env, capture_output=True, text=True)
    assert p.returncode == 0, p.stdout + p.stderr
    return p.stdout


@pytest.mark.slow
def test_late_arriving_update_is_merged(tmp_path):
    raw = tmp_path / "raw"
    db = tmp_path / "wh.duckdb"

    # --- batch 1: three open requests ----------------------------------------------------
    rows = [mk(i, "2026-09-11T01:33:00.000Z") for i in range(3)]
    t, _ = ingest.normalise_rows(rows, "run1", "run1-0000", "2026-09-11T02:00:00")
    ingest.write_batch(t, raw, "2026-09-11", "run1-0000")
    con = state.connect(db)
    state.start_run(con, "run1", "2026-09-11T02:00:00", "2026-09-01T00:00:00", "2026-09-11T02:00:00")
    state.finish_run(con, "run1", "succeeded", 3, 1, "2026-09-11T01:33:00.000Z", set())
    state.ensure_raw_view(con, raw)
    con.close()

    dbt_build(db)
    con = duckdb.connect(str(db), read_only=True)
    assert con.execute("select count(*) from core.fct_service_requests").fetchone()[0] == 3
    assert con.execute("select status, closed_at from core.fct_service_requests where unique_key='50001'").fetchone() == ("Open", None)
    assert con.execute("select count(*) from core.fct_request_status_history").fetchone()[0] == 3
    con.close()

    # --- batch 2: request 50001 closes two days later; plus an overlap duplicate of 50002 ---
    rows = [mk(1, "2026-09-13T01:33:00.000Z", status="Closed", closed_date="2026-09-12T15:30:00.000"),
            mk(2, "2026-09-11T01:33:00.000Z")]  # identical version re-fetched inside the overlap window
    t, _ = ingest.normalise_rows(rows, "run2", "run2-0000", "2026-09-13T02:00:00")
    ingest.write_batch(t, raw, "2026-09-13", "run2-0000")
    con = state.connect(db)
    state.start_run(con, "run2", "2026-09-13T02:00:00", "2026-09-11T01:45:00", "2026-09-13T02:00:00")
    state.finish_run(con, "run2", "succeeded", 2, 1, "2026-09-13T01:33:00.000Z", set())
    state.ensure_raw_view(con, raw)
    con.close()

    dbt_build(db)
    con = duckdb.connect(str(db), read_only=True)
    # still one row per request; 50001 is now closed with a computed duration
    assert con.execute("select count(*) from core.fct_service_requests").fetchone()[0] == 3
    status, hours = con.execute("select status, hours_to_close from core.fct_service_requests where unique_key='50001'").fetchone()
    assert status == "Closed" and abs(hours - 53.5) < 0.01
    # only the new batch was scanned into staging: 50000 keeps its first ingest stamp, 50002's duplicate collapsed
    stamps = dict(con.execute("select unique_key, _ingested_at from staging.stg_311__service_requests").fetchall())
    assert stamps["50000"] == "2026-09-11T02:00:00" and stamps["50001"] == "2026-09-13T02:00:00"
    assert con.execute("select count(*) from staging.stg_311__request_versions").fetchone()[0] == 4   # 3 + 1 new version
    # history shows the transition
    hist = con.execute("select version_no, status, previous_status from core.fct_request_status_history where unique_key='50001' order by version_no").fetchall()
    assert hist == [(1, "Open", None), (2, "Closed", "Open")]
    # marts reflect the change and the health row sees both runs
    assert con.execute("select n_closed from marts.mart_daily_volume").fetchone()[0] == 1
    assert con.execute("select n_successful_runs, last_successful_run_id from marts.mart_pipeline_health").fetchone() == (2, "run2")
    con.close()
