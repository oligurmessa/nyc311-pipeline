"""Asset checks must fail loudly: stale data, schema drift, failed runs, over-fetching."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from dagster import AssetCheckSeverity

from pipeline import config, ingest, state


def _land(raw, db, rows, run_id="run1", status="succeeded", unexpected=set(), ingested_at="2026-09-11T02:00:00"):
    t, _ = ingest.normalise_rows(rows, run_id, f"{run_id}-0000", ingested_at)
    ingest.write_batch(t, raw, "2026-09-11", f"{run_id}-0000")
    con = state.connect(db)
    state.start_run(con, run_id, ingested_at, "2026-09-01T00:00:00", ingested_at)
    state.finish_run(con, run_id, status, len(rows), 1, rows[-1][":updated_at"], unexpected, error="boom" if status == "failed" else None)
    state.ensure_raw_view(con, raw)
    con.close()


def _row(i, updated, **kw):
    r = {":id": f"row-{i:04d}", ":updated_at": updated, ":created_at": "2026-09-01T00:00:00.000Z", ":version": "v",
         "unique_key": str(i), "created_date": "2026-09-10T10:00:00.000", "status": "Open"}
    r.update(kw)
    return r


@pytest.fixture
def env(tmp_path, monkeypatch):
    raw, db = tmp_path / "raw", tmp_path / "wh.duckdb"
    monkeypatch.setattr(config, "RAW_DIR", raw)
    monkeypatch.setattr(config, "DB_PATH", db)
    # import after patching so the module reads the patched config at call time
    from orchestration import definitions as d
    return raw, db, d


def _hours_ago(h):
    return (datetime.now(timezone.utc) - timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def test_freshness_check_passes_warns_errors(env):
    raw, db, d = env
    _land(raw, db, [_row(1, _hours_ago(10))])
    r = d.raw_is_fresh()
    assert r.passed and r.metadata["hours_since_update"].value < 11

    _land(raw, db, [_row(2, _hours_ago(50))], run_id="run2")   # newest row is now 10h old still -> pass
    assert d.raw_is_fresh().passed

    # rebuild from scratch with only stale data
    for f in raw.glob("**/*.parquet"):
        f.unlink()
    db.unlink()
    _land(raw, db, [_row(3, _hours_ago(50))], run_id="run3")
    r = d.raw_is_fresh()
    assert not r.passed and r.severity == AssetCheckSeverity.WARN

    for f in raw.glob("**/*.parquet"):
        f.unlink()
    db.unlink()
    _land(raw, db, [_row(4, _hours_ago(100))], run_id="run4")
    r = d.raw_is_fresh()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR


def test_schema_drift_check_warns_on_unexpected_keys(env):
    raw, db, d = env
    _land(raw, db, [_row(1, _hours_ago(1))])
    assert d.raw_schema_is_stable().passed
    _land(raw, db, [_row(2, _hours_ago(1), brand_new_col="x")], run_id="run2", unexpected={"brand_new_col"}, ingested_at="2026-09-12T02:00:00")
    r = d.raw_schema_is_stable()
    assert not r.passed and r.severity == AssetCheckSeverity.WARN
    assert r.metadata["unexpected_keys"].value == ["brand_new_col"]


def test_last_run_check_fails_on_failed_run(env):
    raw, db, d = env
    _land(raw, db, [_row(1, _hours_ago(1))])
    assert d.last_ingest_run_succeeded().passed
    _land(raw, db, [_row(2, _hours_ago(1))], run_id="run2", status="failed", ingested_at="2026-09-12T02:00:00")
    r = d.last_ingest_run_succeeded()
    assert not r.passed and r.metadata["error"].value == "boom"


def test_duplicate_rate_check(env):
    raw, db, d = env
    ts = _hours_ago(1)   # one fixed timestamp: the same *version* re-landed, not three versions a second apart
    _land(raw, db, [_row(i, ts) for i in range(10)])
    assert d.raw_duplicate_rate_is_sane().passed
    # re-land the same 10 versions twice more: 30 rows, 10 distinct versions -> 67% duplicates
    _land(raw, db, [_row(i, ts) for i in range(10)], run_id="run2")
    _land(raw, db, [_row(i, ts) for i in range(10)], run_id="run3")
    r = d.raw_duplicate_rate_is_sane()
    assert not r.passed and r.metadata["duplicate_rate"].value > 0.6


def test_definitions_load_and_wire_dbt_sources_to_ingest_asset():
    from orchestration.definitions import defs, RAW_KEY, META_KEY
    g = defs.resolve_asset_graph()
    keys = set(g.get_all_asset_keys())
    assert RAW_KEY in keys and META_KEY in keys
    stg = next(k for k in keys if k.path[-1] == "stg_311__service_requests")
    assert RAW_KEY in g.get(stg).parent_keys          # dbt source resolved to the ingest asset
    assert [s.cron_schedule for s in defs.schedules] == ["15 */6 * * *"]
