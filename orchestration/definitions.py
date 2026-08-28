"""
Dagster definitions for the NYC 311 pipeline.

Asset graph
-----------
    raw/service_requests  ─┐
    meta/ingest_runs      ─┴─>  dbt: staging -> core -> marts   (every dbt test is an asset check)

* `ingest_311` is a multi-asset producing the two keys the dbt sources declare, so dagster-dbt wires the
  lineage automatically (dbt source `raw.service_requests` == Dagster key ["raw", "service_requests"]).
* Asset checks on the raw asset enforce freshness, schema stability and run health *before* dbt runs.
* One job materialises everything; a schedule runs it every 6 hours. The publisher refreshes nightly,
  so most runs fetch 0 rows in seconds; the cadence buys early detection when a refresh is late.

Run locally:   DAGSTER_HOME=$PWD/.dagster dagster dev -m orchestration.definitions
Headless:      dagster asset materialize -m orchestration.definitions --select '*'
"""

import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    AssetSpec,
    Definitions,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    asset_check,
    define_asset_job,
    multi_asset,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from pipeline import config
from pipeline.ingest import run_ingest

ROOT = Path(__file__).resolve().parents[1]
RAW_KEY = AssetKey(["raw", "service_requests"])
META_KEY = AssetKey(["meta", "ingest_runs"])

FRESHNESS_WARN_HOURS = 36
FRESHNESS_ERROR_HOURS = 72

# make the venv's dbt resolvable even when the interpreter is invoked by absolute path (dagster-dbt shells out to "dbt")
os.environ["PATH"] = str(Path(sys.executable).parent) + os.pathsep + os.environ.get("PATH", "")
DBT_EXE = shutil.which("dbt") or str(Path(sys.executable).parent / "dbt")
dbt_project = DbtProject(project_dir=ROOT / "dbt", profiles_dir=ROOT / "dbt")
dbt_project.prepare_if_dev()                    # `dagster dev`: re-parse on every reload
if not dbt_project.manifest_path.exists():      # headless / CI: a fresh checkout has no target/manifest.json
    dbt_project.preparer.prepare(dbt_project)   # equivalent to `dbt parse`


# --------------------------------------------------------------------------------------
# Ingestion
# --------------------------------------------------------------------------------------
@multi_asset(
    specs=[
        AssetSpec(RAW_KEY, description="Append-only parquet landing zone of 311 requests, exposed as a DuckDB view.",
                  group_name="ingestion", kinds={"python", "parquet"}),
        AssetSpec(META_KEY, description="Ingestion run log; the incremental watermark is derived from it.",
                  group_name="ingestion", kinds={"duckdb"}),
    ],
    can_subset=False,
)
def ingest_311(context: AssetExecutionContext):
    """Fetch rows updated since the last successful run (see pipeline/ingest.py) and land them raw."""
    result = run_ingest()
    context.log.info("ingest result: %s", json.dumps(result))
    lower, upper = result["window"]
    md = {
        "run_id": result["run_id"],
        "window_from": lower,
        "window_to": upper,
        "rows_fetched": result["rows"],
        "files_written": result["files"],
        "max_updated_seen": result["max_updated_seen"] or "",
        "unexpected_keys": MetadataValue.json(result["unexpected_keys"]),
    }
    yield MaterializeResult(asset_key=RAW_KEY, metadata=md)
    yield MaterializeResult(asset_key=META_KEY, metadata={"run_id": result["run_id"]})


def _con():
    return duckdb.connect(str(config.DB_PATH), read_only=True)


@asset_check(asset=RAW_KEY, description=f"Newest :updated_at in raw is < {FRESHNESS_ERROR_HOURS}h old (warn at {FRESHNESS_WARN_HOURS}h).", blocking=True)
def raw_is_fresh() -> AssetCheckResult:
    with _con() as con:
        newest = con.execute("select max(try_cast(replace(sys_updated_at, 'Z', '') as timestamp)) from raw.service_requests").fetchone()[0]
    if newest is None:
        return AssetCheckResult(passed=False, severity=AssetCheckSeverity.ERROR, metadata={"reason": "raw is empty"})
    hours = (datetime.now(timezone.utc).replace(tzinfo=None) - newest).total_seconds() / 3600
    md = {"newest_source_update": str(newest), "hours_since_update": round(hours, 1)}
    if hours > FRESHNESS_ERROR_HOURS:
        return AssetCheckResult(passed=False, severity=AssetCheckSeverity.ERROR, metadata=md)
    if hours > FRESHNESS_WARN_HOURS:
        return AssetCheckResult(passed=False, severity=AssetCheckSeverity.WARN, metadata=md)
    return AssetCheckResult(passed=True, metadata=md)


@asset_check(asset=RAW_KEY, description="The last run saw no columns outside the declared schema (drift lands in _extra).")
def raw_schema_is_stable() -> AssetCheckResult:
    with _con() as con:
        row = con.execute("select unexpected_keys from meta.ingest_runs order by started_at desc limit 1").fetchone()
    keys = json.loads(row[0]) if row and row[0] else []
    return AssetCheckResult(passed=not keys, severity=AssetCheckSeverity.WARN, metadata={"unexpected_keys": MetadataValue.json(keys)})


@asset_check(asset=RAW_KEY, description="Duplicate copies in raw stay below 20% (otherwise the watermark over-fetches).")
def raw_duplicate_rate_is_sane() -> AssetCheckResult:
    with _con() as con:
        n, v = con.execute("select count(*), count(distinct unique_key || sys_updated_at) from raw.service_requests").fetchone()
    rate = (n - v) / n if n else 0.0
    return AssetCheckResult(passed=rate <= 0.20, severity=AssetCheckSeverity.WARN,
                            metadata={"rows": n, "distinct_versions": v, "duplicate_rate": round(rate, 4)})


@asset_check(asset=META_KEY, description="The most recent ingest run succeeded.", blocking=True)
def last_ingest_run_succeeded() -> AssetCheckResult:
    with _con() as con:
        row = con.execute("select run_id, status, error, rows_fetched from meta.ingest_runs order by started_at desc limit 1").fetchone()
    if row is None:
        return AssetCheckResult(passed=False, metadata={"reason": "no runs"})
    run_id, status, error, rows = row
    return AssetCheckResult(passed=status == "succeeded", metadata={"run_id": run_id, "status": status, "error": error or "", "rows_fetched": rows})


# --------------------------------------------------------------------------------------
# dbt
# --------------------------------------------------------------------------------------
class LayerAsGroup(DagsterDbtTranslator):
    """Group dbt assets by their schema (staging / core / marts) so the lineage graph reads left-to-right."""

    def get_group_name(self, dbt_resource_props):
        return dbt_resource_props.get("schema") or super().get_group_name(dbt_resource_props)


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project, dagster_dbt_translator=LayerAsGroup())
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """`dbt build`: models + tests; each test surfaces as an asset check on its model."""
    yield from dbt.cli(["build"], context=context).stream()


# --------------------------------------------------------------------------------------
# Jobs & schedule
# --------------------------------------------------------------------------------------
refresh_job = define_asset_job(
    name="nyc311_refresh",
    selection=AssetSelection.all(),
    description="Ingest new/updated 311 requests, then dbt build (models + tests).",
)

refresh_schedule = ScheduleDefinition(
    job=refresh_job,
    cron_schedule="15 */6 * * *",   # 00:15, 06:15, 12:15, 18:15 UTC; the nightly refresh lands ~01:33 UTC
    execution_timezone="UTC",
    description="Every 6 hours. Most runs fetch nothing; the 06:15 run picks up the nightly refresh.",
)

defs = Definitions(
    assets=[ingest_311, dbt_models],
    asset_checks=[raw_is_fresh, raw_schema_is_stable, raw_duplicate_rate_is_sane, last_ingest_run_succeeded],
    jobs=[refresh_job],
    schedules=[refresh_schedule],
    resources={"dbt": DbtCliResource(project_dir=dbt_project, dbt_executable=DBT_EXE)},
)
