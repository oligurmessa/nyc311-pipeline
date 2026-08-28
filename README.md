# nyc311-pipeline

An automated, incremental, tested data pipeline on a live, messy public source: **NYC 311 service requests**
(NYC Open Data, dataset `erm2-nwe9`, ~40M rows, updated daily).

```
Socrata API ──> raw parquet (append-only) ──> DuckDB ──> dbt (staging / core / marts, tests) ──> Streamlit
                      ▲                                      ▲
                      └──────────── Dagster schedule + asset checks ────────────┘
```

Status: **steps 1–4 of 6 done** (ingestion; dbt models, tests and freshness; Dagster orchestration; Streamlit
dashboard). CI/scheduled runs on GitHub Actions and operations notes follow.

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m pipeline.ingest --dry-run                       # show the window and row count, fetch nothing
python -m pipeline.ingest --start 2026-09-01T00:00:00     # first run: backfill (~930k rows, ~15 min)
python -m pipeline.ingest                                 # every later run: only new/updated rows
cd dbt && DBT_PROFILES_DIR=. dbt build && dbt source freshness   # transform + test + freshness
make run-pipeline                                         # the same, orchestrated: ingest -> checks -> dbt
make dagster-ui                                           # Dagster UI at :3000 (schedule runs while it is up)
make dashboard                                            # Streamlit dashboard at :8501
pytest                                                    # unit, asset-check and dbt integration tests (~15s)
```
Python 3.11–3.13 (Dagster has no 3.14 wheels yet).

