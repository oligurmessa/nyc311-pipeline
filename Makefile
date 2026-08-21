.PHONY: install test ingest backfill dry-run

install:
	python -m venv .venv && .venv/bin/pip install -q -r requirements.txt

test:
	.venv/bin/python -m pytest -q

dry-run:
	.venv/bin/python -m pipeline.ingest --dry-run

backfill:
	.venv/bin/python -m pipeline.ingest --start $(START)

ingest:
	.venv/bin/python -m pipeline.ingest

# ---- orchestration ----
.PHONY: dagster-ui run-pipeline
dagster-ui:                      ## local Dagster UI + daemon (schedules run while this is up)
	mkdir -p .dagster && DAGSTER_HOME=$(PWD)/.dagster .venv/bin/dagster dev -m orchestration.definitions

run-pipeline:                    ## one headless end-to-end run: ingest -> checks -> dbt build
	mkdir -p .dagster && DAGSTER_HOME=$(PWD)/.dagster .venv/bin/dagster asset materialize -m orchestration.definitions --select '*'

# ---- dashboard ----
.PHONY: dashboard
dashboard:                       ## Streamlit dashboard on :8501 (reads marts read-only)
	.venv/bin/streamlit run dashboard/app.py --browser.gatherUsageStats false
