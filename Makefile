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

# ---- operations ----
.PHONY: restore-state evidence
restore-state:                   ## pull the latest successful GitHub Actions state (raw + run log) and rebuild the warehouse locally
	@run_id=$$(gh run list --repo oligurmessa/nyc311-pipeline --workflow pipeline.yml --status success --limit 1 --json databaseId --jq '.[0].databaseId'); \
	echo "restoring state from run $$run_id"; rm -rf state-restore && \
	gh run download $$run_id --repo oligurmessa/nyc311-pipeline --name pipeline-state --dir state-restore && \
	rm -rf data/raw data/state && mkdir -p data && tar -xzf state-restore/state.tar.gz -C data && rm -rf state-restore && \
	rm -f warehouse.duckdb && .venv/bin/python -m pipeline.state import && \
	cd dbt && DBT_PROFILES_DIR=. ../.venv/bin/dbt build --quiet && cd .. && .venv/bin/python -m pipeline.state show

evidence:                        ## print the README evidence section from the warehouse
	.venv/bin/python scripts/evidence.py
