# nyc311-pipeline

An automated, incremental, tested data pipeline on a live, messy public source: **NYC 311 service requests**
(NYC Open Data, dataset `erm2-nwe9`, ~40M rows, updated daily).

```
Socrata API ──> raw parquet (append-only) ──> DuckDB ──> dbt (staging / core / marts, tests) ──> Streamlit
                      ▲                                      ▲
                      └──────────── Dagster schedule + asset checks ────────────┘
```

[![pipeline](https://github.com/oligurmessa/nyc311-pipeline/actions/workflows/pipeline.yml/badge.svg)](https://github.com/oligurmessa/nyc311-pipeline/actions/workflows/pipeline.yml)
[![ci](https://github.com/oligurmessa/nyc311-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/oligurmessa/nyc311-pipeline/actions/workflows/ci.yml)

Status: **all six steps built.** The pipeline runs unattended on GitHub Actions every six hours. Section 7 holds the
operational evidence and is regenerated from the warehouse with `make evidence` as runs accumulate.

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
make restore-state                                        # pull the latest GitHub Actions state and rebuild the warehouse locally
make evidence                                             # print section 7 of this README from the warehouse
pytest                                                    # unit, asset-check and dbt integration tests (~15s)
```
Python 3.11–3.13 (Dagster has no 3.14 wheels yet).

## Design doc

### 1. Ingestion: how incremental loads work

**Watermark on `:updated_at`, not on `created_date`.** A 311 request is a long-lived record: it is created,
assigned, updated and closed over days or weeks, and every change bumps Socrata's system field `:updated_at`.
Pulling on `created_date` would capture a request once and never see it close. Pulling on `:updated_at` captures
every state change, which is what makes time-to-close and backlog metrics correct.

**Run window.** Each run fetches rows with `:updated_at` in `(watermark − 15 min, run_start_utc]`:

- the upper bound is fixed at run start, so rows updated *during* the run are not half-captured; they fall into
  the next window;
- the lower bound overlaps the previous watermark by 15 minutes to absorb clock skew between this machine and
  Socrata. The overlap re-fetches a few rows; raw is append-only and staging deduplicates, so this is harmless.

**Watermark derivation.** There is no separately stored "last timestamp" that can drift. The watermark is
`max(watermark_to)` over *successful* runs in `meta.ingest_runs`. A failed run never advances it, so the next
run re-fetches the same window. If the warehouse file is deleted, the watermark is rebuilt from the raw parquet
files (`max(sys_updated_at)`): raw is the source of truth.

**Keyset pagination.** Bulk updates give tens of thousands of rows the *same* `:updated_at` (observed: 50k rows
sharing one timestamp), which breaks naive "greater than last timestamp" paging, and `$offset` paging degrades
on a 40M-row table. Pages are therefore ordered by `(:updated_at, :id)` and each page continues from the last
`(timestamp, id)` tuple: stable under ties and O(1) per page at any depth. `tests/test_ingest.py` covers the tie
case explicitly.

**Raw landing zone.** Every page becomes one zstd parquet file at
`data/raw/311/ingest_date=YYYY-MM-DD/<run_id>-<page>.parquet`, written atomically (tmp file, then rename).
All 48 declared columns are present in every file as strings, because the API omits null keys and otherwise every
batch would have a different schema. Unexpected keys (schema drift) are not dropped: they are stored as JSON in
`_extra` and the run log records them. Typing, trimming and deduplication are deliberately left to dbt so the
raw layer is a faithful replay log of what the API returned.

**Failure behaviour.** The client retries with exponential backoff on 429/5xx/timeouts. If a run fails
mid-way, the files already written stay (they are valid data), the run is marked `failed` with the error, the
watermark does not move, and the next run re-fetches the window. The result is duplicate versions in raw, never
missing rows.

**What the first runs showed (evidence for the choices above).** Backfill of `:updated_at > 2026-09-01`
landed 932,098 rows in 19 files (79 MB parquet) in 9 minutes. Of those, 345,626 requests were *created before
September* (some in 2020) but updated in September: the late-arriving-update problem is real, not theoretical.
Only 95 distinct `:updated_at` values exist in the whole month, and 533,927 rows share a single one, because the
publisher refreshes the dataset in a nightly bulk job (~01:33 UTC): plain timestamp paging would have broken on
page one. The run log also recorded 357 requests with `closed_date` before `created_date`, which becomes a dbt
test in step 2. The second run, 24 minutes later, fetched 0 rows (the nightly refresh had not happened yet),
which is the expected behaviour and the reason the schedule only needs to run a few times a day.

### 2. Data model

```
raw.service_requests  (view over parquet; every API record ever landed, text columns)
   │
   ├─ staging.stg_311__service_requests   one row per request = LATEST version; typed, cleaned, flagged
   └─ staging.stg_311__request_versions   one row per (request, source_updated_at): the replay log
   │
   ├─ core.fct_service_requests           grain: request (current state) ── dim_agency
   │                                                                     ── dim_complaint_type
   │                                                                     ── dim_location (zip)
   │                                                                     ── dim_date
   └─ core.fct_request_status_history     grain: request × observed version (previous_status, version_no)
   │
   ├─ marts.mart_daily_volume             created_date × borough × complaint family × agency
   ├─ marts.mart_resolution_time          week × agency: median / p90 hours to close
   ├─ marts.mart_open_backlog             open requests by age bucket × agency × borough
   └─ marts.mart_pipeline_health          one row: freshness, last run, counts (dashboard banner)
```

**Why a star schema with one request-grain fact.** Every question the dashboard answers — how many, how fast,
how many still open — is a question about requests. One fact at that grain keeps measures additive and makes
`unique` on `unique_key` the single most important test in the project. Dimensions exist for the three things
people slice by (agency, complaint type, place) plus a calendar. Complaint type carries a hand-written
`complaint_family` because the source has ~300 raw types and a dashboard needs ~10.

**Why a second fact for status history.** The current-state fact cannot show *when* a request moved from Open
to Closed or how often the source revises a record. `fct_request_status_history` keeps every observed version
(one row per `(unique_key, source_updated_at)`) with `LAG(status)`, so transitions are queryable. It is the
table that proves late-arriving updates are being captured rather than silently overwritten.

**Why dedupe in staging, not in ingestion.** Raw is a faithful log; collapsing to the latest version happens
once, in SQL, with an explicit ordering (`source_updated_at desc, _ingested_at desc, sys_id desc`) that is
tested. If the ordering rule ever needs to change, the raw log can be replayed.

**Incremental models.** Staging and both facts are `incremental` with `delete+insert` on their unique keys,
filtered on `_ingested_at > max(_ingested_at)` already in the model: each build scans only the parquet batches
that landed since the previous build. On the 932k-row backfill the first build took 7s; a no-change rebuild
takes 1.6s end to end. `tests/test_dbt_incremental.py` runs the real dbt project on a throwaway warehouse,
lands a late closure for an existing request, rebuilds, and asserts the fact row changed in place, the history
gained a transition row, the overlap duplicate collapsed, and untouched rows kept their original ingest stamp.

**Coverage start.** The pipeline only sees requests *updated* since 2026-09-01. A request created in 2021 and
closed in September is in the fact table (correct: it is a real closure), but counting it toward "requests
created in 2021" would be a biased sample. Cohort-style marts (`mart_daily_volume`, `mart_resolution_time`)
therefore filter to `created_at >= coverage_start()`, derived from the run log, while the fact table keeps
everything.

**Cleaning rules worth knowing** (`stg_311__service_requests`): timestamps are parsed from Socrata's
offset-less text and kept as naive NYC-local times; zips must match `^[0-9]{5}$` or become null (raw value
kept in `incident_zip_raw`); borough is upper-cased and `Unspecified` becomes null; status is normalised to six
values; nothing is dropped.

### 3. Data quality: what fails loudly, what warns, and why

| check | where | behaviour |
|---|---|---|
| `unique_key` unique and not null | staging, fact | **error** — the one invariant everything depends on |
| FK integrity fact → dims | core | **error** |
| accepted values: status, borough | staging, fact, dims | **error** — a new value means the source changed |
| lat/long inside the NYC bounding box, zip is 5 digits | staging | **error** on cleaned columns (raw values are kept) |
| `created_at` in the future | staging | **error** |
| `hours_to_close` within 0–100k | fact | **error** |
| closed before created | staging | **warn** if > 0, **error** if > 5,000 (singular test) |
| raw duplicate rate > 20 % | raw | **warn** — watermark logic is over-fetching |
| source freshness | `dbt source freshness` | **warn** at 36 h, **error** at 72 h since newest `:updated_at` |
| `hours_since_source_update` ≤ 72 | `mart_pipeline_health` | **error** — same threshold, enforced inside `dbt build` |

The closed-before-created rule is the interesting one. The source contains ~360 such rows per month,
permanently. A test that errors on them would be red forever and get ignored, which is worse than no test.
So the rows are kept, flagged (`has_invalid_close_time`), excluded from duration metrics, counted in the health
mart, and the test *warns* on any and *errors* only if the count jumps by an order of magnitude, which is the
signal that the source's semantics changed. All test failures are stored (`store_failures`) in schema `dq`, so a
failing build leaves the offending rows queryable.

Current state after the backfill: 50 tests pass, 1 warns (357 closed-before-created rows), 0 errors.
Null rates on the cleaned data: 1.1 % no coordinates, 0.6 % no valid zip, 0.1 % no borough.

### 4. Orchestration: Dagster assets, checks and schedule

**Why Dagster, and why assets rather than tasks.** The pipeline is a small DAG of *things that exist*
(a landing zone, a run log, fourteen tables), not a sequence of steps. Dagster's asset model matches that:
every dbt model is an asset with its own lineage and history, every dbt test is an asset check on the model it
tests, and the dbt source `raw.service_requests` resolves automatically to the Python asset that produces it.
A failed check is visible on the asset it belongs to, not buried in a task log.

```
ingest_311  (multi-asset)            asset checks on raw/service_requests
   ├─ raw/service_requests   ──────►    raw_is_fresh            blocking · ERROR > 72 h · WARN > 36 h
   └─ meta/ingest_runs       ──────►    raw_schema_is_stable    WARN if the last run saw undeclared columns
                                        raw_duplicate_rate_is_sane   WARN if > 20 % duplicate versions
                                      asset check on meta/ingest_runs
                                        last_ingest_run_succeeded    blocking
dbt_models  (dbt build)              36 dbt tests → asset checks on their models
                                     groups: ingestion → staging → core → marts (lineage reads left to right)
```

**Blocking checks.** `raw_is_fresh` and `last_ingest_run_succeeded` are *blocking*: if raw is stale or the
ingest run failed, dbt does not run and the marts keep their last good state rather than being rebuilt from
bad input. Non-blocking checks (schema drift, duplicate rate) surface as warnings and never hide data.

**Schedule.** `nyc311_refresh` materialises everything every six hours (`15 */6 * * *` UTC). The publisher
refreshes nightly at ~01:33 UTC, so three of four runs fetch nothing and finish in ~25 s; the 06:15 run carries
the day's updates. The cadence is about detection latency, not throughput: a missed refresh is noticed within
hours, not days. Locally the schedule runs while `dagster dev` is up; in step 5 GitHub Actions becomes the
clock and runs the same job headlessly (`dagster asset materialize --select '*'`).

**What a run records.** The ingestion asset attaches the window, rows fetched, files written, newest
`:updated_at` and any unexpected keys as materialisation metadata, so the Dagster UI shows per-run row counts
over time without querying the warehouse. `tests/test_checks.py` proves each custom check fails on stale data,
schema drift, a failed run and over-fetching, and that the dbt source wiring resolves to the ingest asset.

### 5. Dashboard

![dashboard](images/dashboard.jpg)

`dashboard/app.py` is a single Streamlit page that reads **only the marts and the run log**, never the fact
or staging tables, through a read-only DuckDB connection cached for five minutes. If a pipeline run holds the
write lock it says so and stops rather than showing a half-built state.

- **Freshness banner** from `mart_pipeline_health`: FRESH / LATE / STALE using the same 36 h / 72 h thresholds
  as the dbt and Dagster checks, plus request count, last run, rows fetched and the success/failure tally.
  A failed last run is shown in red with its error.
- **Filters**: created-date range (defaulting to exclude the newest, always-partial day), borough, complaint family.
- **Daily volume** stacked by complaint family or borough, on a complete day × category grid so areas never cross.
- **Time to close by agency** (median and p90 hours, share closed within 24 h) and **open backlog by age bucket**.
- **Data quality panel**: every stored dbt test failure table with its row count, the known closed-before-created
  anomaly, and the **observed status transitions** from `fct_request_status_history`, which is empty until the
  source's next nightly refresh delivers the first late-arriving updates.
- **Ingest run log**: the last 20 runs with windows, rows, files and any schema drift.

![data quality panel](images/dashboard_quality.jpg)

### 6. Operations: how it runs unattended

**Two workflows.**

| workflow | trigger | what it does |
|---|---|---|
| `pipeline.yml` | cron `15 */6 * * *` UTC, or manual | restore state → Dagster job (ingest → checks → dbt build + tests) → `dbt source freshness` → upload state |
| `ci.yml` | push to `main`, pull requests | `dbt parse`, the full pytest suite (unit, asset checks, dbt integration on a fixture warehouse), Dagster definitions load |

**State between stateless runs.** A GitHub runner starts empty, so the pipeline's state travels as a workflow
artifact: `state.tar.gz` holds the raw parquet landing zone (~80 MB, growing ~2–3 MB per day) and the ingest run
log exported to JSON. The next run downloads the artifact from the *last successful* run (`gh run download`),
re-imports the run log, recreates the raw view and rebuilds the DuckDB warehouse with a full `dbt build`
(seconds on ~1M rows). The 224 MB warehouse file itself is never persisted: **raw is the source of truth and the
warehouse is a derived, disposable cache**, which is exactly the property the incremental design was built to
guarantee. On a persistent host (a VM, `dagster dev`, Dagster+) the same code keeps the warehouse and the dbt
models run incrementally, as shown in section 2.

**Concurrency and ordering.** The workflow uses a concurrency group so two scheduled runs can never race on
the same state. Because each run's artifact is the *input* of the next, a failed run uploads nothing and the
next run restores the last good state and re-fetches the same window — the same guarantee the local run log
gives (section 1), now at the artifact level.

**Failing loudly.** A dbt test error, a blocking Dagster asset check, a freshness error or an ingest exception
fails the job → the workflow run turns red → GitHub emails the repository owner (default notification policy)
and the README badge flips. Warnings (closed-before-created count, schema drift, duplicate rate) are visible in
the run log and the job summary without failing the run. The job summary also prints the health mart and the
number of status transitions observed, so a glance at the Actions tab shows whether late-arriving updates are
being captured.

**Recovery playbook.**

| situation | action |
|---|---|
| a run failed (API outage, runner error) | nothing: the next scheduled run restores the last good state and re-fetches the window |
| source went stale (> 72 h) | freshness check blocks dbt; marts keep their last good state; investigate the publisher's status page |
| schema drift warning | new keys are already preserved in `_extra`; add them to `config.DATA_COLUMNS` and the staging model |
| need to re-load a period | manual run with `backfill_start` input → `--force-start`; raw keeps old copies, staging dedupes |
| lost all artifacts (> 14 days of failures) | the first run backfills from `BACKFILL_START` automatically |

**Secrets and cost.** No secrets are required; `SOCRATA_APP_TOKEN` is optional and only raises the API rate
limit. Actions minutes and artifact storage are free for public repositories; a typical no-change run takes
~2 minutes, the daily run that carries the refresh ~5 minutes.

### 7. Operational evidence

Everything in this section is a query result or a workflow log line, not a hand-typed claim. Regenerate the
tables with `make restore-state && make evidence`.

**GitHub Actions, first two unattended runs (2026-10-01).**

| run | restore step | ingest | dbt build | freshness | artifact | duration |
|---|---|---|---|---|---|---|
| [36891684160](https://github.com/oligurmessa/nyc311-pipeline/actions/runs/36891684160) | no previous artifact → backfill from `BACKFILL_START` | 932,098 rows, 19 files | PASS 50 · WARN 1 · ERROR 0 | PASS | 78 MB uploaded | 10 min 39 s |
| [36893117108](https://github.com/oligurmessa/nyc311-pipeline/actions/runs/36893117108) | restored run 1's artifact: 19 files, 80 MB, 1 run imported | 0 rows (no refresh since) | PASS 50 · WARN 1 · ERROR 0 | PASS | 78 MB uploaded | 1 min 48 s |

The second run is the one that matters: a stateless runner recovered the full landing zone and run log from the
previous run, derived the watermark, asked the API only for the window since then, and rebuilt and tested the
warehouse — in under two minutes. The runs before these two (CI red on a flaky test, pipeline red on a missing
dbt manifest) are also in the Actions history; both were fixed in commit `bf71cc7`'s follow-up and are the kind
of failure the workflows exist to surface.

**Local runs (same code, persistent warehouse).**

#### Ingest runs

| run_id | status | watermark_from | watermark_to | rows_fetched | files_written | max_updated_seen | unexpected_keys |
|---|---|---|---|---|---|---|---|
| 20261001T150826Z-9c1313 | succeeded | 2026-09-01T00:00:00 | 2026-10-01T15:08:26 | 932,098 | 19 | 2026-10-01T01:47:27.963Z | [] |
| 20261001T151736Z-2bc88b | succeeded | 2026-10-01T14:53:26 | 2026-10-01T15:17:36 | 0 | 0 |  | [] |
| 20261001T154618Z-82adb4 | succeeded | 2026-10-01T15:02:36 | 2026-10-01T15:46:18 | 0 | 0 |  | [] |
| 20261001T155317Z-3d0c42 | succeeded | 2026-10-01T15:31:18 | 2026-10-01T15:53:17 | 0 | 0 |  | [] |

Each `watermark_from` is 15 minutes before the previous `watermark_to` (the overlap), and each `watermark_to`
is the run's own start time, never the newest timestamp seen. No run has reported an unexpected column.

#### Warehouse state

| n_requests | newest_source_update | hours_since_source_update | n_invalid_close_time | n_successful_runs | n_failed_runs |
|---|---|---|---|---|---|
| 932,098 | 2026-10-01 01:47:27.963 | 9 | 357 | 4 | 0 |

#### Raw layer: versions per request

| n_versions | requests |
|---|---|
| 1 | 932,098 |

#### Late-arriving updates captured

_None yet._ Every request has been observed exactly once because the backfill and all runs so far happened
between two nightly refreshes. The first scheduled run after the publisher's refresh (06:15 UTC on 2026-10-02)
will land second versions for every request that changed overnight; `make evidence` then prints the
transition table (`from_status → to_status`, counts, and days between creation and the observed closure). This
paragraph will be replaced by that table.

#### Data quality (stored failures, last build)

| test | failing_rows |
|---|---|
| assert_closed_not_before_created | 357 |

**Limitations.** The dashboard is not hosted (run `make dashboard` against a restored artifact, or point it at a
persistent warehouse); artifacts expire after 14 days, so a fortnight of consecutive failures would trigger a
fresh backfill; and the free tier offers no retry policy beyond the next scheduled run.

## Repository

```
pipeline/        config.py, socrata.py (client + keyset paging), state.py (run log / watermark), ingest.py (CLI)
dbt/             models/staging, core, marts · tests/ (generic + singular) · macros/ · profiles.yml (DuckDB)
orchestration/   definitions.py: ingest multi-asset, dbt assets, 4 custom asset checks, job + 6-hourly schedule
dashboard/       app.py: Streamlit page over the marts (freshness banner, volume, resolution, backlog, DQ, runs)
.github/         workflows/pipeline.yml (6-hourly scheduled run with state artifact), workflows/ci.yml (tests on push/PR)
scripts/         evidence.py: regenerates README section 7 from the warehouse
tests/           pytest: ingestion (paging, overlap, recovery), asset checks (stale/drift/failed), dbt late-update merge
data/raw/311/    parquet landing zone (git-ignored)
warehouse.duckdb DuckDB warehouse (git-ignored)
```
