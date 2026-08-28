"""
evidence.py — print the operational evidence section of the README as Markdown, straight from the warehouse.

    python scripts/evidence.py                 # local warehouse
    make restore-state && python scripts/evidence.py   # against the latest GitHub Actions state

Everything printed is a query result; nothing is typed in by hand.
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
DB = Path(os.environ.get("NYC311_DB_PATH", ROOT / "warehouse.duckdb"))


def md_table(df) -> str:
    cols = list(df.columns)
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    def fmt(v):
        if v is None or (isinstance(v, float) and v != v):
            return ""
        if isinstance(v, (int, np.integer)):
            return f"{int(v):,}"
        if isinstance(v, (float, np.floating)):
            return f"{float(v):,.1f}"
        return str(v)
    for _, r in df.iterrows():
        out.append("| " + " | ".join(fmt(v) for v in r.values) + " |")
    return "\n".join(out)


def main():
    con = duckdb.connect(str(DB), read_only=True)
    q = lambda s: con.execute(s).df()  # noqa: E731

    print("#### Ingest runs\n")
    runs = q("""select run_id, status, watermark_from, watermark_to, rows_fetched, files_written,
                       max_updated_seen, unexpected_keys
                from meta.ingest_runs order by started_at""")
    print(md_table(runs), "\n")

    print("#### Warehouse state\n")
    print(md_table(q("""select n_requests, newest_source_update::varchar as newest_source_update,
                               hours_since_source_update, n_invalid_close_time, n_successful_runs, n_failed_runs
                        from marts.mart_pipeline_health""")), "\n")

    print("#### Raw layer: versions per request\n")
    print(md_table(q("""with v as (select unique_key, count(distinct sys_updated_at) as n_versions from raw.service_requests group by 1)
                        select n_versions, count(*) as requests from v group by 1 order by 1""")), "\n")

    print("#### Late-arriving updates captured (status transitions in fct_request_status_history)\n")
    tr = q("""select coalesce(previous_status, '(first seen)') as from_status, status as to_status, count(*) as requests
              from core.fct_request_status_history where version_no > 1 group by 1, 2 order by 3 desc limit 12""")
    if tr.empty:
        print("_None yet — every request has been observed exactly once. Appears after the first nightly refresh following the backfill._\n")
    else:
        print(md_table(tr), "\n")
        print(md_table(q("""select count(*) as requests_with_2plus_versions,
                                   round(avg(date_diff('hour', previous_updated_at, source_updated_at)), 1) as avg_hours_between_versions,
                                   max(version_no) as max_versions
                            from core.fct_request_status_history where version_no > 1""")), "\n")
        print(md_table(q("""select date_diff('day', f.created_at, h.source_updated_at) as days_after_creation, count(*) as closures
                            from core.fct_request_status_history h join core.fct_service_requests f using (unique_key)
                            where h.version_no > 1 and h.status = 'Closed' and h.previous_status <> 'Closed'
                            group by 1 order by 1 limit 15""")), "\n")

    print("#### Data quality (stored failures, last build)\n")
    print(md_table(q("""select table_name as test, estimated_size as failing_rows
                        from duckdb_tables() where schema_name = 'dq' and estimated_size > 0 order by 2 desc""")), "\n")
    con.close()


if __name__ == "__main__":
    main()
