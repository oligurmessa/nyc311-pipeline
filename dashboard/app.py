"""
NYC 311 pipeline dashboard (Streamlit). Reads only the dbt marts + run log from the DuckDB warehouse.

    streamlit run dashboard/app.py
"""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = Path(os.environ.get("NYC311_DB_PATH", ROOT / "warehouse.duckdb"))

# fixed categorical order (never cycled), sequential blues, status colours
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948", "#898781", "#52514e", "#b7d3f6"]
GOOD, WARN, BAD = "#0ca30c", "#fab219", "#d03b3b"
FRESH_WARN_H, FRESH_ERROR_H = 36, 72

st.set_page_config(page_title="NYC 311 pipeline", page_icon="📞", layout="wide")


# --------------------------------------------------------------------------------------
@st.cache_data(ttl=300, show_spinner=False)
def q(sql: str) -> pd.DataFrame:
    """Read-only query; cached 5 min so filter changes do not re-hit the file."""
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        return con.execute(sql).df()
    finally:
        con.close()


def load_or_stop():
    if not DB_PATH.exists():
        st.error(f"Warehouse not found at {DB_PATH}. Run `make run-pipeline` first.")
        st.stop()
    try:
        return q("select * from marts.mart_pipeline_health").iloc[0]
    except duckdb.IOException:
        st.warning("The warehouse is locked: a pipeline run is in progress. Refresh in a minute.")
        st.stop()
    except duckdb.CatalogException:
        st.error("Marts are missing. Run `make run-pipeline` to build them.")
        st.stop()


health = load_or_stop()

# --------------------------------------------------------------------------------------
# Banner
# --------------------------------------------------------------------------------------
hours = float(health.hours_since_source_update)
if hours > FRESH_ERROR_H:
    status, colour, label = "STALE", BAD, f"error threshold {FRESH_ERROR_H}h exceeded"
elif hours > FRESH_WARN_H:
    status, colour, label = "LATE", WARN, f"warn threshold {FRESH_WARN_H}h exceeded"
else:
    status, colour, label = "FRESH", GOOD, f"within {FRESH_WARN_H}h"

st.title("NYC 311 service requests — pipeline dashboard")
st.caption("Live data from NYC Open Data (dataset erm2-nwe9), ingested incrementally, modelled and tested with dbt, orchestrated by Dagster.")

b1, b2, b3, b4, b5 = st.columns([1.3, 1, 1, 1, 1.4])
b1.markdown(f"<div style='border-left:6px solid {colour};padding:4px 10px'><b>Data {status}</b><br>"
            f"<span style='color:#666'>newest source update {pd.Timestamp(health.newest_source_update):%Y-%m-%d %H:%M} · {hours:.0f}h ago ({label})</span></div>",
            unsafe_allow_html=True)
b2.metric("Requests in warehouse", f"{int(health.n_requests):,}")
b3.metric("Last run rows fetched", f"{int(health.last_successful_rows):,}")
b4.metric("Runs ok / failed", f"{int(health.n_successful_runs)} / {int(health.n_failed_runs)}")
b5.metric("Last successful run", f"{pd.Timestamp(health.last_successful_run_at):%Y-%m-%d %H:%M} UTC",
          help=f"run_id {health.last_successful_run_id}")
if health.last_run_status != "succeeded":
    st.error(f"Most recent ingest run **{health.last_run_status}**: {health.last_run_error}")

# --------------------------------------------------------------------------------------
# Filters
# --------------------------------------------------------------------------------------
vol = q("select * from marts.mart_daily_volume")
vol["created_date"] = pd.to_datetime(vol.created_date)
dmin, dmax = vol.created_date.min().date(), vol.created_date.max().date()
# the newest created_date is always partial: the publisher snapshots the day before it ends
default_end = max(dmin, dmax - pd.Timedelta(days=1))
f1, f2, f3 = st.columns([1.2, 1.4, 1.4])
dr = f1.date_input("Created between", (dmin, default_end), min_value=dmin, max_value=dmax,
                   help=f"{dmax} is only partially loaded until the next nightly refresh, so it is excluded by default.")
if isinstance(dr, tuple) and len(dr) == 2:
    d0, d1 = dr
else:
    d0, d1 = dmin, dmax
boroughs = sorted(vol.borough.unique())
sel_b = f2.multiselect("Borough", boroughs, default=[b for b in boroughs if b != "UNKNOWN"])
families = vol.groupby("complaint_family").n_requests.sum().sort_values(ascending=False).index.tolist()
sel_f = f3.multiselect("Complaint family", families, default=families)

v = vol[(vol.created_date.dt.date >= d0) & (vol.created_date.dt.date <= d1) & vol.borough.isin(sel_b) & vol.complaint_family.isin(sel_f)]
st.caption(f"{int(v.n_requests.sum()):,} requests created {d0} → {d1} in the current selection · "
           f"{v.n_closed.sum() / max(v.n_requests.sum(), 1):.0%} already closed · {1 - v.n_with_geo.sum() / max(v.n_requests.sum(), 1):.1%} without coordinates")

# --------------------------------------------------------------------------------------
# 1. Volume
# --------------------------------------------------------------------------------------
st.subheader("Daily request volume")
by = st.radio("Colour by", ["complaint_family", "borough"], horizontal=True, label_visibility="collapsed")
# complete day x category grid (zeros where nothing was created) so stacked areas never cross
daily = (v.groupby(["created_date", by]).n_requests.sum().unstack(fill_value=0)
          .reindex(pd.date_range(d0, d1, freq="D"), fill_value=0).rename_axis("created_date").reset_index()
          .melt(id_vars="created_date", var_name=by, value_name="n_requests"))
order = daily.groupby(by).n_requests.sum().sort_values(ascending=False).index.tolist()
chart = (alt.Chart(daily).mark_area(opacity=0.9)
         .encode(x=alt.X("created_date:T", title=None),
                 y=alt.Y("n_requests:Q", title="requests created", stack="zero"),
                 color=alt.Color(f"{by}:N", scale=alt.Scale(domain=order, range=SERIES[:len(order)]), legend=alt.Legend(title=None, orient="bottom", columns=6)),
                 tooltip=["created_date:T", f"{by}:N", alt.Tooltip("n_requests:Q", format=",")])
         .properties(height=300))
st.altair_chart(chart, use_container_width=True)

# --------------------------------------------------------------------------------------
# 2. Resolution time & 3. Backlog
# --------------------------------------------------------------------------------------
c1, c2 = st.columns(2)
with c1:
    st.subheader("Time to close, by agency")
    st.caption("Closed requests created in the selected window; median and p90 hours. Requests with closed < created are excluded.")
    rt = q("select * from marts.mart_resolution_time")
    rt["week_start"] = pd.to_datetime(rt.week_start)
    rt = rt[(rt.week_start.dt.date >= d0 - pd.Timedelta(days=6)) & (rt.week_start.dt.date <= d1)]
    agg = rt.groupby(["agency", "agency_name"]).apply(
        lambda g: pd.Series({"n_closed": g.n_closed.sum(),
                             "median_hours": (g.median_hours_to_close * g.n_closed).sum() / g.n_closed.sum(),
                             "p90_hours": (g.p90_hours_to_close * g.n_closed).sum() / g.n_closed.sum(),
                             "within_24h": (g.share_closed_within_24h * g.n_closed).sum() / g.n_closed.sum()}),
        include_groups=False).reset_index().sort_values("n_closed", ascending=False).head(10)
    bars = (alt.Chart(agg).mark_bar(color=SERIES[0], cornerRadiusEnd=4)
            .encode(y=alt.Y("agency:N", sort="-x", title=None), x=alt.X("median_hours:Q", title="median hours to close"),
                    tooltip=["agency_name:N", alt.Tooltip("n_closed:Q", format=","), alt.Tooltip("median_hours:Q", format=".1f"),
                             alt.Tooltip("p90_hours:Q", format=".1f"), alt.Tooltip("within_24h:Q", format=".0%")])
            .properties(height=320))
    st.altair_chart(bars, use_container_width=True)
    st.dataframe(agg.assign(within_24h=lambda d: (d.within_24h * 100).round(0).astype(int).astype(str) + "%",
                            median_hours=lambda d: d.median_hours.round(1), p90_hours=lambda d: d.p90_hours.round(1))
                 [["agency", "agency_name", "n_closed", "median_hours", "p90_hours", "within_24h"]],
                 hide_index=True, use_container_width=True, height=240)

with c2:
    st.subheader("Open backlog by age")
    st.caption("Requests not closed as of the last build, bucketed by age. Includes requests created before the coverage start.")
    bl = q("select * from marts.mart_open_backlog")
    bl = bl[bl.borough.isin(sel_b)]
    top = bl.groupby("agency").n_open.sum().sort_values(ascending=False).head(10).index.tolist()
    blt = bl[bl.agency.isin(top)].groupby(["agency", "age_bucket"]).n_open.sum().reset_index()
    buckets = sorted(blt.age_bucket.unique())
    blues = ["#cde2fb", "#86b6ef", "#3987e5", "#1c5cab", "#0d366b"][: len(buckets)]
    stack = (alt.Chart(blt).mark_bar()
             .encode(y=alt.Y("agency:N", sort=top, title=None), x=alt.X("n_open:Q", title="open requests"),
                     color=alt.Color("age_bucket:N", scale=alt.Scale(domain=buckets, range=blues), legend=alt.Legend(title="age", orient="bottom")),
                     order=alt.Order("age_bucket:N"),
                     tooltip=["agency:N", "age_bucket:N", alt.Tooltip("n_open:Q", format=",")])
             .properties(height=320))
    st.altair_chart(stack, use_container_width=True)
    tot = bl.groupby("age_bucket").n_open.sum()
    m = st.columns(len(tot))
    for col, (k, val) in zip(m, tot.items()):
        col.metric(k.split(". ")[1], f"{int(val):,}")

# --------------------------------------------------------------------------------------
# 4. Data quality & 5. Runs
# --------------------------------------------------------------------------------------
st.subheader("Data quality and pipeline runs")
d1c, d2c = st.columns([1.1, 1.4])
with d1c:
    st.markdown("**dbt test failures stored in schema `dq`** (rows = failing records at the last build)")
    dq = q("select table_name as test, estimated_size as failing_rows from duckdb_tables() where schema_name = 'dq' order by failing_rows desc, test")
    n_fail = int((dq.failing_rows > 0).sum())
    st.markdown(f"{len(dq)} tests stored · <span style='color:{WARN if n_fail else GOOD}'><b>{n_fail} with failing rows</b></span>", unsafe_allow_html=True)
    st.dataframe(dq[dq.failing_rows > 0] if n_fail else dq.head(8), hide_index=True, use_container_width=True, height=160)
    inv = int(health.n_invalid_close_time)
    st.markdown(f"Known source anomaly: **{inv:,}** requests closed before they were created "
                f"(kept, flagged `has_invalid_close_time`, excluded from duration metrics; warn > 0, error > 5,000).")
    hist = q("""select coalesce(previous_status, '(first seen)') as from_status, status as to_status, count(*) as n
                from core.fct_request_status_history where version_no > 1 group by 1, 2 order by 3 desc limit 10""")
    st.markdown("**Observed status transitions** (late-arriving updates captured by the pipeline)")
    if hist.empty:
        st.caption("None yet: every request has been observed once. Transitions appear after the source's next nightly refresh.")
    else:
        st.dataframe(hist, hide_index=True, use_container_width=True, height=200)

with d2c:
    st.markdown("**Ingest runs** (`meta.ingest_runs`, newest first)")
    runs = q("""select run_id, status, started_at, watermark_from, watermark_to, rows_fetched, files_written, unexpected_keys, error
                from meta.ingest_runs order by started_at desc limit 20""")
    st.dataframe(runs, hide_index=True, use_container_width=True, height=380)

st.caption("Design doc, tests and source: see README.md. Coverage start: requests updated since 2026-09-01; cohort charts show created dates from the coverage start only.")
