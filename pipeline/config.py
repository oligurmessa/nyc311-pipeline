"""Central configuration for the NYC 311 pipeline. Everything here can be overridden by environment variables."""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(os.environ.get("NYC311_ROOT", Path(__file__).resolve().parents[1]))
DATA_DIR = ROOT / "data"
RAW_DIR = Path(os.environ.get("NYC311_RAW_DIR", DATA_DIR / "raw" / "311"))
DB_PATH = Path(os.environ.get("NYC311_DB_PATH", ROOT / "warehouse.duckdb"))

SOCRATA_DOMAIN = "data.cityofnewyork.us"
DATASET_ID = "erm2-nwe9"  # 311 Service Requests from 2010 to Present
BASE_URL = f"https://{SOCRATA_DOMAIN}/resource/{DATASET_ID}.json"
APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN")  # optional; raises rate limits

PAGE_SIZE = int(os.environ.get("NYC311_PAGE_SIZE", 50_000))
OVERLAP_MINUTES = int(os.environ.get("NYC311_OVERLAP_MINUTES", 15))
BACKFILL_START = os.environ.get("NYC311_BACKFILL_START", "2026-09-01T00:00:00")
REQUEST_TIMEOUT = int(os.environ.get("NYC311_REQUEST_TIMEOUT", 300))
MAX_RETRIES = 5

# Socrata system fields we keep, and the names they get in raw
SYSTEM_FIELDS = {":id": "sys_id", ":updated_at": "sys_updated_at", ":created_at": "sys_created_at", ":version": "sys_version"}

# The dataset's declared columns (from /api/views/erm2-nwe9). The API omits null keys from JSON,
# so every batch is normalised to this list; unexpected keys are kept in `_extra` as JSON.
DATA_COLUMNS = [
    "unique_key", "created_date", "closed_date", "agency", "agency_name", "complaint_type", "descriptor",
    "descriptor_2", "location_type", "incident_zip", "incident_address", "street_name", "cross_street_1",
    "cross_street_2", "intersection_street_1", "intersection_street_2", "address_type", "city", "landmark",
    "facility_type", "status", "due_date", "resolution_description", "resolution_action_updated_date",
    "community_board", "council_district", "police_precinct", "bbl", "borough", "x_coordinate_state_plane",
    "y_coordinate_state_plane", "open_data_channel_type", "park_facility_name", "park_borough", "vehicle_type",
    "taxi_company_borough", "taxi_pick_up_location", "bridge_highway_name", "bridge_highway_direction",
    "road_ramp", "bridge_highway_segment", "latitude", "longitude", "location",
    ":@computed_region_f5dn_yrer", ":@computed_region_yeji_bk3q", ":@computed_region_sbqj_enih",
    ":@computed_region_92fq_4b7q",
]
# parquet-friendly names for the computed-region columns
COLUMN_RENAMES = {c: c.replace(":@", "sys_") for c in DATA_COLUMNS if c.startswith(":@")}
META_COLUMNS = ["_run_id", "_batch_id", "_ingested_at", "_extra"]
RAW_COLUMNS = list(SYSTEM_FIELDS.values()) + [COLUMN_RENAMES.get(c, c) for c in DATA_COLUMNS] + META_COLUMNS
