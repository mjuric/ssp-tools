"""The contract between the SSO delivery work packages (extract, build,
deliver).

See docs/design/sso-delivery.md. Work packages must not change this file; a
needed change goes to the integrating session.

The three stages talk only through files:

    ssp-extract-sso-inputs INPUTS_DIR   stage 1 (MPC replica, ClickHouse)
    ssp-build-sso INPUTS_DIR RUN_DIR    stage 2 (files only)
    ssp-upload-sso CONFIG RUN_DIR [--dry-run]   stage 3 (GCS, Pub/Sub)
    ssp-sso-daily WORK_DIR [--upload CONFIG]    all three
"""

from pathlib import Path

import yaml

#: The vendored schemas (sdm_schemas tickets/DM-55375; see their README).
SCHEMA_DIR = Path(__file__).resolve().parent.parent / "tests" / "data" / "sdm_schemas"

# --------------------------------------------------------------------------
# Stage 1 -> 2: the input files (INPUTS_DIR)
# --------------------------------------------------------------------------

#: name -> (file, what it is). Any source may produce these files, as long
#: as it honours the contract. A file may carry extra columns; the build
#: stage selects what it needs.
INPUT_FILES = {
    "obs_sbn": ("obs_sbn.parquet",
                "MPC obs_sbn, the X05 rows (stn = 'X05'), every column"),
    "mpc_orbits": ("mpc_orbits.parquet", "MPC mpc_orbits, every row and column"),
    "current_identifications": ("current_identifications.parquet",
                                "MPC current_identifications, every row and column"),
    "numbered_identifications": ("numbered_identifications.parquet",
                                 "MPC numbered_identifications, every row and column"),
    "dia_sources": ("dia_sources.parquet",
                    "the measurement each obs_sbn X05 row was submitted from: today "
                    "extract-submitted-sources output (ssp.SubmittableSources), with "
                    "shutter-corrected times (ssp.sssource_contract, 'Shutter-motion')"),
    "ppdb_dia_sources": ("ppdb_dia_sources.parquet",
                         "PPDB DiaSource: diaSourceId, visit, midpointMjdTai, ra, dec"),
}

#: The four MPC files must come from one consistent snapshot (one database
#: transaction when they come from the MPC replica).
MPC_SNAPSHOT = ("obs_sbn", "mpc_orbits", "current_identifications", "numbered_identifications")

#: Columns the build stage requires in each input (others are allowed).
REQUIRED_INPUT_COLUMNS = {
    "obs_sbn": ["obsid", "status", "provid", "permid", "trksub", "trkid", "submission_id",
                "obssubid"],
    "mpc_orbits": ["unpacked_primary_provisional_designation",
                   "packed_primary_provisional_designation", "mpc_orb_jsonb"],
    "current_identifications": ["packed_primary_provisional_designation"],
    "numbered_identifications": ["packed_primary_provisional_designation"],
    "dia_sources": ["obsid", "diaSourceId", "processing", "measuredOn", "midpointMjdTai",
                    "ra", "dec"],
    "ppdb_dia_sources": ["diaSourceId", "visit", "midpointMjdTai", "ra", "dec"],
}

#: INPUTS_DIR/manifest.json, written last by stage 1 (or by whatever else
#: produces the inputs). The build stage refuses inputs without a manifest
#: whose files, row counts and md5s match.
MANIFEST_FILE = "manifest.json"
MANIFEST_FIELDS = {
    "created_utc": "ISO 8601 time the manifest was written",
    "producer": "what produced the inputs, e.g. 'ssp-extract-sso-inputs <version> (<commit>)'",
    "mpc_snapshot_utc": "ISO 8601 time of the MPC snapshot (the transaction's start)",
    "files": "{name: {file, rows, md5, source, extracted_utc}} for every INPUT_FILES name;"
             " the dia_sources entry also has obs_sbn_md5",
}

#: The manifest field the shutter-motion correction adds (WP S1); it joins
#: MANIFEST_FIELDS once WP S1 writes it (docs/design/shutter-timing.md).
SHUTTER_MANIFEST_FIELD = ("shutter_timing",
                          "{table_dir, table_format, calibration_id, package_version,"
                          " obstime_basis: {visit, corrected, both}: row counts,"
                          " status: {ok, degraded, omitted, not_built, outside_coverage,"
                          " time_mismatch}: row counts,"
                          " not_built_visits: [visit, ...], time_mismatch_visits: [visit, ...],"
                          " first_night: the table's first day_obs}")

#: dia_sources columns added by the shutter-motion correction (WP S1); the
#: build requires them once WP S2 lands (then they join
#: REQUIRED_INPUT_COLUMNS["dia_sources"]).
SHUTTER_INPUT_COLUMNS = ["midpointMjdTaiVisit", "midpointMjdTai_flag",
                         "midpointMjdTai_flag_degraded", "obstime_basis"]

# Matching an obs_sbn row to its measurement (extract-submitted-sources):
# with the correction, a candidate's time passes if |obstime - visit time|
# or |obstime - corrected time| is within DT_MS (10 ms); the minute-bucket
# position fallback covers both times. The correction is applied before
# matching. NOT_BUILT visits: the visit time, flagged, with a warning; more
# than ssp.sssource_contract.MAX_NOT_BUILT_VISITS of them fail the extract.
#
# ssp-sso-daily runs a stage 0 before the extract: shutter-timing-table
# --out <corrections dir> (resumes; at most 32 workers); a calibration
# mismatch stops the run with a clear message.

#: dia_sources is derived from obs_sbn: its manifest entry records the md5 of
#: the obs_sbn it was built from (``obs_sbn_md5``), and every stage refuses
#: inputs where it differs from files.obs_sbn.md5.
DERIVED_FROM = {"dia_sources": "obs_sbn"}

# --------------------------------------------------------------------------
# Stage 2 -> 3: the delivery (RUN_DIR/delivery) and the report
# --------------------------------------------------------------------------

#: The PPDB Solar System tables, delivered as RUN_DIR/delivery/<Table>.parquet.
#: Their columns, order, types and nullability are ppdb.yaml's tables of
#: these names, resolved through its columnRefs into sso_base.yaml (see
#: delivery_schema()).
DELIVERY_TABLES = ("SSSource", "SSObject", "NearbySSO",
                   "mpc_orbits", "current_identifications", "numbered_identifications")
DELIVERY_DIR = "delivery"

#: Build steps, in order (ssp-build-sso --from STEP).
BUILD_STEPS = ("mpc", "sssource", "ssobject", "nearbysso", "check")

#: RUN_DIR/report.json, written by stage 2 and updated by stage 3.
REPORT_FILE = "report.json"
REPORT_FIELDS = {
    "inputs": "the input manifest, as read",
    "ssp_tools_commit": "git commit of the code that built the delivery",
    "steps": "{step: {status: 'ok'|'failed'|'skipped', started_utc, wall_s, max_rss_gb, log}}",
    "tables": "{Table: {file, rows, md5, bytes}} for every delivered table",
    "checks": "{check: {status: 'PASS'|'FAIL', report}}",
    "deliverable": "true only if every step and check passed; stage 3 refuses otherwise",
    "upload": "set by stage 3: {config, bucket, object_prefix, tables, message_id, dry_run, utc}",
}

# --------------------------------------------------------------------------
# Stage 3: the upload (DM-55678 contract; mirrors lsst/dax_ppdb
# bigquery/sso_uploader.py)
# --------------------------------------------------------------------------
#
# Config (YAML; config/sso-upload/<env>.yaml):
#   bucket_name: ppdb-dev-sso-ingest
#   topic: load-sso-topic
#   project: ppdb-dev-5c07          # the Pub/Sub topic's project
#   tables: [...]                   # optional, default DELIVERY_TABLES
# Credentials: Google application-default credentials (a service account
# with upload-only access to the bucket and publish access to the topic).
#
# Upload: a fresh prefix per upload, UPLOAD_PREFIX_FORMAT of the UTC time
# with milliseconds (datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")[:-3]);
# each table to gs://<bucket>/<prefix>/<Table>.parquet with
# if_generation_match=0 (never overwrite); on any failure, delete what this
# upload wrote and fail. Then publish one Pub/Sub message, JSON:
#   {"bucket": <bucket>, "object_prefix": <prefix>,
#    "uploaded_tables": [<Table>, ...]}
UPLOAD_PREFIX_FORMAT = "%Y%m%dT%H%M%S%f"   # then [:-3]: milliseconds
UPLOAD_MESSAGE_FIELDS = ("bucket", "object_prefix", "uploaded_tables")


def delivery_schema(schema_dir=SCHEMA_DIR):
    """{Table: [Felis column dicts, in ppdb.yaml's order]} for
    DELIVERY_TABLES, each column taken from the sso_base.yaml table that
    ppdb.yaml refers to."""
    ppdb = yaml.safe_load(open(Path(schema_dir) / "ppdb.yaml"))
    base = {t["name"]: t for t in yaml.safe_load(open(Path(schema_dir) / "sso_base.yaml"))["tables"]}
    out = {}
    for t in ppdb["tables"]:
        if t["name"] not in DELIVERY_TABLES:
            continue
        (src_table, cols), = t["columnRefs"]["sso_base"].items()
        defs = {c["name"]: c for c in base[src_table]["columns"]}
        out[t["name"]] = [defs[c] for c in cols]
    missing = set(DELIVERY_TABLES) - set(out)
    if missing:
        raise ValueError(f"ppdb.yaml lacks delivery tables {sorted(missing)}")
    return out

# --------------------------------------------------------------------------
# The delivery check (WP H: ssp/delivery_check.py; called by build step
# "check")
# --------------------------------------------------------------------------
#
# check_table(table, parquet_path, schema_dir=SCHEMA_DIR)
#     -> list[CheckResult]
#   Every column of delivery_schema()[table], in order, with a compatible
#   Arrow type (char/string/text -> string or dictionary<string>,
#   long -> int64, int -> int32,
#   short -> int16, float -> float32, double -> float64,
#   boolean -> bool, timestamp -> timestamp) and no extra columns; no NULLs
#   in nullable: false columns; the primary key unique and non-NULL.
# check_delivery(delivery_dir, schema_dir=SCHEMA_DIR, tables=DELIVERY_TABLES)
#     -> dict[table, list[CheckResult]]
#   Fails (a FAIL result) for a missing file.
# CheckResult: a NamedTuple (name: str, ok: bool, detail: str).
# CLI: python -m ssp.delivery_check DELIVERY_DIR [--tables ...]
#   (exit 0 if every result is ok).
