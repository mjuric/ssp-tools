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
                    "shutter-corrected times (ssp.ssobservation_contract, 'Shutter-motion')"),
    "ppdb_dia_sources": ("ppdb_dia_sources.parquet",
                         "PPDB DiaSource: diaSourceId, visit, detector, midpointMjdTai, ra, dec"),
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
    "ppdb_dia_sources": ["diaSourceId", "visit", "detector", "midpointMjdTai", "ra", "dec"],
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
             " the dia_sources entry also has obs_sbn_md5; and IERS_A_NAME:"
             " {IERS_A_MANIFEST_FIELDS} (the build requires it)",
}

#: The manifest field the shutter-motion correction adds (WP S1); it joins
#: MANIFEST_FIELDS once WP S1 writes it (docs/design/shutter-timing.md).
SHUTTER_MANIFEST_FIELD = ("shutter_timing",
                          "{table_dir, table_format, calibration_id, package_version,"
                          " obstime_basis: {visit, corrected, both}: row counts,"
                          " status: {ok, degraded, omitted, not_built, outside_coverage,"
                          " time_mismatch, large_shift, already_corrected}: row counts,"
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
# matching. NOT_BUILT visits: the visit time, flagged, with a warning,
# counted and listed in the manifest; they never fail the extract (a later
# day's rebuild fixes them once the table covers them).
#
# The extract only reads the correction table; ssp-daily builds it
# (shutter-timing-table, hourly). A table the extract can't use (missing,
# mixed calibrations, unknown format, failing integrity) fails the extract.

#: The IERS-A Earth-orientation file (docs/design/iers-a.md; ssp-daily's
#: docs/design/iers-a.md is the approved source). ``ssp-extract-sso-inputs
#: --iers-a-store DIR`` downloads it into INPUTS_DIR (or, when the download
#: fails, copies the store's last good copy) and records it in the manifest
#: as ``files[IERS_A_NAME]``; it is not one of INPUT_FILES (not Parquet). The
#: build requires that entry and uses exactly that file for UT1-UTC and polar
#: motion, never astropy's bundled table or a download. ssp-daily reads these
#: fields with strict type checks: keep the names and types.
IERS_A_NAME = "iers_a"
IERS_A_FILE = "finals2000A.all"
IERS_A_MANIFEST_FIELDS = {
    "file": "str: IERS_A_FILE",
    "md5": "str: as for the other files",
    "rows": "int: lines of the table file",
    "source": "str: the URL it came from, or 'reused <store path> (download failed: <error>)'",
    "extracted_utc": "str: ISO 8601 UTC, when it was downloaded, or copied from the store",
    "predictions_start": "str: ISO date (YYYY-MM-DD) of the first predicted (not measured) row",
    "age_days": "number (float, 2 decimals): extracted_utc minus predictions_start (its 00:00 UTC), "
                "in days",
    "reused": "bool: false for a fresh download, true for the store's copy",
}
#: The store's copy: DIR/IERS_A_FILE (the last good download).
#: A reused copy older than IERS_A_STALE_DAYS gets the stronger warning; one
#: older than IERS_A_MAX_AGE_DAYS fails the extract, and the build refuses
#: a manifest whose age_days exceeds it.
IERS_A_STALE_DAYS = 30
IERS_A_MAX_AGE_DAYS = 90
#: A usable file's predictions span at least this many days (from its first
#: predicted row to its last row with predicted UT1-UTC and polar motion);
#: finals2000A.all carries about a year. A shorter one (e.g. a truncated
#: download) is refused.
IERS_A_MIN_PREDICTION_DAYS = 180
#: The build passes the file to every step subprocess in this environment
#: variable; ssp.iers.install_from_env() installs it there.
IERS_A_ENV = "SSP_IERS_A_FILE"
#: The manifest fields the build copies to report.json's ``iers_a`` (with
#: ``file``, the absolute path it used).
IERS_A_REPORT_FIELDS = ("md5", "predictions_start", "age_days", "reused")

#: dia_sources is derived from obs_sbn: its manifest entry records the md5 of
#: the obs_sbn it was built from (``obs_sbn_md5``), and every stage refuses
#: inputs where it differs from files.obs_sbn.md5.
DERIVED_FROM = {"dia_sources": "obs_sbn"}

# --------------------------------------------------------------------------
# Stage 2 -> 3: the delivery (RUN_DIR/delivery) and the report
# --------------------------------------------------------------------------

#: The PPDB Solar System tables, delivered as RUN_DIR/delivery/<Table>.parquet,
#: except SSObservation, which is delivered as parts plus a manifest and a
#: sidecar (ssp.ssobservation_contract: PART_FILE_FORMAT,
#: SSOBSERVATION_MANIFEST_FILE, SIDECAR_FILE; docs/design/
#: ssobservation-delivery.md).
#: Their columns, order, types and nullability are ppdb.yaml's tables of
#: these names, resolved through its columnRefs into sso_base.yaml (see
#: delivery_schema()).
DELIVERY_TABLES = ("SSObservation", "SSObject", "NearbySSO",
                   "mpc_orbits", "current_identifications", "numbered_identifications")
DELIVERY_DIR = "delivery"

#: Build steps, in order (ssp-build-sso --from STEP).
BUILD_STEPS = ("mpc", "ssobservation", "ssobject", "nearbysso", "check")

#: RUN_DIR/report.json, written by stage 2 and updated by stage 3.
REPORT_FILE = "report.json"
REPORT_FIELDS = {
    "inputs": "the input manifest, as read",
    "iers_a": "{file (the absolute path used), IERS_A_REPORT_FIELDS...}: the IERS-A file the steps used",
    "ssp_tools_commit": "git commit of the code that built the delivery",
    "steps": "{step: {status: 'ok'|'failed'|'skipped', started_utc, wall_s, max_rss_gb, log}}",
    "tables": "{Table: {file, rows, md5, bytes}} for every delivered table; NearbySSO's also "
              "{sidecar, sidecar_md5, sidecar_bytes} (ssp.nearbysso._contract.NEARBYSSO_SIDECAR_FILE, "
              "next to it, never uploaded); for SSObservation "
              "{manifest, manifest_md5, parts: [file, ...], sidecar, rows, bytes} (rows and bytes "
              "are totals over the parts, not counting the manifest or the sidecar; each part's md5 "
              "is in the manifest, and the manifest's own md5 is manifest_md5)",
    "checks": "{check: {status: 'PASS'|'FAIL', report}}",
    "deliverable": "true only if every step and check passed; stage 3 refuses otherwise",
    "upload": "set by stage 3: {config, bucket, object_prefix, tables, message_id, dry_run, published, "
              "utc, parallel, manifest, load}; manifest is the upload manifest written (for a dry run, "
              "planned): {uploaded_at, tables: {Table: {part-NNNNN.parquet: {row_count, checksum "
              "(SHA-256 hex), size_bytes, source_path}}}} (UPLOAD_MANIFEST_*); load is the load's "
              "result, {status (UPLOAD_LOAD_STATUSES), job_id (succeeded) | error (failed; also a "
              "timeout whose results couldn't be read), waited_s}, null while a published upload waits "
              "for it; error, when the publish wasn't confirmed (published null: the objects are "
              "kept); parallel, the chunked uploads: {chunk_bytes, workers, min_bytes, tables, parts} "
              "(null with --no-parallel)",
}

# --------------------------------------------------------------------------
# Stage 3: the upload (DM-55678; the PPDB team's manifest contract, DM-56960:
# lsst/dax_ppdb bigquery/sso/{uploader,manifest,loader}.py at ec576a6;
# docs/design/sso-upload-manifest.md)
# --------------------------------------------------------------------------
#
# Config (YAML; config/sso-upload/<env>.yaml):
#   bucket_name: ppdb-int-sso-ingest
#   project: ppdb-int-6c62          # of the topic and the subscription
#   tables: [...]                   # optional; default UPLOAD_TABLE_ORDER
#   parallel: true                  # optional; default true (--no-parallel)
# The topic and the result subscription are fixed (UPLOAD_TOPIC,
# UPLOAD_RESULT_SUBSCRIPTION); an old config's "topic" key is accepted only
# if it is UPLOAD_TOPIC. Credentials: Google application-default credentials
# (the sso-uploader service account: object-user access to the bucket,
# publish access to the topic, subscriber access to the subscription), e.g.
# GOOGLE_APPLICATION_CREDENTIALS pointing at its key.
#
# Layout: a fresh prefix per upload, UPLOAD_PREFIX_FORMAT of the UTC time
# with milliseconds (datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")[:-3]),
# refused if any object is under it already. Each table's files go to
# <prefix>/<Table>/UPLOAD_PART_FORMAT, numbered from 0 per table, the
# tables in UPLOAD_TABLE_ORDER: one part for a single-file table, and one
# per delivery part for SSObservation, in its manifest's (sorted) order.
# No sidecar and no SSObservation manifest is ever uploaded. By default
# (parallel; opt out with --no-parallel or parallel: false) a file of at
# least ssp.sso_upload.PARALLEL_MIN_BYTES (128 MiB) goes as an XML multipart
# upload in chunks over parallel streams (docs/design/sso-upload-parallel.md):
# its object has a CRC32C, checked against the local file's, and no MD5; GCS
# refuses preconditions on multipart uploads, so it is never-overwrite by a
# check just before the upload plus the fresh prefix. Every other file goes
# in one stream with if_generation_match=0 (never overwrite).
#
# Then <prefix>/UPLOAD_MANIFEST_FILE, last, with if_generation_match=0, so
# that its presence means the upload is complete: JSON, exactly dax_ppdb's
# sso.Manifest (extra="forbid"; UPLOAD_MANIFEST_FIELDS,
# UPLOAD_MANIFEST_ENTRY_FIELDS):
#   {"uploaded_at": <ISO 8601 UTC>,
#    "tables": {<Table>: {"part-00000.parquet": {"row_count": >= 1,
#        "checksum": <SHA-256 hex>, "size_bytes": >= 1,
#        "source_path": <absolute local path>}, ...}, ...}}
# row_count comes from the file's Parquet metadata and must equal the
# report's (per SSObservation part, its manifest's). A file with 0 rows
# refuses the whole upload. On any failure up to the hand-over of the
# message, delete what this upload wrote (the manifest first; and abort its
# incomplete multipart uploads) and fail. Then publish one Pub/Sub message
# to projects/<project>/topics/UPLOAD_TOPIC, JSON, exactly dax_ppdb's:
#   {"bucket": <bucket>, "object_prefix": <prefix>}   (UPLOAD_MESSAGE_FIELDS)
# and (unless --no-wait) wait for the load's result on
# projects/<project>/subscriptions/UPLOAD_RESULT_SUBSCRIPTION, JSON
# {object_prefix, status: succeeded | failed, job_id | error}; every message
# pulled is acknowledged, and other prefixes' results are skipped.
# --no-publish uploads the parts and the manifest and stops before the
# message (no load starts; the objects are kept; the record has
# published: false).
#
# Exit status of ssp-upload-sso (and of ssp-sso-daily --upload, which passes
# it through): 0 loaded, or no wait asked for; 1 refused or failed;
# UPLOAD_EXIT_LOAD_FAILED; UPLOAD_EXIT_LOAD_TIMEOUT (no result within the
# wait: published, and it may still load).
UPLOAD_PREFIX_FORMAT = "%Y%m%dT%H%M%S%f"   # then [:-3]: milliseconds
UPLOAD_PART_FORMAT = "part-{:05d}.parquet"
UPLOAD_MANIFEST_FILE = "manifest.json"
UPLOAD_MANIFEST_FIELDS = ("uploaded_at", "tables")
UPLOAD_MANIFEST_ENTRY_FIELDS = ("row_count", "checksum", "size_bytes", "source_path")
UPLOAD_MESSAGE_FIELDS = ("bucket", "object_prefix")
UPLOAD_TOPIC = "load-sso-topic"
UPLOAD_RESULT_SUBSCRIPTION = "load-sso-result-sub"
UPLOAD_LOAD_STATUSES = ("succeeded", "failed", "timeout", "not_waited")
UPLOAD_EXIT_LOAD_FAILED = 3
UPLOAD_EXIT_LOAD_TIMEOUT = 4

#: dax_ppdb's SSO_TABLES order (bigquery/schema/constants.py): the upload
#: order, and the manifest's.
UPLOAD_TABLE_ORDER = ("SSObservation", "NearbySSO", "SSObject",
                      "mpc_orbits", "current_identifications", "numbered_identifications")


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
#   in nullable: false columns; the primary key unique and non-NULL; with
#   text_checks (off by default; ssp-build-sso --text-checks), also every
#   string column ASCII (Felis char/text; docs/design/mpc-orbits-ascii.md).
#   For SSObservation, SSObject and NearbySSO (delivery_check's
#   NAN_FREE_TABLES), also no NaN in any float column: a missing value is
#   NULL (docs/design/elongation-ssobject-nulls.md). Not the MPC tables:
#   mpc_orbits mirrors the replica's NaN.
# check_delivery(delivery_dir, schema_dir=SCHEMA_DIR, tables=DELIVERY_TABLES)
#     -> dict[table, list[CheckResult]]
#   Fails (a FAIL result) for a missing file. SSObservation is checked
#   through its manifest (check_ssobservation_parts below) and check_table
#   runs on every part; then check_elongation and check_offsets on all its
#   parts.
# check_elongation(part_files) -> CheckResult
#   (docs/design/elongation-ssobject-nulls.md) On every row with non-NULL
#   topo_* and helio_* (positions and velocities), ``elongation`` within
#   its tolerance of the apparent elongation from the delivered columns
#   (delivery_check.elongation_tolerance_arcsec: 1" plus twice the rounding
#   bound of the recomputed Sun direction, by the stored vectors' type
#   (2**-24 for float32, 2**-53 for float64), plus the Sun's motion over
#   both light-time legs, object-to-observer and Sun-to-object (topo - helio
#   is the Sun as the object saw it); ~1" nearby, and at 90 AU ~7" for
#   float32 vectors, ~3" for float64): the
#   angle between the observed (ra, dec) and topo - helio, both aberrated
#   (first order in v/c) by helio_v - topo_v. Reports the rows checked, the
#   largest difference, and (for information) the rows where the angle to
#   topo's direction would differ by more than 0.01 deg.
# check_offsets(part_files) -> CheckResult
#   (docs/design/precision-doubles.md, F-16) On every row with non-NULL
#   ephOffsetRa, ephOffsetDec, ephOffset and ephDec: where the separation
#   and the RA difference are within 1 deg (the tangent plane),
#   |hypot(ephOffsetRa, ephOffsetDec) - ephOffset| within
#   delivery_check.offsets_tolerance_arcsec: ephOffset x (4 x 2**-24 for the
#   float32 columns + 2 x (|tan(ephDec)| |ephOffsetDec| / 2 + (1 +
#   tan^2(ephDec)) ephOffset^2 / 8), angles in radians) + 1e-9" (float64
#   floor). The factor 2 (OFFSETS_TOL_SAFETY): the tangent-plane bound is
#   near the true leading term, and the check is for gross errors (an
#   unwrapped RA difference is off by 1e3-1e6"), so it leaves room for
#   what the derivation misses. Beyond:
#   |ephOffsetDec| <= ephOffset and |ephOffsetRa| <= 180 deg x |cos(ephDec)|.
#   A NaN fails. Catches an RA difference not wrapped across RA 0/360.
#   Reports the rows checked, the worst fraction of the tolerance and its
#   separation, and the rows beyond the tangent plane.
# check_ssobservation_parts(delivery_dir) -> list[CheckResult]
#   The manifest has SSOBSERVATION_MANIFEST_FIELDS (format_version
#   MANIFEST_FORMAT_VERSION)
#   and its parts PART_FIELDS; the files on disk matching PART_GLOB are
#   exactly the manifest's, named PART_FILE_FORMAT contiguously from 0;
#   each part's rows, bytes and md5 match, and its ssObjectId min/max and
#   null_ssObjectId match its contents; ranged parts ascend and are
#   disjoint, no ssObjectId appears in two parts, NULL parts come last and
#   hold only NULLs; every ranged part but the last has >= part_rows rows,
#   every NULL part but the last exactly part_rows; rows sum to "rows";
#   rows are in SSOBSERVATION_SORT order within and across parts; the
#   sidecar matches SIDECAR_FIELDS, has obsid followed by its columns with
#   SSOBSERVATION_INTERNAL_DTYPE's types, non-null, the same obsid sequence
#   as the parts concatenated, and its rows/bytes/md5; the sidecar's
#   columns are not in the delivered schema.
# check_nearbysso(delivery_dir) -> list[CheckResult]
#   (run by check_delivery after NearbySSO's schema checks;
#   docs/design/nearbysso-unmatched.md, ssp.nearbysso._contract) The
#   sidecar NEARBYSSO_SIDECAR_FILE exists with NearbySSO's row count,
#   NEARBYSSO_SIDECAR_DTYPE's columns and types, NULLs only where allowed
#   (eph_x/eph_y/ephDetectorEdgeDist together), and NEARBYSSO_KEY equal to
#   NearbySSO's row by row; NEARBYSSO_UNMATCHED_NULL NULL together; the row
#   order; unmatched rows placed and within PLACEMENT_MARGIN_ARCSEC; rows
#   without a placement flagged; times in their visit's night with a
#   bounded spread per visit; day_obs and detector ranges; no (visit,
#   designation) both matched and unmatched; no NaN in the sidecar's float
#   columns.
# CheckResult: a NamedTuple (name: str, ok: bool, detail: str).
# CLI: python -m ssp.delivery_check DELIVERY_DIR [--tables ...]
#   (exit 0 if every result is ok).
