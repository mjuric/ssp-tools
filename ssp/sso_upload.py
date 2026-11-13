"""Stage 3 of the SSO delivery: upload a checked delivery to the PPDB's SSO
ingestion (DM-55678).

    ssp-upload-sso CONFIG RUN_DIR [--dry-run | --no-publish] [--tables T ...]
                   [--force] [--allow-partial] [--include-unsupported]
                   [--no-parallel | --chunk-mib N --parallel-workers N]

It follows the contract in ssp/delivery_contract.py, which mirrors
lsst/dax_ppdb python/lsst/dax/ppdb/bigquery/sso_uploader.py (tickets/
DM-55375) and the loader, lsst-dm/ppdb-cloud-functions load_sso, which
reads exactly gs://<bucket>/<prefix>/<Table>.parquet, one object per table
(docs/design/sso-upload-int.md):

1. Refuse unless RUN_DIR/report.json says ``deliverable: true`` and every
   table to upload, RUN_DIR/delivery/<Table>.parquet, has the md5 (and
   size) the report recorded. SSObservation is partitioned
   (ssp.ssobservation_contract, "The partitioned delivery"): its manifest,
   SSObservation.manifest.json, must have the report's manifest_md5 and
   list the parts the report recorded, with the report's total rows and
   bytes, and each part must have the size and md5 the manifest gives.
2. Combine SSObservation's parts, in manifest order, into one file,
   RUN_DIR/upload/SSObservation.parquet: the parts' schema (they must all
   have the same), zstd, each row group of a part one row group of the
   combined file, streamed one row group at a time. It must have the
   manifest's rows and the parts' schema, and is recorded in
   RUN_DIR/upload/SSObservation.json ({file, rows, bytes, md5,
   manifest_md5}). It is kept, and reused while the record names the
   report's manifest_md5 and the file has the recorded size and md5;
   otherwise it is rebuilt. A dry run builds (or reuses) it too.
3. Upload each table as one object, gs://<bucket>/<prefix>/<Table>.parquet
   (SSObservation from the combined file, the others from delivery/), in
   dax_ppdb's SSO_TABLES order (UPLOAD_TABLE_ORDER), where the prefix is
   the UTC time to the millisecond, so that nothing is ever overwritten:
   files under PARALLEL_MIN_BYTES in one stream with if_generation_match=0,
   bigger ones in parallel chunks ("Parallel chunked uploads", below).
   No manifest, part or sidecar
   (SSObservation_internal.parquet, NearbySSO_internal.parquet) is ever
   uploaded. On any failure, including a failure to hand the message to
   Pub/Sub, delete every object this upload wrote and fail.
4. Publish one JSON message, exactly dax_ppdb SSOUploader's,
   {"bucket", "object_prefix", "uploaded_tables"} (the tables in upload
   order), to projects/<project>/topics/<topic>, and wait (up to
   PUBLISH_TIMEOUT_S) for its message id. If the wait times out, fails or
   is interrupted (e.g. Ctrl-C), the objects are kept and the upload is
   recorded with published null: the message may have gone through.
   With --no-publish, stop before this step: nothing is published, so no
   load starts. The objects are kept (delete them by hand when done), and
   the prefix and the message that would have been sent are printed.
5. Record the upload in report.json: ``upload`` (the last one) and
   ``uploads`` (all of them, dry runs and --no-publish runs included).
   ``published`` is true for a published upload, false for a dry run or a
   --no-publish run, and null when the publish wasn't confirmed.

Parallel chunked uploads (the default)
-------------------------------------
By default (owner, 2026-11-13), each file of at least PARALLEL_MIN_BYTES
(today SSObservation and mpc_orbits) is uploaded in chunks over parallel
streams, with google-cloud-storage's
transfer_manager.upload_chunks_concurrently (GCS's XML multipart upload,
threads; --chunk-mib, default PARALLEL_CHUNK_MIB, and --parallel-workers,
default PARALLEL_WORKERS). Smaller files go as in step 3. Opt out with
--no-parallel or ``parallel: false`` in the config; the command line wins
over the config (docs/design/sso-upload-parallel.md); every file then goes
in one stream with if_generation_match=0. A chunked upload differs:

- It has a CRC32C but no MD5. After the upload, its CRC32C (and size) must
  equal the local file's (google-crc32c), or the object is deleted and the
  upload fails.
- GCS refuses preconditions on multipart uploads ("Object preconditions are
  not supported for multipart uploads"), so if_generation_match=0 can't be
  given. Instead the object is checked not to exist just before the upload,
  under the fresh, per-upload prefix.
- On any failure (or interrupt), the incomplete multipart uploads of the
  object are aborted (listed with the XML API, ?uploads). The object
  itself is deleted only if our multipart upload completed (e.g. a CRC32C
  or size mismatch), with if_generation_match on the generation read back,
  so never an object this run didn't write. An upload killed outright
  (SIGKILL, power loss) can leave an incomplete multipart upload, which is
  left to the bucket's lifecycle rule (AbortIncompleteMultipartUpload, on
  the PPDB side) or a manual abort.
- Interrupts: the library joins its thread pool, so a first Ctrl-C (or a
  failed part) waits for the parts already queued to finish before the
  abort and cleanup run; a second Ctrl-C stops at once (and may leave an
  incomplete multipart upload).

Permissions: besides storage.objects.create and .delete, the chunked path
needs storage.objects.get (the existence check and the read-back) and
storage.multipartUploads.create, .list and .abort. roles/storage.objectUser
(what idf_deploy gives the sso-uploader account on the bucket) has them;
roles/storage.objectCreator does not. --no-parallel needs none of these.

The upload record's ``parallel`` is {chunk_bytes, workers, min_bytes,
tables} (the tables that went in chunks), or null with --no-parallel.

The message is unchanged.

Which tables
------------
The config's ``tables`` (default: ACCEPTED_TABLES, all six delivery
tables) is the complete set. Uploading a subset of it is refused without
--allow-partial: the loader replaces each table it is sent
(WRITE_TRUNCATE), so a subset would mix snapshots in the PPDB.

UNSUPPORTED_TABLES lists delivery tables the loader doesn't accept yet;
asking for one is refused without --include-unsupported. It is empty: the
loader takes all six tables, NearbySSO included (live in int since
2026-10-13). It stays as a hook for the next table the loader lags on.

One upload per load window
--------------------------
load_sso launches its Dataflow job under a fixed job name ("load-sso"), and
Dataflow refuses to launch while a job of that name is running; load_sso
then acknowledges the message and does nothing. So an upload published
while an earlier load is still running is silently dropped. Upload at most
once per load window, and check that the previous load has finished before
uploading again (e.g. with --force). A dry run or a --no-publish run starts
no load, and doesn't count as an earlier upload.

Config and credentials
----------------------
CONFIG is a name (dev, int, prod), looked up in the repository's
config/sso-upload/, or a path: anything ending in .yaml or .yml, or
containing a "/". Credentials are Google application-default credentials,
e.g. GOOGLE_APPLICATION_CREDENTIALS pointing at the sso-uploader service
account's key. The Google client libraries are the ``upload`` extra
(``pip install 'ssp[upload]'``); a dry run doesn't need them.
"""

import argparse
import base64
import concurrent.futures
import hashlib
import json
import logging
import math
import os
import posixpath
import secrets
import sys
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import yaml

from .delivery_contract import (
    DELIVERY_DIR,
    DELIVERY_TABLES,
    REPORT_FILE,
    UPLOAD_DIR,
    UPLOAD_MESSAGE_FIELDS,
    UPLOAD_PREFIX_FORMAT,
    UPLOAD_SSOBSERVATION_FILE,
    UPLOAD_SSOBSERVATION_RECORD,
    UPLOAD_TABLE_ORDER,
)
from .nearbysso._contract import NEARBYSSO_SIDECAR_FILE
from .ssobservation_contract import SIDECAR_FILE, SSOBSERVATION_MANIFEST_FILE

#: Files of the delivery that are never uploaded (internal columns).
SIDECARS = (SIDECAR_FILE, NEARBYSSO_SIDECAR_FILE)

#: The delivered table that is delivered as parts plus a manifest, and
#: uploaded as one combined file.
PARTITIONED = "SSObservation"

_LOG = logging.getLogger("ssp.sso_upload")

#: Where the named configs live (the repository's config/sso-upload/).
CONFIG_DIR = Path(__file__).resolve().parent.parent / "config" / "sso-upload"

CONFIG_KEYS_REQUIRED = ("bucket_name", "topic", "project")
CONFIG_KEYS_OPTIONAL = ("tables", "parallel")

#: Delivery tables the DM-55678 side (dax_ppdb SSO_TABLES, load_sso) doesn't
#: accept yet; sending one fails the whole load. None today: a hook for the
#: next table the loader lags on.
UNSUPPORTED_TABLES = ()
UNSUPPORTED_NOTE = ("not accepted by DM-55678's dax_ppdb SSO_TABLES and load_sso yet; "
                    "sending it fails the entire load")

#: The tables the loader accepts, in the upload order: the default set.
ACCEPTED_TABLES = tuple(t for t in UPLOAD_TABLE_ORDER if t not in UNSUPPORTED_TABLES)

#: How long to wait for Pub/Sub to confirm the message.
PUBLISH_TIMEOUT_S = 120

#: Files at least this big are uploaded in parallel chunks (unless
#: --no-parallel).
PARALLEL_MIN_BYTES = 1 << 30
#: The default chunk size (MiB) and number of upload threads for --parallel:
#: the fastest setting measured, at about 1.2 GB peak memory
#: (docs/design/sso-upload-parallel.md, Results).
PARALLEL_CHUNK_MIB = 64
PARALLEL_WORKERS = 16
#: At most this many upload threads (the shared nodes' limits).
PARALLEL_MAX_WORKERS = 16
#: GCS XML multipart limits: parts of at least 5 MiB (but the last), at
#: most 5 GiB, at most 10,000 of them.
MPU_MIN_PART_BYTES = 5 << 20
MPU_MAX_PART_BYTES = 5 << 30
MPU_MAX_PARTS = 10000


class SSOUploadError(RuntimeError):
    """The delivery can't be, or wasn't, uploaded."""


class PublishUnconfirmedError(SSOUploadError):
    """The objects are uploaded, but the publish wasn't confirmed in time.
    The message may or may not have gone through; the objects are kept."""

    def __init__(self, msg, object_prefix):
        super().__init__(msg)
        self.object_prefix = object_prefix


def _now():
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------
# Config, report and file checks
# --------------------------------------------------------------------------

def _named_configs():
    return sorted(q.stem for q in CONFIG_DIR.glob("*.yaml"))


def resolve_config_path(spec):
    """A name (``dev``) in CONFIG_DIR, or a path if ``spec`` ends in .yaml or
    .yml or contains a "/"."""
    spec = str(spec)
    if spec.endswith((".yaml", ".yml")) or "/" in spec:
        p = Path(spec)
        if not p.is_file():
            raise SSOUploadError(f"no config file {spec}")
        return p
    p = CONFIG_DIR / f"{spec}.yaml"
    if not p.is_file():
        raise SSOUploadError(f"no config named {spec!r}: the names are {_named_configs()} "
                             f"(in {CONFIG_DIR}); a path must end in .yaml or contain a '/'")
    return p


def load_config(spec):
    """Read and validate an upload config. Returns (path, dict)."""
    path = resolve_config_path(spec)
    with open(path) as f:
        cfg = yaml.safe_load(f)
    if not cfg:
        raise SSOUploadError(f"{path} has no settings (is it a commented-out template?)")
    if not isinstance(cfg, dict):
        raise SSOUploadError(f"{path}: expected a mapping, got {type(cfg).__name__}")
    unknown = set(cfg) - set(CONFIG_KEYS_REQUIRED) - set(CONFIG_KEYS_OPTIONAL)
    if unknown:
        raise SSOUploadError(f"{path}: unknown keys {sorted(unknown)}")
    missing = [k for k in CONFIG_KEYS_REQUIRED if not cfg.get(k)]
    if missing:
        raise SSOUploadError(f"{path}: missing {missing}")
    for k in CONFIG_KEYS_REQUIRED:
        if not isinstance(cfg[k], str):
            raise SSOUploadError(f"{path}: {k} must be a string")
    if "tables" in cfg:
        check_tables(cfg["tables"], f"{path}: tables")
    if "parallel" in cfg and not isinstance(cfg["parallel"], bool):
        raise SSOUploadError(f"{path}: parallel must be true or false")
    return path, cfg


def parallel_options(chunk_mib=PARALLEL_CHUNK_MIB, workers=PARALLEL_WORKERS):
    """The --parallel settings, {"chunk_bytes", "workers"}, checked against
    GCS's multipart limits and PARALLEL_MAX_WORKERS."""
    if (not isinstance(workers, int) or isinstance(workers, bool)
            or not 1 <= workers <= PARALLEL_MAX_WORKERS):
        raise SSOUploadError(f"--parallel-workers must be 1 to {PARALLEL_MAX_WORKERS}, not {workers!r}")
    chunk = (int(chunk_mib * (1 << 20)) if isinstance(chunk_mib, (int, float))
             and not isinstance(chunk_mib, bool) and math.isfinite(chunk_mib) else None)
    if chunk is None or not MPU_MIN_PART_BYTES <= chunk <= MPU_MAX_PART_BYTES:
        raise SSOUploadError(f"--chunk-mib must be {MPU_MIN_PART_BYTES >> 20} to {MPU_MAX_PART_BYTES >> 20} "
                             f"(GCS multipart part sizes), not {chunk_mib!r}")
    return {"chunk_bytes": chunk, "workers": workers}


def check_tables(tables, what="tables"):
    if not tables or not isinstance(tables, (list, tuple)):
        raise SSOUploadError(f"{what}: expected a non-empty list")
    bad = [t for t in tables if t not in DELIVERY_TABLES]
    if bad:
        raise SSOUploadError(f"{what}: {bad} are not delivery tables {list(DELIVERY_TABLES)}")
    if len(set(tables)) != len(tables):
        raise SSOUploadError(f"{what}: duplicates in {list(tables)}")


def select_tables(cfg, cfg_path, tables=None, allow_partial=False, include_unsupported=False):
    """The tables to upload, in UPLOAD_TABLE_ORDER, after the
    unsupported-table and partial-upload checks."""
    complete = list(cfg.get("tables") or ACCEPTED_TABLES)
    if tables is not None:
        check_tables(tables, "--tables")
    requested = list(tables or complete)

    unsupported = [t for t in requested if t in UNSUPPORTED_TABLES]
    if unsupported and not include_unsupported:
        src = "--tables" if tables else f"{cfg_path}: tables"
        raise SSOUploadError(f"{src} asks for {unsupported}: {UNSUPPORTED_NOTE}. "
                             "Refusing without --include-unsupported")
    absent = [t for t in complete if t not in requested]
    if absent and not allow_partial:
        raise SSOUploadError(
            f"--tables leaves out {absent} of the config's complete set {complete}; the loader replaces "
            "each table it is sent (WRITE_TRUNCATE), so a subset mixes snapshots. "
            "Refusing without --allow-partial")
    if absent:
        _LOG.warning("partial upload (--allow-partial): leaving out %s", absent)
    for t in unsupported:
        _LOG.warning("%s: uploading it (--include-unsupported), but it is %s", t, UNSUPPORTED_NOTE)
    for t in UNSUPPORTED_TABLES:
        if t not in requested:
            _LOG.info("%s: built and staged, %s", t, UNSUPPORTED_NOTE)
    return [t for t in UPLOAD_TABLE_ORDER if t in requested]


def resolve_run_dir(path):
    """RUN_DIR, also accepting RUN_DIR/delivery."""
    p = Path(path)
    if not (p / REPORT_FILE).is_file() and p.name == DELIVERY_DIR and (p.parent / REPORT_FILE).is_file():
        return p.parent
    return p


def md5_file(path, bufsize=1 << 22):
    h = hashlib.md5()
    with open(path, "rb") as f:
        while chunk := f.read(bufsize):
            h.update(chunk)
    return h.hexdigest()


def crc32c_file(path, bufsize=1 << 22):
    """A file's CRC32C as GCS gives it (``Blob.crc32c``): base64 of the
    big-endian 4 bytes."""
    import google_crc32c
    c = google_crc32c.Checksum()
    with open(path, "rb") as f:
        while chunk := f.read(bufsize):
            c.update(chunk)
    return base64.b64encode(c.digest()).decode("ascii")


def read_report(run_dir):
    path = Path(run_dir) / REPORT_FILE
    if not path.is_file():
        raise SSOUploadError(f"no {path}; build the delivery with ssp-build-sso first")
    with open(path) as f:
        return json.load(f)


def _verify_partitioned(run_dir, table, rec):
    """The problems with partitioned ``table`` (SSObservation) against its
    report entry ``rec`` and its manifest, and its parts, in part order."""
    delivery = Path(run_dir) / DELIVERY_DIR
    mpath = delivery / SSOBSERVATION_MANIFEST_FILE
    if not rec or not rec.get("parts"):
        return [f"{table}: no parts in the report"], []
    if not mpath.is_file():
        return [f"{table}: {mpath} is missing"], []
    if not rec.get("manifest_md5"):
        return [f"{table}: no manifest_md5 in the report"], []
    got = md5_file(mpath)
    if got != rec["manifest_md5"]:
        return [f"{table}: {mpath} has md5 {got}, the report says {rec['manifest_md5']}"], []
    try:
        parts, rows = _read_manifest(mpath)
    except (OSError, ValueError, KeyError, TypeError) as e:
        return [f"{table}: cannot read {mpath}: {type(e).__name__}: {e}"], []
    problems = []
    names = [f for f, _, _ in parts]
    if [Path(f).name for f in rec["parts"]] != names:
        problems.append(f"{table}: the manifest lists the parts {names}, the report {rec['parts']}")
    if rec.get("rows") is not None and rows != rec["rows"]:
        problems.append(f"{table}: the manifest says {rows} rows, the report {rec['rows']}")
    if rec.get("bytes") is not None and sum(b for _, b, _ in parts) != rec["bytes"]:
        problems.append(f"{table}: the manifest's parts are {sum(b for _, b, _ in parts)} bytes, the "
                        f"report says {rec['bytes']}")
    paths = []
    for name, size, md5 in parts:
        if not _is_part_name(name):
            problems.append(f"{table}: the manifest lists {name!r}, not a part")
            continue
        path = delivery / name
        if not path.is_file():
            problems.append(f"{table}: {path} is missing")
        elif path.stat().st_size != size:
            problems.append(f"{table}: {path} is {path.stat().st_size} bytes, the manifest says {size}")
        else:
            got = md5_file(path)
            if got != md5:
                problems.append(f"{table}: {path} has md5 {got}, the manifest says {md5}")
        paths.append(path)
    return problems, paths


def _read_manifest(mpath, raw=None):
    """([(file, bytes, md5), ...], rows) of SSObservation's manifest."""
    m = json.loads(raw if raw is not None else Path(mpath).read_bytes())
    return [(p["file"], p["bytes"], p["md5"]) for p in m["parts"]], m["rows"]


def _is_part_name(name):
    return (isinstance(name, str) and Path(name).name == name and name not in SIDECARS
            and name != SSOBSERVATION_MANIFEST_FILE)


def verify_delivery(run_dir, report, tables):
    """Refuse unless the report is deliverable and each table's files match
    it. Returns {Table: [Path, ...]}, in the order of ``tables``: a table's
    one file, or SSObservation's parts in part order (never its manifest or
    sidecar)."""
    if report.get("deliverable") is not True:
        failed = [s for s, v in (report.get("steps") or {}).items()
                  if isinstance(v, dict) and v.get("status") != "ok"]
        failed += [c for c, v in (report.get("checks") or {}).items()
                   if isinstance(v, dict) and v.get("status") != "PASS"]
        raise SSOUploadError(
            f"{Path(run_dir) / REPORT_FILE}: deliverable is {report.get('deliverable')!r}, "
            f"not true; refusing to upload" + (f" (not ok: {failed})" if failed else ""))
    recorded = report.get("tables") or {}
    file_map, problems = {}, []
    for t in tables:
        if t == PARTITIONED:
            p, file_map[t] = _verify_partitioned(run_dir, t, recorded.get(t))
            problems += p
            continue
        path = Path(run_dir) / DELIVERY_DIR / f"{t}.parquet"
        rec = recorded.get(t)
        if not rec or not rec.get("md5"):
            problems.append(f"{t}: no md5 in the report")
        elif not path.is_file():
            problems.append(f"{t}: {path} is missing")
        elif rec.get("bytes") is not None and path.stat().st_size != rec["bytes"]:
            problems.append(f"{t}: {path} is {path.stat().st_size} bytes, the report says {rec['bytes']}")
        else:
            md5 = md5_file(path)
            if md5 != rec["md5"]:
                problems.append(f"{t}: {path} has md5 {md5}, the report says {rec['md5']}")
        file_map[t] = [path]
    if problems:
        raise SSOUploadError("the delivery doesn't match its report; refusing to upload:\n  "
                             + "\n  ".join(problems))
    return file_map


# --------------------------------------------------------------------------
# The combined SSObservation (RUN_DIR/upload/SSObservation.parquet)
# --------------------------------------------------------------------------

def _tmp_name(path):
    """A temporary name next to ``path``, unique to this process and call
    (<name>.<pid>.<random>.tmp), so concurrent runs on one RUN_DIR never
    write into each other's file. A run killed mid-write leaves its .tmp
    behind; such files are never read, and can be removed by hand."""
    path = Path(path)
    return path.with_name(f"{path.name}.{os.getpid()}.{secrets.token_hex(4)}.tmp")


def combined_paths(run_dir):
    """(the combined file, its record) under RUN_DIR/UPLOAD_DIR."""
    d = Path(run_dir) / UPLOAD_DIR
    return d / UPLOAD_SSOBSERVATION_FILE, d / UPLOAD_SSOBSERVATION_RECORD


def _why_not_reuse(path, rec_path, manifest_md5, rows):
    """Why the combined file can't be reused (a string), or None if it can.
    Returns (reason, record)."""
    if not path.is_file() and not rec_path.is_file():
        return "none built yet", None
    if not rec_path.is_file():
        return f"no {rec_path}", None
    try:
        rec = json.loads(rec_path.read_text())
        built_from, rec_rows, size, md5 = rec["manifest_md5"], rec["rows"], rec["bytes"], rec["md5"]
    except (OSError, ValueError, KeyError, TypeError) as e:
        return f"cannot read {rec_path}: {type(e).__name__}: {e}", None
    if built_from != manifest_md5:
        return f"it was built from manifest md5 {built_from}, the report's is {manifest_md5}", None
    if rec_rows != rows:
        return f"{rec_path} says {rec_rows} rows, the manifest {rows}", None
    if not path.is_file():
        return f"{path} is missing", None
    if path.stat().st_size != size:
        return f"{path} is {path.stat().st_size} bytes, its record says {size}", None
    got = md5_file(path)
    if got != md5:
        return f"{path} has md5 {got}, its record says {md5}", None
    return None, rec


def _write_combined(parts, out):
    """Stream ``parts`` into ``out``, one row group at a time, each input row
    group one output row group. Returns (schema, [rows of each row group])."""
    import pyarrow.parquet as pq

    if not parts:
        raise SSOUploadError(f"{PARTITIONED}: the manifest lists no parts")
    schema = None
    for p in parts:
        with pq.ParquetFile(p) as pf:
            s = pf.schema_arrow
        if schema is None:
            schema, first = s, p
        elif not s.equals(schema, check_metadata=True):
            diff = [f"{f}" for f in s if schema.get_field_index(f.name) < 0
                    or not schema.field(f.name).equals(f, check_metadata=True)]
            diff += [f"{f.name} missing" for f in schema if s.get_field_index(f.name) < 0]
            raise SSOUploadError(f"{PARTITIONED}: {p} has another schema than {first} "
                                 f"(differs in {diff[:5] or 'column order or metadata'}); "
                                 "refusing to combine the parts")
    rg_rows = []
    with pq.ParquetWriter(out, schema, compression="zstd") as w:
        for p in parts:
            with pq.ParquetFile(p) as pf:
                for i in range(pf.num_row_groups):
                    t = pf.read_row_group(i)
                    # one row group, never split (the default caps it
                    # at 1 Mi rows)
                    w.write_table(t, row_group_size=max(t.num_rows, 1))
                    rg_rows.append(t.num_rows)
                    del t
    return schema, rg_rows


def _check_combined(path, schema, rows, rg_rows):
    """Refuse a combined file without the manifest's rows, the parts'
    schema, or the parts' row groups."""
    import pyarrow.parquet as pq

    with pq.ParquetFile(path) as pf:
        md = pf.metadata
        problems = []
        if md.num_rows != rows:
            problems.append(f"{md.num_rows} rows, the manifest says {rows}")
        if not pf.schema_arrow.equals(schema, check_metadata=True):
            problems.append("a schema other than the parts'")
        got = [md.row_group(i).num_rows for i in range(md.num_row_groups)]
        if got != rg_rows:
            problems.append(f"row groups of {got} rows, the parts' are {rg_rows}")
    if problems:
        raise SSOUploadError(f"{PARTITIONED}: the combined {path} has " + "; ".join(problems))


def combine_ssobservation(run_dir, manifest_md5):
    """The combined SSObservation,
    RUN_DIR/UPLOAD_DIR/UPLOAD_SSOBSERVATION_FILE, built from the parts the
    manifest lists (after verify_delivery has checked them), or reused if
    its record names ``manifest_md5`` and the file has the recorded size
    and md5. Returns (path, record, reused)."""
    run_dir = Path(run_dir)
    mpath = run_dir / DELIVERY_DIR / SSOBSERVATION_MANIFEST_FILE
    raw = mpath.read_bytes()
    if hashlib.md5(raw).hexdigest() != manifest_md5:
        raise SSOUploadError(f"{PARTITIONED}: {mpath} changed: its md5 is no longer {manifest_md5}")
    parts, rows = _read_manifest(mpath, raw)
    path, rec_path = combined_paths(run_dir)

    why, rec = _why_not_reuse(path, rec_path, manifest_md5, rows)
    if why is None:
        _LOG.info("reusing %s (%d rows, md5 %s, built from manifest md5 %s)", path, rec["rows"],
                  rec["md5"], manifest_md5)
        return path, rec, True
    _LOG.info("building %s from %d parts (%s)", path, len(parts), why)

    path.parent.mkdir(parents=True, exist_ok=True)
    rec_path.unlink(missing_ok=True)
    tmp = _tmp_name(path)
    t0 = time.monotonic()
    try:
        schema, rg_rows = _write_combined([run_dir / DELIVERY_DIR / f for f, _, _ in parts], tmp)
        _check_combined(tmp, schema, rows, rg_rows)
        rec = {"file": f"{UPLOAD_DIR}/{UPLOAD_SSOBSERVATION_FILE}", "rows": rows,
               "bytes": tmp.stat().st_size, "md5": md5_file(tmp), "manifest_md5": manifest_md5}
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    _write_json(rec_path, rec)
    _LOG.info("built %s in %.1f s: %d rows in %d row groups, %d bytes, md5 %s", path,
              time.monotonic() - t0, rows, len(rg_rows), rec["bytes"], rec["md5"])
    return path, rec, False


# --------------------------------------------------------------------------
# The report
# --------------------------------------------------------------------------

def _write_json(path, obj):
    """Write ``obj`` to ``path`` atomically."""
    path = Path(path)
    tmp = _tmp_name(path)
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=2)
        f.write("\n")
    os.replace(tmp, path)


def write_report(run_dir, report):
    """Rewrite report.json atomically."""
    _write_json(Path(run_dir) / REPORT_FILE, report)


def counts_as_upload(record):
    """Whether an upload record is one the --force check counts: not a dry
    run, and not unpublished (--no-publish). A publish that wasn't
    confirmed (published null) counts: the message may have gone through."""
    return (isinstance(record, dict) and not record.get("dry_run")
            and record.get("published") is not False)


def record_upload(run_dir, report, record):
    """Append to ``uploads``; set ``upload`` unless a dry run or a
    --no-publish run would replace a published upload's record."""
    report.setdefault("uploads", []).append(record)
    if not counts_as_upload(record) and counts_as_upload(report.get("upload")):
        _LOG.info("this run goes in 'uploads' only: 'upload' records a published upload")
    else:
        report["upload"] = record
    write_report(run_dir, report)


# --------------------------------------------------------------------------
# The upload (mirrors dax_ppdb SSOUploader)
# --------------------------------------------------------------------------

def generate_prefix(now=None):
    """The object prefix: UTC time to the millisecond, e.g.
    20260914T200401001."""
    now = now or _now()
    return now.astimezone(timezone.utc).strftime(UPLOAD_PREFIX_FORMAT)[:-3]


def object_name(object_prefix, table):
    """A table's object name: <prefix>/<Table>.parquet."""
    return posixpath.join(object_prefix, f"{table}.parquet")


def message_body(bucket_name, object_prefix, tables):
    """The Pub/Sub message, as the bytes published: exactly dax_ppdb
    SSOUploader's, the delivery contract's UPLOAD_MESSAGE_FIELDS."""
    data = dict(zip(UPLOAD_MESSAGE_FIELDS, (bucket_name, object_prefix, list(tables)), strict=True))
    return json.dumps(data).encode("utf-8")


def planned_objects(bucket_name, object_prefix, tables):
    """{Table: gs:// URI}, in the order of ``tables``."""
    return {t: f"gs://{bucket_name}/{object_name(object_prefix, t)}" for t in tables}


def _upload_order(file_map):
    """``file_map``'s tables in UPLOAD_TABLE_ORDER; refuses anything that
    isn't a table, or a sidecar."""
    bad = [t for t in file_map if t not in UPLOAD_TABLE_ORDER]
    if bad:
        raise SSOUploadError(f"{bad} are not upload tables {list(UPLOAD_TABLE_ORDER)}")
    side = sorted({Path(p).name for p in file_map.values()} & set(SIDECARS))
    if side:
        raise SSOUploadError(f"{', '.join(side)} (internal columns) is never uploaded")
    return [t for t in UPLOAD_TABLE_ORDER if t in file_map]


def _storage_client():
    """A GCS client with application-default credentials (patched in tests)."""
    from google.cloud import storage
    return storage.Client()


def _publisher_client():
    """A Pub/Sub publisher with application-default credentials (patched in
    tests)."""
    from google.cloud import pubsub_v1
    return pubsub_v1.PublisherClient()


def is_chunked(path, parallel):
    """Whether ``path`` goes in parallel chunks with the ``parallel``
    settings (None: never)."""
    return bool(parallel) and Path(path).stat().st_size >= PARALLEL_MIN_BYTES


def upload(cfg, file_map, object_prefix=None, publish_timeout=PUBLISH_TIMEOUT_S, publish=True,
           on_unconfirmed=None, parallel=None):
    """Upload ``file_map`` ({Table: Path}), each table as
    <prefix>/<Table>.parquet in UPLOAD_TABLE_ORDER, and publish the message
    (unless ``publish`` is false: then the objects are kept and nothing is
    published). With ``parallel`` (parallel_options()), files of at least
    PARALLEL_MIN_BYTES go in parallel chunks (_upload_chunked).

    Returns (object_prefix, message_id); message_id is None without
    ``publish``. On any failure before the message is handed to Pub/Sub
    (publisher.publish() returned), deletes the objects this call uploaded
    and raises SSOUploadError. After that, the message may have gone
    through, so anything that stops the wait for its confirmation (a
    timeout, an error, KeyboardInterrupt, SystemExit, ...) keeps the
    objects, calls ``on_unconfirmed(object_prefix, message)`` if given, and
    raises PublishUnconfirmedError for an Exception, or re-raises a
    BaseException that isn't one.
    """
    try:
        from google.api_core.exceptions import PreconditionFailed
    except ImportError as e:
        raise SSOUploadError("the Google client libraries are missing: pip install 'ssp[upload]'") from e

    bucket_name, topic, project = cfg["bucket_name"], cfg["topic"], cfg["project"]
    object_prefix = object_prefix or generate_prefix()
    tables = _upload_order(file_map)
    if any(is_chunked(file_map[t], parallel) for t in tables):
        _require_crc32c_c()
    try:
        bucket = _storage_client().bucket(bucket_name)
    except Exception as e:      # e.g. GoogleAuthError: no application-default credentials
        raise SSOUploadError(f"can't make a GCS client: {e!r}") from e

    uploaded = []
    message_id = None
    handed_over = False
    try:
        for table in tables:
            path = file_map[table]
            name = object_name(object_prefix, table)
            size = Path(path).stat().st_size
            t0 = time.monotonic()
            if is_chunked(path, parallel):
                # cleans up after itself on failure
                _upload_chunked(bucket, bucket_name, name, path, size, **parallel)
            else:
                try:
                    _LOG.info("uploading %s to gs://%s/%s", path, bucket_name, name)
                    bucket.blob(name).upload_from_filename(str(path), if_generation_match=0)
                except PreconditionFailed as e:
                    raise SSOUploadError(f"gs://{bucket_name}/{name} already exists; "
                                         "a naming collision for this upload prefix") from e
                except Exception as e:      # GoogleAPIError, GoogleAuthError, OSError, ...
                    raise SSOUploadError(f"failed to upload {path} to gs://{bucket_name}/{name}: "
                                         f"{e!r}") from e
            uploaded.append(name)
            dt = time.monotonic() - t0
            _LOG.info("uploaded %s: %d bytes in %.1f s (%.1f MB/s)", name, size, dt,
                      size / dt / 1e6 if dt > 0 else float("nan"))
        _LOG.info("uploaded %d tables to gs://%s/%s", len(uploaded), bucket_name, object_prefix)

        data = message_body(bucket_name, object_prefix, tables)
        topic_path = f"projects/{project}/topics/{topic}"
        if not publish:
            _LOG.info("--no-publish: not publishing to %s: %s", topic_path, data.decode())
            return object_prefix, None
        try:
            publisher = _publisher_client()
            topic_path = publisher.topic_path(project, topic)
            future = publisher.publish(topic_path, data)
        except Exception as e:
            raise SSOUploadError(f"failed to publish to {topic_path}: {data.decode()}: {e!r}") from e
        handed_over = True      # from here on the message may go through
        try:
            message_id = future.result(timeout=publish_timeout)
        except BaseException as e:
            if isinstance(e, (concurrent.futures.TimeoutError, TimeoutError)):
                what = f"wasn't confirmed within {publish_timeout} s"
            else:
                what = f"was interrupted or failed before it was confirmed ({e!r})"
            msg = (f"the publish to {topic_path} {what}; it may or may not have gone through. "
                   f"The objects are KEPT at gs://{bucket_name}/{object_prefix}/. Check whether the "
                   f"load ran before uploading again or deleting them. Message: {data.decode()}")
            _LOG.error("%s", msg)
            if on_unconfirmed is not None:
                try:
                    on_unconfirmed(object_prefix, msg)
                except Exception:
                    _LOG.exception("failed to record the unconfirmed publish")
            if isinstance(e, Exception):
                raise PublishUnconfirmedError(msg, object_prefix) from e
            raise
        _LOG.info("published message %s to %s: %s", message_id, topic_path, data.decode())
    except BaseException:
        if not handed_over:
            _cleanup(bucket, bucket_name, uploaded)
        raise
    return object_prefix, message_id


def _cleanup(bucket, bucket_name, names):
    """Delete what a failed upload wrote; never raises."""
    for name in names:
        try:
            bucket.blob(name).delete()
            _LOG.warning("deleted gs://%s/%s during cleanup", bucket_name, name)
        except Exception:
            _LOG.exception("failed to delete gs://%s/%s during cleanup; remove it by hand",
                           bucket_name, name)


# --------------------------------------------------------------------------
# Parallel chunked uploads (the default; --no-parallel opts out;
# docs/design/sso-upload-parallel.md)
# --------------------------------------------------------------------------

def _require_crc32c_c():
    """Refuse without google-crc32c's C extension: the whole-file CRC32C of
    a few GB would take minutes in pure Python."""
    try:
        import google_crc32c
    except ImportError as e:
        raise SSOUploadError("parallel chunked uploads (the default) need google-crc32c (it comes with "
                             "google-cloud-storage); --no-parallel doesn't") from e
    if google_crc32c.implementation != "c":
        raise SSOUploadError("parallel chunked uploads (the default) need google-crc32c's C extension; it "
                             f"has the {google_crc32c.implementation!r} one. --no-parallel doesn't need it")


def _size_pool(session, workers):
    """Give the client's requests session an https connection pool of at
    least ``workers`` connections, so the upload threads don't reconnect
    (urllib3: "Connection pool is full, discarding connection"; the default
    is 10). Only a plain HTTPAdapter is replaced, keeping its retries; any
    other (e.g. google-auth's mutual-TLS adapters) is left alone."""
    import requests.adapters
    old = getattr(session, "adapters", {}).get("https://")
    if type(old) is not requests.adapters.HTTPAdapter or old._pool_maxsize >= workers:
        return
    session.mount("https://", requests.adapters.HTTPAdapter(
        pool_connections=old._pool_connections, pool_maxsize=workers, max_retries=old.max_retries,
        pool_block=old._pool_block))


def _chunks_concurrently(path, blob, chunk_bytes, workers):
    """One XML multipart upload of ``path`` to ``blob``, in parts of
    ``chunk_bytes`` sent by ``workers`` threads, each part checked with
    CRC32C (patched in tests). The library cancels the multipart upload if a
    part fails."""
    from google.cloud.storage import transfer_manager
    _size_pool(blob.client._http, workers)
    transfer_manager.upload_chunks_concurrently(
        str(path), blob, chunk_size=chunk_bytes, worker_type=transfer_manager.THREAD,
        max_workers=workers, checksum="crc32c")


def _xml_api(bucket):
    """(the bucket's XML API URL, an authorized session)."""
    from google.cloud.storage.blob import _get_host_name
    client = bucket.client
    return f"{_get_host_name(client._connection)}/{bucket.name}", client._http


def _list_multipart_uploads(bucket, prefix):
    """[(object name, upload id), ...] of the incomplete XML multipart
    uploads under ``prefix`` (GET ?uploads; patched in tests)."""
    url, http = _xml_api(bucket)
    out, params = [], {"uploads": "", "prefix": prefix}
    while True:
        r = http.get(url, params=params)
        if r.status_code != 200:
            raise SSOUploadError(f"listing the multipart uploads under {prefix} failed: HTTP {r.status_code} "
                                 f"{r.text[:300]}")
        root = ET.fromstring(r.content)

        def text(e, tag):
            # {*}: any namespace, or none
            x = e.find(f"{{*}}{tag}")
            return x.text if x is not None else None

        out += [(text(e, "Key"), text(e, "UploadId")) for e in root.findall("{*}Upload")]
        if text(root, "IsTruncated") != "true":
            return out
        markers = (text(root, "NextKeyMarker"), text(root, "NextUploadIdMarker"))
        if not all(markers) or markers == (params.get("key-marker"), params.get("upload-id-marker")):
            raise SSOUploadError(f"listing the multipart uploads under {prefix}: the listing is truncated "
                                 f"but its next markers are missing or unchanged ({markers}); stopping")
        params = {**params, "key-marker": markers[0], "upload-id-marker": markers[1]}


def _abort_multipart_upload(bucket, name, upload_id):
    """Abort one incomplete XML multipart upload (patched in tests)."""
    from urllib.parse import quote
    url, http = _xml_api(bucket)
    r = http.delete(f"{url}/{quote(name, safe='/')}", params={"uploadId": upload_id})
    if r.status_code not in (200, 204, 404):
        raise SSOUploadError(f"aborting multipart upload {upload_id} of {name} failed: HTTP {r.status_code} "
                             f"{r.text[:300]}")


def _abort_multipart(bucket, bucket_name, name):
    """Abort every incomplete multipart upload of object ``name``; never
    raises."""
    try:
        ups = [u for k, u in _list_multipart_uploads(bucket, name) if k == name]
    except Exception:
        _LOG.exception("failed to list the multipart uploads of gs://%s/%s during cleanup; check for "
                       "incomplete ones by hand", bucket_name, name)
        return
    for u in ups:
        try:
            _abort_multipart_upload(bucket, name, u)
            _LOG.warning("aborted multipart upload %s of gs://%s/%s during cleanup", u, bucket_name, name)
        except Exception:
            _LOG.exception("failed to abort multipart upload %s of gs://%s/%s during cleanup; "
                           "abort it by hand", u, bucket_name, name)


class _Background:
    """``fn(*args)`` in a daemon thread (an interrupt doesn't wait for it)."""

    def __init__(self, fn, *args):
        self._result = self._exc = None
        self._t = threading.Thread(target=self._run, args=(fn, args), daemon=True)
        self._t.start()

    def _run(self, fn, args):
        try:
            self._result = fn(*args)
        except BaseException as e:
            self._exc = e

    def result(self):
        self._t.join()
        if self._exc is not None:
            raise self._exc
        return self._result


def _upload_chunked(bucket, bucket_name, name, path, size, chunk_bytes, workers):
    """Upload ``path`` (``size`` bytes) to ``name`` in parallel chunks.

    GCS takes no preconditions on multipart uploads, so the object is
    checked not to exist first (the prefix is fresh). The local CRC32C is
    computed while the upload runs; the object's CRC32C and size must equal
    the local file's. On any failure or interrupt after the check, aborts
    the object's incomplete multipart uploads and raises (SSOUploadError
    for an Exception). The object is deleted only if our multipart upload
    completed and its generation was read back (e.g. a CRC32C or size
    mismatch), and then only that generation: never an object we didn't
    write."""
    uri = f"gs://{bucket_name}/{name}"
    parts = -(-size // chunk_bytes)
    if parts > MPU_MAX_PARTS:
        raise SSOUploadError(f"{path}: {parts} parts of {chunk_bytes} bytes; "
                             f"GCS takes at most {MPU_MAX_PARTS}")
    blob = bucket.blob(name)
    try:
        exists = blob.exists()
    except Exception as e:
        hint = ""
        if getattr(e, "code", None) == 403:
            hint = (" (HTTP 403: the account lacks storage.objects.get, which the parallel chunked "
                    "upload needs; --no-parallel avoids it)")
        raise SSOUploadError(f"can't check whether {uri} exists: {e!r}{hint}") from e
    if exists:
        raise SSOUploadError(f"{uri} already exists; a naming collision for this upload prefix")

    _LOG.info("uploading %s to %s in %d chunks of %d MiB, %d threads (no MD5: checked by CRC32C)",
              path, uri, parts, chunk_bytes >> 20, workers)
    local = _Background(crc32c_file, path)
    completed, generation = False, None
    try:
        try:
            _chunks_concurrently(path, blob, chunk_bytes, workers)
        except Exception as e:
            raise SSOUploadError(f"failed to upload {path} to {uri} in chunks: {e!r}") from e
        completed = True        # the multipart upload's complete succeeded: the object is ours
        try:
            blob.reload()
        except Exception as e:
            raise SSOUploadError(f"can't read back {uri} after uploading it: {e!r}") from e
        generation = blob.generation
        try:
            want = local.result()
        except Exception as e:
            raise SSOUploadError(f"can't compute the CRC32C of {path}: {e!r}") from e
        if blob.crc32c != want or blob.size != size:
            raise SSOUploadError(f"{uri} has CRC32C {blob.crc32c} and {blob.size} bytes; {path} has "
                                 f"{want} and {size}: the upload is corrupt")
        _LOG.info("%s: CRC32C %s, %d bytes, equal to %s's", uri, want, size, path)
    except BaseException:
        _abort_multipart(bucket, bucket_name, name)
        if completed and generation is not None:
            try:
                bucket.blob(name).delete(if_generation_match=generation)
                _LOG.warning("deleted gs://%s/%s (generation %s) during cleanup", bucket_name, name,
                             generation)
            except Exception:
                _LOG.exception("failed to delete gs://%s/%s (generation %s) during cleanup; remove it "
                               "by hand", bucket_name, name, generation)
        elif completed:
            _LOG.error("gs://%s/%s was uploaded but its generation couldn't be read back, so it is not "
                       "deleted; check it and remove it by hand", bucket_name, name)
        raise


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def run(config, run_dir, tables=None, dry_run=False, force=False, allow_partial=False,
        include_unsupported=False, no_publish=False, out=None, parallel=None,
        chunk_mib=PARALLEL_CHUNK_MIB, workers=PARALLEL_WORKERS):
    """The whole of stage 3. Returns the upload record. ``parallel``: True or
    False, or None for the config's ``parallel`` (default True)."""
    out = out or sys.stdout
    if dry_run and no_publish:
        raise SSOUploadError("--no-publish uploads and --dry-run doesn't: give one or the other")
    cfg_path, cfg = load_config(config)
    if parallel is None:
        parallel = cfg.get("parallel", True)
    par = parallel_options(chunk_mib, workers) if parallel else None
    tables = select_tables(cfg, cfg_path, tables, allow_partial, include_unsupported)

    run_dir = resolve_run_dir(run_dir)
    report = read_report(run_dir)
    prev = report.get("upload")
    if counts_as_upload(prev) and not force and not dry_run and not no_publish:
        raise SSOUploadError(
            f"{run_dir / REPORT_FILE} records an upload already (gs://{prev.get('bucket')}/"
            f"{prev.get('object_prefix')}, message {prev.get('message_id')}); --force to upload again, "
            "once the previous load has finished")
    file_map = verify_delivery(run_dir, report, tables)

    upload_map, ssobs = {}, None
    for t in tables:
        if t == PARTITIONED:
            path, ssobs, reused = combine_ssobservation(
                run_dir, report["tables"][PARTITIONED]["manifest_md5"])
            print(f"{PARTITIONED}: {'reused' if reused else 'built'} {path} from "
                  f"{len(file_map[t])} parts: {ssobs['rows']} rows, {ssobs['bytes']} bytes, "
                  f"md5 {ssobs['md5']}", file=out)
            upload_map[t] = path
        else:
            upload_map[t], = file_map[t]

    record = {
        "config": str(cfg_path),
        "bucket": cfg["bucket_name"],
        "object_prefix": None,
        "tables": list(upload_map),
        "message_id": None,
        "dry_run": dry_run,
        "published": False,
        "utc": None,
    }
    if ssobs is not None:
        record["ssobservation"] = ssobs
    chunked = [t for t in upload_map if is_chunked(upload_map[t], par)]
    # which objects go in chunks (they get a CRC32C but no MD5); null with
    # --no-parallel
    record["parallel"] = (None if par is None else
                          {**par, "min_bytes": PARALLEL_MIN_BYTES, "tables": chunked})
    topic_path = f"projects/{cfg['project']}/topics/{cfg['topic']}"
    if dry_run:
        record["object_prefix"] = generate_prefix()
        print(f"dry run: config {cfg_path}; nothing is uploaded or published", file=out)
        if par is None:
            print("  parallel: off (--no-parallel or parallel: false); every file in one stream", file=out)
        else:
            print(f"  parallel: files of at least {PARALLEL_MIN_BYTES} bytes in chunks of "
                  f"{par['chunk_bytes'] >> 20} MiB, {par['workers']} threads: {chunked or 'none'}", file=out)
        for t, uri in planned_objects(cfg["bucket_name"], record["object_prefix"], upload_map).items():
            how = (f"in chunks of {par['chunk_bytes'] >> 20} MiB, {par['workers']} threads; checked not "
                   "to exist, CRC32C verified" if t in chunked else "if_generation_match=0")
            print(f"  {upload_map[t]} -> {uri}  ({how})", file=out)
        print(f"  publish to {topic_path}:", file=out)
        print(f"    {message_body(cfg['bucket_name'], record['object_prefix'], upload_map).decode()}",
              file=out)
    else:
        def unconfirmed(object_prefix, msg):
            # the publish may have gone through: record it (published null),
            # whatever stopped the wait (upload() then raises)
            record.update(object_prefix=object_prefix, published=None,
                          utc=_now().isoformat(timespec="seconds"), error=msg)
            record_upload(run_dir, report, record)

        record["object_prefix"], record["message_id"] = upload(
            cfg, upload_map, publish=not no_publish, on_unconfirmed=unconfirmed, parallel=par)
        where = f"gs://{cfg['bucket_name']}/{record['object_prefix']}"
        if no_publish:
            print(f"uploaded {len(upload_map)} tables to {where}/ and published NOTHING (--no-publish); "
                  "the objects are KEPT: delete them by hand when done", file=out)
            print(f"  the message that would have gone to {topic_path}:", file=out)
            print(f"    {message_body(cfg['bucket_name'], record['object_prefix'], upload_map).decode()}",
                  file=out)
        else:
            record["published"] = True
            print(f"uploaded {len(upload_map)} tables to {where}; message {record['message_id']}",
                  file=out)
    record["utc"] = _now().isoformat(timespec="seconds")
    record_upload(run_dir, report, record)
    return record


def main(argv=None):
    unsupported = (f" {', '.join(UNSUPPORTED_TABLES)} is {UNSUPPORTED_NOTE}." if UNSUPPORTED_TABLES
                   else "")
    parser = argparse.ArgumentParser(
        prog="ssp-upload-sso",
        description="Upload a checked SSO delivery to the PPDB's SSO ingestion bucket, one object per "
                    "table, gs://<bucket>/<prefix>/<Table>.parquet (SSObservation's parts combined into "
                    f"RUN_DIR/{UPLOAD_DIR}/{UPLOAD_SSOBSERVATION_FILE} first; no manifest, part or "
                    "sidecar is uploaded), and announce it on Pub/Sub (DM-55678).",
        epilog=f"Named configs: {_named_configs()} in {CONFIG_DIR}. "
               "Credentials: Google application-default credentials "
               "(GOOGLE_APPLICATION_CREDENTIALS, e.g. the sso-uploader service account's key, or "
               f"gcloud auth application-default login).{unsupported} Upload at most once per load "
               "window: the loader silently drops an upload made while a load is running.",
    )
    parser.add_argument("config", metavar="CONFIG",
                        help="upload config: a name (dev, int, prod), or a path (*.yaml, *.yml, or with a /)")
    parser.add_argument("run_dir", metavar="RUN_DIR",
                        help="the ssp-build-sso run directory (with report.json)")
    parser.add_argument("--dry-run", action="store_true",
                        help="validate, build (or reuse) the combined SSObservation, print the planned "
                             "objects and the message, and touch nothing remote")
    parser.add_argument("--no-publish", action="store_true",
                        help="upload the objects but publish nothing (no load starts); the objects are "
                             "kept, to be deleted by hand. For tests; not with --dry-run")
    parser.add_argument("--tables", nargs="+", metavar="TABLE",
                        help=f"tables to upload (default: the config's, else {' '.join(ACCEPTED_TABLES)})")
    parser.add_argument("--allow-partial", action="store_true",
                        help="allow --tables to leave out tables of the config's set")
    parser.add_argument("--include-unsupported", action="store_true",
                        help="allow tables the loader doesn't accept yet "
                             f"({', '.join(UNSUPPORTED_TABLES) or 'none today'})")
    parser.add_argument("--force", action="store_true",
                        help="upload even though report.json records an earlier upload")
    parser.add_argument("--parallel", action=argparse.BooleanOptionalAction, default=None,
                        help=f"upload files of at least {PARALLEL_MIN_BYTES >> 30} GiB in parallel chunks "
                             "(GCS multipart; the objects get a CRC32C, verified, but no MD5). On by "
                             "default; --no-parallel sends every file in one stream (if_generation_match=0, "
                             "with an MD5). Overrides the config's 'parallel' (default true). "
                             "docs/design/sso-upload-parallel.md")
    parser.add_argument("--chunk-mib", type=float, default=PARALLEL_CHUNK_MIB, metavar="MIB",
                        help=f"parallel chunk size in MiB (default {PARALLEL_CHUNK_MIB})")
    parser.add_argument("--parallel-workers", type=int, default=PARALLEL_WORKERS, metavar="N",
                        help=f"parallel upload threads, at most {PARALLEL_MAX_WORKERS} "
                             f"(default {PARALLEL_WORKERS})")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        run(args.config, args.run_dir, tables=args.tables, dry_run=args.dry_run, force=args.force,
            allow_partial=args.allow_partial, include_unsupported=args.include_unsupported,
            no_publish=args.no_publish, parallel=args.parallel, chunk_mib=args.chunk_mib,
            workers=args.parallel_workers)
    except SSOUploadError as e:
        _LOG.error("%s", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
