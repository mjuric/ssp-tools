"""Stage 3 of the SSO delivery: upload a checked delivery to the PPDB's SSO
ingestion (DM-55678) and wait for it to load.

    ssp-upload-sso CONFIG RUN_DIR [--dry-run | --no-publish] [--tables T ...]
                   [--force] [--allow-partial] [--include-unsupported]
                   [--no-parallel | --chunk-mib N --parallel-workers N]
                   [--no-wait | --wait-timeout SECONDS]

It follows the contract in ssp/delivery_contract.py, which mirrors the PPDB
team's uploader, lsst/dax_ppdb python/lsst/dax/ppdb/bigquery/sso/
uploader.py and manifest.py (DM-56960, read at ec576a6), and its loader,
lsst-dm/ppdb-cloud-functions load_sso (dax_ppdb sso/loader.py), which reads
<prefix>/manifest.json and loads the parts it lists
(docs/design/sso-upload-manifest.md):

1. Refuse unless RUN_DIR/report.json says ``deliverable: true`` and every
   table to upload, RUN_DIR/delivery/<Table>.parquet, has the md5, size
   and rows the report recorded. SSObservation is partitioned
   (ssp.ssobservation_contract, "The partitioned delivery"): its manifest,
   SSObservation.manifest.json, must have the report's manifest_md5 and
   list the parts the report recorded, with the report's total rows and
   bytes, and each part must have the size, md5 and rows the manifest
   gives. Each file's SHA-256 is computed in the same read as its md5, and
   its rows come from its Parquet metadata. A file with 0 rows refuses the
   whole upload: the PPDB takes none (row_count >= 1), and uploading the
   other tables without it would mix snapshots.
2. Build the upload manifest, exactly dax_ppdb's sso.Manifest:
   {"uploaded_at": ISO 8601 UTC, "tables": {Table: {"part-NNNNN.parquet":
   {"row_count", "checksum" (SHA-256 hex), "size_bytes", "source_path"
   (the absolute local path)}}}}, the tables in dax_ppdb's SSO_TABLES order
   (UPLOAD_TABLE_ORDER), parts numbered from 00000 per table: one for each
   single-file table, SSObservation's in its manifest's (sorted) order. No
   manifest of ours or sidecar (SSObservation_internal.parquet,
   NearbySSO_internal.parquet) is ever a part.
3. Make the prefix, the UTC time to the millisecond, and refuse it if any
   object is under it already.
4. Upload the parts, in table and part order, as
   gs://<bucket>/<prefix>/<Table>/part-NNNNN.parquet: files under
   PARALLEL_MIN_BYTES in one stream with if_generation_match=0, bigger ones
   in parallel chunks ("Parallel chunked uploads", below).
5. Upload <prefix>/manifest.json last, with if_generation_match=0: its
   presence means the upload is complete. On any failure up to the hand-over
   of the message to Pub/Sub, delete every object this upload wrote, the
   manifest first, and fail.
6. Publish one JSON message, exactly dax_ppdb's, {"bucket",
   "object_prefix"}, to projects/<project>/topics/load-sso-topic, and wait
   (up to PUBLISH_TIMEOUT_S) for its message id. If the wait times out,
   fails or is interrupted (e.g. Ctrl-C), the objects are kept and the
   upload is recorded with published null: the message may have gone
   through. With --no-publish, stop before this step: nothing is
   published, so no load starts. The objects are kept (delete them by hand
   when done), and the prefix and the message that would have been sent
   are printed.
7. Wait for the load's result (unless --no-wait), up to WAIT_TIMEOUT_S
   (--wait-timeout), pulling projects/<project>/subscriptions/
   load-sso-result-sub in pulls of at most PULL_TIMEOUT_S. Like dax_ppdb,
   every message is acknowledged, and other prefixes' results are logged and
   skipped (as are malformed ones). Acking everything means a second waiter
   on the subscription would lose its result: upload once per load window.
8. Record the upload in report.json: ``upload`` (the last one) and
   ``uploads`` (all of them, dry runs and --no-publish runs included), with
   the ``manifest`` and the ``load``: {status: succeeded | failed | timeout
   | not_waited, job_id | error, waited_s}. A published upload is recorded
   before the wait, with load null, and updated after it. ``published`` is
   true for a published upload, false for a dry run or a --no-publish run,
   and null when the publish wasn't confirmed.

Exit status: 0 loaded, or no wait asked for (--no-wait, --no-publish,
--dry-run); 1 refused, or failed before or during the upload (an
unconfirmed publish included); 3 (EXIT_LOAD_FAILED) the load failed, its
error printed and recorded; 4 (EXIT_LOAD_TIMEOUT) no result within the
wait (or the results couldn't be read): the upload is published and may
still load, so check before uploading again.

--dry-run verifies, builds the manifest and prints the planned objects, the
manifest and the message; it touches nothing remote and needs no Google
library.

Parallel chunked uploads (the default)
-------------------------------------
By default (owner, 2026-11-13), each file of at least PARALLEL_MIN_BYTES
(128 MiB: the four big SSObservation parts, NearbySSO and mpc_orbits) is
uploaded in chunks over parallel streams, with google-cloud-storage's
transfer_manager.upload_chunks_concurrently (GCS's XML multipart upload,
threads; --chunk-mib, default PARALLEL_CHUNK_MIB, and --parallel-workers,
default PARALLEL_WORKERS). Smaller files, and the manifest, go in one
stream. Opt out with --no-parallel or ``parallel: false`` in the config;
the command line wins over the config (docs/design/sso-upload-parallel.md);
every file then goes in one stream with if_generation_match=0. A chunked
upload differs:

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
  (SIGKILL, power loss) can leave an incomplete multipart upload. The
  bucket has no AbortIncompleteMultipartUpload lifecycle rule (only a
  30-day Delete rule, idf_deploy services/gcs.tf), so it must be aborted by
  hand (docs/runbooks/sso-daily.md).
- Interrupts: the library joins its thread pool, so a first Ctrl-C (or a
  failed part) waits for the parts already queued to finish before the
  abort and cleanup run; a second Ctrl-C stops at once (and may leave an
  incomplete multipart upload).

Permissions: besides storage.objects.create and .delete, the chunked path
needs storage.objects.get (the existence check and the read-back) and
storage.multipartUploads.create, .list and .abort. roles/storage.objectUser
(what idf_deploy gives the sso-uploader account on the bucket) has them;
roles/storage.objectCreator does not. --no-parallel needs none of these.
The prefix check needs storage.objects.list, and the wait
pubsub.subscriptions.consume on load-sso-result-sub (roles/pubsub.subscriber).

The upload record's ``parallel`` is {chunk_bytes, workers, min_bytes,
tables, parts}: the tables with a part that went in chunks, and those
parts (<Table>/part-NNNNN.parquet); null with --no-parallel.

Which tables
------------
The config's ``tables`` (default: ACCEPTED_TABLES, all six delivery
tables) is the complete set. Uploading a subset of it is refused without
--allow-partial: the loader replaces each table in the manifest, so a
subset would mix snapshots in the PPDB.

UNSUPPORTED_TABLES lists delivery tables the loader doesn't accept yet;
asking for one is refused without --include-unsupported. It is empty: the
loader takes all six tables. It stays as a hook for the next table the
loader lags on.

One upload per load window
--------------------------
The loader runs one load at a time (one Cloud Run instance, one message at
a time), and the PPDB's docs say not to upload while another load runs.
With the wait, a run normally ends after its load, so the refusal of a
second upload of a run without --force matters after a timeout or
--no-wait: check that the previous load has finished first. A dry run or a
--no-publish run starts no load, and doesn't count as an earlier upload.

Config and credentials
----------------------
CONFIG is a name (dev, int, prod), looked up in the repository's
config/sso-upload/, or a path: anything ending in .yaml or .yml, or
containing a "/". Its keys are bucket_name, project, and optionally tables
and parallel. The topic and the result subscription are fixed names
(UPLOAD_TOPIC, UPLOAD_RESULT_SUBSCRIPTION); an old config's ``topic`` is
accepted only if it is UPLOAD_TOPIC. Credentials are Google
application-default credentials, e.g. GOOGLE_APPLICATION_CREDENTIALS
pointing at the sso-uploader service account's key. The Google client
libraries are the ``upload`` extra (``pip install 'ssp[upload]'``); a dry
run doesn't need them.
"""

import argparse
import base64
import concurrent.futures
import dataclasses
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
    UPLOAD_EXIT_LOAD_FAILED,
    UPLOAD_EXIT_LOAD_TIMEOUT,
    UPLOAD_LOAD_STATUSES,
    UPLOAD_MANIFEST_ENTRY_FIELDS,
    UPLOAD_MANIFEST_FIELDS,
    UPLOAD_MANIFEST_FILE,
    UPLOAD_MESSAGE_FIELDS,
    UPLOAD_PART_FORMAT,
    UPLOAD_PREFIX_FORMAT,
    UPLOAD_RESULT_SUBSCRIPTION,
    UPLOAD_TABLE_ORDER,
    UPLOAD_TOPIC,
)
from .nearbysso._contract import NEARBYSSO_SIDECAR_FILE
from .ssobservation_contract import SIDECAR_FILE, SSOBSERVATION_MANIFEST_FILE

#: Files of the delivery that are never uploaded (internal columns).
SIDECARS = (SIDECAR_FILE, NEARBYSSO_SIDECAR_FILE)
#: Files of the delivery that are never a part: the sidecars and
#: SSObservation's manifest.
NEVER_PARTS = SIDECARS + (SSOBSERVATION_MANIFEST_FILE,)

#: The delivered table that is delivered as parts plus a manifest; each
#: part is one part of the upload.
PARTITIONED = "SSObservation"

_LOG = logging.getLogger("ssp.sso_upload")

#: Where the named configs live (the repository's config/sso-upload/).
CONFIG_DIR = Path(__file__).resolve().parent.parent / "config" / "sso-upload"

CONFIG_KEYS_REQUIRED = ("bucket_name", "project")
CONFIG_KEYS_OPTIONAL = ("tables", "parallel")
#: Keys of old configs, accepted only with the one value the contract
#: fixes: {key: value}.
CONFIG_KEYS_FIXED = {"topic": UPLOAD_TOPIC}

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

#: How long to wait for the load's result by default (dax_ppdb
#: Uploader.WAIT_TIMEOUT, the load-sso function's timeout), the longest
#: single pull, and the most results one pull takes (dax_ppdb's
#: PULL_TIMEOUT_SECONDS and PULL_MAX_MESSAGES).
WAIT_TIMEOUT_S = 300
PULL_TIMEOUT_S = 60
PULL_MAX_MESSAGES = 10

#: Files at least this big are uploaded in parallel chunks (unless
#: --no-parallel; owner, 2026-11-23).
PARALLEL_MIN_BYTES = 128 << 20
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


def _monotonic():
    return time.monotonic()


def _sleep(seconds):
    time.sleep(seconds)


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
    unknown = set(cfg) - set(CONFIG_KEYS_REQUIRED) - set(CONFIG_KEYS_OPTIONAL) - set(CONFIG_KEYS_FIXED)
    if unknown:
        raise SSOUploadError(f"{path}: unknown keys {sorted(unknown)}")
    missing = [k for k in CONFIG_KEYS_REQUIRED if not cfg.get(k)]
    if missing:
        raise SSOUploadError(f"{path}: missing {missing}")
    for k in CONFIG_KEYS_REQUIRED:
        if not isinstance(cfg[k], str):
            raise SSOUploadError(f"{path}: {k} must be a string")
    for k, fixed in CONFIG_KEYS_FIXED.items():
        if k in cfg:
            if cfg[k] != fixed:
                raise SSOUploadError(f"{path}: {k} is {cfg[k]!r}, but the PPDB's uploader contract fixes it "
                                     f"as {fixed!r}; remove the key")
            _LOG.info("%s: %s %r is the fixed name; the key is no longer needed", path, k, fixed)
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


def check_wait_timeout(seconds):
    """--wait-timeout: a positive, finite number of seconds."""
    if (not isinstance(seconds, (int, float)) or isinstance(seconds, bool) or not math.isfinite(seconds)
            or seconds <= 0):
        raise SSOUploadError(f"--wait-timeout must be a positive number of seconds, not {seconds!r} "
                             "(--no-wait not to wait)")
    return seconds


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
            "each table in the manifest, so a subset mixes snapshots. "
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


def hash_file(path, bufsize=1 << 22):
    """(md5, SHA-256) hex digests of a file, in one read."""
    m, s = hashlib.md5(), hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(bufsize):
            m.update(chunk)
            s.update(chunk)
    return m.hexdigest(), s.hexdigest()


def parquet_rows(path):
    """A Parquet file's rows, from its metadata."""
    import pyarrow.parquet as pq
    return pq.read_metadata(path).num_rows


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


@dataclasses.dataclass(frozen=True)
class LocalFile:
    """A verified file of the delivery: one part of the upload."""
    path: Path          # absolute
    rows: int           # from its Parquet metadata, equal to the report's
    size: int
    md5: str
    sha256: str


def _check_file(table, path, size, md5, rows, whose):
    """Check one file against its expected ``size`` (None: not checked),
    ``md5`` and ``rows`` (``whose``: "the report", "the manifest").
    Returns (problems, LocalFile or None)."""
    if not path.is_file():
        return [f"{table}: {path} is missing"], None
    got_size = path.stat().st_size
    if size is not None and got_size != size:
        return [f"{table}: {path} is {got_size} bytes, {whose} says {size}"], None
    got_md5, sha256 = hash_file(path)
    if got_md5 != md5:
        return [f"{table}: {path} has md5 {got_md5}, {whose} says {md5}"], None
    try:
        got_rows = parquet_rows(path)
    except Exception as e:      # ArrowInvalid, OSError, ...
        return [f"{table}: cannot read the Parquet metadata of {path}: {type(e).__name__}: {e}"], None
    if rows is None:
        return [f"{table}: {whose} gives no rows for {path.name}"], None
    if got_rows != rows:
        return [f"{table}: {path} has {got_rows} rows, {whose} says {rows}"], None
    if got_rows < 1:
        return [f"{table}: {path} has 0 rows; the PPDB takes no empty file (row_count >= 1), and "
                "uploading the other tables without it would mix snapshots: refusing the whole upload"], None
    return [], LocalFile(Path(os.path.abspath(path)), got_rows, got_size, got_md5, sha256)


def _verify_partitioned(run_dir, table, rec):
    """The problems with partitioned ``table`` (SSObservation) against its
    report entry ``rec`` and its manifest, and its parts (LocalFile), in
    part order."""
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
    names = [f for f, _, _, _ in parts]
    if [Path(f).name for f in rec["parts"]] != names:
        problems.append(f"{table}: the manifest lists the parts {names}, the report {rec['parts']}")
    if rec.get("rows") is not None and rows != rec["rows"]:
        problems.append(f"{table}: the manifest says {rows} rows, the report {rec['rows']}")
    if sum(r for _, _, _, r in parts) != rows:
        problems.append(f"{table}: the manifest's parts have {sum(r for _, _, _, r in parts)} rows, its "
                        f"total says {rows}")
    if rec.get("bytes") is not None and sum(b for _, b, _, _ in parts) != rec["bytes"]:
        problems.append(f"{table}: the manifest's parts are {sum(b for _, b, _, _ in parts)} bytes, the "
                        f"report says {rec['bytes']}")
    files = []
    for name, size, md5, part_rows in parts:
        if not _is_part_name(name):
            problems.append(f"{table}: the manifest lists {name!r}, not a part")
            continue
        p, f = _check_file(table, delivery / name, size, md5, part_rows, "the manifest")
        problems += p
        if f is not None:
            files.append(f)
    return problems, files


def _read_manifest(mpath, raw=None):
    """([(file, bytes, md5, rows), ...], rows) of SSObservation's manifest."""
    m = json.loads(raw if raw is not None else Path(mpath).read_bytes())
    return [(p["file"], p["bytes"], p["md5"], p["rows"]) for p in m["parts"]], m["rows"]


def _is_part_name(name):
    return isinstance(name, str) and Path(name).name == name and name not in NEVER_PARTS


def verify_delivery(run_dir, report, tables):
    """Refuse unless the report is deliverable and each table's files match
    it (size, md5, and rows from the Parquet metadata; none empty). Returns
    {Table: [LocalFile, ...]}, in the order of ``tables``: a table's one
    file, or SSObservation's parts in part order (never its manifest or a
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
            continue
        p, f = _check_file(t, path, rec.get("bytes"), rec["md5"], rec.get("rows"), "the report")
        problems += p
        file_map[t] = [f] if f is not None else []
    if problems:
        raise SSOUploadError("the delivery doesn't match its report; refusing to upload:\n  "
                             + "\n  ".join(problems))
    return file_map


# --------------------------------------------------------------------------
# The upload manifest (dax_ppdb sso.Manifest)
# --------------------------------------------------------------------------

def _iso_utc(t):
    """ISO 8601 in UTC, as pydantic serializes an aware datetime
    (2026-11-23T12:00:00.123456Z)."""
    return t.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def part_name(index):
    """The object name of a table's part ``index``: part-NNNNN.parquet."""
    return UPLOAD_PART_FORMAT.format(index)


def build_manifest(files, uploaded_at=None):
    """The upload manifest for ``files`` ({Table: [LocalFile, ...]}), as a
    dict exactly in dax_ppdb's sso.Manifest form (UPLOAD_MANIFEST_FIELDS,
    UPLOAD_MANIFEST_ENTRY_FIELDS): the tables in UPLOAD_TABLE_ORDER, each
    table's files in their order as part-00000.parquet, ... Refuses an
    unknown table, a table without files, an empty file, a file that is
    never a part, or a file given twice."""
    bad = [t for t in files if t not in UPLOAD_TABLE_ORDER]
    if bad:
        raise SSOUploadError(f"{bad} are not upload tables {list(UPLOAD_TABLE_ORDER)}")
    seen, tables = set(), {}
    for t in (t for t in UPLOAD_TABLE_ORDER if t in files):
        if not files[t]:
            raise SSOUploadError(f"{t}: no files to upload; the PPDB takes no empty table")
        entries = {}
        for i, f in enumerate(files[t]):
            path = Path(f.path)
            if path.name in NEVER_PARTS:
                raise SSOUploadError(f"{t}: {path.name} (a sidecar or SSObservation's manifest) is never "
                                     "uploaded")
            if not path.is_absolute():
                raise SSOUploadError(f"{t}: {path} is not an absolute path")
            if path in seen:
                raise SSOUploadError(f"{t}: {path} is given more than once")
            seen.add(path)
            if f.rows < 1 or f.size < 1:
                raise SSOUploadError(f"{t}: {path} has {f.rows} rows and {f.size} bytes; the PPDB takes no "
                                     "empty file (row_count, size_bytes >= 1)")
            entries[part_name(i)] = dict(zip(UPLOAD_MANIFEST_ENTRY_FIELDS,
                                             (f.rows, f.sha256, f.size, str(path)), strict=True))
        tables[t] = entries
    return dict(zip(UPLOAD_MANIFEST_FIELDS, (_iso_utc(uploaded_at or _now()), tables), strict=True))


def manifest_json(manifest):
    """The manifest as uploaded: JSON, indented by 2 (as dax_ppdb's
    model_dump_json(indent=2))."""
    return json.dumps(manifest, indent=2)


def manifest_parts(manifest):
    """[(Table, part name, entry), ...] of ``manifest``, in its order, after
    checking its tables, part names and entries."""
    if not isinstance(manifest, dict) or set(manifest) != set(UPLOAD_MANIFEST_FIELDS):
        raise SSOUploadError(f"a manifest has exactly the fields {list(UPLOAD_MANIFEST_FIELDS)}")
    tables = manifest["tables"]
    bad = [t for t in tables if t not in UPLOAD_TABLE_ORDER]
    if bad:
        raise SSOUploadError(f"{bad} are not upload tables {list(UPLOAD_TABLE_ORDER)}")
    if not tables:
        raise SSOUploadError("the manifest has no tables")
    out = []
    for t in (t for t in UPLOAD_TABLE_ORDER if t in tables):
        if not tables[t]:
            raise SSOUploadError(f"{t}: no parts in the manifest")
        for i, (name, e) in enumerate(tables[t].items()):
            if name != part_name(i):
                raise SSOUploadError(f"{t}: part {i} is named {name!r}, not {part_name(i)!r}")
            if not isinstance(e, dict) or set(e) != set(UPLOAD_MANIFEST_ENTRY_FIELDS):
                raise SSOUploadError(f"{t}/{name}: an entry has exactly the fields "
                                     f"{list(UPLOAD_MANIFEST_ENTRY_FIELDS)}")
            if Path(e["source_path"]).name in NEVER_PARTS:
                raise SSOUploadError(f"{Path(e['source_path']).name} (a sidecar or SSObservation's manifest) "
                                     "is never uploaded")
            out.append((t, name, e))
    return out


# --------------------------------------------------------------------------
# The report
# --------------------------------------------------------------------------

def _tmp_name(path):
    """A temporary name next to ``path``, unique to this process and call
    (<name>.<pid>.<random>.tmp), so concurrent runs never write into each
    other's file."""
    path = Path(path)
    return path.with_name(f"{path.name}.{os.getpid()}.{secrets.token_hex(4)}.tmp")


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
# The upload (mirrors dax_ppdb sso.Uploader)
# --------------------------------------------------------------------------

def generate_prefix(now=None):
    """The object prefix: UTC time to the millisecond, e.g.
    20260914T200401001."""
    now = now or _now()
    return now.astimezone(timezone.utc).strftime(UPLOAD_PREFIX_FORMAT)[:-3]


def object_name(object_prefix, table, part):
    """A part's object name: <prefix>/<Table>/part-NNNNN.parquet."""
    return posixpath.join(object_prefix, table, part)


def manifest_object_name(object_prefix):
    """<prefix>/manifest.json."""
    return posixpath.join(object_prefix, UPLOAD_MANIFEST_FILE)


def topic_path(project):
    return f"projects/{project}/topics/{UPLOAD_TOPIC}"


def subscription_path(project):
    return f"projects/{project}/subscriptions/{UPLOAD_RESULT_SUBSCRIPTION}"


def message_body(bucket_name, object_prefix):
    """The Pub/Sub message, as the bytes published: exactly dax_ppdb
    Uploader's, the delivery contract's UPLOAD_MESSAGE_FIELDS."""
    data = dict(zip(UPLOAD_MESSAGE_FIELDS, (bucket_name, object_prefix), strict=True))
    return json.dumps(data).encode("utf-8")


def planned_objects(bucket_name, object_prefix, manifest):
    """[(source path, gs:// URI), ...] of the parts, in upload order, then
    (None, the manifest's URI)."""
    out = [(e["source_path"], f"gs://{bucket_name}/{object_name(object_prefix, t, n)}")
           for t, n, e in manifest_parts(manifest)]
    return out + [(None, f"gs://{bucket_name}/{manifest_object_name(object_prefix)}")]


def _storage_client():
    """A GCS client with application-default credentials (patched in tests)."""
    from google.cloud import storage
    return storage.Client()


def _publisher_client():
    """A Pub/Sub publisher with application-default credentials (patched in
    tests)."""
    from google.cloud import pubsub_v1
    return pubsub_v1.PublisherClient()


def _subscriber_client():
    """A Pub/Sub subscriber with application-default credentials (patched in
    tests)."""
    from google.cloud import pubsub_v1
    return pubsub_v1.SubscriberClient()


def is_chunked(size, parallel):
    """Whether a file of ``size`` bytes goes in parallel chunks with the
    ``parallel`` settings (None: never)."""
    return bool(parallel) and size >= PARALLEL_MIN_BYTES


def upload(cfg, manifest, object_prefix=None, publish_timeout=PUBLISH_TIMEOUT_S, publish=True,
           on_unconfirmed=None, parallel=None):
    """Upload the parts ``manifest`` lists (build_manifest()) from their
    source_path, as <prefix>/<Table>/part-NNNNN.parquet in its order, then
    the manifest as <prefix>/manifest.json, and publish the message (unless
    ``publish`` is false: then the objects are kept and nothing is
    published). Refuses a prefix that holds any object. With ``parallel``
    (parallel_options()), files of at least PARALLEL_MIN_BYTES go in
    parallel chunks (_upload_chunked).

    Returns (object_prefix, message_id); message_id is None without
    ``publish``. On any failure before the message is handed to Pub/Sub
    (publisher.publish() returned), deletes the objects this call wrote,
    the manifest first, and raises SSOUploadError. After that, the message
    may have gone through, so anything that stops the wait for its
    confirmation (a timeout, an error, KeyboardInterrupt, SystemExit, ...)
    keeps the objects, calls ``on_unconfirmed(object_prefix, message)`` if
    given, and raises PublishUnconfirmedError for an Exception, or re-raises
    a BaseException that isn't one.
    """
    try:
        from google.api_core.exceptions import PreconditionFailed
    except ImportError as e:
        raise SSOUploadError("the Google client libraries are missing: pip install 'ssp[upload]'") from e

    bucket_name, project = cfg["bucket_name"], cfg["project"]
    parts = manifest_parts(manifest)
    body = manifest_json(manifest)
    object_prefix = object_prefix or generate_prefix()
    if any(is_chunked(e["size_bytes"], parallel) for _, _, e in parts):
        _require_crc32c_c()
    try:
        bucket = _storage_client().bucket(bucket_name)
    except Exception as e:      # e.g. GoogleAuthError: no application-default credentials
        raise SSOUploadError(f"can't make a GCS client: {e!r}") from e

    # refuse a prefix that holds anything (e.g. an upload started at the
    # same millisecond), before writing to it
    where = f"gs://{bucket_name}/{object_prefix}/"
    try:
        existing = list(bucket.list_blobs(prefix=f"{object_prefix}/", max_results=1))
    except Exception as e:
        raise SSOUploadError(f"can't check whether {where} is empty: {e!r}") from e
    if existing:
        raise SSOUploadError(f"{where} already holds objects; refusing to upload under it")

    written = []        # what this call may have written, for the cleanup
    message_id = None
    handed_over = False
    try:
        for table, part, e in parts:
            path, size = e["source_path"], e["size_bytes"]
            name = object_name(object_prefix, table, part)
            try:
                now_size = Path(path).stat().st_size
            except OSError as exc:
                raise SSOUploadError(f"can't read {path}: {exc!r}") from exc
            if now_size != size:
                raise SSOUploadError(f"{path} is {now_size} bytes now, the manifest says {size}: it changed")
            t0 = time.monotonic()
            if is_chunked(size, parallel):
                # cleans up after itself on failure
                _upload_chunked(bucket, bucket_name, name, path, size, **parallel)
                written.append(name)
            else:
                _upload_one(bucket, bucket_name, name, written, PreconditionFailed, path=path)
            dt = time.monotonic() - t0
            _LOG.info("uploaded %s: %d bytes in %.1f s (%.1f MB/s)", name, size, dt,
                      size / dt / 1e6 if dt > 0 else float("nan"))
        _LOG.info("uploaded %d parts of %d tables to %s", len(parts), len(manifest["tables"]), where)

        # the manifest last: its presence means the upload is complete
        mname = manifest_object_name(object_prefix)
        _upload_one(bucket, bucket_name, mname, written, PreconditionFailed, data=body)
        _LOG.info("uploaded the manifest, gs://%s/%s", bucket_name, mname)

        data = message_body(bucket_name, object_prefix)
        tpath = topic_path(project)
        if not publish:
            _LOG.info("--no-publish: not publishing to %s: %s", tpath, data.decode())
            return object_prefix, None
        try:
            publisher = _publisher_client()
            tpath = publisher.topic_path(project, UPLOAD_TOPIC)
            future = publisher.publish(tpath, data)
        except Exception as e:
            raise SSOUploadError(f"failed to publish to {tpath}: {data.decode()}: {e!r}") from e
        handed_over = True      # from here on the message may go through
        try:
            message_id = future.result(timeout=publish_timeout)
        except BaseException as e:
            if isinstance(e, (concurrent.futures.TimeoutError, TimeoutError)):
                what = f"wasn't confirmed within {publish_timeout} s"
            else:
                what = f"was interrupted or failed before it was confirmed ({e!r})"
            msg = (f"the publish to {tpath} {what}; it may or may not have gone through. "
                   f"The objects are KEPT at {where}. Check whether the "
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
        _LOG.info("published message %s to %s: %s", message_id, tpath, data.decode())
    except BaseException:
        if not handed_over:
            _cleanup(bucket, bucket_name, written)
        raise
    return object_prefix, message_id


def _upload_one(bucket, bucket_name, name, written, precondition_failed, path=None, data=None):
    """Upload a file (``path``) or the manifest (``data``) to ``name`` in one
    stream with if_generation_match=0. ``name`` is added to ``written``
    before the call: a call that fails (or is interrupted) after GCS took
    the object may still have written it. A PreconditionFailed means the
    object is someone else's: it is taken off ``written`` again."""
    uri = f"gs://{bucket_name}/{name}"
    written.append(name)
    try:
        blob = bucket.blob(name)
        if path is not None:
            _LOG.info("uploading %s to %s", path, uri)
            blob.upload_from_filename(str(path), if_generation_match=0)
        else:
            blob.upload_from_string(data, content_type="application/json", if_generation_match=0)
    except precondition_failed as e:
        written.remove(name)
        raise SSOUploadError(f"{uri} already exists; a naming collision for this upload prefix") from e
    except Exception as e:      # GoogleAPIError, GoogleAuthError, OSError, ...
        raise SSOUploadError(f"failed to upload {path or 'the manifest'} to {uri}: {e!r}") from e


def _cleanup(bucket, bucket_name, names):
    """Delete what a failed upload wrote, the manifest first (so that a
    cleanup that stops half-way never leaves a complete-looking upload);
    an object that isn't there is fine. Never raises."""
    try:
        from google.api_core.exceptions import NotFound
    except ImportError:         # pragma: no cover - upload() needs it already
        NotFound = ()
    order = ([n for n in names if posixpath.basename(n) == UPLOAD_MANIFEST_FILE]
             + [n for n in names if posixpath.basename(n) != UPLOAD_MANIFEST_FILE])
    for name in order:
        try:
            bucket.blob(name).delete()
            _LOG.warning("deleted gs://%s/%s during cleanup", bucket_name, name)
        except NotFound:
            _LOG.info("gs://%s/%s isn't there (never written); nothing to delete", bucket_name, name)
        except Exception:
            _LOG.exception("failed to delete gs://%s/%s during cleanup; remove it by hand",
                           bucket_name, name)


# --------------------------------------------------------------------------
# Waiting for the load (mirrors dax_ppdb Uploader.wait_for_load)
# --------------------------------------------------------------------------

def _parse_result(data):
    """A load result ({object_prefix, status, job_id | error}) from a
    message's bytes, or None (logged) if it is malformed."""
    try:
        r = json.loads(data)
    except (ValueError, TypeError) as e:
        _LOG.warning("skipping a malformed load result (%s): %r", e, data[:300])
        return None
    if (not isinstance(r, dict) or not isinstance(r.get("object_prefix"), str)
            or r.get("status") not in ("succeeded", "failed")):
        _LOG.warning("skipping a malformed load result: %r", data[:300])
        return None
    return r


def wait_for_load(project, object_prefix, timeout_s=WAIT_TIMEOUT_S, pull_timeout_s=PULL_TIMEOUT_S):
    """Wait for the result of the load of ``object_prefix`` on
    UPLOAD_RESULT_SUBSCRIPTION, up to ``timeout_s``. Every message pulled is
    acknowledged; other prefixes' results and malformed messages are logged
    and skipped. Returns the load record: {"status": "succeeded", "job_id"}
    or {"status": "failed", "error"} or {"status": "timeout"} (with an
    "error" if the results couldn't be read), and "waited_s". Raises only
    for a BaseException that isn't an Exception (e.g. Ctrl-C)."""
    sub = subscription_path(project)
    t0 = _monotonic()
    deadline = t0 + timeout_s

    def done(status, **kw):
        return {"status": status, **kw, "waited_s": round(_monotonic() - t0, 1)}

    try:
        from google.api_core.exceptions import DeadlineExceeded
        subscriber = _subscriber_client()
    except Exception as e:
        _LOG.error("can't read the load results from %s: %r", sub, e)
        return done("timeout", error=f"can't read the load results from {sub}: {e!r}")
    _LOG.info("waiting up to %s s for the load of %s on %s", timeout_s, object_prefix, sub)
    with subscriber:
        while (remaining := deadline - _monotonic()) > 0:
            t_pull = _monotonic()
            try:
                response = subscriber.pull(subscription=sub, max_messages=PULL_MAX_MESSAGES,
                                           timeout=min(remaining, pull_timeout_s))
            except DeadlineExceeded:
                continue            # nothing during this pull
            except Exception as e:
                _LOG.error("can't read the load results from %s: %r", sub, e)
                return done("timeout", error=f"can't read the load results from {sub}: {e!r}")
            received = list(response.received_messages)
            if received:
                try:
                    subscriber.acknowledge(subscription=sub, ack_ids=[r.ack_id for r in received])
                except Exception:
                    _LOG.exception("failed to acknowledge %d load results on %s", len(received), sub)
            elif _monotonic() - t_pull < 1:
                _sleep(min(1.0, max(0.0, deadline - _monotonic())))     # never spin
            for r in received:
                result = _parse_result(r.message.data)
                if result is None:
                    continue
                if result["object_prefix"] != object_prefix:
                    _LOG.info("skipping the load result of another upload, %s (%s)",
                              result["object_prefix"], result["status"])
                    continue
                if result["status"] == "succeeded":
                    if not result.get("job_id"):
                        _LOG.warning("the load of %s succeeded, but its result gives no job_id: %r",
                                     object_prefix, result)
                    return done("succeeded", job_id=result.get("job_id"))
                return done("failed", error=result.get("error"))
    return done("timeout")


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

def _not_waited():
    return {"status": "not_waited", "waited_s": 0}


def run(config, run_dir, tables=None, dry_run=False, force=False, allow_partial=False,
        include_unsupported=False, no_publish=False, out=None, parallel=None,
        chunk_mib=PARALLEL_CHUNK_MIB, workers=PARALLEL_WORKERS, wait=True, wait_timeout=WAIT_TIMEOUT_S):
    """The whole of stage 3. Returns the upload record. ``parallel``: True or
    False, or None for the config's ``parallel`` (default True). ``wait``:
    wait up to ``wait_timeout`` s for the load's result (published uploads
    only)."""
    out = out or sys.stdout
    if dry_run and no_publish:
        raise SSOUploadError("--no-publish uploads and --dry-run doesn't: give one or the other")
    if wait:
        check_wait_timeout(wait_timeout)
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
    files = verify_delivery(run_dir, report, tables)
    manifest = build_manifest(files)
    parts = manifest_parts(manifest)

    record = {
        "config": str(cfg_path),
        "bucket": cfg["bucket_name"],
        "object_prefix": None,
        "tables": list(manifest["tables"]),
        "message_id": None,
        "dry_run": dry_run,
        "published": False,
        "utc": None,
    }
    chunked = [(t, n) for t, n, e in parts if is_chunked(e["size_bytes"], par)]
    # which objects go in chunks (they get a CRC32C but no MD5); null with
    # --no-parallel
    record["parallel"] = (None if par is None else
                          {**par, "min_bytes": PARALLEL_MIN_BYTES,
                           "tables": [t for t in manifest["tables"] if any(c[0] == t for c in chunked)],
                           "parts": [f"{t}/{n}" for t, n in chunked]})
    record["manifest"] = manifest
    record["load"] = None
    tpath, spath = topic_path(cfg["project"]), subscription_path(cfg["project"])
    if dry_run:
        record["object_prefix"] = generate_prefix()
        record["load"] = _not_waited()
        print(f"dry run: config {cfg_path}; nothing is uploaded or published", file=out)
        if par is None:
            print("  parallel: off (--no-parallel or parallel: false); every file in one stream", file=out)
        else:
            print(f"  parallel: files of at least {PARALLEL_MIN_BYTES} bytes in chunks of "
                  f"{par['chunk_bytes'] >> 20} MiB, {par['workers']} threads: "
                  f"{record['parallel']['parts'] or 'none'}", file=out)
        print(f"  refuse gs://{cfg['bucket_name']}/{record['object_prefix']}/ if it holds any object",
              file=out)
        planned = planned_objects(cfg["bucket_name"], record["object_prefix"], manifest)
        for (src, uri), (t, n, e) in zip(planned, parts + [(None, None, None)], strict=True):
            if src is None:
                print(f"  then the manifest -> {uri}  (last; if_generation_match=0)", file=out)
                continue
            how = (f"in chunks of {par['chunk_bytes'] >> 20} MiB, {par['workers']} threads; checked not "
                   "to exist, CRC32C verified" if (t, n) in chunked else "if_generation_match=0")
            print(f"  {src} -> {uri}  ({e['row_count']} rows, {e['size_bytes']} bytes; {how})", file=out)
        print("  the manifest:", file=out)
        for line in manifest_json(manifest).splitlines():
            print(f"    {line}", file=out)
        print(f"  publish to {tpath}:", file=out)
        print(f"    {message_body(cfg['bucket_name'], record['object_prefix']).decode()}", file=out)
        then = (f"wait up to {wait_timeout} s for the load result on {spath}" if wait
                else "not wait (--no-wait)")
        print(f"  then {then}", file=out)
        record["utc"] = _now().isoformat(timespec="seconds")
        record_upload(run_dir, report, record)
        return record

    def unconfirmed(object_prefix, msg):
        # the publish may have gone through: record it (published null),
        # whatever stopped the wait (upload() then raises)
        record.update(object_prefix=object_prefix, published=None, load=_not_waited(),
                      utc=_now().isoformat(timespec="seconds"), error=msg)
        record_upload(run_dir, report, record)

    record["object_prefix"], record["message_id"] = upload(
        cfg, manifest, publish=not no_publish, on_unconfirmed=unconfirmed, parallel=par)
    where = f"gs://{cfg['bucket_name']}/{record['object_prefix']}"
    record["utc"] = _now().isoformat(timespec="seconds")
    if no_publish:
        record["load"] = _not_waited()
        print(f"uploaded {len(parts)} parts of {len(manifest['tables'])} tables and the manifest to {where}/ "
              "and published NOTHING (--no-publish); the objects are KEPT: delete them by hand when done",
              file=out)
        print(f"  the message that would have gone to {tpath}:", file=out)
        print(f"    {message_body(cfg['bucket_name'], record['object_prefix']).decode()}", file=out)
        record_upload(run_dir, report, record)
        return record

    record["published"] = True
    print(f"uploaded {len(parts)} parts of {len(manifest['tables'])} tables and the manifest to {where}/; "
          f"message {record['message_id']}", file=out)
    if not wait:
        record["load"] = _not_waited()
        print(f"  not waiting for the load (--no-wait); its result goes to {spath}", file=out)
        record_upload(run_dir, report, record)
        return record

    # recorded before the wait (load null), so that a run killed during it
    # still counts as an upload; updated after it
    record_upload(run_dir, report, record)
    try:
        record["load"] = wait_for_load(cfg["project"], record["object_prefix"], wait_timeout)
    except BaseException as e:
        record["load"] = {"status": "timeout", "error": f"the wait was interrupted ({e!r})", "waited_s": None}
        write_report(run_dir, report)
        raise
    write_report(run_dir, report)
    load = record["load"]
    if load["status"] == "succeeded":
        print(f"loaded: job {load['job_id']} ({load['waited_s']} s)", file=out)
    elif load["status"] == "failed":
        print(f"the load of {where}/ FAILED: {load['error']}", file=out)
    else:
        print(f"no load result for {where}/ within {wait_timeout} s"
              + (f" ({load['error']})" if load.get("error") else "")
              + ": it may still load. Check the load-sso function's logs, or the tables, before uploading "
              "again", file=out)
    return record


def exit_status(record):
    """The exit status for an upload record: EXIT_LOAD_FAILED for a failed
    load, EXIT_LOAD_TIMEOUT for no result within the wait, else 0."""
    status = ((record or {}).get("load") or {}).get("status")
    assert status is None or status in UPLOAD_LOAD_STATUSES, status
    return {"failed": UPLOAD_EXIT_LOAD_FAILED, "timeout": UPLOAD_EXIT_LOAD_TIMEOUT}.get(status, 0)


def main(argv=None):
    unsupported = (f" {', '.join(UNSUPPORTED_TABLES)} is {UNSUPPORTED_NOTE}." if UNSUPPORTED_TABLES
                   else "")
    parser = argparse.ArgumentParser(
        prog="ssp-upload-sso",
        description="Upload a checked SSO delivery to the PPDB's SSO ingestion bucket as "
                    "gs://<bucket>/<prefix>/<Table>/part-NNNNN.parquet "
                    f"(SSObservation one part per delivery part), then <prefix>/{UPLOAD_MANIFEST_FILE} "
                    "(dax_ppdb's manifest, last), announce it on Pub/Sub "
                    f"({UPLOAD_TOPIC}), and wait for the load's result ({UPLOAD_RESULT_SUBSCRIPTION}). "
                    "No sidecar or SSObservation manifest is uploaded (DM-55678, DM-56960).",
        epilog=f"Exit status: 0 loaded (or no wait asked for); 1 refused or failed; "
               f"{UPLOAD_EXIT_LOAD_FAILED} the load failed; {UPLOAD_EXIT_LOAD_TIMEOUT} no result within the "
               "wait (published; it may still load: check before uploading again). "
               f"Named configs: {_named_configs()} in {CONFIG_DIR}. "
               "Credentials: Google application-default credentials "
               "(GOOGLE_APPLICATION_CREDENTIALS, e.g. the sso-uploader service account's key, or "
               f"gcloud auth application-default login).{unsupported} Upload at most once per load "
               "window: the loader runs one load at a time.",
    )
    parser.add_argument("config", metavar="CONFIG",
                        help="upload config: a name (dev, int, prod), or a path (*.yaml, *.yml, or with a /)")
    parser.add_argument("run_dir", metavar="RUN_DIR",
                        help="the ssp-build-sso run directory (with report.json)")
    parser.add_argument("--dry-run", action="store_true",
                        help="validate, build the manifest, print the planned objects, the manifest and the "
                             "message, and touch nothing remote")
    parser.add_argument("--no-publish", action="store_true",
                        help="upload the parts and the manifest but publish nothing (no load starts) and "
                             "don't wait; the objects are kept, to be deleted by hand. For tests; not with "
                             "--dry-run")
    parser.add_argument("--no-wait", dest="wait", action="store_false",
                        help="don't wait for the load's result after publishing (exit 0 once published)")
    parser.add_argument("--wait-timeout", type=float, default=WAIT_TIMEOUT_S, metavar="SECONDS",
                        help=f"how long to wait for the load's result (default {WAIT_TIMEOUT_S}); without "
                             f"one, exit {UPLOAD_EXIT_LOAD_TIMEOUT}")
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
                        help=f"upload files of at least {PARALLEL_MIN_BYTES >> 20} MiB in parallel chunks "
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
        record = run(args.config, args.run_dir, tables=args.tables, dry_run=args.dry_run, force=args.force,
                     allow_partial=args.allow_partial, include_unsupported=args.include_unsupported,
                     no_publish=args.no_publish, parallel=args.parallel, chunk_mib=args.chunk_mib,
                     workers=args.parallel_workers, wait=args.wait, wait_timeout=args.wait_timeout)
    except SSOUploadError as e:
        _LOG.error("%s", e)
        return 1
    rc = exit_status(record)
    if rc == UPLOAD_EXIT_LOAD_FAILED:
        _LOG.error("the load of gs://%s/%s failed: %s", record["bucket"], record["object_prefix"],
                   record["load"].get("error"))
    elif rc == UPLOAD_EXIT_LOAD_TIMEOUT:
        _LOG.error("no load result for gs://%s/%s within the wait; it may still load: check before "
                   "uploading again", record["bucket"], record["object_prefix"])
    return rc


if __name__ == "__main__":
    sys.exit(main())
