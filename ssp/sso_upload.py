"""Stage 3 of the SSO delivery: upload a checked delivery to the PPDB's SSO
ingestion (DM-55678).

    ssp-upload-sso CONFIG RUN_DIR [--dry-run] [--tables T ...] [--force]

It follows the contract in ssp/delivery_contract.py, which mirrors
lsst/dax_ppdb python/lsst/dax/ppdb/bigquery/sso_uploader.py:

1. Refuse unless RUN_DIR/report.json says ``deliverable: true`` and every
   table to upload, RUN_DIR/delivery/<Table>.parquet, has the md5 (and
   size) the report recorded.
2. Upload each table to gs://<bucket>/<prefix>/<Table>.parquet, where the
   prefix is the UTC time to the millisecond, with if_generation_match=0
   so nothing is ever overwritten. On any failure, including a failure to
   publish, delete what this upload wrote and fail.
3. Publish one JSON message, {"bucket", "object_prefix",
   "uploaded_tables"}, to projects/<project>/topics/<topic>, and wait for
   its message id.
4. Record ``upload`` in report.json.

CONFIG is a YAML file, or the name of one shipped in config/sso-upload/
(dev, int, prod). Credentials are Google application-default credentials.
The Google client libraries are the ``upload`` extra
(``pip install 'ssp[upload]'``); a dry run doesn't need them.
"""

import argparse
import hashlib
import json
import logging
import os
import posixpath
import sys
from datetime import UTC, datetime
from pathlib import Path

import yaml

from .delivery_contract import (
    DELIVERY_DIR,
    DELIVERY_TABLES,
    REPORT_FILE,
    UPLOAD_MESSAGE_FIELDS,
    UPLOAD_PREFIX_FORMAT,
)

_LOG = logging.getLogger("ssp.sso_upload")

#: Where the named configs live (the repository's config/sso-upload/).
CONFIG_DIR = Path(__file__).resolve().parent.parent / "config" / "sso-upload"

CONFIG_KEYS_REQUIRED = ("bucket_name", "topic", "project")
CONFIG_KEYS_OPTIONAL = ("tables",)

#: Tables the DM-55678 side (dax_ppdb SSO_TABLES, load_sso) doesn't accept yet.
NOT_YET_ACCEPTED_DOWNSTREAM = ("NearbySSO",)


class SSOUploadError(RuntimeError):
    """The delivery can't be, or wasn't, uploaded."""


# --------------------------------------------------------------------------
# Config, report and file checks
# --------------------------------------------------------------------------

def resolve_config_path(spec):
    """A path to a YAML file, or a name (``dev``) in CONFIG_DIR."""
    p = Path(spec)
    if p.is_file():
        return p
    named = CONFIG_DIR / f"{spec}.yaml"
    if os.sep not in str(spec) and named.is_file():
        return named
    raise SSOUploadError(f"no config {spec!r}: not a file, nor one of "
                         f"{sorted(q.stem for q in CONFIG_DIR.glob('*.yaml'))} in {CONFIG_DIR}")


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
    return path, cfg


def check_tables(tables, what="tables"):
    if not tables or not isinstance(tables, (list, tuple)):
        raise SSOUploadError(f"{what}: expected a non-empty list")
    bad = [t for t in tables if t not in DELIVERY_TABLES]
    if bad:
        raise SSOUploadError(f"{what}: {bad} are not delivery tables {list(DELIVERY_TABLES)}")
    if len(set(tables)) != len(tables):
        raise SSOUploadError(f"{what}: duplicates in {list(tables)}")


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


def read_report(run_dir):
    path = Path(run_dir) / REPORT_FILE
    if not path.is_file():
        raise SSOUploadError(f"no {path}; build the delivery with ssp-build-sso first")
    with open(path) as f:
        return json.load(f)


def verify_delivery(run_dir, report, tables):
    """Refuse unless the report is deliverable and each table's file matches
    it. Returns {Table: Path}, in the order of ``tables``."""
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
        file_map[t] = path
    if problems:
        raise SSOUploadError("the delivery doesn't match its report; refusing to upload:\n  "
                             + "\n  ".join(problems))
    return file_map


def write_report(run_dir, report):
    """Rewrite report.json atomically."""
    path = Path(run_dir) / REPORT_FILE
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w") as f:
        json.dump(report, f, indent=2)
        f.write("\n")
    os.replace(tmp, path)


# --------------------------------------------------------------------------
# The upload (mirrors dax_ppdb SSOUploader)
# --------------------------------------------------------------------------

def generate_prefix(now=None):
    """The object prefix: UTC time to the millisecond, e.g.
    20260914T200401001."""
    now = now or datetime.now(UTC)
    return now.strftime(UPLOAD_PREFIX_FORMAT)[:-3]


def message_body(bucket_name, object_prefix, tables):
    """The Pub/Sub message, as the bytes published."""
    data = dict(zip(UPLOAD_MESSAGE_FIELDS, (bucket_name, object_prefix, list(tables))))
    return json.dumps(data).encode("utf-8")


def planned_objects(bucket_name, object_prefix, file_map):
    return {t: f"gs://{bucket_name}/{posixpath.join(object_prefix, f'{t}.parquet')}" for t in file_map}


def _storage_client():
    """A GCS client with application-default credentials (patched in tests)."""
    from google.cloud import storage
    return storage.Client()


def _publisher_client():
    """A Pub/Sub publisher with application-default credentials (patched in
    tests)."""
    from google.cloud import pubsub_v1
    return pubsub_v1.PublisherClient()


def upload(cfg, file_map, object_prefix=None):
    """Upload ``file_map`` ({Table: Path}) and publish the message.

    Returns (object_prefix, message_id). On any failure, deletes the objects
    this call uploaded and raises SSOUploadError (or the original error).
    """
    try:
        from google.api_core.exceptions import GoogleAPIError, PreconditionFailed
    except ImportError as e:
        raise SSOUploadError("the Google client libraries are missing: pip install 'ssp[upload]'") from e

    bucket_name, topic, project = cfg["bucket_name"], cfg["topic"], cfg["project"]
    object_prefix = object_prefix or generate_prefix()
    try:
        bucket = _storage_client().bucket(bucket_name)
    except Exception as e:      # e.g. no application-default credentials
        raise SSOUploadError(f"can't make a GCS client: {e}") from e

    uploaded = []
    try:
        for table, path in file_map.items():
            name = posixpath.join(object_prefix, f"{table}.parquet")
            blob = bucket.blob(name)
            try:
                _LOG.info("uploading %s to gs://%s/%s", path, bucket_name, name)
                blob.upload_from_filename(str(path), if_generation_match=0)
            except PreconditionFailed as e:
                raise SSOUploadError(f"gs://{bucket_name}/{name} already exists; "
                                     "a naming collision for this upload prefix") from e
            except (GoogleAPIError, OSError) as e:
                raise SSOUploadError(f"failed to upload {path} to gs://{bucket_name}/{name}: {e}") from e
            uploaded.append(name)
        _LOG.info("uploaded %d tables to gs://%s/%s", len(uploaded), bucket_name, object_prefix)

        data = message_body(bucket_name, object_prefix, file_map)
        try:
            publisher = _publisher_client()
            topic_path = publisher.topic_path(project, topic)
            message_id = publisher.publish(topic_path, data).result()
        except Exception as e:
            raise SSOUploadError(f"failed to publish to projects/{project}/topics/{topic}: "
                                 f"{data.decode()}: {e}") from e
        _LOG.info("published message %s to %s: %s", message_id, topic_path, data.decode())
    except BaseException:
        _cleanup(bucket, bucket_name, uploaded)
        raise
    return object_prefix, message_id


def _cleanup(bucket, bucket_name, names):
    from google.api_core.exceptions import GoogleAPIError
    for name in names:
        try:
            bucket.blob(name).delete()
            _LOG.warning("deleted gs://%s/%s during cleanup", bucket_name, name)
        except GoogleAPIError:
            _LOG.exception("failed to delete gs://%s/%s during cleanup; remove it by hand",
                           bucket_name, name)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def run(config, run_dir, tables=None, dry_run=False, force=False, out=None):
    """The whole of stage 3. Returns the ``upload`` record."""
    out = out or sys.stdout
    cfg_path, cfg = load_config(config)
    if tables is not None:
        check_tables(tables, "--tables")
    tables = list(tables or cfg.get("tables") or DELIVERY_TABLES)
    tables = [t for t in DELIVERY_TABLES if t in tables]       # the contract's order

    run_dir = resolve_run_dir(run_dir)
    report = read_report(run_dir)
    prev = report.get("upload")
    if prev and not prev.get("dry_run") and not force and not dry_run:
        raise SSOUploadError(
            f"{run_dir / REPORT_FILE} records an upload already (gs://{prev.get('bucket')}/"
            f"{prev.get('object_prefix')}, message {prev.get('message_id')}); --force to upload again")
    file_map = verify_delivery(run_dir, report, tables)

    late = [t for t in tables if t in NOT_YET_ACCEPTED_DOWNSTREAM]
    if late:
        _LOG.warning("%s: uploaded per the owner's decision, but the DM-55678 side (dax_ppdb "
                     "SSO_TABLES, load_sso) doesn't accept it yet", ", ".join(late))

    if dry_run:
        object_prefix, message_id = generate_prefix(), None
        print(f"dry run: config {cfg_path}; nothing is uploaded or published", file=out)
        for t, uri in planned_objects(cfg["bucket_name"], object_prefix, file_map).items():
            print(f"  {file_map[t]} -> {uri}  (if_generation_match=0)", file=out)
        print(f"  publish to projects/{cfg['project']}/topics/{cfg['topic']}:", file=out)
        print(f"    {message_body(cfg['bucket_name'], object_prefix, file_map).decode()}", file=out)
    else:
        object_prefix, message_id = upload(cfg, file_map)
        print(f"uploaded {len(file_map)} tables to gs://{cfg['bucket_name']}/{object_prefix}; "
              f"message {message_id}", file=out)

    record = {
        "config": str(cfg_path),
        "bucket": cfg["bucket_name"],
        "object_prefix": object_prefix,
        "tables": list(file_map),
        "message_id": message_id,
        "dry_run": dry_run,
        "utc": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if dry_run and prev and not prev.get("dry_run"):
        _LOG.info("not recording the dry run: report.json records a real upload")
    else:
        report["upload"] = record
        write_report(run_dir, report)
    return record


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="ssp-upload-sso",
        description="Upload a checked SSO delivery (RUN_DIR/delivery/<Table>.parquet) to the PPDB's "
                    "SSO ingestion bucket and announce it on Pub/Sub (DM-55678).",
        epilog=f"Named configs: {sorted(q.stem for q in CONFIG_DIR.glob('*.yaml'))} in {CONFIG_DIR}. "
               "Credentials: Google application-default credentials "
               "(GOOGLE_APPLICATION_CREDENTIALS or gcloud auth application-default login).",
    )
    parser.add_argument("config", metavar="CONFIG",
                        help="upload config: a YAML path, or a name (dev, int, prod)")
    parser.add_argument("run_dir", metavar="RUN_DIR",
                        help="the ssp-build-sso run directory (with report.json)")
    parser.add_argument("--dry-run", action="store_true",
                        help="validate, print the planned objects and the message, and touch nothing remote")
    parser.add_argument("--tables", nargs="+", metavar="TABLE",
                        help=f"tables to upload (default: the config's, else {' '.join(DELIVERY_TABLES)})")
    parser.add_argument("--force", action="store_true",
                        help="upload even though report.json records an earlier upload")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        run(args.config, args.run_dir, tables=args.tables, dry_run=args.dry_run, force=args.force)
    except SSOUploadError as e:
        _LOG.error("%s", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
