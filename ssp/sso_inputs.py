"""Stage 1 of the SSO delivery: extract the inputs.

    ssp-extract-sso-inputs INPUTS_DIR

Writes every ``ssp.delivery_contract.INPUT_FILES`` file into INPUTS_DIR,
then ``manifest.json`` last (see docs/design/sso-delivery.md, "Stage 1,
extract"). The steps:

``mpc``
    ``obs_sbn`` (the X05 rows), ``mpc_orbits``, ``current_identifications``
    and ``numbered_identifications`` from the USDF MPC replica, in one
    REPEATABLE READ transaction (``fast-export``'s batch mode). The
    transaction's start is the manifest's ``mpc_snapshot_utc``.
``dia_sources``
    ``extract-submitted-sources`` on that ``obs_sbn`` (ClickHouse
    ``ssp.SubmittableSources``, at most 8 concurrent queries). It also
    leaves ``dia_sources.unresolved.parquet``, which is not an input.
``ppdb_dia_sources``
    The five ``ppdb.DiaSource`` columns NearbySSO reads, one read-only
    query.

Every file is written under ``INPUTS_DIR/.partial/`` and moved into place
when its step succeeds. The manifest is written only if every step
succeeded; with ``--force`` an existing manifest is removed before anything
else, so a failed rerun never leaves a manifest describing other files.

``--reuse NAME=PATH`` takes an existing file instead of extracting it (it
is hard-linked, or copied across filesystems), and the manifest records it
as reused. The four MPC files are one snapshot: reuse all four or none.
``--only``/``--skip`` redo parts of an earlier run in the same INPUTS_DIR:
a step not run keeps its files, and its manifest entries are carried over
from the previous manifest after checking the files still match it.

Credentials: the MPC password from ``~/.pgpass`` (libpq), ClickHouse from
``SSP_CH_USER``/``SSP_CH_PASSWORD`` or ``~/.chpass``
(``ssp.export.submittable.credentials``).
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq

from ssp.delivery_contract import (
    INPUT_FILES,
    MANIFEST_FIELDS,
    MANIFEST_FILE,
    MPC_SNAPSHOT,
    REQUIRED_INPUT_COLUMNS,
)

PROG = "ssp-extract-sso-inputs"

MPC_HOST = "mpcorb-db.slac.stanford.edu"
MPC_PORT = 5432
MPC_DBNAME = "mpc_sbn"
MPC_USER = "rubin"

CH_HOST = "sdfiana035.sdf.slac.stanford.edu"
CH_PORT = 8123
CH_DATABASE = "ssp"     # the database the ~/.chpass line is for

#: name -> SQL, for the MPC snapshot (in this order, in one transaction).
MPC_SQL = {
    "obs_sbn": "SELECT * FROM obs_sbn WHERE stn='X05'",
    "mpc_orbits": "SELECT * FROM mpc_orbits",
    "current_identifications": "SELECT * FROM current_identifications",
    "numbered_identifications": "SELECT * FROM numbered_identifications",
}
assert tuple(MPC_SQL) == MPC_SNAPSHOT

PPDB_SQL = ("SELECT diaSourceId, visit, midpointMjdTai, ra, dec FROM ppdb.DiaSource "
            "ORDER BY visit, diaSourceId")
PPDB_SETTINGS = {"readonly": 1, "output_format_parquet_compression_method": "zstd"}

#: step -> the INPUT_FILES names it produces, in run order.
STEPS = {
    "mpc": MPC_SNAPSHOT,
    "dia_sources": ("dia_sources",),
    "ppdb_dia_sources": ("ppdb_dia_sources",),
}
assert sorted(n for ns in STEPS.values() for n in ns) == sorted(INPUT_FILES)

PARTIAL = ".partial"


class ExtractError(RuntimeError):
    """A step failed, or the inputs are inconsistent; no manifest is
    written."""


def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)


def iso(t):
    """ISO 8601 UTC, to the second, with a Z."""
    return t.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def md5sum(path, bufsize=1 << 24):
    h = hashlib.md5()
    with open(path, "rb") as f:
        while chunk := f.read(bufsize):
            h.update(chunk)
    return h.hexdigest()


def commit():
    """The ssp-tools git commit this code runs from (``-dirty`` if the tree
    has local changes to tracked files), or ``unknown``."""
    root = Path(__file__).resolve().parent.parent
    try:
        sha = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True,
                             text=True, check=True).stdout.strip()
        dirty = subprocess.run(["git", "-C", str(root), "status", "--porcelain", "--untracked-files=no"],
                               capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return sha + ("-dirty" if dirty else "")


def producer():
    try:
        from importlib.metadata import version
        v = version("ssp")
    except Exception:
        v = "unknown"
    return f"{PROG} {v} ({commit()})"


def check_columns(name, path):
    """Fail unless ``path`` has every REQUIRED_INPUT_COLUMNS[name] column."""
    have = set(pq.read_schema(path).names)
    missing = [c for c in REQUIRED_INPUT_COLUMNS[name] if c not in have]
    if missing:
        raise ExtractError(f"{name}: {path} lacks required columns {missing}")


def describe(name, path, source, extracted_utc):
    """The manifest entry for one input file."""
    check_columns(name, path)
    return {
        "file": INPUT_FILES[name][0],
        "rows": pq.ParquetFile(path).metadata.num_rows,
        "md5": md5sum(path),
        "source": source,
        "extracted_utc": extracted_utc,
    }


#
# The sources. Each writes its files into ``tmp`` (INPUTS_DIR/.partial);
# tests replace these functions.
#


def export_mpc(tmp, args):
    """The MPC snapshot into ``tmp``; returns the transaction's start time
    (an aware datetime)."""
    from ssp.export.postgres import export_in_transaction

    dsn = (f"host={args.mpc_host} port={args.mpc_port} dbname={args.mpc_dbname} user={args.mpc_user} "
           "options='-c extra_float_digits=3'")
    exports = [{"sql": sql, "out": str(tmp / INPUT_FILES[name][0])} for name, sql in MPC_SQL.items()]
    return export_in_transaction(dsn, exports, log=lambda m: print(m, flush=True))


def extract_dia_sources(obs_path, out_path, args):
    """``extract-submitted-sources`` on ``obs_path``, as a library call."""
    from ssp.export import submittable as S

    def fetch(tasks):
        return S.run_queries(tasks, args.ch_host, args.ch_port, S.DEFAULT_DATABASE, args.ch_user,
                             args.workers)

    rc = S.extract(obs_path, out_path, fetch)
    if rc != 0:
        raise ExtractError(f"extract-submitted-sources returned {rc}")


def export_ppdb(out_path, args):
    """The five ``ppdb.DiaSource`` columns, streamed to ``out_path``."""
    import clickhouse_connect

    from ssp.export.submittable import credentials

    user, password = credentials(args.ch_host, args.ch_port, CH_DATABASE, args.ch_user)
    client = clickhouse_connect.get_client(
        host=args.ch_host, port=args.ch_port, username=user, password=password,
        settings={"max_execution_time": 3600, "cancel_http_readonly_queries_on_client_close": 1},
    )
    try:
        with open(out_path, "wb") as f, \
                client.raw_stream(PPDB_SQL, settings=PPDB_SETTINGS, fmt="Parquet") as stream:
            shutil.copyfileobj(stream, f, 1 << 24)
    finally:
        client.close()


def mpc_source(args):
    return f"MPC replica {args.mpc_user}@{args.mpc_host}:{args.mpc_port}/{args.mpc_dbname}"


def ch_source(args):
    return f"ClickHouse {args.ch_host}:{args.ch_port}"


#
# The run
#


def place(tmp_path, final_path):
    os.replace(tmp_path, final_path)


def take(src, dst):
    """Hard-link ``src`` to ``dst``, or copy it across filesystems."""
    if dst.exists() or dst.is_symlink():
        dst.unlink()
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst)


def reused_entry(name, src, dst):
    """The manifest entry for a reused file: its extraction time from a
    manifest beside it that describes the same file, else its mtime."""
    md5 = md5sum(dst)
    when, origin = None, None
    side = Path(src).resolve().parent / MANIFEST_FILE
    if side.exists():
        try:
            e = json.loads(side.read_text())["files"][name]
            if e["md5"] == md5:
                when, origin = e["extracted_utc"], e["source"]
        except (KeyError, TypeError, ValueError):
            pass
    if when is None:
        when = iso(datetime.datetime.fromtimestamp(Path(src).stat().st_mtime, datetime.timezone.utc))
    source = f"reused {Path(src).resolve()}" + (f" (originally: {origin})" if origin else "")
    entry = describe(name, dst, source, when)
    assert entry["md5"] == md5
    return entry


def reused_snapshot(reuse):
    """``mpc_snapshot_utc`` of reused MPC files, from a manifest beside
    them, or None."""
    times = set()
    for name in MPC_SNAPSHOT:
        side = Path(reuse[name]).resolve().parent / MANIFEST_FILE
        try:
            times.add(json.loads(side.read_text())["mpc_snapshot_utc"])
        except (OSError, KeyError, ValueError):
            return None
    return times.pop() if len(times) == 1 else None


def plan(args):
    """The steps to run, from --only/--skip."""
    steps = list(STEPS)
    if args.only:
        steps = [s for s in steps if s in args.only]
    if args.skip:
        steps = [s for s in steps if s not in args.skip]
    return steps


def parse_reuse(items):
    reuse = {}
    for item in items or []:
        name, sep, path = item.partition("=")
        if not sep or name not in INPUT_FILES:
            raise ExtractError(f"--reuse {item!r}: expected NAME=PATH with NAME one of {list(INPUT_FILES)}")
        if not Path(path).is_file():
            raise ExtractError(f"--reuse {name}: no such file {path}")
        reuse[name] = path
    some = [n for n in MPC_SNAPSHOT if n in reuse]
    if some and len(some) != len(MPC_SNAPSHOT):
        raise ExtractError(f"--reuse: the MPC files {list(MPC_SNAPSHOT)} are one snapshot; reuse all "
                           f"four or none (got {some})")
    return reuse


def run(args):
    """Extract into ``args.inputs_dir``; returns the manifest written."""
    out = Path(args.inputs_dir)
    out.mkdir(parents=True, exist_ok=True)
    manifest_path = out / MANIFEST_FILE
    reuse = parse_reuse(args.reuse)
    steps = plan(args)

    previous = None
    if manifest_path.exists():
        if not args.force:
            raise ExtractError(f"{manifest_path} exists: these inputs are complete. Use a new "
                               "INPUTS_DIR, or --force to redo them (with --only/--skip for parts)")
        previous = json.loads(manifest_path.read_text())
        # From here on the directory is not a valid set of inputs until a
        # new manifest is written.
        manifest_path.unlink()

    tmp = out / PARTIAL
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir()

    files, snapshot, timings = {}, None, {}

    # Reused files first: the extraction may depend on them (obs_sbn).
    for name, src in reuse.items():
        dst = out / INPUT_FILES[name][0]
        take(src, dst)
        files[name] = reused_entry(name, src, dst)
        print(f"{name}: reused {src}", flush=True)
    if "obs_sbn" in reuse:
        snapshot = args.mpc_snapshot_utc or reused_snapshot(reuse)
        if snapshot is None:
            raise ExtractError("--reuse of the MPC files: give --mpc-snapshot-utc (no manifest beside "
                               "them gives it)")

    for step, names in STEPS.items():
        todo = [n for n in names if n not in reuse]
        if not todo:
            continue
        if step not in steps:
            # Not run: carry the previous entries over, if the files match.
            for name in todo:
                path = out / INPUT_FILES[name][0]
                e = (previous or {}).get("files", {}).get(name)
                if e is None or not path.exists():
                    raise ExtractError(f"step {step} is not run but {name} has no previous file and "
                                       "manifest entry; run the step or --reuse the file")
                if md5sum(path) != e["md5"]:
                    raise ExtractError(f"{path} no longer matches the previous manifest")
                check_columns(name, path)
                files[name] = e
            if step == "mpc":
                snapshot = previous["mpc_snapshot_utc"]
            print(f"{step}: not run; kept {', '.join(todo)}", flush=True)
            continue

        print(f"\n=== {step} ===", flush=True)
        t0 = time.time()
        started = utcnow()
        if step == "mpc":
            snap = export_mpc(tmp, args)
            snapshot = iso(snap)
            for name in todo:
                place(tmp / INPUT_FILES[name][0], out / INPUT_FILES[name][0])
                files[name] = describe(name, out / INPUT_FILES[name][0],
                                       f"{mpc_source(args)}: {MPC_SQL[name]}", snapshot)
        elif step == "dia_sources":
            obs = out / INPUT_FILES["obs_sbn"][0]
            tmp_out = tmp / INPUT_FILES["dia_sources"][0]
            extract_dia_sources(obs, tmp_out, args)
            stem = tmp_out.name[:-len(".parquet")]
            for extra in tmp.glob(f"{stem}.*.parquet"):     # the .unresolved side file
                place(extra, out / extra.name)
            place(tmp_out, out / tmp_out.name)
            files["dia_sources"] = describe(
                "dia_sources", out / tmp_out.name,
                f"extract-submitted-sources --workers {args.workers} on obs_sbn (md5 "
                f"{files['obs_sbn']['md5']}), against {ch_source(args)} ssp.SubmittableSources",
                iso(started))
        elif step == "ppdb_dia_sources":
            tmp_out = tmp / INPUT_FILES["ppdb_dia_sources"][0]
            export_ppdb(tmp_out, args)
            place(tmp_out, out / tmp_out.name)
            files["ppdb_dia_sources"] = describe(
                "ppdb_dia_sources", out / tmp_out.name,
                f"{ch_source(args)}: {PPDB_SQL}", iso(started))
        timings[step] = time.time() - t0
        for name in todo:
            e = files[name]
            print(f"{name}: {e['rows']:,} rows, md5 {e['md5']}", flush=True)
        print(f"{step}: {timings[step]:.1f} s", flush=True)

    shutil.rmtree(tmp)
    manifest = {
        "created_utc": iso(utcnow()),
        "producer": producer(),
        "mpc_snapshot_utc": snapshot,
        "files": {name: files[name] for name in INPUT_FILES},
    }
    assert list(manifest) == list(MANIFEST_FIELDS)
    # Written last, and atomically.
    tmp_manifest = out / f".{MANIFEST_FILE}.tmp"
    tmp_manifest.write_text(json.dumps(manifest, indent=2) + "\n")
    os.replace(tmp_manifest, manifest_path)
    print(f"\nwrote {manifest_path}", flush=True)
    for k, v in timings.items():
        print(f"  {k:20s} {v:8.1f} s")
    return manifest


def build_parser():
    from ssp.export.submittable import MAX_WORKERS

    p = argparse.ArgumentParser(
        prog=PROG,
        description="Stage 1 of the SSO delivery: extract the input files (the MPC snapshot, "
                    "dia_sources, ppdb_dia_sources) into INPUTS_DIR, then manifest.json.",
        epilog="Credentials: the MPC password from ~/.pgpass; ClickHouse from "
               "SSP_CH_USER/SSP_CH_PASSWORD or ~/.chpass. Steps: " + ", ".join(STEPS) + ".",
    )
    p.add_argument("inputs_dir", help="Output directory (INPUTS_DIR)")
    p.add_argument("--only", action="append", choices=list(STEPS), metavar="STEP",
                   help="Run only this step (repeatable); others keep their files")
    p.add_argument("--skip", action="append", choices=list(STEPS), metavar="STEP",
                   help="Do not run this step (repeatable); it keeps its files")
    p.add_argument("--reuse", action="append", metavar="NAME=PATH",
                   help=f"Take PATH as input NAME instead of extracting it (repeatable); "
                        f"NAME is one of {', '.join(INPUT_FILES)}")
    p.add_argument("--mpc-snapshot-utc", help="The snapshot time of reused MPC files, if no manifest "
                                              "beside them gives it")
    p.add_argument("--force", action="store_true",
                   help="Redo inputs in a directory that already has a manifest")
    g = p.add_argument_group("MPC replica")
    g.add_argument("--mpc-host", default=MPC_HOST, help="(default: %(default)s)")
    g.add_argument("--mpc-port", type=int, default=MPC_PORT, help="(default: %(default)s)")
    g.add_argument("--mpc-dbname", default=MPC_DBNAME, help="(default: %(default)s)")
    g.add_argument("--mpc-user", default=MPC_USER, help="(default: %(default)s; password from ~/.pgpass)")
    g = p.add_argument_group("ClickHouse")
    g.add_argument("--ch-host", default=CH_HOST, help="(default: %(default)s)")
    g.add_argument("--ch-port", type=int, default=CH_PORT, help="(default: %(default)s)")
    g.add_argument("--ch-user", default=None, help="(default: from the credentials)")
    g.add_argument("--workers", type=int, default=MAX_WORKERS,
                   help=f"Concurrent extract-submitted-sources queries, at most {MAX_WORKERS} "
                        "(the server is shared; default: %(default)s)")
    return p


def main(argv=None):
    from ssp.export.submittable import MAX_WORKERS

    p = build_parser()
    args = p.parse_args(argv)
    if not 1 <= args.workers <= MAX_WORKERS:
        p.error(f"--workers must be between 1 and {MAX_WORKERS} (the server is shared)")
    t0 = time.time()
    try:
        run(args)
    except ExtractError as e:
        print(f"{PROG}: error: {e}; no manifest written", file=sys.stderr)
        sys.exit(1)
    print(f"total wall time: {time.time() - t0:.1f} s")


if __name__ == "__main__":
    main()
