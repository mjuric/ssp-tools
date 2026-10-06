"""Stage 1 of the SSO delivery: extract the inputs.

    ssp-extract-sso-inputs INPUTS_DIR

Writes every ``ssp.delivery_contract.INPUT_FILES`` file into INPUTS_DIR,
then ``manifest.json`` last (see docs/design/sso-delivery.md, "Stage 1,
extract"). The steps:

``mpc``
    ``obs_sbn`` (the X05 rows), ``mpc_orbits``, ``current_identifications``
    and ``numbered_identifications`` from the USDF MPC replica, in one
    REPEATABLE READ, READ ONLY transaction (``fast-export``'s batch mode).
    The transaction's start is the manifest's ``mpc_snapshot_utc``.
``dia_sources``
    ``extract-submitted-sources`` on that ``obs_sbn`` (ClickHouse
    ``ssp.SubmittableSources``, at most 8 concurrent queries), with the
    shutter-motion correction (``--correction-table``; the manifest's
    ``shutter_timing`` entry records the table and the counts, and is
    carried over or taken from a reused file's manifest like the file's
    entry, else null). It also
    leaves ``dia_sources.unresolved.parquet``, which is not an input. Its
    manifest entry carries ``obs_sbn_md5``, the md5 of the ``obs_sbn`` it
    was built from; a manifest whose ``obs_sbn_md5`` differs from
    ``obs_sbn``'s md5 is never written. A new ``obs_sbn`` (the ``mpc``
    step run, or a reused one) therefore also runs ``dia_sources``, unless
    ``dia_sources`` is itself reused.
``ppdb_dia_sources``
    The five ``ppdb.DiaSource`` columns NearbySSO reads, one read-only
    query.

Nothing in INPUTS_DIR changes until every step has succeeded: the steps
write under ``INPUTS_DIR/.partial/`` (the MPC export's temporary CSVs
too), and only then are the files moved into place, the old manifest kept
aside as ``.manifest.previous.json`` and the new manifest written (fsynced,
atomically). A failed or refused run leaves the previous inputs and their
manifest as they were. A lock file (``.lock``) keeps two runs out of one
INPUTS_DIR.

An INPUTS_DIR with a manifest is complete: redoing any of it needs
``--force``. ``--only``/``--skip`` redo parts of it: a step not run keeps
its files, whose manifest entries are carried over (from ``manifest.json``,
else ``.manifest.previous.json``) once their md5s are checked, before
anything is run. ``--reuse NAME=PATH`` takes an existing file instead of
extracting it (hard-linked, or copied across filesystems; a path that is
already the INPUTS_DIR file is used in place), and the manifest records it
as reused. The four MPC files are one snapshot: reuse all four or none.

Credentials: the MPC password from ``~/.pgpass`` (libpq), ClickHouse from
``SSP_CH_USER``/``SSP_CH_PASSWORD`` or ``~/.chpass``
(``ssp.export.submittable.credentials``).
"""


from __future__ import annotations

import argparse
import contextlib
import datetime
import hashlib
import json
import os
import re
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
    SHUTTER_MANIFEST_FIELD,
)

PROG = "ssp-extract-sso-inputs"

MPC_HOST = "mpcorb-db.slac.stanford.edu"
MPC_PORT = 5432
MPC_DBNAME = "mpc_sbn"
MPC_USER = "rubin"

from ssp.export.submittable import current_host  # noqa: E402 (the ClickHouse host, see there)
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
LOCK_FILE = ".lock"
PREVIOUS_MANIFEST = ".manifest.previous.json"   # stage 2 never reads it


class ExtractError(RuntimeError):
    """The inputs are inconsistent, or a step cannot run; INPUTS_DIR is
    left as it was."""


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
    return export_in_transaction(dsn, exports, tmp_dir=tmp, log=lambda m: print(m, flush=True))


def extract_dia_sources(obs_path, out_path, args):
    """``extract-submitted-sources`` on ``obs_path``, as a library call.
    Returns the manifest's SHUTTER_MANIFEST_FIELD entry (None without a
    correction table)."""
    from ssp.export import submittable as S

    def fetch(tasks):
        return S.run_queries(tasks, args.ch_host, args.ch_port, S.DEFAULT_DATABASE, args.ch_user,
                             args.workers)

    report = {}
    try:
        rc = S.extract(obs_path, out_path, fetch, correction_table=S.correction_table(args),
                       max_not_built_visits=args.max_not_built_visits, report=report)
    except S.CorrectionError as e:
        raise ExtractError(f"dia_sources: shutter-motion correction: {e}") from None
    if rc != 0:
        raise ExtractError(f"extract-submitted-sources returned {rc}")
    return report.get(SHUTTER_MANIFEST_FIELD[0])


def export_ppdb(out_path, args):
    """The five ``ppdb.DiaSource`` columns, streamed to ``out_path``."""
    import clickhouse_connect

    from ssp.export.submittable import bypass_proxy, credentials

    user, password = credentials(args.ch_host, args.ch_port, CH_DATABASE, args.ch_user)
    bypass_proxy(args.ch_host)
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


def load_json(path):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return None


def parse_utc(value):
    """``--mpc-snapshot-utc``: ISO 8601 with a time zone, as ``iso()``."""
    try:
        t = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ExtractError(f"--mpc-snapshot-utc {value!r}: not an ISO 8601 time") from None
    if t.tzinfo is None:
        raise ExtractError(f"--mpc-snapshot-utc {value!r}: give the time zone (e.g. a trailing Z)")
    return iso(t)


_OBS_MD5_RE = re.compile(r"obs_sbn \(md5 ([0-9a-f]{32})\)")


def recorded_obs_md5(entry):
    """The md5 of the ``obs_sbn`` a ``dia_sources`` manifest entry was built
    from: its ``obs_sbn_md5``, else (manifests before that field) the md5
    in its ``source``; None if unknown."""
    if not entry:
        return None
    if entry.get("obs_sbn_md5"):
        return entry["obs_sbn_md5"]
    m = _OBS_MD5_RE.search(entry.get("source", ""))
    return m.group(1) if m else None


@contextlib.contextmanager
def lock(out):
    """An exclusive ``flock`` on ``INPUTS_DIR/.lock`` (released when the
    process ends, however it ends)."""
    import fcntl

    fd = os.open(out / LOCK_FILE, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise ExtractError(f"{out} is in use by another {PROG} (locked {out / LOCK_FILE})") from None
        os.ftruncate(fd, 0)
        os.write(fd, f"{os.getpid()}\n".encode())
        yield
    finally:
        os.close(fd)


def take(src, dst):
    """Hard-link ``src`` to ``dst``, or copy it across filesystems."""
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst)


def write_manifest(path, manifest):
    """Atomically, and durably: write, fsync, rename, fsync the
    directory."""
    tmp = path.parent / f".{path.name}.tmp"
    with open(tmp, "w") as f:
        f.write(json.dumps(manifest, indent=2) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    dfd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)


class Reused:
    """A ``--reuse NAME=PATH`` file, checked: its md5, and what a manifest
    beside it says about it (when that manifest describes this very
    file)."""

    def __init__(self, name, path, dst):
        self.name, self.path = name, Path(path).resolve()
        if not self.path.is_file():
            raise ExtractError(f"--reuse {name}: no such file {path}")
        check_columns(name, self.path)
        self.in_place = dst.exists() and os.path.samefile(self.path, dst)
        self.md5 = md5sum(self.path)
        side = load_json(self.path.parent / MANIFEST_FILE) or {}
        e = (side.get("files") or {}).get(name)
        self.side_entry = e if isinstance(e, dict) and e.get("md5") == self.md5 else None
        self.side_snapshot = side.get("mpc_snapshot_utc") if self.side_entry else None
        self.side_shutter = side.get(SHUTTER_MANIFEST_FIELD[0]) if self.side_entry else None

    def entry(self):
        e = self.side_entry
        when = e["extracted_utc"] if e else iso(
            datetime.datetime.fromtimestamp(self.path.stat().st_mtime, datetime.timezone.utc))
        source = f"reused {self.path}" + (f" (originally: {e['source']})" if e else "")
        out = {"file": INPUT_FILES[self.name][0], "rows": pq.ParquetFile(self.path).metadata.num_rows,
               "md5": self.md5, "source": source, "extracted_utc": when}
        return out


def parse_reuse(items, out):
    reuse = {}
    for item in items or []:
        name, sep, path = item.partition("=")
        if not sep or name not in INPUT_FILES:
            raise ExtractError(f"--reuse {item!r}: expected NAME=PATH with NAME one of {list(INPUT_FILES)}")
        reuse[name] = path
    some = [n for n in MPC_SNAPSHOT if n in reuse]
    if some and len(some) != len(MPC_SNAPSHOT):
        raise ExtractError(f"--reuse: the MPC files {list(MPC_SNAPSHOT)} are one snapshot; reuse all "
                           f"four or none (got {some})")
    return {n: Reused(n, p, out / INPUT_FILES[n][0]) for n, p in reuse.items()}


def plan(args, reuse, carried_obs_md5):
    """The steps to run: --only/--skip, plus ``dia_sources`` whenever the
    ``obs_sbn`` is new (``mpc`` run, or a reused ``obs_sbn`` other than the
    one the kept ``dia_sources`` was built from) and ``dia_sources`` is not
    reused."""
    steps = [s for s in STEPS if (not args.only or s in args.only) and s not in (args.skip or ())]
    steps = [s for s in steps if not all(n in reuse for n in STEPS[s])]
    new_obs = "mpc" in steps or ("obs_sbn" in reuse and reuse["obs_sbn"].md5 != carried_obs_md5)
    if new_obs and "dia_sources" not in reuse and "dia_sources" not in steps:
        if "dia_sources" in (args.skip or ()):
            raise ExtractError("--skip dia_sources: a new obs_sbn needs a new dia_sources (or "
                               "--reuse dia_sources=PATH built from it)")
        print("dia_sources: also run, since obs_sbn is new", flush=True)
        steps.append("dia_sources")
    return [s for s in STEPS if s in steps]


def run(args):
    """Extract into ``args.inputs_dir``; returns the manifest written."""
    out = Path(args.inputs_dir)
    out.mkdir(parents=True, exist_ok=True)
    with lock(out):
        return _run(args, out)


def _run(args, out):
    manifest_path, previous_path = out / MANIFEST_FILE, out / PREVIOUS_MANIFEST
    if manifest_path.exists() and not args.force:
        raise ExtractError(f"{manifest_path} exists: these inputs are complete. Use a new "
                           "INPUTS_DIR, or --force to redo them (with --only/--skip for parts)")
    previous = load_json(manifest_path) if manifest_path.exists() else load_json(previous_path)
    prev_files = (previous or {}).get("files") or {}
    snapshot_arg = parse_utc(args.mpc_snapshot_utc) if args.mpc_snapshot_utc else None

    # Everything is checked before anything is run or changed.
    reuse = parse_reuse(args.reuse, out)
    carry_dia = prev_files.get("dia_sources") if "dia_sources" not in reuse else None
    steps = plan(args, reuse, recorded_obs_md5(carry_dia))

    files, snapshot = {}, None
    # the shutter_timing entry of a carried-over dia_sources (checked below)
    shutter = (previous or {}).get(SHUTTER_MANIFEST_FIELD[0])
    for step, names in STEPS.items():
        for name in names:
            if name in reuse or step in steps:
                continue
            path, e = out / INPUT_FILES[name][0], prev_files.get(name)
            if e is None or not path.exists():
                raise ExtractError(f"step {step} is not run but {name} has no previous file and "
                                   "manifest entry; run the step or --reuse the file")
            if md5sum(path) != e["md5"]:
                raise ExtractError(f"{path} no longer matches the previous manifest; run step {step}")
            check_columns(name, path)
            files[name] = dict(e)
    if "obs_sbn" in reuse:
        snaps = {r.side_snapshot for r in (reuse[n] for n in MPC_SNAPSHOT)}
        snapshot = snapshot_arg or (snaps.pop() if len(snaps) == 1 and None not in snaps else None)
        if snapshot is None:
            raise ExtractError("--reuse of the MPC files: give --mpc-snapshot-utc (no manifest beside "
                               "them describes all four files with one snapshot)")
    elif "mpc" not in steps:
        snapshot = previous["mpc_snapshot_utc"]
    for name, r in reuse.items():
        files[name] = r.entry()
    if "dia_sources" in reuse:
        r = reuse["dia_sources"]
        shutter = r.side_shutter
        obs_md5 = recorded_obs_md5(r.side_entry)
        if obs_md5 is None and "obs_sbn" in reuse and reuse["obs_sbn"].path.parent == r.path.parent:
            obs_md5 = reuse["obs_sbn"].md5      # reused together, from one directory
        if obs_md5 is None:
            raise ExtractError(f"--reuse dia_sources={r.path}: which obs_sbn it was built from is "
                               "unknown (no manifest beside it records it); reuse it together with "
                               "its obs_sbn from the same directory")
        files["dia_sources"]["obs_sbn_md5"] = obs_md5
    elif "dia_sources" in files:
        files["dia_sources"]["obs_sbn_md5"] = recorded_obs_md5(files["dia_sources"])
    if "dia_sources" not in steps and "mpc" not in steps:
        check_pairing(files, files["obs_sbn"], early=True)

    # Run the steps, into .partial.
    tmp = out / PARTIAL
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir()
    staged, timings = {}, {}      # name -> path in tmp

    def path_of(name):
        return staged.get(name, out / INPUT_FILES[name][0])

    for name, r in reuse.items():
        if not r.in_place:
            take(r.path, tmp / INPUT_FILES[name][0])
            staged[name] = tmp / INPUT_FILES[name][0]
        print(f"{name}: reused {r.path}" + (" (in place)" if r.in_place else ""), flush=True)

    for step in steps:
        print(f"\n=== {step} ===", flush=True)
        t0, started = time.time(), utcnow()
        if step == "mpc":
            snapshot = iso(export_mpc(tmp, args))
            for name in MPC_SNAPSHOT:
                staged[name] = tmp / INPUT_FILES[name][0]
                files[name] = describe(name, staged[name], f"{mpc_source(args)}: {MPC_SQL[name]}", snapshot)
        elif step == "dia_sources":
            staged["dia_sources"] = tmp / INPUT_FILES["dia_sources"][0]
            shutter = extract_dia_sources(path_of("obs_sbn"), staged["dia_sources"], args)
            obs_md5 = files["obs_sbn"]["md5"]
            files["dia_sources"] = describe(
                "dia_sources", staged["dia_sources"],
                f"extract-submitted-sources --workers {args.workers} on obs_sbn (md5 {obs_md5}), "
                f"against {ch_source(args)} ssp.SubmittableSources, correction table "
                f"{args.correction_table}", iso(started))
            files["dia_sources"]["obs_sbn_md5"] = obs_md5
        elif step == "ppdb_dia_sources":
            staged["ppdb_dia_sources"] = tmp / INPUT_FILES["ppdb_dia_sources"][0]
            export_ppdb(staged["ppdb_dia_sources"], args)
            files["ppdb_dia_sources"] = describe("ppdb_dia_sources", staged["ppdb_dia_sources"],
                                                 f"{ch_source(args)}: {PPDB_SQL}", iso(started))
        timings[step] = time.time() - t0
        for name in STEPS[step]:
            print(f"{name}: {files[name]['rows']:,} rows, md5 {files[name]['md5']}", flush=True)
        print(f"{step}: {timings[step]:.1f} s", flush=True)

    check_pairing(files, files["obs_sbn"])
    manifest = {
        "created_utc": iso(utcnow()),
        "producer": producer(),
        "mpc_snapshot_utc": snapshot,
        "files": {name: files[name] for name in INPUT_FILES},
    }
    # SHUTTER_MANIFEST_FIELD joins MANIFEST_FIELDS when the build requires it
    manifest.setdefault(SHUTTER_MANIFEST_FIELD[0], shutter)
    assert [k for k in manifest if k in MANIFEST_FIELDS] == list(MANIFEST_FIELDS)
    assert set(manifest) <= set(MANIFEST_FIELDS) | {SHUTTER_MANIFEST_FIELD[0]}

    # Commit: the old manifest aside, the files into place, the new manifest.
    if manifest_path.exists():
        os.replace(manifest_path, previous_path)
    for name, path in staged.items():
        os.replace(path, out / path.name)
    side = out / "dia_sources.unresolved.parquet"
    if (tmp / side.name).exists():
        os.replace(tmp / side.name, side)
    elif "dia_sources" in staged and side.exists():
        side.unlink()       # it belonged to the replaced dia_sources
    write_manifest(manifest_path, manifest)
    shutil.rmtree(tmp)
    print(f"\nwrote {manifest_path}", flush=True)
    for k, v in timings.items():
        print(f"  {k:20s} {v:8.1f} s")
    return manifest


def check_pairing(files, obs_entry, early=False):
    """Refuse a ``dia_sources`` not built from this ``obs_sbn``."""
    dia = files.get("dia_sources")
    if dia is None:
        return
    if dia.get("obs_sbn_md5") != obs_entry["md5"]:
        raise ExtractError(
            f"dia_sources was built from obs_sbn md5 {dia.get('obs_sbn_md5')}, but obs_sbn is "
            f"{obs_entry['md5']}" + ("; run dia_sources too" if early else ""))


def build_parser():
    from ssp.export.submittable import MAX_WORKERS, add_correction_args

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
    p.add_argument("--mpc-snapshot-utc", help="The snapshot time (ISO 8601) of reused MPC files, if "
                                              "no manifest beside them gives it")
    p.add_argument("--force", action="store_true",
                   help="Redo inputs in a directory that already has a manifest")
    g = p.add_argument_group("MPC replica")
    g.add_argument("--mpc-host", default=MPC_HOST, help="(default: %(default)s)")
    g.add_argument("--mpc-port", type=int, default=MPC_PORT, help="(default: %(default)s)")
    g.add_argument("--mpc-dbname", default=MPC_DBNAME, help="(default: %(default)s)")
    g.add_argument("--mpc-user", default=MPC_USER, help="(default: %(default)s; password from ~/.pgpass)")
    g = p.add_argument_group("ClickHouse")
    g.add_argument("--ch-host", default=None, help="(default: from ~/.clickhouse.host)")
    g.add_argument("--ch-port", type=int, default=CH_PORT, help="(default: %(default)s)")
    g.add_argument("--ch-user", default=None, help="(default: from the credentials)")
    g.add_argument("--workers", type=int, default=MAX_WORKERS,
                   help=f"Concurrent extract-submitted-sources queries, at most {MAX_WORKERS} "
                        "(the server is shared; default: %(default)s)")
    add_correction_args(p.add_argument_group("Shutter-motion correction (dia_sources)"))
    return p


def main(argv=None):
    from ssp.export.submittable import MAX_WORKERS

    p = build_parser()
    args = p.parse_args(argv)
    args.ch_host = args.ch_host or current_host()
    if not 1 <= args.workers <= MAX_WORKERS:
        p.error(f"--workers must be between 1 and {MAX_WORKERS} (the server is shared)")
    t0 = time.time()
    try:
        run(args)
    except ExtractError as e:
        print(f"{PROG}: error: {e}; INPUTS_DIR left as it was", file=sys.stderr)
        sys.exit(1)
    print(f"total wall time: {time.time() - t0:.1f} s")


if __name__ == "__main__":
    main()
