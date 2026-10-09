"""Stage 2 of the SSO delivery: build the PPDB Solar System tables from the
input files (``ssp-build-sso INPUTS_DIR RUN_DIR``).

See docs/design/sso-delivery.md ("Stage 2, build") and the contract,
ssp/delivery_contract.py. Files in, files out: no database access.

    ssp-build-sso INPUTS_DIR RUN_DIR [--from STEP] [--workers N]
                  [--part-rows N] [--internal-columns A,B,...]

1. Validate ``INPUTS_DIR/manifest.json``: every input file present, with
   the manifest's row count and md5, and the required columns.
2. Run the BUILD_STEPS, each as a subprocess with its log in
   ``RUN_DIR/logs/<step>.log`` and its wall time and peak RSS in the
   report:

   - ``mpc``: shape the three MPC tables to delivery_schema();
   - ``ssobservation``, ``ssobject``, ``nearbysso``: the existing builders,
     reading the shaped MPC tables (as delivered) and the other inputs
     as given;
   - ``check``: the delivery check (ssp.delivery_check) of all six tables,
     plus SSObservation's ``conformance`` and ``offsets``
     (bench/ssobservation_validate.py), into ``RUN_DIR/checks/``.

   The delivered tables are ``RUN_DIR/delivery/<Table>.parquet``, except
   SSObservation, delivered as parts, ``SSObservation.partNNNN.parquet``,
   with ``SSObservation.manifest.json`` and the sidecar of internal
   columns, ``SSObservation_internal.parquet`` (not uploaded; see
   ssp.ssobservation_contract, "The partitioned delivery"). ``--part-rows``
   and ``--internal-columns`` go to the ssobservation builder, and are
   recorded in the report's ``steps.ssobservation``. The builders' other
   outputs stay in ``RUN_DIR/work/``.
3. ``RUN_DIR/report.json`` (REPORT_FIELDS), rewritten after every step, so
   a failed run shows how far it got. ``deliverable`` is true only if every
   step and check passed.

``--from STEP`` reruns from STEP, keeping the earlier steps' outputs and
report entries; it refuses unless those steps succeeded on the same inputs.
"""

import argparse
import datetime
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import get_context
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .delivery_check import check_delivery, format_report
from .delivery_contract import (
    BUILD_STEPS,
    DELIVERY_DIR,
    DELIVERY_TABLES,
    DERIVED_FROM,
    INPUT_FILES,
    MPC_SNAPSHOT,
    MANIFEST_FIELDS,
    MANIFEST_FILE,
    REPORT_FILE,
    REQUIRED_INPUT_COLUMNS,
    SHUTTER_INPUT_COLUMNS,
    delivery_schema,
)
from .ssobservation_contract import (
    PART_GLOB,
    PART_ROWS_DEFAULT,
    SIDECAR_FILE,
    SSOBSERVATION_INTERNAL_DEFAULT,
    SSOBSERVATION_INTERNAL_DTYPE,
    SSOBSERVATION_MANIFEST_FILE,
)

REPO_ROOT = Path(__file__).resolve().parent.parent

#: The MPC tables the ``mpc`` step shapes.
MPC_TABLES = ("mpc_orbits", "current_identifications", "numbered_identifications")

#: Delivered columns the ``mpc`` step derives, rather than copies:
#: (table, column) -> the input column it is a copy of.
MPC_DERIVED = {("mpc_orbits", "designation"): "unpacked_primary_provisional_designation"}

#: The delivered tables each step writes (``check`` writes RUN_DIR/checks).
STEP_TABLES = {
    "mpc": MPC_TABLES,
    "ssobservation": ("SSObservation",),
    "ssobject": ("SSObject",),
    "nearbysso": ("NearbySSO",),
    "check": (),
}

#: Timestamps outside this range fail the ``mpc`` step. The MPC tables'
#: timestamps are database bookkeeping times (2020 onwards in 2026), so
#: one in 1970 is a unit or epoch error, not a real date.
TIMESTAMP_RANGE = (datetime.datetime(1990, 1, 1), datetime.datetime(2100, 1, 1))

CHECKS_DIR = "checks"
LOGS_DIR = "logs"
WORK_DIR = "work"
CHECK_RESULTS = "results.json"

#: The SSObservation content checks of bench/ssobservation_validate.py run by
#: ``check``.
SSOBSERVATION_CHECKS = ("conformance", "offsets")

#: The checks a deliverable run must have passed, by name.
EXPECTED_CHECKS = (tuple(f"delivery:{t}" for t in DELIVERY_TABLES)
                   + tuple(f"ssobservation:{c}" for c in SSOBSERVATION_CHECKS))

MPC_BATCH_ROWS = 131_072


# --------------------------------------------------------------------------
# The partitioned SSObservation (ssp.ssobservation_contract, "The partitioned
# delivery")
# --------------------------------------------------------------------------

#: The delivered table that is parts, a manifest and a sidecar rather than
#: one <Table>.parquet.
PARTITIONED = "SSObservation"


def parse_internal_columns(value):
    """The internal columns of ``--internal-columns``: a comma-separated
    string (empty: none) or a sequence; None gives the default,
    SSOBSERVATION_INTERNAL_DEFAULT. Raises ValueError for a column that
    cannot be internal (not in SSOBSERVATION_INTERNAL_DTYPE) or a repeat."""
    if value is None:
        return list(SSOBSERVATION_INTERNAL_DEFAULT)
    if isinstance(value, str):
        cols = [c.strip() for c in value.split(",") if c.strip()]
    else:
        cols = list(value)
    bad = [c for c in cols if c not in SSOBSERVATION_INTERNAL_DTYPE]
    if bad:
        raise ValueError(f"internal columns {bad} cannot be internal; the columns that can be are "
                         f"{list(SSOBSERVATION_INTERNAL_DTYPE)}")
    if len(set(cols)) != len(cols):
        raise ValueError(f"internal columns {cols} repeat a column")
    return cols


def ssobservation_files(directory):
    """The names of the SSObservation files in ``directory``, as its
    manifest lists them: the parts in part order, the sidecar, then the
    manifest. Raises ValueError for a missing or unreadable manifest, or a
    listed file that is not there. (It does not check the files' contents:
    that is ssp.delivery_check's job.)"""
    d = Path(directory)
    mpath = d / SSOBSERVATION_MANIFEST_FILE
    if not mpath.is_file():
        raise ValueError(f"no {mpath}")
    try:
        with open(mpath) as f:
            m = json.load(f)
        names = [p["file"] for p in m["parts"]] + [m["sidecar"]["file"]]
    except (OSError, ValueError, KeyError, TypeError) as e:
        raise ValueError(f"{mpath}: cannot read the parts and sidecar: {type(e).__name__}: {e}") from e
    bad = [n for n in names if not isinstance(n, str) or not n or Path(n).name != n]
    if bad or not names[:-1]:
        raise ValueError(f"{mpath}: " + (f"file names {bad} are not plain names" if bad else "no parts"))
    missing = [n for n in names if not (d / n).is_file()]
    if missing:
        raise ValueError(f"{mpath} lists files that are not in {d}: {missing}")
    return names + [SSOBSERVATION_MANIFEST_FILE]


def delivered_paths(run_dir, table):
    """The files of delivered ``table`` now in RUN_DIR/delivery: its
    <Table>.parquet, or for SSObservation every part (PART_GLOB), the
    manifest and the sidecar there."""
    d = Path(run_dir) / DELIVERY_DIR
    if table == PARTITIONED:
        return sorted(d.glob(PART_GLOB)) + [p for p in (d / SSOBSERVATION_MANIFEST_FILE, d / SIDECAR_FILE)
                                            if p.exists()]
    p = d / f"{table}.parquet"
    return [p] if p.exists() else []


def move_ssobservation(src_dir, dst_dir):
    """Move the SSObservation files the builder wrote in ``src_dir`` (as its
    manifest lists them) to ``dst_dir``, under their own names, the
    manifest last. Raises ValueError, moving nothing, if one is missing."""
    names = ssobservation_files(src_dir)
    for n in names:
        os.replace(Path(src_dir) / n, Path(dst_dir) / n)
    return names


class ManifestError(ValueError):
    """The inputs do not honour the contract (see validate_manifest)."""


class ShapeError(ValueError):
    """An MPC table cannot be shaped to the delivery schema."""


def _log(*args):
    print(*args, flush=True)


def _utcnow():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def _md5(path, blocksize=8 << 20):
    h = hashlib.md5()
    with open(path, "rb") as f:
        while block := f.read(blocksize):
            h.update(block)
    return h.hexdigest()


def _write_json(obj, path):
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=2, default=str)
        f.write("\n")
    os.replace(tmp, path)


def ssp_tools_commit():
    """The git commit of this code, with ``-dirty`` if the checkout has
    uncommitted changes, or untracked files under ssp/ or bench/ (code that
    may run); None outside a git checkout."""
    git = ["git", "-C", str(REPO_ROOT)]
    try:
        sha = subprocess.run([*git, "rev-parse", "HEAD"], capture_output=True,
                             text=True, check=True).stdout.strip()
        dirty = subprocess.run([*git, "status", "--porcelain", "--untracked-files=no"],
                               capture_output=True, text=True, check=True).stdout.strip()
        dirty += subprocess.run([*git, "ls-files", "--others", "--exclude-standard", "--", "ssp", "bench"],
                                capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None
    return sha + ("-dirty" if dirty else "")


# --------------------------------------------------------------------------
# The manifest
# --------------------------------------------------------------------------

def input_path(inputs_dir, manifest, name):
    """The path of input ``name``, from the manifest's ``file``."""
    return Path(inputs_dir) / manifest["files"][name]["file"]


def read_manifest(inputs_dir):
    path = Path(inputs_dir) / MANIFEST_FILE
    if not path.exists():
        raise ManifestError(f"no {MANIFEST_FILE} in {inputs_dir}: the build refuses inputs without one")
    try:
        with open(path) as f:
            manifest = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        raise ManifestError(f"cannot read {path}: {e}") from e
    if not isinstance(manifest, dict):
        raise ManifestError(f"{path}: not a JSON object")
    return manifest


_MD5_RE = re.compile(r"[0-9a-f]{32}")


def _parse_utc(value):
    """An ISO 8601 time (``Z`` allowed) as an aware datetime, or None."""
    if not isinstance(value, str):
        return None
    try:
        t = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return t if t.tzinfo is not None else None


def input_md5s(inputs_dir, manifest, workers=8):
    """{name: md5} of every input file, hashed now (in threads)."""
    names = list(INPUT_FILES)
    with ThreadPoolExecutor(max(1, min(workers, len(names)))) as pool:
        md5s = list(pool.map(lambda n: _md5(input_path(inputs_dir, manifest, n)), names))
    return dict(zip(names, md5s))


def validate_manifest(inputs_dir, workers=8):
    """Read and check ``INPUTS_DIR/manifest.json`` against the files; return
    it. Raises ManifestError listing every problem: a missing or malformed
    manifest field, a missing input (INPUT_FILES) or file, a row count or
    md5 that does not match, a missing REQUIRED_INPUT_COLUMNS column, a
    DERIVED_FROM input built from another parent, or MPC_SNAPSHOT inputs
    not all extracted at ``mpc_snapshot_utc``. An OSError reading a file is
    a ManifestError too."""
    try:
        return _validate_manifest(inputs_dir, workers)
    except OSError as e:
        raise ManifestError(f"invalid inputs: {type(e).__name__}: {e}") from e


def _validate_manifest(inputs_dir, workers):
    manifest = read_manifest(inputs_dir)
    problems = [f"manifest lacks {k!r}" for k in MANIFEST_FIELDS if k not in manifest]
    files = manifest.get("files")
    if not isinstance(files, dict):
        raise ManifestError("; ".join(problems + ["manifest 'files' is not an object"]))

    todo = []
    for name in INPUT_FILES:
        entry = files.get(name)
        if not isinstance(entry, dict):
            problems.append(f"manifest lacks input {name!r}")
            continue
        missing = [k for k in ("file", "rows", "md5") if k not in entry]
        if missing:
            problems.append(f"{name}: manifest entry lacks {missing}")
            continue
        bad = []
        if not isinstance(entry["file"], str) or not entry["file"]:
            bad.append(f"file {entry['file']!r} is not a path")
        if not isinstance(entry["rows"], int) or isinstance(entry["rows"], bool) or entry["rows"] < 0:
            bad.append(f"rows {entry['rows']!r} is not a row count")
        if not isinstance(entry["md5"], str) or not _MD5_RE.fullmatch(entry["md5"]):
            bad.append(f"md5 {entry['md5']!r} is not an md5")
        if bad:
            problems.append(f"{name}: malformed manifest entry: {'; '.join(bad)}")
            continue
        path = input_path(inputs_dir, manifest, name)
        if not path.is_file():
            problems.append(f"{name}: {path} does not exist")
            continue
        try:
            pf = pq.ParquetFile(path)
        except Exception as e:
            problems.append(f"{name}: {path} is not a readable Parquet file: {e}")
            continue
        if pf.metadata.num_rows != entry["rows"]:
            problems.append(f"{name}: {pf.metadata.num_rows:,} rows, the manifest says {entry['rows']:,}")
        cols = set(pf.schema_arrow.names)
        lacking = [c for c in REQUIRED_INPUT_COLUMNS[name] if c not in cols]
        if lacking:
            problems.append(f"{name}: lacks required columns {lacking}")
        if name == "dia_sources":
            # the shutter correction's columns: all or none
            shutter = [c for c in SHUTTER_INPUT_COLUMNS if c not in cols]
            if 0 < len(shutter) < len(SHUTTER_INPUT_COLUMNS):
                problems.append(f"{name}: has some of the shutter-correction columns, but lacks {shutter}")
        todo.append((name, path, entry["md5"]))

    with ThreadPoolExecutor(max(1, min(workers, len(todo) or 1))) as pool:
        md5s = list(pool.map(lambda t: _md5(t[1]), todo))
    for (name, path, want), got in zip(todo, md5s):
        if got != want:
            problems.append(f"{name}: md5 of {path} is {got}, the manifest says {want}")

    for name, parent in DERIVED_FROM.items():
        if not isinstance(files.get(name), dict) or not isinstance(files.get(parent), dict):
            continue
        recorded = recorded_parent_md5(files[name], parent)
        if recorded is None:
            problems.append(f"{name}: the manifest does not record the md5 of the {parent} it was built "
                            f"from ({parent}_md5)")
        elif recorded != files[parent].get("md5"):
            problems.append(f"{name} was built from a {parent} of md5 {recorded}, not this one "
                            f"({files[parent].get('md5')}): the two are out of step")

    # one MPC snapshot: every MPC_SNAPSHOT input extracted at mpc_snapshot_utc
    snap = _parse_utc(manifest.get("mpc_snapshot_utc"))
    if "mpc_snapshot_utc" in manifest and snap is None:
        problems.append(f"mpc_snapshot_utc {manifest['mpc_snapshot_utc']!r} is not an ISO 8601 time "
                        "with a time zone")
    elif snap is not None:
        for name in MPC_SNAPSHOT:
            entry = files.get(name)
            if not isinstance(entry, dict):
                continue
            if _parse_utc(entry.get("extracted_utc")) != snap:
                problems.append(f"{name}: extracted_utc {entry.get('extracted_utc')!r} is not the MPC "
                                f"snapshot's ({manifest['mpc_snapshot_utc']}): the MPC inputs must come "
                                "from one snapshot")

    if problems:
        raise ManifestError("invalid inputs: " + "; ".join(problems))
    return manifest


def recorded_parent_md5(entry, parent):
    """The md5 of the ``parent`` input a derived input's manifest ``entry``
    was built from (DERIVED_FROM): its ``<parent>_md5``, else (manifests
    before that field) the ``<parent> (md5 ...)`` in its ``source``, as
    ssp-extract-sso-inputs writes it; None if neither is there. A
    ``<parent>_md5`` that is present is returned as is, even if empty
    (and then refused), never replaced by the fallback."""
    key = f"{parent}_md5"
    if key in entry:
        return entry[key]
    m = re.search(rf"\b{re.escape(parent)} \(md5 ([0-9a-f]{{32}})\)", str(entry.get("source", "")))
    return m.group(1) if m else None


def write_manifest(inputs_dir, files, producer="hand-made", mpc_snapshot_utc=None, **extra):
    """Write ``INPUTS_DIR/manifest.json`` for ``files`` ({name: file,
    relative to INPUTS_DIR}), with each file's rows and md5, and each
    DERIVED_FROM input recorded as built from the given parent (or from
    ``<name>_<parent>_md5=``). For tests and for inputs from other sources;
    stage 1 writes its own."""
    now = _utcnow()
    snapshot = mpc_snapshot_utc or now
    entries = {}
    for name, file in files.items():
        path = Path(inputs_dir) / file
        entries[name] = dict(file=str(file), rows=pq.ParquetFile(path).metadata.num_rows, md5=_md5(path),
                             source=extra.pop(f"{name}_source", "file"),
                             extracted_utc=snapshot if name in MPC_SNAPSHOT else now)
    for name, parent in DERIVED_FROM.items():
        if name in entries and parent in entries:
            entries[name][f"{parent}_md5"] = extra.pop(f"{name}_{parent}_md5", entries[parent]["md5"])
    manifest = dict(created_utc=now, producer=producer, mpc_snapshot_utc=snapshot,
                    files=entries, **extra)
    _write_json(manifest, Path(inputs_dir) / MANIFEST_FILE)
    return manifest


# --------------------------------------------------------------------------
# Step mpc: shape the MPC tables to the delivery schema
# --------------------------------------------------------------------------

FELIS_STRING = ("char", "text", "string", "unicode")
FELIS_ARROW = {
    "long": pa.int64(), "int": pa.int32(), "short": pa.int16(), "byte": pa.int8(),
    "float": pa.float32(), "double": pa.float64(), "boolean": pa.bool_(),
}
TIMESTAMP_UNITS = {0: "s", 3: "ms", 6: "us", 9: "ns"}


def arrow_type(col):
    """The Arrow type of Felis column ``col`` in the delivered MPC tables."""
    dt = col["datatype"]
    if dt in FELIS_STRING:
        return pa.string()
    if dt == "timestamp":
        prec = col.get("precision", 6)
        if prec not in TIMESTAMP_UNITS:
            raise ShapeError(f"column {col['name']!r}: unsupported timestamp precision {prec}")
        return pa.timestamp(TIMESTAMP_UNITS[prec])
    if dt in FELIS_ARROW:
        return FELIS_ARROW[dt]
    raise ShapeError(f"column {col['name']!r}: unsupported Felis datatype {dt!r}")


def is_json_column(col):
    return col["name"].endswith(("_json", "_jsonb"))


def arrow_schema(columns):
    """The delivered Arrow schema of a table's Felis ``columns``."""
    return pa.schema([pa.field(c["name"], arrow_type(c), nullable=c.get("nullable", True)) for c in columns])


def _decode(arr):
    if isinstance(arr, pa.ChunkedArray):
        arr = arr.combine_chunks()
    if pa.types.is_dictionary(arr.type):
        arr = arr.dictionary_decode()
    return arr


def cast_column(where, arr, col):
    """Cast Arrow array ``arr`` to Felis column ``col``'s delivered type, and
    check it (``where`` names it in errors). Raises ShapeError for a cast
    that is impossible or loses information, a timestamp outside
    TIMESTAMP_RANGE, or a NULL in a non-nullable column."""
    target = arrow_type(col)
    arr = _decode(arr)
    src = arr.type
    try:
        if pa.types.is_string(target):
            if not (pa.types.is_string(src) or pa.types.is_large_string(src)
                    or pa.types.is_binary(src) or pa.types.is_large_binary(src)):
                raise ShapeError(f"{where}: cannot cast {src} to a string")
            out = arr.cast(target)
        elif pa.types.is_timestamp(target):
            if pa.types.is_timestamp(src) and src.tz is not None:
                arr = arr.cast(pa.timestamp(src.unit))       # (UTC wall time)
            if pa.types.is_string(src) or pa.types.is_large_string(src):
                try:
                    out = arr.cast(target)
                except pa.ArrowInvalid:                      # (with a zone offset)
                    out = arr.cast(pa.timestamp(target.unit, "UTC")).cast(target)
            elif pa.types.is_timestamp(src) or pa.types.is_date(src):
                out = arr.cast(target, safe=True)
            else:
                raise ShapeError(f"{where}: cannot cast {src} to {target}")
            if len(out) > out.null_count:
                mm = pc.min_max(out)
                lo, hi = mm["min"].as_py(), mm["max"].as_py()
                if lo < TIMESTAMP_RANGE[0] or hi >= TIMESTAMP_RANGE[1]:
                    raise ShapeError(f"{where}: timestamps {lo} .. {hi} outside {TIMESTAMP_RANGE[0]:%Y} .. "
                                     f"{TIMESTAMP_RANGE[1]:%Y} (a unit error?)")
        elif pa.types.is_boolean(target):
            if pa.types.is_integer(src):
                bad = pc.sum(pc.cast(pc.and_(pc.is_valid(arr), pc.invert(pc.is_in(
                    arr, value_set=pa.array([0, 1], src)))), pa.int64())).as_py()
                if bad:
                    raise ShapeError(f"{where}: {bad:,} integer values other than 0 and 1 for a boolean")
            elif not pa.types.is_boolean(src):
                raise ShapeError(f"{where}: cannot cast {src} to bool")
            out = arr.cast(target)
        elif pa.types.is_floating(target):
            out = arr.cast(target, safe=False)
            if pa.types.is_floating(src) and pc.any(
                    pc.and_kleene(pc.is_finite(arr), pc.invert(pc.is_finite(out)))).as_py():
                raise ShapeError(f"{where}: values overflow {target}")
        else:   # integers: safe, so overflow and truncation raise
            if not (pa.types.is_integer(src) or pa.types.is_floating(src) or pa.types.is_boolean(src)
                    or pa.types.is_string(src) or pa.types.is_large_string(src)):
                raise ShapeError(f"{where}: cannot cast {src} to {target}")
            out = arr.cast(target, safe=True)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as e:
        raise ShapeError(f"{where}: cannot cast {src} to {target}: {e}") from e

    if not col.get("nullable", True) and out.null_count:
        raise ShapeError(f"{where}: {out.null_count:,} NULL values in a non-nullable column")
    return out


def _no_constant(name):
    raise ValueError(f"{name} is not JSON")


def _check_json(values, offset):
    """(row, error) of the first value of ``values`` (strings or None) that
    is not a JSON object (NaN and Infinity, which Python's json accepts,
    are not JSON), or None."""
    for i, s in enumerate(values):
        if s is None:
            continue
        try:
            v = json.loads(s, parse_constant=_no_constant)
        except ValueError as e:
            return offset + i, f"invalid JSON: {e}"
        if not isinstance(v, dict):
            return offset + i, f"JSON {type(v).__name__}, not an object"
    return None


def mpc_columns(table, schema=None):
    """The input columns the ``mpc`` step reads for delivered ``table``."""
    cols = (schema or delivery_schema())[table]
    names = [MPC_DERIVED.get((table, c["name"]), c["name"]) for c in cols]
    return list(dict.fromkeys(names))


def shape_batch(table, batch, columns):
    """The RecordBatch of delivered ``table`` (Felis ``columns``, in order)
    from input ``batch``. Raises ShapeError for a missing column or a
    failed cast (cast_column)."""
    present = set(batch.schema.names)
    arrays = []
    for col in columns:
        name = col["name"]
        src = MPC_DERIVED.get((table, name), name)
        if src not in present:
            raise ShapeError(f"{table}: the input lacks column {src!r}"
                             + (f" (for {name!r})" if src != name else ""))
        arrays.append(cast_column(f"{table}.{name}", batch.column(src), col))
    return pa.RecordBatch.from_arrays(arrays, schema=arrow_schema(columns))


def shape_mpc_table(table, src, dst, schema=None, workers=1, batch_rows=MPC_BATCH_ROWS, log=_log):
    """Shape MPC input ``src`` to delivered ``table`` at ``dst``
    (zstd Parquet): exactly delivery_schema()'s columns, in order, cast to
    their types (cast_column), with the MPC_DERIVED columns added and the
    input's other columns dropped; JSON columns must hold JSON objects.
    The rows keep the input's order. Written to a temporary file first, so
    a failure leaves no ``dst``. Returns the row count."""
    schema = schema or delivery_schema()
    columns = schema[table]
    pf = pq.ParquetFile(src)
    present = pf.schema_arrow.names
    need = mpc_columns(table, schema)
    missing = [c for c in need if c not in present]
    if missing:
        raise ShapeError(f"{table}: the input {src} lacks columns {missing}")
    dropped = [c for c in present if c not in need]
    log(f"{table}: {pf.metadata.num_rows:,} rows; {len(columns)} delivered columns; "
        f"dropped input columns {dropped}")
    json_cols = [i for i, c in enumerate(columns) if is_json_column(c)]

    tmp = f"{dst}.tmp"
    n = 0
    # (forkserver: forking this process, with Arrow threads, is unsafe)
    pool = (ProcessPoolExecutor(workers, mp_context=get_context("forkserver"))
            if json_cols and workers > 1 else None)
    pending = []

    def _settle(futs):
        for fut, name in futs:
            bad = fut.result()
            if bad is not None:
                raise ShapeError(f"{table}.{name}: row {bad[0]:,}: {bad[1]}")

    try:
        with pq.ParquetWriter(tmp, arrow_schema(columns), compression="zstd") as w:
            for batch in pf.iter_batches(batch_size=batch_rows, columns=need):
                out = shape_batch(table, batch, columns)
                futs = []
                for i in json_cols:
                    values = out.column(i).to_pylist()
                    if pool is None:
                        futs.append((_Done(_check_json(values, n)), columns[i]["name"]))
                    else:
                        step = -(-len(values) // workers)
                        futs += [(pool.submit(_check_json, values[k:k + step], n + k), columns[i]["name"])
                                 for k in range(0, len(values), step)]
                _settle(pending)          # (the previous batch's, while this one's run)
                pending = futs
                w.write_batch(out)
                n += out.num_rows
            _settle(pending)
    except BaseException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    finally:
        if pool is not None:
            pool.shutdown(cancel_futures=True)
    os.replace(tmp, dst)
    log(f"{table}: wrote {dst}: {n:,} rows")
    return n


class _Done:
    def __init__(self, value):
        self.value = value

    def result(self):
        return self.value


def step_mpc(inputs_dir, run_dir, workers=1):
    manifest = read_manifest(inputs_dir)
    schema = delivery_schema()
    out = Path(run_dir) / DELIVERY_DIR
    for table in MPC_TABLES:
        shape_mpc_table(table, input_path(inputs_dir, manifest, table), out / f"{table}.parquet", schema,
                        workers=workers)


# --------------------------------------------------------------------------
# Step check
# --------------------------------------------------------------------------

def have_ssobservation_validate():
    return (REPO_ROOT / "bench" / "ssobservation_validate.py").is_file()


def step_check(run_dir, log=_log):
    """The delivery check of every delivered table, and SSObservation's content
    checks; writes ``RUN_DIR/checks/<check>.txt`` and ``results.json``
    ({check: {status, report}}). Returns True if every check passed. The
    SSObservation checks need bench/ (a source checkout): without it they FAIL,
    as not available."""
    run_dir = Path(run_dir)
    delivery = run_dir / DELIVERY_DIR
    checks = run_dir / CHECKS_DIR
    checks.mkdir(parents=True, exist_ok=True)
    results = {}

    def record(name, ok, report_file):
        results[name] = dict(status="PASS" if ok else "FAIL", report=str(report_file.relative_to(run_dir)))
        log(f"{name}: {results[name]['status']} ({report_file})")
        _write_json(results, checks / CHECK_RESULTS)

    # SSObservation's content checks (bench/ssobservation_validate.py, when
    # present)
    if have_ssobservation_validate():
        for name in SSOBSERVATION_CHECKS:
            rep = checks / f"ssobservation-{name}.txt"
            cmd = [sys.executable, "-m", "bench.ssobservation_validate", name,
                   str(delivery), "--out", str(rep)]
            log("$ " + " ".join(cmd))
            sys.stdout.flush()
            r = subprocess.run(cmd, cwd=REPO_ROOT, stdout=sys.stdout, stderr=sys.stderr)
            if not rep.exists():
                rep.write_text(f"{' '.join(cmd)} exited {r.returncode} without a report\n")
            record(f"ssobservation:{name}", r.returncode == 0, rep)
    else:
        for name in SSOBSERVATION_CHECKS:
            rep = checks / f"ssobservation-{name}.txt"
            rep.write_text(f"FAIL: not available: {REPO_ROOT / 'bench' / 'ssobservation_validate.py'} not "
                           "found; run from a source checkout\n")
            record(f"ssobservation:{name}", False, rep)

    # The delivery check (ssp.delivery_check, WP H)
    per_table = check_delivery(delivery)
    for table in DELIVERY_TABLES:
        res = per_table.get(table)
        rep = checks / f"delivery-{table}.txt"
        if not res:
            rep.write_text(f"FAIL: no results for {table}\n")
            record(f"delivery:{table}", False, rep)
            continue
        ok = all(r.ok for r in res)
        rep.write_text(format_report({table: res}) + "\n")
        record(f"delivery:{table}", ok, rep)
    _write_json(results, checks / CHECK_RESULTS)
    return all(v["status"] == "PASS" for v in results.values())


# --------------------------------------------------------------------------
# The builder steps
# --------------------------------------------------------------------------

def builder_input(inputs_dir, run_dir, manifest, name):
    """The file the builders read for input ``name``: the delivered
    (shaped) table for the MPC tables, so that they see the schema's types
    whatever the source wrote, and build from exactly the tables delivered
    with them; the input file otherwise."""
    if name in MPC_TABLES:
        return Path(run_dir).resolve() / DELIVERY_DIR / f"{name}.parquet"
    return input_path(inputs_dir, manifest, name).resolve()


def _ssobservation_input_dir(inputs_dir, run_dir, manifest):
    """RUN_DIR/work/ssobservation/in: symlinks to the builder inputs
    (builder_input), with the names ssp-build-ssobservation expects."""
    d = Path(run_dir) / WORK_DIR / "ssobservation" / "in"
    d.mkdir(parents=True, exist_ok=True)
    for name in ("dia_sources", "obs_sbn", "mpc_orbits", "current_identifications",
                 "numbered_identifications"):
        link = d / f"{name}.parquet"
        if link.is_symlink() or link.exists():
            link.unlink()
        link.symlink_to(builder_input(inputs_dir, run_dir, manifest, name))
    return d


def _entry_point(module):
    """argv running ``module``'s main() (its console script; ssp.ssobject's
    ``__main__`` block is something else)."""
    return [sys.executable, "-c", f"import sys; from {module} import main; sys.exit(main())"]


def step_command(step, inputs_dir, run_dir, manifest, workers, options=None):
    """(argv, {produced: delivered}) of ``step``. ``produced`` is a file,
    moved to the delivered file; or (SSObservation) the builder's output
    directory, whose parts, sidecar and manifest move into the delivered
    directory (move_ssobservation). ``options``: the ssobservation step's
    ``part_rows`` and ``internal_columns`` (default: the contract's)."""
    options = options or {}
    inputs_dir, run_dir = Path(inputs_dir).resolve(), Path(run_dir).resolve()
    work = run_dir / WORK_DIR / step
    delivery = run_dir / DELIVERY_DIR
    me = [*_entry_point("ssp.sso_build"), str(inputs_dir), str(run_dir), "--workers", str(workers),
          "--run-step"]
    if step in ("mpc", "check"):
        return me + [step], {}
    if step == "ssobservation":
        d = _ssobservation_input_dir(inputs_dir, run_dir, manifest)
        out = work / "out"
        out.mkdir(parents=True, exist_ok=True)
        part_rows = options.get("part_rows", PART_ROWS_DEFAULT)
        internal = parse_internal_columns(options.get("internal_columns"))
        return ([*_entry_point("ssp.ssobservation"), "--input-dir", str(d), "--output-dir", str(out),
                 "--workers", str(workers), "--part-rows", str(part_rows),
                 "--internal-columns", ",".join(internal)],
                {out: delivery})
    if step == "ssobject":
        out = work / "ssobject.parquet"
        return ([*_entry_point("ssp.ssobject"), str(delivery / SSOBSERVATION_MANIFEST_FILE),
                 str(builder_input(inputs_dir, run_dir, manifest, "mpc_orbits")), "--output", str(out),
                 "--workers", str(workers)], {out: delivery / "SSObject.parquet"})
    if step == "nearbysso":
        out = work / "nearbysso.parquet"
        return ([*_entry_point("ssp.nearbysso.build"), "--dia",
                 str(input_path(inputs_dir, manifest, "ppdb_dia_sources")),
                 "--orbits", str(builder_input(inputs_dir, run_dir, manifest, "mpc_orbits")),
                 "--ssobject", str(delivery / "SSObject.parquet"), "--output", str(out),
                 "--workers", str(workers)], {out: delivery / "NearbySSO.parquet"})
    raise ValueError(f"unknown step {step!r}")


#: Runs argv[2:] and writes its exit code and rusage, as JSON, to argv[1].
#: The step runs as this launcher's child, not the driver's: a child exec'd
#: straight from the driver would inherit the driver's peak RSS as its own.
_LAUNCHER = """
import json, os, subprocess, sys
p = subprocess.Popen(sys.argv[2:])
_, status, ru = os.wait4(p.pid, 0)
code = os.waitstatus_to_exitcode(status)
json.dump(dict(code=code, maxrss_kb=ru.ru_maxrss), open(sys.argv[1], "w"))
sys.exit(code if code >= 0 else 128 - code)
"""


def run_logged(cmd, log_path, env=None):
    """Run ``cmd`` with its stdout and stderr to ``log_path``; return
    (exit code, wall s, max RSS GB). The max RSS is the peak RSS of the
    largest single process (``cmd`` or one of its descendants, e.g. a pool
    worker), not the step's total: the report's ``max_rss_gb``."""
    env = dict(os.environ if env is None else env)
    env.setdefault("OMP_NUM_THREADS", "1")
    rusage = f"{log_path}.rusage.json"
    if os.path.exists(rusage):
        os.remove(rusage)
    with open(log_path, "w") as log:
        log.write(f"# {_utcnow()} $ {' '.join(map(str, cmd))}\n")
        log.flush()
        t0 = time.perf_counter()
        code = subprocess.run([sys.executable, "-c", _LAUNCHER, rusage, *map(str, cmd)], stdout=log,
                              stderr=subprocess.STDOUT, cwd=REPO_ROOT, env=env).returncode
        wall = time.perf_counter() - t0
        try:
            with open(rusage) as f:
                ru = json.load(f)
            os.remove(rusage)
            code, rss = ru["code"], ru["maxrss_kb"] / 2**20
        except (OSError, ValueError):
            rss = None
        log.write(f"# exit {code}, wall {wall:.1f} s, max RSS "
                  + ("unknown" if rss is None else f"{rss:.2f} GB") + "\n")
    return code, wall, rss


# --------------------------------------------------------------------------
# The driver
# --------------------------------------------------------------------------

def table_entry(run_dir, table):
    """The report's ``tables`` entry of ``table`` (REPORT_FIELDS): {file,
    rows, md5, bytes}; for SSObservation {manifest, parts, sidecar, rows,
    bytes}, the rows and bytes totals over the parts (each part's md5 is in
    the manifest). Paths are relative to RUN_DIR."""
    run_dir = Path(run_dir)
    d = run_dir / DELIVERY_DIR
    if table == PARTITIONED:
        names = ssobservation_files(d)
        parts = [d / n for n in names[:-2]]
        return dict(manifest=str((d / names[-1]).relative_to(run_dir)), manifest_md5=_md5(d / names[-1]),
                    parts=[str(p.relative_to(run_dir)) for p in parts],
                    sidecar=str((d / names[-2]).relative_to(run_dir)),
                    rows=sum(pq.ParquetFile(p).metadata.num_rows for p in parts),
                    bytes=sum(p.stat().st_size for p in parts))
    path = d / f"{table}.parquet"
    return dict(file=str(path.relative_to(run_dir)), rows=pq.ParquetFile(path).metadata.num_rows,
                md5=_md5(path), bytes=path.stat().st_size)


def _deliverable(report):
    """Every step ok, every EXPECTED_CHECKS check (and any other) PASS, and
    every delivered table present."""
    checks = report.get("checks") or {}
    return (all(report["steps"].get(s, {}).get("status") == "ok" for s in BUILD_STEPS)
            and all(checks.get(c, {}).get("status") == "PASS" for c in EXPECTED_CHECKS)
            and all(c.get("status") == "PASS" for c in checks.values())
            and all(t in report["tables"] for t in DELIVERY_TABLES))


def _clear_step_outputs(run_dir, steps):
    for step in steps:
        for table in STEP_TABLES[step]:
            for p in delivered_paths(run_dir, table):
                p.unlink()
        for p in (Path(run_dir) / WORK_DIR / step, *(
                [Path(run_dir) / CHECKS_DIR] if step == "check" else [])):
            if p.exists():
                shutil.rmtree(p)
        log = Path(run_dir) / LOGS_DIR / f"{step}.log"
        if log.exists():
            log.unlink()


def _remove_strays(run_dir, log):
    """Remove anything in RUN_DIR/delivery other than the delivered tables
    (for SSObservation: its manifest and the files it lists)."""
    d = Path(run_dir) / DELIVERY_DIR
    if not d.is_dir():
        return
    keep = {f"{t}.parquet" for t in DELIVERY_TABLES if t != PARTITIONED}
    try:
        keep.update(ssobservation_files(d))
    except ValueError:
        pass
    for p in sorted(d.iterdir()):
        if p.name not in keep:
            log(f"WARNING: removing {p}, which is not a delivered table")
            shutil.rmtree(p) if p.is_dir() and not p.is_symlink() else p.unlink()


#: The report fields stage 3 writes, carried over by a rebuild.
UPLOAD_FIELDS = ("upload", "uploads")


def real_uploads(report):
    """The upload records of ``report`` that are not dry runs."""
    recs = [report.get("upload")] + list(report.get("uploads") or [])
    return [r for r in recs if isinstance(r, dict) and r.get("dry_run") is not True]


def build(inputs_dir, run_dir, from_step=None, workers=1, force_rebuild=False, allow_mixed_commits=False,
          part_rows=None, internal_columns=None, log=_log):
    """Run stage 2 (see the module docstring); return the report. A failed
    step or invalid inputs do not raise: see the report's ``deliverable``
    (and, for the inputs, ``error``).

    Raises ValueError, changing nothing, if RUN_DIR was uploaded (a real
    upload in its report) and not ``force_rebuild`` (which keeps the upload
    history); or if ``from_step`` cannot rerun this RUN_DIR: no earlier
    report, other inputs, an earlier step that failed, a kept table missing
    or changed since, or kept steps built by another commit (unless
    ``allow_mixed_commits``, recorded in the report as ``mixed_commits``);
    or if ``part_rows`` or ``internal_columns`` (the ssobservation step's
    options; None: the contract's defaults, PART_ROWS_DEFAULT and
    SSOBSERVATION_INTERNAL_DEFAULT) are invalid, or are given with a
    ``from_step`` that keeps an ssobservation step built with others.
    """
    inputs_dir, run_dir = Path(inputs_dir), Path(run_dir)
    if from_step is not None and from_step not in BUILD_STEPS:
        raise ValueError(f"--from: unknown step {from_step!r} (one of {', '.join(BUILD_STEPS)})")
    if part_rows is not None and (isinstance(part_rows, bool) or not isinstance(part_rows, int)
                                  or part_rows < 1):
        raise ValueError(f"--part-rows {part_rows!r}: must be a positive integer")
    options = dict(part_rows=PART_ROWS_DEFAULT if part_rows is None else part_rows,
                   internal_columns=parse_internal_columns(internal_columns))
    report_path = run_dir / REPORT_FILE
    start = BUILD_STEPS.index(from_step) if from_step else 0
    previous = None
    if report_path.exists():
        try:
            with open(report_path) as f:
                previous = json.load(f)
            if not isinstance(previous, dict):
                raise ValueError("not a JSON object")
        except ValueError as e:
            if start > 0 or not force_rebuild:
                raise ValueError(f"cannot read {report_path} ({e}): it may record an upload; "
                                 "use --force-rebuild to build over it anyway") from e
            previous = None
    if start > 0 and previous is None:
        raise ValueError(f"--from {from_step}: no {report_path} of an earlier run")
    if previous is not None and real_uploads(previous) and not force_rebuild:
        raise ValueError(f"{run_dir} was uploaded (see {report_path}): a rebuild would replace the "
                         "uploaded delivery; build into a new RUN_DIR, or use --force-rebuild")
    run_dir.mkdir(parents=True, exist_ok=True)

    commit = ssp_tools_commit()
    report = dict(inputs=None, ssp_tools_commit=commit,
                  steps={s: dict(status="skipped") for s in BUILD_STEPS}, tables={}, checks={},
                  deliverable=False)
    for k in UPLOAD_FIELDS:            # (stage 3's history survives a rebuild)
        if previous is not None and k in previous:
            report[k] = previous[k]

    def save():
        report["deliverable"] = _deliverable(report)
        _write_json(report, report_path)

    try:
        report["inputs"] = read_manifest(inputs_dir)
        manifest = validate_manifest(inputs_dir, workers=workers)
        report["inputs"] = manifest
        report["input_paths"] = {n: str(input_path(inputs_dir, manifest, n).resolve()) for n in INPUT_FILES}
    except (ManifestError, OSError) as e:
        report["error"] = str(e)
        log(f"ERROR: {e}")
        save()
        return report

    if start > 0:
        _check_from(previous, manifest, run_dir, from_step, commit, allow_mixed_commits, report)
        _check_kept_options(previous, from_step, part_rows, internal_columns, options)
        for s in BUILD_STEPS[:start]:
            report["steps"][s] = previous["steps"][s]
            for t in STEP_TABLES[s]:
                report["tables"][t] = previous["tables"][t]
    else:
        _clear_step_outputs(run_dir, BUILD_STEPS)
    _clear_step_outputs(run_dir, BUILD_STEPS[start:])
    _remove_strays(run_dir, log)
    for d in (DELIVERY_DIR, LOGS_DIR, WORK_DIR):
        (run_dir / d).mkdir(parents=True, exist_ok=True)
    save()

    for step in BUILD_STEPS[start:]:
        log_path = run_dir / LOGS_DIR / f"{step}.log"
        entry = dict(status="running", started_utc=_utcnow(), wall_s=None, max_rss_gb=None,
                     log=str(log_path.relative_to(run_dir)), ssp_tools_commit=commit)
        if step == "ssobservation":
            entry.update(options)
        report["steps"][step] = entry
        save()
        (run_dir / WORK_DIR / step).mkdir(parents=True, exist_ok=True)
        log(f"[{step}] started {entry['started_utc']}; log {log_path}")
        error = None
        try:
            if step == "check":
                error = _inputs_changed(inputs_dir, manifest, workers)
            if error is None:
                cmd, moves = step_command(step, inputs_dir, run_dir, manifest, workers, options)
                code, wall, rss = run_logged(cmd, log_path)
                entry.update(wall_s=round(wall, 1), max_rss_gb=None if rss is None else round(rss, 3))
                if code != 0:
                    error = f"exited {code}; see {log_path}"
                else:
                    for src, dst in moves.items():
                        if src.is_dir():        # (the partitioned SSObservation)
                            try:
                                move_ssobservation(src, dst)
                            except ValueError as e:
                                error = f"did not write a complete SSObservation: {e}"
                                break
                            continue
                        if not src.exists():
                            error = f"did not write {src}"
                            break
                        os.replace(src, dst)
            if error is None:
                for t in STEP_TABLES[step]:
                    report["tables"][t] = table_entry(run_dir, t)
        except Exception as e:      # (a step's setup, or a missing output)
            error = f"{type(e).__name__}: {e}"
        if step == "check":
            res = run_dir / CHECKS_DIR / CHECK_RESULTS
            if res.exists():        # (not when the inputs changed: the checks did not run)
                with open(res) as f:
                    report["checks"] = json.load(f)
        entry["status"] = "failed" if error else "ok"
        if error:
            entry["error"] = error
            for t in STEP_TABLES[step]:      # (a failed step delivers nothing)
                report["tables"].pop(t, None)
                for p in delivered_paths(run_dir, t):
                    p.unlink()
        log(f"[{step}] {entry['status']}" + (f": {error}" if error else "")
            + f" (wall {entry['wall_s']} s, max RSS {entry['max_rss_gb']} GB)")
        save()
        if error:
            break
    log(f"deliverable: {report['deliverable']}; report {report_path}")
    return report


def _inputs_changed(inputs_dir, manifest, workers):
    """None if every input still has its manifest md5 (hashed again, after
    the builders, so the check covers what they read), else the error."""
    now = input_md5s(inputs_dir, manifest, workers)
    changed = [n for n, md5 in now.items() if md5 != manifest["files"][n]["md5"]]
    if changed:
        return f"inputs changed during the build: {changed} no longer match the manifest's md5"
    return None


def _check_from(previous, manifest, run_dir, from_step, commit, allow_mixed_commits, report):
    """Refuse (ValueError) a --from rerun that would keep outputs it cannot
    trust; see build."""
    start = BUILD_STEPS.index(from_step)
    old = (previous.get("inputs") or {}).get("files") or {}
    if {k: v.get("md5") for k, v in old.items()} != {k: v["md5"] for k, v in manifest["files"].items()}:
        raise ValueError(f"--from {from_step}: the inputs differ from those of the run in {run_dir}")
    mixed = {}
    for s in BUILD_STEPS[:start]:
        entry = previous.get("steps", {}).get(s, {})
        if entry.get("status") != "ok":
            raise ValueError(f"--from {from_step}: step {s} did not succeed in the earlier run")
        for t in STEP_TABLES[s]:
            if t == PARTITIONED:
                _check_kept_ssobservation(previous, run_dir, from_step, s, t)
                continue
            path = run_dir / DELIVERY_DIR / f"{t}.parquet"
            if not path.exists():
                raise ValueError(f"--from {from_step}: {t}, from step {s}, is missing")
            want = (previous.get("tables") or {}).get(t, {}).get("md5")
            if _md5(path) != want:
                raise ValueError(f"--from {from_step}: {path} has changed since step {s} wrote it "
                                 f"(md5 {want} in the report)")
        built_by = entry.get("ssp_tools_commit", previous.get("ssp_tools_commit"))
        if built_by != commit:
            mixed[s] = built_by
    if mixed:
        if not allow_mixed_commits:
            raise ValueError(f"--from {from_step}: the kept steps were built by other code ({mixed}; this is "
                             f"{commit}); rerun them, or use --allow-mixed-commits")
        report["mixed_commits"] = dict(mixed, **{s: commit for s in BUILD_STEPS[start:]})


def _check_kept_ssobservation(previous, run_dir, from_step, step, table):
    """The kept partitioned SSObservation is complete, its manifest has the
    md5 the report recorded (manifest_md5), each part and the sidecar has
    its manifest md5, and it is what the report recorded."""
    d = run_dir / DELIVERY_DIR
    try:
        ssobservation_files(d)
    except ValueError as e:
        raise ValueError(f"--from {from_step}: {table}, from step {step}, is missing: {e}") from e
    mpath = d / SSOBSERVATION_MANIFEST_FILE
    want_md5 = ((previous.get("tables") or {}).get(table) or {}).get("manifest_md5")
    if _md5(mpath) != want_md5:
        raise ValueError(f"--from {from_step}: {mpath} has changed since step {step} wrote it "
                         f"(manifest_md5 {want_md5} in the report)")
    with open(mpath) as f:
        m = json.load(f)
    for entry in [*m["parts"], m["sidecar"]]:
        path = d / entry["file"]
        if _md5(path) != entry.get("md5"):
            raise ValueError(f"--from {from_step}: {path} has changed since step {step} wrote it "
                             f"(md5 {entry.get('md5')} in the manifest)")
    want = (previous.get("tables") or {}).get(table)
    have = table_entry(run_dir, table)
    if have != want:
        raise ValueError(f"--from {from_step}: {table} has changed since step {step} wrote it: "
                         f"{have} now, {want} in the report")


def _check_kept_options(previous, from_step, part_rows, internal_columns, options):
    """Refuse ssobservation options given with a --from that keeps an
    ssobservation step built with other ones: they would not apply."""
    if BUILD_STEPS.index(from_step) <= BUILD_STEPS.index("ssobservation"):
        return
    kept = previous["steps"]["ssobservation"]
    for name, given in (("part_rows", part_rows), ("internal_columns", internal_columns)):
        if given is not None and kept.get(name) != options[name]:
            raise ValueError(f"--from {from_step}: the kept ssobservation step was built with {name} "
                             f"{kept.get(name)!r}, not {options[name]!r}; rerun from ssobservation")


def _default_workers():
    return max(1, min(32, len(os.sched_getaffinity(0))))


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="ssp-build-sso",
        description="Stage 2 of the SSO delivery: build the PPDB Solar System tables "
                    f"({', '.join(DELIVERY_TABLES)}) from the input files in INPUTS_DIR "
                    "(stage 1's, or any source honouring ssp/delivery_contract.py).",
        epilog=f"Steps: {', '.join(BUILD_STEPS)}. Writes RUN_DIR/{DELIVERY_DIR}/<Table>.parquet "
               f"(SSObservation: {PART_GLOB}, {SSOBSERVATION_MANIFEST_FILE} and {SIDECAR_FILE}), "
               f"RUN_DIR/{LOGS_DIR}/<step>.log, RUN_DIR/{CHECKS_DIR}/ and RUN_DIR/{REPORT_FILE}. "
               "Needs SSP_ASSIST_PLANETS and SSP_ASSIST_ASTEROIDS for the ssobservation and nearbysso "
               "steps. Exits 0 only if the delivery is deliverable.",
    )
    parser.add_argument("inputs_dir", metavar="INPUTS_DIR")
    parser.add_argument("run_dir", metavar="RUN_DIR")
    parser.add_argument("--from", dest="from_step", choices=BUILD_STEPS, default=None,
                        help="Rerun from this step, keeping the earlier steps' outputs")
    parser.add_argument("--workers", type=int, default=_default_workers(),
                        help="Worker processes for each step (default: min(32, usable CPUs))")
    parser.add_argument("--force-rebuild", action="store_true",
                        help="Build into a RUN_DIR that stage 3 has uploaded (its upload history is kept)")
    parser.add_argument("--allow-mixed-commits", action="store_true",
                        help="With --from, keep steps built by another ssp-tools commit (recorded in "
                             "the report as mixed_commits)")
    parser.add_argument("--part-rows", type=int, default=None, metavar="N",
                        help="SSObservation parts: close a part at the first object boundary at or after N "
                             f"rows (default: {PART_ROWS_DEFAULT:,})")
    parser.add_argument("--internal-columns", default=None, metavar="A,B,...",
                        help="SSObservation columns written to the sidecar "
                             f"({SIDECAR_FILE}, not uploaded) instead of the delivered table; '' for none "
                             f"(default: {','.join(SSOBSERVATION_INTERNAL_DEFAULT)}; possible: "
                             f"{', '.join(SSOBSERVATION_INTERNAL_DTYPE)})")
    parser.add_argument("--run-step", choices=("mpc", "check"), default=None, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.workers < 1:
        parser.error("--workers must be at least 1")
    if args.part_rows is not None and args.part_rows < 1:
        parser.error("--part-rows must be at least 1")
    try:
        parse_internal_columns(args.internal_columns)
    except ValueError as e:
        parser.error(f"--internal-columns: {e}")

    if args.run_step == "mpc":
        step_mpc(args.inputs_dir, args.run_dir, workers=args.workers)
        return 0
    if args.run_step == "check":
        return 0 if step_check(args.run_dir) else 1

    try:
        report = build(args.inputs_dir, args.run_dir, from_step=args.from_step, workers=args.workers,
                       force_rebuild=args.force_rebuild, allow_mixed_commits=args.allow_mixed_commits,
                       part_rows=args.part_rows, internal_columns=args.internal_columns)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2
    return 0 if report["deliverable"] else 1


if __name__ == "__main__":
    sys.exit(main())
