"""The delivery check: every delivered PPDB Solar System table against the
schema (ssp.delivery_contract, "the delivery check").

A black box: it reads only the Parquet files and the vendored Felis YAML
(ppdb.yaml, resolved into sso_base.yaml by delivery_schema()), never the
builders. Per table it checks

  * the columns: exactly the schema's, in its order;
  * the types: each Arrow type compatible with the Felis datatype;
  * NULLs: none in ``nullable: false`` columns;
  * the primary key (ppdb.yaml's ``primaryKey``): unique and non-NULL;
  * char lengths: string values no longer than the column's ``length``.

SSObservation is delivered as parts plus a manifest and a sidecar
(ssp.ssobservation_contract, "The partitioned delivery"):
check_ssobservation_parts checks the partitioning, the manifest and the
sidecar, and check_table runs on every part.

Columns are read one at a time (per part), to bound memory; everything is
Arrow.

    python -m ssp.delivery_check DELIVERY_DIR [--tables T ...] [--schema-dir D]

Exit status 0 only if every result is ok.
"""

from __future__ import annotations

import argparse
import datetime
import fnmatch
import hashlib
import json
import re
import sys
import time
from functools import lru_cache
from pathlib import Path
from typing import NamedTuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import yaml

from ssp.delivery_contract import DELIVERY_TABLES, SCHEMA_DIR, delivery_schema
from ssp.ssobservation_contract import (
    MANIFEST_FORMAT_VERSION,
    PART_FIELDS,
    PART_FILE_FORMAT,
    PART_GLOB,
    SIDECAR_FIELDS,
    SIDECAR_FILE,
    SIDECAR_KEY,
    SSOBSERVATION_DICTIONARY,
    SSOBSERVATION_INTERNAL_DTYPE,
    SSOBSERVATION_MANIFEST_FIELDS,
    SSOBSERVATION_MANIFEST_FILE,
    SSOBSERVATION_SORT,
)


class CheckResult(NamedTuple):
    name: str
    ok: bool
    detail: str


#: Felis datatype -> the Arrow type it must be (the contract's map). The
#: string-like datatypes are handled apart (STRING_TYPES); timestamp accepts
#: any Arrow timestamp.
FELIS_ARROW = {
    "long": pa.int64(), "int": pa.int32(), "short": pa.int16(),
    "float": pa.float32(), "double": pa.float64(), "boolean": pa.bool_(),
}
#: Felis datatypes stored as Arrow string, large_string, or a dictionary of
#: either. The contract names char and string; text (unbounded, used by
#: mpc_orbits.mpc_orb_jsonb and numbered_identifications) is the same thing.
STRING_TYPES = ("char", "string", "text")
TIMESTAMP_TYPES = ("timestamp",)

#: How many offending values to quote in a detail.
N_EXAMPLES = 3


def _is_string(t):
    if pa.types.is_dictionary(t):
        t = t.value_type
    return pa.types.is_string(t) or pa.types.is_large_string(t)


def type_ok(felis, t):
    """``(ok, problem)``: is Arrow type ``t`` compatible with Felis datatype
    ``felis``? ``problem`` is None, or a reason for an unknown datatype."""
    if felis in STRING_TYPES:
        return _is_string(t), None
    if felis in TIMESTAMP_TYPES:
        return pa.types.is_timestamp(t), None
    want = FELIS_ARROW.get(felis)
    if want is None:
        return False, f"Felis datatype {felis!r} has no Arrow mapping in the delivery contract"
    return t == want, None


def _want(felis):
    if felis in STRING_TYPES:
        return "string"
    if felis in TIMESTAMP_TYPES:
        return "timestamp"
    return str(FELIS_ARROW.get(felis, "?"))


@lru_cache(maxsize=8)
def _schema(schema_dir):
    return delivery_schema(schema_dir)


@lru_cache(maxsize=8)
def primary_keys(schema_dir):
    """{Table: [key column, ...]} from ppdb.yaml's primaryKey ('#Table.col',
    or a list of them), for the delivery tables."""
    ppdb = yaml.safe_load(open(Path(schema_dir) / "ppdb.yaml"))
    out = {}
    for t in ppdb["tables"]:
        if t["name"] not in DELIVERY_TABLES:
            continue
        pk = t.get("primaryKey")
        if pk is None:
            out[t["name"]] = []
            continue
        refs = [pk] if isinstance(pk, str) else list(pk)
        out[t["name"]] = [r.lstrip("#").split(".", 1)[-1] for r in refs]
    return out


def _decoded(col):
    """A ChunkedArray with any dictionary encoding removed."""
    if pa.types.is_dictionary(col.type):
        return pa.chunked_array([c.dictionary_decode() for c in col.chunks],
                                type=col.type.value_type)
    return col


def _max_length(col):
    """The longest string (in characters) in ``col``, or None if all NULL.
    For a dictionary column the dictionaries are measured first; only if one
    holds an over-long entry are the values themselves decoded (an unused
    dictionary entry is not a delivered value)."""
    if pa.types.is_dictionary(col.type):
        m = max((pc.max(pc.utf8_length(c.dictionary)).as_py() or 0 for c in col.chunks
                 if len(c.dictionary)), default=None)
        return m, (lambda: _max_length(_decoded(col))[0])
    m = pc.max(pc.utf8_length(col)).as_py() if len(col) else None
    return m, None


def _fmt_names(names, n=8):
    s = ", ".join(names[:n])
    return s + (f", ... ({len(names)} in all)" if len(names) > n else "")


def check_table(table, parquet_path, schema_dir=SCHEMA_DIR):
    """Check one delivered table's Parquet file against the schema; a list
    of CheckResult (name, ok, detail)."""
    schema_dir = str(schema_dir)
    cols = _schema(schema_dir)[table]
    spec = {c["name"]: c for c in cols}
    names = [c["name"] for c in cols]
    try:
        pf = pq.ParquetFile(parquet_path)
    except Exception as e:  # missing, unreadable, not Parquet
        return [CheckResult("file", False, f"{parquet_path}: cannot read ({type(e).__name__}: {e})")]
    arrow = pf.schema_arrow
    nrows = pf.metadata.num_rows
    res = [CheckResult("file", True, f"{parquet_path}: {nrows:,} rows, {len(arrow)} columns")]

    # columns: exactly the schema's, in order
    got = arrow.names
    if got == names:
        res.append(CheckResult("columns", True, f"{len(names)} columns, in schema order"))
    else:
        missing = [c for c in names if c not in got]
        extra = [c for c in got if c not in spec]
        dups = sorted({c for c in got if got.count(c) > 1})
        parts = []
        if missing:
            parts.append(f"missing {_fmt_names(missing)}")
        if extra:
            parts.append(f"extra {_fmt_names(extra)}")
        if dups:
            parts.append(f"duplicated {_fmt_names(dups)}")
        common_got = [c for c in got if c in spec]
        common_want = [c for c in names if c in got]
        if common_got != common_want:
            k = next(k for k, (a, b) in enumerate(zip(common_got, common_want)) if a != b)
            parts.append(f"out of order: first at {common_got[k]!r} where the schema has "
                         f"{common_want[k]!r}")
        res.append(CheckResult("columns", False, "; ".join(parts) or "differ from the schema"))

    present = [c for c in names if c in got and got.count(c) == 1]

    # types
    bad = []
    for c in present:
        felis = spec[c]["datatype"]
        t = arrow.field(c).type
        ok, problem = type_ok(felis, t)
        if not ok:
            bad.append(f"{c}: {problem}" if problem
                       else f"{c}: {t}, want {_want(felis)} (Felis {felis})")
    res.append(CheckResult("types", not bad, "; ".join(bad) if bad
                           else f"{len(present)} columns compatible"))

    # NULLs and char lengths, one column at a time
    nonnull = [c for c in present if spec[c].get("nullable", True) is False]
    sized = [c for c in present if spec[c].get("length") and spec[c]["datatype"] in STRING_TYPES
             and _is_string(arrow.field(c).type)]
    with_nulls, too_long = [], []
    for c in sorted(set(nonnull) | set(sized), key=names.index):
        col = pf.read(columns=[c])[c]
        if c in nonnull and col.null_count:
            with_nulls.append(f"{c} ({col.null_count:,} NULLs)")
        if c in sized:
            L = spec[c]["length"]
            m, exact = _max_length(col)
            if m is not None and m > L and exact is not None:
                m = exact()
            if m is not None and m > L:
                vals = _decoded(col)
                over = pc.filter(vals, pc.greater(pc.utf8_length(vals), L))
                ex = [v[:40] for v in over.slice(0, N_EXAMPLES).to_pylist()]
                too_long.append(f"{c} ({len(over):,} values over {L}, max {m}; e.g. {ex})")
        del col
    res.append(CheckResult("nulls", not with_nulls,
                           "NULLs in nullable: false " + "; ".join(with_nulls) if with_nulls
                           else f"no NULLs in the {len(nonnull)} nullable: false columns"))
    res.append(CheckResult("char lengths", not too_long, "; ".join(too_long) if too_long
                           else f"{len(sized)} sized string columns within length"))

    res.append(_check_key(pf, primary_keys(schema_dir).get(table, []), got, nrows))
    return res


def _check_key(pf, keys, got, nrows):
    if not keys:
        return CheckResult("primary key", False, "the schema gives no primaryKey")
    label = ", ".join(keys)
    absent = [k for k in keys if k not in got]
    if absent:
        return CheckResult("primary key", False, f"({label}): column(s) {absent} not in the file")
    t = pf.read(columns=keys)
    t = pa.table({k: _decoded(t[k]) for k in keys})
    problems = []
    nulls = {k: t[k].null_count for k in keys if t[k].null_count}
    if nulls:
        problems.append("NULLs in " + ", ".join(f"{k} ({n:,})" for k, n in nulls.items()))
        t = t.drop_null()
    counts = t.group_by(keys).aggregate([([], "count_all")])
    ndup = len(t) - len(counts)
    if ndup:
        dup = counts.filter(pc.greater(counts["count_all"], 1))
        ex = [tuple(r[k] for k in keys) if len(keys) > 1 else r[keys[0]]
              for r in dup.slice(0, N_EXAMPLES).to_pylist()]
        problems.append(f"{ndup:,} duplicate rows over {len(dup):,} key values, e.g. {ex}")
    if problems:
        return CheckResult("primary key", False, f"({label}): " + "; ".join(problems))
    return CheckResult("primary key", True, f"({label}): {nrows:,} unique, non-NULL")


# --------------------------------------------------------------------------
# SSObservation: the parts, the manifest and the sidecar
# --------------------------------------------------------------------------

def _np_dtype_ok(np_dtype, t):
    """Is Arrow type ``t`` the sidecar's representation of NumPy dtype
    string ``np_dtype`` (as in SSObservationDtype: '<U16', '|b1', ...)?"""
    kind = np_dtype.lstrip("<>|=")[0]
    size = np_dtype.lstrip("<>|=")[1:]
    if kind == "U":
        return _is_string(t)
    if kind == "b":
        return pa.types.is_boolean(t)
    if kind in "iuf":
        want = {"i": {"1": pa.int8(), "2": pa.int16(), "4": pa.int32(), "8": pa.int64()},
                "u": {"1": pa.uint8(), "2": pa.uint16(), "4": pa.uint32(), "8": pa.uint64()},
                "f": {"4": pa.float32(), "8": pa.float64()}}[kind].get(size)
        return t == want
    return False


def file_md5(path, block=1 << 24):
    """The hex md5 of a file, read in blocks."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(block), b""):
            h.update(b)
    return h.hexdigest()


def _is_int(x):
    return isinstance(x, int) and not isinstance(x, bool)


def _quote(items, n=N_EXAMPLES):
    items = list(items)
    s = "; ".join(str(x) for x in items[:n])
    return s + (f"; ... ({len(items)} in all)" if len(items) > n else "")


def _sort_violations(prev, sid, mjd, obsid):
    """Positions i (in the table [prev] + rows) where row i+1 sorts before
    row i in SSOBSERVATION_SORT order, NULL ssObjectId last. ``prev`` is
    the previous part's last (sid, mjd, obsid), or None. Returns the
    indices into the rows of this part of the row that is out of place."""
    sid = pa.chunked_array([sid]) if isinstance(sid, pa.Array) else sid
    if prev is not None:
        sid = pa.chunked_array([pa.array([prev[0]], pa.int64()), *[c.cast(pa.int64()) for c in sid.chunks]])
        mjd = pa.chunked_array([pa.array([prev[1]], pa.float64()),
                                *[c.cast(pa.float64()) for c in _decoded(mjd).chunks]])
        obsid = pa.chunked_array([pa.array([prev[2]], pa.string()),
                                  *[c.cast(pa.string()) for c in _decoded(obsid).chunks]])
    else:
        mjd, obsid = _decoded(mjd), _decoded(obsid)
    n = len(sid)
    if n < 2:
        return np.zeros(0, dtype=np.int64)
    isnull = np.asarray(sid.is_null().to_numpy(zero_copy_only=False), dtype=bool)
    s = pc.fill_null(sid, 0).to_numpy()
    m = np.asarray(mjd.to_numpy(), dtype=np.float64) if mjd.null_count == 0 else \
        np.asarray(pc.fill_null(mjd, np.nan).to_numpy(), dtype=np.float64)
    o = obsid.combine_chunks() if isinstance(obsid, pa.ChunkedArray) else obsid
    a_null, b_null = isnull[:-1], isnull[1:]
    a_s, b_s = s[:-1], s[1:]
    a_m, b_m = m[:-1], m[1:]
    o_lt = np.asarray(pc.fill_null(pc.less(o.slice(1), o.slice(0, n - 1)), False)
                      .to_numpy(zero_copy_only=False), dtype=bool)
    same_sid = (a_null & b_null) | (~a_null & ~b_null & (a_s == b_s))
    bad = (a_null & ~b_null) | (~a_null & ~b_null & (b_s < a_s))
    bad |= same_sid & ((b_m < a_m) | ((b_m == a_m) & o_lt))
    idx = np.flatnonzero(bad) + 1                  # the later row of each bad pair
    return idx - (1 if prev is not None else 0)


#: The manifest's fixed schema entry (sso_base.yaml of sdm_schemas
#: tickets/DM-55375; its md5 is not recomputed here).
MANIFEST_SCHEMA_SOURCE = "lsst/sdm_schemas tickets/DM-55375"
MANIFEST_SCHEMA_FILE = "sso_base.yaml"

_HEX32 = re.compile(r"[0-9a-f]{32}")
_HEX = re.compile(r"[0-9a-f]{7,64}")


def _is_md5(x):
    return isinstance(x, str) and _HEX32.fullmatch(x) is not None


def _is_utc_iso(x):
    """An ISO 8601 time in UTC ('...Z' or '+00:00')."""
    if not isinstance(x, str):
        return False
    try:
        t = datetime.datetime.fromisoformat(x[:-1] + "+00:00" if x.endswith("Z") else x)
    except ValueError:
        return False
    return t.utcoffset() == datetime.timedelta(0)


def _manifest_problems(m):
    """Problems with the manifest's fields and their types and values (a
    list of strings). Fields the contract does not name are allowed, at
    every level, so that a later format may add some (forward
    compatibility); the fields it names must be present and valid."""
    probs = []
    missing = [f for f in SSOBSERVATION_MANIFEST_FIELDS if f not in m]
    if missing:
        probs.append(f"missing fields {missing}")
    want = {"table": "SSObservation", "format_version": MANIFEST_FORMAT_VERSION,
            "partition_key": "ssObjectId", "sort": list(SSOBSERVATION_SORT)}
    for k, v in want.items():
        if k in m and (m[k] != v or type(m[k]) is not type(v)):
            probs.append(f"{k} is {m[k]!r}, want {v!r}")
    if "part_rows" in m and not (_is_int(m["part_rows"]) and m["part_rows"] > 0):
        probs.append(f"part_rows {m['part_rows']!r} is not a positive integer")
    if "rows" in m and not (_is_int(m["rows"]) and m["rows"] >= 0):
        probs.append(f"rows {m['rows']!r} is not a non-negative integer")
    if "created_utc" in m and not _is_utc_iso(m["created_utc"]):
        probs.append(f"created_utc {m['created_utc']!r} is not an ISO 8601 UTC time")
    if "ssp_tools_commit" in m and m["ssp_tools_commit"] is not None and \
            not (isinstance(m["ssp_tools_commit"], str) and _HEX.fullmatch(m["ssp_tools_commit"])):
        probs.append(f"ssp_tools_commit {m['ssp_tools_commit']!r} is neither null nor a hex commit")
    if "schema" in m:
        sch = m["schema"]
        if not isinstance(sch, dict) or not {"source", "file", "md5"} <= set(sch):
            probs.append(f"schema {sch!r} lacks source/file/md5")
        else:
            if sch["source"] != MANIFEST_SCHEMA_SOURCE or sch["file"] != MANIFEST_SCHEMA_FILE:
                probs.append(f"schema source/file {sch['source']!r}/{sch['file']!r}, want "
                             f"{MANIFEST_SCHEMA_SOURCE!r}/{MANIFEST_SCHEMA_FILE!r}")
            if sch["md5"] is not None and not _is_md5(sch["md5"]):
                probs.append(f"schema md5 {sch['md5']!r} is neither null nor 32 hex digits")
    if "parts" in m and (not isinstance(m["parts"], list) or not m["parts"]):
        probs.append("parts is not a non-empty list")
    elif isinstance(m.get("parts"), list):
        for k, p in enumerate(m["parts"]):
            miss = [f for f in PART_FIELDS if not isinstance(p, dict) or f not in p]
            if miss:
                probs.append(f"parts[{k}] lacks {miss}")
                continue
            bad = [] if isinstance(p["file"], str) else ["file"]
            bad += [f for f in ("rows", "bytes") if not (_is_int(p[f]) and p[f] >= 0)]
            bad += [f for f in ("ssObjectId_min", "ssObjectId_max") if p[f] is not None and not _is_int(p[f])]
            if not isinstance(p["null_ssObjectId"], bool):
                bad.append("null_ssObjectId")
            if not _is_md5(p["md5"]):
                bad.append("md5")
            if bad:
                probs.append(f"parts[{k}] ({p['file']!r}): bad {bad}")
    if "sidecar" in m:
        sc = m["sidecar"]
        miss = [f for f in SIDECAR_FIELDS if not isinstance(sc, dict) or f not in sc]
        if miss:
            probs.append(f"sidecar lacks {miss}")
        else:
            bad = [] if sc["file"] == SIDECAR_FILE else [f"file {sc['file']!r} (want {SIDECAR_FILE!r})"]
            if sc["key"] != SIDECAR_KEY:
                bad.append(f"key {sc['key']!r} (want {SIDECAR_KEY!r})")
            if not (isinstance(sc["columns"], list) and all(isinstance(c, str) for c in sc["columns"])):
                bad.append(f"columns {sc['columns']!r} (want a list of names)")
            bad += [f for f in ("rows", "bytes") if not (_is_int(sc[f]) and sc[f] >= 0)]
            if not _is_md5(sc["md5"]):
                bad.append("md5")
            if bad:
                probs.append(f"sidecar: bad {bad}")
    return probs


def check_ssobservation_parts(delivery_dir, schema_dir=SCHEMA_DIR):
    """Check the partitioned SSObservation in ``delivery_dir`` through its
    manifest (ssp.delivery_contract, "check_ssobservation_parts"): the
    manifest's fields, the part files, their rows/bytes/md5 and ranges,
    the cutting rules, the sort order within and across parts, the primary
    key across parts, and the sidecar. A list of CheckResult.

    Reads the parts one at a time and only the columns the checks need
    (the primary key, ssObjectId, midpointMjdTai), so memory is a few
    columns, not the table; the key and obsid columns of every part are
    held at once for the uniqueness and sidecar checks. (Memory is linear
    in the rows: about 250 B/row, ~2.5 GB for 8M rows.)

    Any part or sidecar that cannot be read or checked is a FAIL result,
    never an exception."""
    d = Path(delivery_dir)
    mpath = d / SSOBSERVATION_MANIFEST_FILE
    res = []
    try:
        m = json.loads(mpath.read_text())
        if not isinstance(m, dict):
            raise ValueError("not a JSON object")
    except FileNotFoundError:
        return [CheckResult("manifest", False, f"{mpath}: missing")]
    except Exception as e:
        return [CheckResult("manifest", False, f"{mpath}: cannot read ({type(e).__name__}: {e})")]

    probs = _manifest_problems(m)
    res.append(CheckResult("manifest", not probs, "; ".join(probs) if probs else
                           f"{mpath.name}: format_version {m['format_version']}, {len(m['parts'])} parts, "
                           f"part_rows {m['part_rows']:,}, {m['rows']:,} rows"))
    entries = m.get("parts") if isinstance(m.get("parts"), list) else []
    # (an entry without a usable file name FAILs the manifest check and the
    # names below; only the usable ones can be read)
    parts = [p for p in entries if isinstance(p, dict) and isinstance(p.get("file"), str)
             and Path(p["file"]).name == p["file"]]
    part_rows = m.get("part_rows") if _is_int(m.get("part_rows")) and m.get("part_rows") > 0 else None

    # the part files: exactly the manifest's, named contiguously from 0
    names = [p.get("file") if isinstance(p, dict) else p for p in entries]
    want = [PART_FILE_FORMAT.format(k) for k in range(len(names))]
    on_disk = sorted(f.name for f in d.glob(PART_GLOB))
    probs = []
    if names != want:
        probs.append(f"manifest names {_fmt_names([repr(x) for x in names])}, want {_fmt_names(want)} "
                     "(contiguous from 0)")
    missing = [f for f in names if isinstance(f, str) and f not in on_disk]
    extra = [f for f in on_disk if f not in names]
    if missing:
        probs.append(f"missing on disk: {_fmt_names(missing)}")
    if extra:
        probs.append(f"on disk but not in the manifest: {_fmt_names(extra)}")
    span = f"{names[0]} .. {names[-1]}" if names else "-"
    res.append(CheckResult("part files", not probs, "; ".join(probs) or f"{len(names)} parts, {span}"))

    # no other SSObservation files (a stale SSObservation.parquet, a .bak,
    # ...): any name starting with "ssobservation", in any case, that is not
    # a part, the manifest or the sidecar (stray part-named files are the
    # 'part files' check's)
    known = {SSOBSERVATION_MANIFEST_FILE, SIDECAR_FILE}
    stray = sorted(f.name for f in d.iterdir() if f.name.lower().startswith("ssobservation")
                   and f.name not in known and not fnmatch.fnmatchcase(f.name, PART_GLOB))
    res.append(CheckResult("stray files", not stray, f"not part of the delivery: {_fmt_names(stray)}"
                           if stray else "no other SSObservation* files"))

    # per part: integrity, contents, sort order, schema
    integ, contents, order_bad, schema_bad = [], [], [], []
    keys = primary_keys(str(schema_dir)).get("SSObservation", [])
    key_cols = {k: [] for k in keys}
    obsids = []
    actual = []            # per part: dict(file, rows, null, nonnull, min, max, last_count)
    prev = None
    schema0 = None
    n_read = 0
    for p in parts:
        f = d / p["file"]
        if not f.exists():
            continue
        try:
            try:
                pf = pq.ParquetFile(f)
            except Exception as e:
                integ.append(f"{p['file']}: cannot read ({type(e).__name__})")
                continue
            nrows = pf.metadata.num_rows
            nbytes = f.stat().st_size
            bad = []
            if p.get("rows") != nrows:
                bad.append(f"rows {p.get('rows')!r} vs {nrows:,}")
            if p.get("bytes") != nbytes:
                bad.append(f"bytes {p.get('bytes')!r} vs {nbytes:,}")
            md5 = file_md5(f)
            if p.get("md5") != md5:
                bad.append(f"md5 {p.get('md5')!r} vs {md5}")
            if bad:
                integ.append(f"{p['file']}: " + ", ".join(bad))
            sch = pf.schema_arrow.remove_metadata()
            if schema0 is None:
                schema0 = (p["file"], sch)
            elif not sch.equals(schema0[1]):
                diff = [fl.name for fl, f0 in zip(sch, schema0[1]) if not fl.equals(f0)]
                where = (f" at {_fmt_names(diff, 3)}" if diff
                         else f" ({len(sch)} vs {len(schema0[1])} columns)")
                schema_bad.append(f"{p['file']} differs from {schema0[0]}{where}")
            need = [c for c in dict.fromkeys([*SSOBSERVATION_SORT, *keys, SIDECAR_KEY]) if c in sch.names]
            t = pf.read(columns=need)
            n_read += 1
            for k in keys:
                if k in t.column_names:
                    key_cols[k].append(_decoded(t[k]))
            if SIDECAR_KEY in t.column_names:
                obsids.append(_decoded(t[SIDECAR_KEY]))
            if "ssObjectId" not in t.column_names:
                contents.append(f"{p['file']}: no ssObjectId column")
                continue
            sid = t["ssObjectId"]
            if not pa.types.is_int64(sid.type):
                contents.append(f"{p['file']}: ssObjectId is {sid.type}, want int64 (its range and order "
                                "not checked)")
                continue
            n_null = sid.null_count
            mm = pc.min_max(sid)
            mn, mx = mm["min"].as_py(), mm["max"].as_py()
            last_count = 0
            if nrows - n_null and mx is not None:
                last_count = int(pc.sum(pc.equal(sid, mx)).as_py() or 0)
            actual.append(dict(file=p["file"], rows=nrows, null=n_null, min=mn, max=mx, last=last_count,
                               manifest=p))
            # the manifest's range and null flag against the contents
            bad = []
            is_null_part = bool(p.get("null_ssObjectId"))
            if is_null_part:
                if n_null != nrows:
                    bad.append(f"null_ssObjectId true but {nrows - n_null:,} rows have an ssObjectId")
                if nrows == 0:
                    bad.append("a NULL part with no rows")
            else:
                if n_null:
                    bad.append(f"{n_null:,} NULL ssObjectId rows in a ranged part")
                if nrows == 0 and len(parts) > 1:
                    bad.append("an empty part (only the empty table's single part may be empty)")
            if p.get("ssObjectId_min") != (None if is_null_part else mn) or \
                    p.get("ssObjectId_max") != (None if is_null_part else mx):
                bad.append(f"ssObjectId_min/max {p.get('ssObjectId_min')!r}/{p.get('ssObjectId_max')!r}, "
                           f"contents {mn!r}/{mx!r}"
                           + (" (a NULL part's must be null)" if is_null_part else ""))
            if bad:
                contents.append(f"{p['file']}: " + "; ".join(bad))
            # the sort order, carrying the previous part's last row
            if all(c in t.column_names for c in SSOBSERVATION_SORT) and nrows:
                mjd = t["midpointMjdTai"]
                if not pa.types.is_floating(mjd.type):
                    order_bad.append(f"{p['file']}: midpointMjdTai is {mjd.type}, not checked")
                    continue
                # a NaN (or NULL) sort key compares false both ways, so it
                # could hide disorder: a FAIL of its own
                n_nan = (pc.sum(pc.is_nan(mjd)).as_py() or 0) + mjd.null_count
                if n_nan:
                    order_bad.append(f"{p['file']}: {n_nan:,} NaN or NULL midpointMjdTai (the order is "
                                     "undefined)")
                v = _sort_violations(prev, sid, mjd, t[SIDECAR_KEY])
                if len(v):
                    i = int(v[0])
                    where = "its first row (after the previous part's last)" if i == 0 else f"row {i:,}"
                    order_bad.append(f"{p['file']}: {len(v):,} rows out of order, first at {where}")
                last = t.slice(nrows - 1, 1)
                prev = (last["ssObjectId"][0].as_py(), last["midpointMjdTai"][0].as_py(),
                        _decoded(last[SIDECAR_KEY])[0].as_py())
            del t
        except Exception as e:      # an unreadable or malformed part: a FAIL, not a crash
            contents.append(f"{p['file']}: cannot check ({type(e).__name__}: {e})")

    res.append(CheckResult("part integrity", not integ, "; ".join(integ) or
                           f"rows, bytes and md5 match for {len(actual)} parts"))
    res.append(CheckResult("part contents", not contents, "; ".join(contents) or
                           "each part's ssObjectId_min/max and null_ssObjectId match its rows"))

    # ranges: ranged parts ascend and are disjoint, no object in two parts,
    # NULL parts last
    probs = []
    flags = [bool(a["manifest"].get("null_ssObjectId")) for a in actual]
    if True in flags and not all(flags[flags.index(True):]):
        probs.append("a NULL part before a ranged part: "
                     + ", ".join(a["file"] + (" (NULL)" if f else "") for a, f in zip(actual, flags)))
    ranged = [a for a in actual if a["rows"] - a["null"] > 0]
    for a, b in zip(ranged, ranged[1:]):
        if b["min"] == a["max"]:
            probs.append(f"ssObjectId {a['max']} split across {a['file']} and {b['file']}")
        elif b["min"] < a["max"]:
            probs.append(f"ranges overlap or descend: {a['file']} [{a['min']}, {a['max']}], "
                         f"{b['file']} [{b['min']}, {b['max']}]")
    res.append(CheckResult("part ranges", not probs, _quote(probs) if probs else
                           f"{len(ranged)} ranged parts, ascending and disjoint; "
                           f"{sum(flags)} NULL parts, last"))

    # sizes: the cutting rules
    probs = []
    if part_rows is not None:
        rp = [a for a, f in zip(actual, flags) if not f]
        npart = [a for a, f in zip(actual, flags) if f]
        for a in rp[:-1]:
            if a["rows"] < part_rows:
                probs.append(f"{a['file']}: {a['rows']:,} rows < part_rows {part_rows:,} (not the last "
                             "ranged part)")
        for a in rp:
            if a["rows"] - a["last"] >= part_rows:
                probs.append(f"{a['file']}: not closed at the first object boundary at or after part_rows "
                             f"({a['rows'] - a['last']:,} rows before its last object)")
        for a in npart[:-1]:
            if a["rows"] != part_rows:
                probs.append(f"{a['file']}: {a['rows']:,} rows, a NULL part but the last must have "
                             f"part_rows {part_rows:,}")
        if npart and not 0 < npart[-1]["rows"] <= part_rows:
            probs.append(f"{npart[-1]['file']}: {npart[-1]['rows']:,} rows, the last NULL part must have "
                         f"1..{part_rows:,}")
        if len(actual) == len(parts) == 1 and actual[0]["rows"] == 0 and flags[0]:
            probs.append("the empty table's part must have null_ssObjectId false")
    else:
        probs.append("no valid part_rows in the manifest")
    res.append(CheckResult("part sizes", not probs, _quote(probs) if probs else
                           f"part_rows {part_rows:,}: ranged parts but the last >= part_rows and closed at "
                           "the first object boundary; NULL parts but the last == part_rows"))

    total = sum(a["rows"] for a in actual)
    ok = _is_int(m.get("rows")) and m["rows"] == total and len(actual) == len(parts)
    res.append(CheckResult("rows total", ok, f"manifest rows {m.get('rows')!r}, parts' rows sum to {total:,}"
                           + ("" if len(actual) == len(parts) else " (parts missing)")))
    res.append(CheckResult("sort order", not order_bad, "; ".join(order_bad) or
                           f"by {SSOBSERVATION_SORT}, NULL ssObjectId last, within and across parts"))
    res.append(CheckResult("part schemas", not schema_bad, "; ".join(schema_bad) or
                           "every part has the same Arrow schema"))

    # the primary key across parts
    if not keys:
        res.append(CheckResult("primary key across parts", False, "the schema gives no primaryKey"))
    elif any(len(v) != n_read for v in key_cols.values()):
        res.append(CheckResult("primary key across parts", False, f"({', '.join(keys)}): not in the parts"))
    else:
        kt = pa.table({k: pa.chunked_array([c.cast(v[0].type) for col in v for c in col.chunks],
                                           type=v[0].type if v else pa.string())
                       for k, v in key_cols.items()})
        nn = kt.drop_null()
        counts = nn.group_by(keys).aggregate([([], "count_all")])
        ndup = len(nn) - len(counts)
        detail = f"({', '.join(keys)}): {len(kt):,} rows over {len(actual)} parts"
        if ndup:
            dup = counts.filter(pc.greater(counts["count_all"], 1))
            ex = [r[keys[0]] if len(keys) == 1 else tuple(r[k] for k in keys)
                  for r in dup.slice(0, N_EXAMPLES).to_pylist()]
            detail += f"; {ndup:,} duplicate rows over {len(dup):,} key values, e.g. {ex}"
        res.append(CheckResult("primary key across parts", not ndup, detail +
                               ("" if ndup else ", unique")))
        del kt, nn, counts, key_cols

    res.extend(_check_sidecar(d, m, obsids, total, schema_dir))
    return res


def _check_sidecar(d, m, obsids, total, schema_dir):
    """The sidecar: its manifest entry, file, columns and types, encoding,
    NULLs, and the obsid sequence (``obsids``: the parts' obsid columns, in
    part order). Anything unreadable is a FAIL, not an exception."""
    sc = m.get("sidecar")
    if not isinstance(sc, dict) or not isinstance(sc.get("file"), str):
        return [CheckResult("sidecar", False, "the manifest has no usable sidecar entry")]
    if sc["file"] != SIDECAR_FILE:
        # never opened: it may point outside the delivery directory
        return [CheckResult("sidecar", False, f"the manifest names {sc['file']!r}, want {SIDECAR_FILE!r}")]
    f = d / SIDECAR_FILE
    if not f.exists():
        return [CheckResult("sidecar", False, f"{f}: missing")]
    try:
        pf = pq.ParquetFile(f)
        nrows, nbytes, md5 = pf.metadata.num_rows, f.stat().st_size, file_md5(f)
    except Exception as e:
        return [CheckResult("sidecar", False, f"{f}: cannot read ({type(e).__name__}: {e})")]
    res = []
    bad = []
    if sc.get("rows") != nrows:
        bad.append(f"rows {sc.get('rows')!r} vs {nrows:,}")
    if nrows != total:
        bad.append(f"{nrows:,} rows, the parts {total:,}")
    if sc.get("bytes") != nbytes:
        bad.append(f"bytes {sc.get('bytes')!r} vs {nbytes:,}")
    if sc.get("md5") != md5:
        bad.append(f"md5 {sc.get('md5')!r} vs {md5}")
    res.append(CheckResult("sidecar", not bad, "; ".join(bad) or f"{f.name}: {nrows:,} rows, md5 matches"))

    # columns: obsid, then the manifest's columns, of the contract's types,
    # none of them delivered, none twice
    arrow = pf.schema_arrow
    names = arrow.names
    dups = sorted({c for c in names if names.count(c) > 1})
    cols = sc.get("columns") if isinstance(sc.get("columns"), list) else []
    want = [SIDECAR_KEY, *cols]
    delivered = {c["name"] for c in _schema(str(schema_dir))["SSObservation"]}
    probs = []
    if names != want:
        probs.append(f"columns {names}, want {want} (obsid, then the manifest's columns)")
    if dups:
        probs.append(f"duplicated columns {dups}")
    types = {}
    for fld in arrow:
        types.setdefault(fld.name, fld.type)
    if SIDECAR_KEY in types and not _is_string(types[SIDECAR_KEY]):
        probs.append(f"{SIDECAR_KEY}: {types[SIDECAR_KEY]}, want string")
    for c in dict.fromkeys([*[c for c in cols if isinstance(c, str)],
                            *[n for n in names if n != SIDECAR_KEY]]):
        if c in delivered:
            probs.append(f"{c} is also in the delivered schema")
        elif c not in SSOBSERVATION_INTERNAL_DTYPE:
            probs.append(f"{c} is not a column that may be internal (SSOBSERVATION_INTERNAL_DTYPE)")
        elif c in types and not _np_dtype_ok(SSOBSERVATION_INTERNAL_DTYPE[c], types[c]):
            probs.append(f"{c}: {types[c]}, want {SSOBSERVATION_INTERNAL_DTYPE[c]}")
    res.append(CheckResult("sidecar columns", not probs, "; ".join(probs) or
                           f"{SIDECAR_KEY} + {cols}, contract types, none delivered"))

    # encoding: zstd, the SSOBSERVATION_DICTIONARY columns dictionary-encoded
    # (column chunks without values are skipped: they carry no encoding)
    probs = set()
    md = pf.metadata
    for rg in range(md.num_row_groups):
        g = md.row_group(rg)
        for k in range(g.num_columns):
            cc = g.column(k)
            if not cc.num_values:
                continue
            if cc.compression != "ZSTD":
                probs.add(f"{cc.path_in_schema}: {cc.compression}, want ZSTD")
            if cc.path_in_schema in SSOBSERVATION_DICTIONARY and not any("DICT" in e for e in cc.encodings):
                probs.add(f"{cc.path_in_schema}: not dictionary-encoded")
    res.append(CheckResult("sidecar encoding", not probs, "; ".join(sorted(probs)) or
                           "zstd; dictionary columns dictionary-encoded"))

    # NULLs, one column at a time (a duplicated name can't be read by name)
    with_nulls = []
    for c in dict.fromkeys(names):
        if c in dups:
            with_nulls.append(f"{c} (duplicated: not checked)")
            continue
        n = pf.read(columns=[c])[c].null_count
        if n:
            with_nulls.append(f"{c} ({n:,} NULLs)")
    res.append(CheckResult("sidecar nulls", not with_nulls, "; ".join(with_nulls) or
                           f"no NULLs in its {len(names)} columns"))

    # the same obsid sequence as the parts, in part order
    if SIDECAR_KEY not in names or SIDECAR_KEY in dups:
        res.append(CheckResult("sidecar obsid", False, f"no single {SIDECAR_KEY} column"))
        return res
    try:
        side = _decoded(pf.read(columns=[SIDECAR_KEY])[SIDECAR_KEY]).cast(pa.string())
        parts = pa.chunked_array([c.cast(pa.string()) for col in obsids for c in col.chunks],
                                 type=pa.string())
    except Exception as e:
        res.append(CheckResult("sidecar obsid", False, f"cannot compare ({type(e).__name__}: {e})"))
        return res
    if len(side) != len(parts):
        res.append(CheckResult("sidecar obsid", False, f"{len(side):,} rows, the parts {len(parts):,}"))
        return res
    eq = pc.fill_null(pc.equal(side, parts), False)
    n_bad = len(eq) - (pc.sum(eq).as_py() or 0)
    if n_bad:
        first = int(np.flatnonzero(~np.asarray(eq.to_numpy(zero_copy_only=False), dtype=bool))[0])
        res.append(CheckResult("sidecar obsid", False,
                               f"{n_bad:,} rows differ from the parts' obsid, first at row {first:,} "
                               f"({side[first].as_py()!r} vs {parts[first].as_py()!r})"))
    else:
        res.append(CheckResult("sidecar obsid", True, f"the parts' {len(parts):,} obsids, in order"))
    return res


def ssobservation_part_files(delivery_dir):
    """The SSObservation part files to run check_table on: the manifest's,
    or (without a readable manifest) the files matching PART_GLOB."""
    d = Path(delivery_dir)
    try:
        m = json.loads((d / SSOBSERVATION_MANIFEST_FILE).read_text())
        return [d / p["file"] for p in m["parts"]
                if isinstance(p, dict) and isinstance(p.get("file"), str)
                and Path(p["file"]).name == p["file"]]
    except Exception:
        return sorted(d.glob(PART_GLOB))


def check_delivery(delivery_dir, schema_dir=SCHEMA_DIR, tables=DELIVERY_TABLES):
    """{Table: [CheckResult, ...]}: each table's DELIVERY_DIR/<Table>.parquet,
    except SSObservation, checked through its manifest
    (check_ssobservation_parts) with check_table on every part (results
    named '<check> [<part file>]'); a missing file is a FAIL."""
    out = {}
    for table in tables:
        if table == "SSObservation":
            try:
                res = check_ssobservation_parts(delivery_dir, schema_dir)
            except Exception as e:      # a safety net: a FAIL, never a crash
                res = [CheckResult("partitioning", False, f"cannot check ({type(e).__name__}: {e})")]
            files = ssobservation_part_files(delivery_dir)
            if not files:
                res.append(CheckResult("parts", False, f"{Path(delivery_dir)}: no {PART_GLOB}"))
            for f in files:
                try:
                    rs = check_table(table, f, schema_dir)
                except Exception as e:
                    rs = [CheckResult("file", False, f"{f}: cannot check ({type(e).__name__}: {e})")]
                for r in rs:
                    res.append(CheckResult(f"{r.name} [{f.name}]", r.ok, r.detail))
            out[table] = res
            continue
        path = Path(delivery_dir) / f"{table}.parquet"
        if not path.exists():
            out[table] = [CheckResult("file", False, f"{path}: missing")]
        else:
            out[table] = check_table(table, path, schema_dir)
    return out


def format_report(results, timings=None):
    lines = []
    for table, rs in results.items():
        ok = all(r.ok for r in rs)
        t = f"  ({timings[table]:.1f} s)" if timings and table in timings else ""
        lines.append(f"{table}: {'PASS' if ok else 'FAIL'}{t}")
        for r in rs:
            lines.append(f"  {'PASS' if r.ok else 'FAIL'}  {r.name:<13} {r.detail}")
    npass = sum(all(r.ok for r in rs) for rs in results.values())
    lines.append(f"\n{npass}/{len(results)} tables pass: "
                 f"{'DELIVERABLE' if npass == len(results) else 'NOT DELIVERABLE'}")
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser(prog="python -m ssp.delivery_check",
                                 description="Check a delivery's tables against the PPDB schema.")
    ap.add_argument("delivery_dir", help="directory holding <Table>.parquet (SSObservation: its parts, "
                    "manifest and sidecar)")
    ap.add_argument("--tables", nargs="+", default=list(DELIVERY_TABLES), metavar="TABLE",
                    help=f"tables to check (default: all of {', '.join(DELIVERY_TABLES)})")
    ap.add_argument("--schema-dir", default=str(SCHEMA_DIR),
                    help="directory with ppdb.yaml and sso_base.yaml (default: the vendored copy)")
    args = ap.parse_args(argv)
    unknown = [t for t in args.tables if t not in DELIVERY_TABLES]
    if unknown:
        ap.error(f"unknown tables {unknown}; known: {', '.join(DELIVERY_TABLES)}")

    results, timings = {}, {}
    for table in args.tables:
        t0 = time.perf_counter()
        results.update(check_delivery(args.delivery_dir, args.schema_dir, [table]))
        timings[table] = time.perf_counter() - t0
    print(f"delivery check: {args.delivery_dir} (schema: {args.schema_dir})")
    print(format_report(results, timings))
    return 0 if all(r.ok for rs in results.values() for r in rs) else 1


if __name__ == "__main__":
    sys.exit(main())
