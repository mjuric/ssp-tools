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

Columns are read one at a time, to bound memory; everything is Arrow.

    python -m ssp.delivery_check DELIVERY_DIR [--tables T ...] [--schema-dir D]

Exit status 0 only if every result is ok.
"""

from __future__ import annotations

import argparse
import sys
import time
from functools import lru_cache
from pathlib import Path
from typing import NamedTuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import yaml

from ssp.delivery_contract import DELIVERY_TABLES, SCHEMA_DIR, delivery_schema


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


def check_delivery(delivery_dir, schema_dir=SCHEMA_DIR, tables=DELIVERY_TABLES):
    """{Table: [CheckResult, ...]} for each table's
    DELIVERY_DIR/<Table>.parquet; a missing file is a FAIL."""
    out = {}
    for table in tables:
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
    ap.add_argument("delivery_dir", help="directory holding <Table>.parquet")
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
