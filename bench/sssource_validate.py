"""Validation harness for the widened SSSource table (WP4 of
docs/design/sssource-widened.md, "Validation").

Black-box checks of ``sssource.parquet``, written from the design, the
contract (``ssp/sssource_contract.py``) and ``sso_base.yaml`` only. The YAML
is read directly (not through ``ssp/schema_ppdb.py``), and none of the
writer's code (``ssp.sssource``, ``ssp.sssource_ellipse``) is used.

Subcommands (each prints a text report, optionally also to ``--out FILE``)::

  conformance SSSOURCE [--schema sso_base.yaml]
      names, order, Felis -> Arrow types, char lengths, NULLs, obsid
      uniqueness, sort order, matchMethod/measuredOn values, the id split,
      the ssObjectId NULL rule, the ellipse NULL rule, one primary row per
      measurement, zstd + dictionary encoding
  copied SSSOURCE DIA_SOURCES
      blocks 1 (the copied part), 3 and 4 against dia_sources.parquet, on obsid
  clickhouse SSSOURCE [--n 10000]
      a sample stratified by processing, re-fetched from ssp.SubmittableSources
      by (processing, id); blocks 3 and 4 compared
  offsets SSSOURCE
      ephOffsetAlongTrack/CrossTrack recomputed from ephOffsetRa/Dec and
      ephRateRa/Dec by the contract's formula (also run by conformance)
  regression NEW_SSSOURCE REF_SSSOURCE
      the ephemeris/geometry columns bitwise equal to today's SSSource
      (except the computed ellipse and along/cross-track columns)
  ssobject-permutation SSSOURCE [DIA] MPCORB [--max-objects N]
      ssp-build-ssobject, run as a black box on copies of SSSource with
      the rows of each object permuted, must write byte-identical SSObject
      files. SSObject reads its photometry from SSSource; DIA (passed on to
      the builder, which ignores it) is needed only for --permute-dia
  ellipse SSSOURCE NEARBYSSO --orbits-a A --orbits-b B
      the error ellipse against NearbySSO's, for rows in both whose orbit is
      identical in the two mpc_orbits snapshots
  counts SSSOURCE OBS_SBN [--dia-sources DIA]
      rows against obs_sbn X05, status, and the I / #7 / non-primary counts
  mock REF_SSSOURCE DIA_SOURCES OBS_SBN OUT [--nearbysso N] [--fault F ...]
      (development) a widened SSSource faked from today's SSSource and
      dia_sources.parquet, optionally with injected faults

Comparison rules (copied, clickhouse):
  * same type: exact (``==``);
  * float64 -> float32: equal after casting the source value to float32;
  * integers / booleans / strings: equal values, equal NULL masks;
  * floats: NULL and NaN are the same thing (the contract's "NULL (NaN)").

Exit status: 0 pass, 1 fail.

CLICKHOUSE ETIQUETTE: the server is shared. Read-only, at most 4 concurrent
queries, ids batched per processing, at most 50,000 sampled rows per run.
Never run the clickhouse subcommand from the test suite.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import yaml

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from ssp.sssource_contract import (  # noqa: E402
    ELLIPSE_COLUMNS,
    ID_SPLIT,
    MATCH_METHODS,
    SSSOURCE_DICTIONARY,
    SSSOURCE_SORT,
    VIEW_DROPPED,
)

DEFAULT_SCHEMA = os.path.join(_ROOT, "tests", "data", "sdm_schemas", "sso_base.yaml")

# ClickHouse (read-only; the server is shared)
from ssp.export.submittable import current_host  # noqa: E402 (the ClickHouse host, see there)
CH_PORT = 8123
CH_DATABASE = "ssp"
CH_VIEW = "SubmittableSources"
CH_MAX_WORKERS = 4
CH_MAX_ROWS = 50_000
CH_CHUNK = 2_000

#: Felis datatype -> the one Arrow type it maps to (strings handled apart).
FELIS_ARROW = {
    "long": pa.int64(), "int": pa.int32(), "short": pa.int16(), "byte": pa.int8(),
    "float": pa.float32(), "double": pa.float64(), "boolean": pa.bool_(),
}
FELIS_STRING = ("char", "string", "unicode", "text")

#: Columns in the reference (today's SSSource) that must be bitwise equal:
#: everything ephemeris and geometry except the ellipse and the along/cross-
#: track offsets, which are new computations. (diaDistanceRank is no longer
#: in SSSource; it moved to NearbySSO.)
REGRESSION_PREFIXES = ("ecl", "gal", "topo", "helio", "eph")
REGRESSION_EXACT = ("elongation", "phaseAngle")

#: The along/cross-track offsets (block 6), computed per the contract.
TRACK_COLUMNS = ("ephOffsetAlongTrack", "ephOffsetCrossTrack")
#: ... and the columns they are computed from.
TRACK_INPUTS = ("ephOffsetRa", "ephOffsetDec", "ephRateRa", "ephRateDec")
REGRESSION_EXCLUDED = (*ELLIPSE_COLUMNS, *TRACK_COLUMNS)

#: Tolerance of the recomputed along/cross-track offsets, in float32 epsilons
#: times |ephOffset|: the inputs' (float32 rates) and the output's rounding.
TRACK_EPS = 8
#: along^2 + cross^2 vs ephOffset^2: gated on rows with ephOffset at most
#: this [arcsec] (beyond, the tangent-plane offsets and the great-circle
#: separation part ways), to this relative tolerance on the root.
TRACK_SEP_GATE_ARCSEC = 60.0
TRACK_SEP_RTOL = 1e-4

#: mpc_orbits columns that must be equal for two snapshots' orbits to count
#: as identical (the elements, their uncertainties, H/G, and the JSON that
#: carries the covariance).
ORBIT_IDENTITY = (
    "mpc_orb_jsonb", "epoch_mjd", "a", "q", "e", "i", "node", "argperi", "peri_time", "yarkovsky",
    "srp", "a1", "a2", "a3", "dt", "mean_anomaly", "period", "mean_motion",
    "a_unc", "q_unc", "e_unc", "i_unc", "node_unc", "argperi_unc", "peri_time_unc", "yarkovsky_unc",
    "srp_unc", "a1_unc", "a2_unc", "a3_unc", "dt_unc", "mean_anomaly_unc", "period_unc",
    "mean_motion_unc", "h", "g",
)
ORBIT_KEY = "unpacked_primary_provisional_designation"

ID_COLUMNS = tuple(c for pair in ID_SPLIT.values() for c in pair)


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------

class Report:
    """A list of PASS/FAIL checks plus informational lines."""

    def __init__(self, title):
        self.title = title
        self.lines = [title, "=" * len(title)]
        self.results = []          # (name, ok)

    def check(self, name, ok, detail=""):
        ok = bool(ok)
        self.results.append((name, ok))
        self.lines.append(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f": {detail}" if detail else ""))
        return ok

    def info(self, text=""):
        for line in str(text).splitlines() or [""]:
            self.lines.append(f"       {line}" if line else "")

    @property
    def failed(self):
        return [n for n, ok in self.results if not ok]

    @property
    def ok(self):
        return not self.failed

    def text(self):
        n = len(self.results)
        tail = (f"RESULT: PASS ({n} checks)" if self.ok
                else f"RESULT: FAIL ({len(self.failed)} of {n} checks failed: {', '.join(self.failed)})")
        return "\n".join(self.lines + ["", tail]) + "\n"

    def finish(self, out=None):
        text = self.text()
        sys.stdout.write(text)
        if out:
            os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
            with open(out, "w") as f:
                f.write(text)
        return 0 if self.ok else 1


def _counts(values):
    s = pd.Series(np.asarray(values, dtype=object)).fillna("<NULL>")
    vc = s.value_counts()
    return ", ".join(f"{k}: {v:,}" for k, v in vc.items()) or "-"


def _examples(keys, mask, *cols, k=5):
    """Up to k 'key: a vs b' strings for the rows in mask."""
    idx = np.flatnonzero(mask)[:k]
    return "; ".join(f"{keys[i]}: " + " vs ".join(repr(_py(c[i])) for c in cols) for i in idx)


def _py(x):
    if isinstance(x, np.generic):
        return x.item()
    return x


# --------------------------------------------------------------------------
# The schema, straight from the YAML
# --------------------------------------------------------------------------

def schema_columns(path=DEFAULT_SCHEMA, table="SSSource"):
    """The YAML's column dicts for ``table``, in order."""
    with open(path) as f:
        doc = yaml.safe_load(f)
    for t in doc["tables"]:
        if t["name"] == table:
            return t["columns"]
    raise SystemExit(f"{path}: no table {table}")


def blocks(names):
    """The design's blocks: ``{1, 2, 3, 4, 6: [names]}``."""
    i = names.index
    cut = [0, i("ssObjectId"), i("measuredOn"), i("visit"), i("eclLambda"), len(names)]
    return {b: names[cut[k]:cut[k + 1]] for k, b in enumerate((1, 2, 3, 4, 6))}


def arrow_type_ok(felis, t):
    """Is Arrow type ``t`` a correct representation of Felis ``felis``?"""
    if felis in FELIS_STRING:
        if pa.types.is_dictionary(t):
            t = t.value_type
        return pa.types.is_string(t) or pa.types.is_large_string(t)
    want = FELIS_ARROW.get(felis)
    return want is not None and t == want


def felis_arrow_type(felis, dictionary=False):
    if felis in FELIS_STRING:
        return pa.dictionary(pa.int32(), pa.string()) if dictionary else pa.string()
    return FELIS_ARROW[felis]


# --------------------------------------------------------------------------
# Arrow -> NumPy, and comparisons
# --------------------------------------------------------------------------

def _flat(col):
    if isinstance(col, pa.ChunkedArray):
        col = col.combine_chunks() if col.num_chunks != 1 else col.chunk(0)
    if pa.types.is_dictionary(col.type):
        col = col.dictionary_decode()
    return col


def to_np(col):
    """``(values, valid, kind)``; kind is 'f', 'i', 'b' or 's'. Float NULLs
    are NaN in ``values``; other NULLs are filled (0, False, None)."""
    col = _flat(col)
    t = col.type
    valid = np.asarray(col.is_valid().to_numpy(zero_copy_only=False), dtype=bool)
    if pa.types.is_floating(t):
        return np.asarray(col.to_numpy(zero_copy_only=False)), valid, "f"
    if pa.types.is_integer(t):
        return pc.fill_null(col, 0).to_numpy(), valid, "i"
    if pa.types.is_boolean(t):
        return np.asarray(pc.fill_null(col, False).to_numpy(zero_copy_only=False), dtype=bool), valid, "b"
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return np.asarray(col.to_numpy(zero_copy_only=False), dtype=object), valid, "s"
    if pa.types.is_null(t):
        return np.zeros(len(col)), valid, "n"
    raise TypeError(f"unsupported Arrow type {t}")


def compare(actual, expected):
    """Boolean mismatch mask of ``actual`` (the SSSource column) against
    ``expected`` (its source). Floats narrowed to a smaller width are
    compared after casting ``expected`` to it; floats treat NULL == NaN;
    everything else compares values and NULL masks exactly."""
    a, va, ka = to_np(actual)
    e, ve, ke = to_np(expected)
    if len(a) != len(e):
        raise ValueError("length mismatch")
    if "n" in (ka, ke):              # an all-NULL column of no type
        miss_a = ~va | (np.isnan(a) if ka == "f" else False)
        miss_e = ~ve | (np.isnan(e) if ke == "f" else False)
        return miss_a != miss_e
    if ka == "f" and ke == "f":
        if e.dtype != a.dtype:
            with np.errstate(over="ignore", invalid="ignore"):
                e = e.astype(a.dtype)
        miss_a, miss_e = ~va | np.isnan(a), ~ve | np.isnan(e)
        return np.where(miss_a | miss_e, miss_a != miss_e, a != e)
    if ka in "ib" and ke in "ib":
        return (va != ve) | (va & ve & (a.astype(np.int64) != e.astype(np.int64)))
    if ka == "s" and ke == "s":
        both = va & ve
        neq = np.zeros(len(a), dtype=bool)
        neq[both] = a[both] != e[both]
        return (va != ve) | neq
    raise TypeError(f"cannot compare kinds {ka} and {ke}")


def bitwise_mismatch(actual, expected):
    """``(mismatch mask, n_null_vs_nan)``: equal bit patterns, or both
    missing. For floats a NULL and a NaN both count as missing (counted
    separately, as a representation difference)."""
    a, va, ka = to_np(actual)
    e, ve, ke = to_np(expected)
    if ka == "f" and ke == "f":
        if a.dtype != e.dtype:
            raise TypeError(f"float widths differ: {a.dtype} vs {e.dtype}")
        nan_a, nan_e = np.isnan(a), np.isnan(e)
        miss_a, miss_e = ~va | nan_a, ~ve | nan_e
        ui = np.uint32 if a.dtype == np.float32 else np.uint64
        bits = a.view(ui) != e.view(ui)
        mism = np.where(miss_a | miss_e, miss_a != miss_e, bits)
        repr_diff = int(np.sum(miss_a & miss_e & (va != ve)))
        return mism, repr_diff
    return compare(actual, expected), 0


def _read(path, columns):
    """Read those of ``columns`` that ``path`` has."""
    have = set(pq.read_schema(path).names)
    return pq.read_table(path, columns=[c for c in columns if c in have])


def _obsid_index(ref_obsid, query_obsid):
    """Positions of ``query_obsid`` in ``ref_obsid`` (-1 if absent). The
    reference must be unique."""
    idx = pd.Index(np.asarray(ref_obsid, dtype=object))
    if not idx.is_unique:
        raise ValueError("obsid is not unique in the reference table")
    return idx.get_indexer(np.asarray(query_obsid, dtype=object))


# --------------------------------------------------------------------------
# 1. conformance
# --------------------------------------------------------------------------

def check_conformance(path, schema=DEFAULT_SCHEMA, rep=None):
    rep = rep or Report(f"SSSource conformance: {path}")
    cols = schema_columns(schema)
    names = [c["name"] for c in cols]
    spec = {c["name"]: c for c in cols}
    pf = pq.ParquetFile(path)
    arrow = pf.schema_arrow
    n = pf.metadata.num_rows
    rep.info(f"schema: {schema} ({len(names)} SSSource columns); file: {n:,} rows, {len(arrow)} columns")

    # names and order
    got = arrow.names
    if got == names:
        rep.check("column names and order", True, f"{len(names)} columns")
    else:
        missing = [c for c in names if c not in got]
        extra = [c for c in got if c not in names]
        first = next((k for k, (a, b) in enumerate(zip(got, names)) if a != b), min(len(got), len(names)))
        rep.check("column names and order", False,
                  f"missing {missing}, extra {extra}, first difference at position {first}: "
                  f"{got[first] if first < len(got) else '<end>'} vs "
                  f"{names[first] if first < len(names) else '<end>'}")

    present = [c for c in names if c in got]

    # types
    bad = [f"{c}: {arrow.field(c).type} (Felis {spec[c]['datatype']})"
           for c in present if not arrow_type_ok(spec[c]["datatype"], arrow.field(c).type)]
    rep.check("Arrow types match the Felis datatypes", not bad, "; ".join(bad))
    bad = [c for c in present if arrow.field(c).nullable != spec[c].get("nullable", True)]
    rep.check("Arrow field nullability matches the YAML", not bad,
              ", ".join(f"{c} ({'nullable' if arrow.field(c).nullable else 'required'})" for c in bad))

    # per-column NULLs and string lengths (one column at a time)
    nonnull = [c for c in present if spec[c].get("nullable", True) is False]
    with_nulls, too_long = [], []
    for c in present:
        col = pf.read(columns=[c])[c]
        if c in nonnull and col.null_count:
            with_nulls.append(f"{c} ({col.null_count:,})")
        L = spec[c].get("length")
        if L and spec[c]["datatype"] in FELIS_STRING and arrow_type_ok("char", col.type):
            lengths = pc.utf8_length(_flat(col))
            m = pc.max(lengths).as_py()
            if m is not None and m > L:
                too_long.append(f"{c} (max {m} > {L})")
    rep.check("no NULLs in nullable: false columns", not with_nulls,
              f"{len(nonnull)} non-null columns"
              + (f"; NULLs in {', '.join(with_nulls)}" if with_nulls else ""))
    rep.check("char values fit their length", not too_long, ", ".join(too_long))

    need = ["obsid", "ssObjectId", "midpointMjdTai", "matchMethod", "measuredOn", "processing",
            "designation", "status", "primary", *ID_COLUMNS, *ELLIPSE_COLUMNS,
            *[c for c in names if c.startswith("eph")]]
    t = pf.read(columns=[c for c in dict.fromkeys(need) if c in got])

    def has(*cs):
        miss = [c for c in cs if c not in t.column_names]
        if miss:
            rep.check(f"columns needed by the content checks present ({', '.join(cs)})", False,
                      f"missing {miss}")
        return not miss

    # obsid unique
    if has("obsid"):
        nd = pc.count_distinct(t["obsid"]).as_py()
        obsid = to_np(t["obsid"])[0]
        dup = pd.Series(obsid).duplicated(keep=False).to_numpy()
        rep.check("obsid unique", nd == n, f"{n - nd:,} duplicates" + (
            f", e.g. {sorted(set(obsid[dup]))[:5]}" if nd != n else ""))

    # sort order
    if has(*SSSOURCE_SORT):
        check_sort(t, rep)

    # categorical values
    if has("matchMethod"):
        v, valid, _ = to_np(t["matchMethod"])
        bad = sorted(set(v[valid]) - set(MATCH_METHODS))
        rep.check("matchMethod values in MATCH_METHODS", not bad,
                  f"unexpected {bad}" if bad else _counts(v))
    if has("measuredOn"):
        v, valid, _ = to_np(t["measuredOn"])
        bad = sorted(set(v[valid]) - set(ID_SPLIT))
        rep.check("measuredOn values in ID_SPLIT", not bad, f"unexpected {bad}" if bad else _counts(v))

    if has("measuredOn", *ID_COLUMNS):
        check_id_split(t, rep)
    if has("ssObjectId", "designation", "ephRa", "ephDec"):
        check_ssobjectid_rule(t, rep)
    if has("ephRa", *ELLIPSE_COLUMNS):
        check_ellipse_rule(t, rep)
    if has("processing", "measuredOn", "primary", *ID_COLUMNS):
        check_primary(t, rep)
    if has("ephRa", "ephOffset", *TRACK_INPUTS, *TRACK_COLUMNS):
        check_offsets_table(t, rep)

    # Parquet layout
    comp, nodict = set(), set()
    md = pf.metadata
    for rg in range(md.num_row_groups):
        g = md.row_group(rg)
        for k in range(g.num_columns):
            cc = g.column(k)
            comp.add(cc.compression)
            name = cc.path_in_schema
            if name in SSSOURCE_DICTIONARY and not any("DICT" in e for e in cc.encodings):
                nodict.add(name)
    rep.check("zstd compression", comp == {"ZSTD"}, ", ".join(sorted(comp)))
    rep.check("SSSOURCE_DICTIONARY columns dictionary-encoded", not nodict,
              f"not dictionary-encoded: {sorted(nodict)}" if nodict else ", ".join(SSSOURCE_DICTIONARY))
    return rep


def check_sort(t, rep):
    """Ascending SSSOURCE_SORT, NULL ssObjectId last."""
    key0, *rest = SSSOURCE_SORT
    sid, valid, _ = to_np(t[key0])
    keys = [np.where(valid, sid, 0), ~valid]          # NULL rows last
    for c in rest:
        col = _flat(t[c])
        if pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
            keys.insert(0, pc.rank(col, sort_keys="ascending", tiebreaker="dense").to_numpy())
        else:
            keys.insert(0, to_np(col)[0])
    order = np.lexsort(keys)
    out = np.flatnonzero(order != np.arange(len(order)))
    detail = f"by {SSSOURCE_SORT}, NULL {key0} last"
    if len(out):
        i = out[0]
        first = _py(to_np(t["obsid"])[0][i])
        detail += f"; {len(out):,} rows out of place, first at row {i:,} (obsid {first})"
    rep.check("sort order", not len(out), detail)


def check_id_split(t, rep):
    on, on_valid, _ = to_np(t["measuredOn"])
    bad = []
    for kind, (idc, parent) in ID_SPLIT.items():
        mine = on_valid & (on == kind)
        _, v_id, _ = to_np(t[idc])
        _, v_par, _ = to_np(t[parent])
        n_missing = int(np.sum(mine & ~v_id))
        n_stray = int(np.sum(~mine & v_id))
        n_pstray = int(np.sum(~mine & v_par))
        if n_missing:
            bad.append(f"{n_missing:,} {kind} rows without {idc}")
        if n_stray:
            bad.append(f"{n_stray:,} non-{kind} rows with {idc}")
        if n_pstray:
            bad.append(f"{n_pstray:,} non-{kind} rows with {parent}")
    rep.check("id split by measuredOn", not bad, "; ".join(bad) or
              "; ".join(f"{k}: {a}/{b}" for k, (a, b) in ID_SPLIT.items()))


def _designated(t):
    """Rows with a designation (not NULL, not blank)."""
    n = pc.utf8_length(pc.utf8_trim_whitespace(_flat(t["designation"]).cast(pa.string())))
    return np.asarray(pc.fill_null(pc.greater(n, 0), False).to_numpy(zero_copy_only=False), dtype=bool)


def check_ssobjectid_rule(t, rep):
    """ssObjectId NULL exactly when there is no SSObject: no designation,
    or no orbit (ephRa/ephDec NULL); and NULL eph* all together."""
    sid, has_sid, _ = to_np(t["ssObjectId"])
    designated = _designated(t)
    ra, ra_valid, _ = to_np(t["ephRa"])
    dec, dec_valid, _ = to_np(t["ephDec"])
    orbit_ra = ra_valid & ~np.isnan(ra)
    orbit_dec = dec_valid & ~np.isnan(dec)
    expect = designated & orbit_ra
    wrong_null = expect & ~has_sid
    wrong_set = ~expect & has_sid
    obsid = to_np(t["obsid"])[0]
    detail = (f"{int(has_sid.sum()):,} with ssObjectId; NULL: {int((~designated).sum()):,} undesignated, "
              f"{int((designated & ~orbit_ra).sum()):,} designated without an orbit (#7)")
    if wrong_null.any():
        detail += f"; {int(wrong_null.sum()):,} NULL with designation+orbit ({_examples(obsid, wrong_null)})"
    if wrong_set.any():
        detail += (f"; {int(wrong_set.sum()):,} set without designation or orbit "
                   f"({_examples(obsid, wrong_set)})")
    rep.check("ssObjectId NULL exactly when no designation or no orbit",
              not (wrong_null.any() or wrong_set.any()), detail)

    # the eph* columns are NULL together (the ellipse may be NULL on its own)
    eph = [c for c in t.column_names if c.startswith("eph") and c not in ELLIPSE_COLUMNS]
    bad = [f"ephDec ({int((orbit_ra != orbit_dec).sum()):,} rows)"] if (orbit_ra != orbit_dec).any() else []
    for c in eph:
        v, valid, k = to_np(t[c])
        present = valid & (~np.isnan(v) if k == "f" else True)
        n_bad = int(np.sum(~orbit_ra & present))
        if n_bad:
            bad.append(f"{c} ({n_bad:,} rows)")
    rep.check("eph* all NULL where ephRa is NULL", not bad, "; ".join(bad) or f"{len(eph)} columns")

    if "status" in t.column_names:
        st, sv, _ = to_np(t["status"])
        isI = sv & (st == "I")
        n_bad = int(np.sum(isI & has_sid))
        rep.check("status 'I' rows have NULL ssObjectId", not n_bad,
                  f"{int(isI.sum()):,} I rows" + (f", {n_bad:,} with ssObjectId" if n_bad else ""))
        rep.info(f"I rows with a designation: {int(np.sum(isI & designated)):,}; "
                 f"undesignated non-I rows: {int(np.sum(~isI & ~designated)):,}")

    # ssObjectId <-> designation one-to-one
    d = to_np(t["designation"])[0]
    df = pd.DataFrame({"sid": sid[has_sid], "des": d[has_sid]})
    n1 = int((df.groupby("des")["sid"].nunique() > 1).sum())
    n2 = int((df.groupby("sid")["des"].nunique() > 1).sum())
    rep.check("ssObjectId <-> designation one-to-one", not (n1 or n2),
              f"{df['sid'].nunique():,} objects" + (
                  f"; {n1} designations with several ids, {n2} ids with several designations"
                  if (n1 or n2) else ""))


def check_ellipse_rule(t, rep):
    ra, ra_valid, _ = to_np(t["ephRa"])
    no_orbit = ~ra_valid | np.isnan(ra)
    bad, present = [], {}
    for c in ELLIPSE_COLUMNS:
        v, valid, _ = to_np(t[c])
        present[c] = valid & ~np.isnan(v)
        n_bad = int(np.sum(no_orbit & present[c]))
        if n_bad:
            bad.append(f"{c} ({n_bad:,} rows)")
    rep.check("ellipse NULL where ephRa is NULL", not bad, "; ".join(bad) or
              f"{int(no_orbit.sum()):,} rows without ephRa")
    together = present[ELLIPSE_COLUMNS[0]]
    split = int(sum(np.sum(together != present[c]) for c in ELLIPSE_COLUMNS[1:]))
    a, b, cov = (to_np(t[c])[0] for c in ELLIPSE_COLUMNS)
    with np.errstate(invalid="ignore"):
        ok = (a > 0) & (b > 0) & (np.abs(cov) <= a.astype(np.float64) * b * (1 + 1e-5))
    n_bad = int(np.sum(together & ~ok))
    rep.check("ellipse sane (NULL together, errors > 0, |cov| <= raErr*decErr)", not (split or n_bad),
              f"{int(together.sum()):,} rows with an ellipse" +
              (f"; {split:,} partially NULL, {n_bad:,} not positive-definite" if (split or n_bad) else ""))


def expected_track_offsets(off_ra, off_dec, rate_ra, rate_dec):
    """The contract's along/cross-track offsets [arcsec], in float64, NaN
    where ephRate = hypot(ephRateRa, ephRateDec) is 0 or an input missing:
    along = (dRa*vRa + dDec*vDec)/|v|, cross = (-dRa*vDec + dDec*vRa)/|v|."""
    x, y, vx, vy = (np.asarray(a, dtype=np.float64) for a in (off_ra, off_dec, rate_ra, rate_dec))
    v = np.hypot(vx, vy)
    with np.errstate(divide="ignore", invalid="ignore"):
        along = np.where(v > 0, (x * vx + y * vy) / v, np.nan)
        cross = np.where(v > 0, (-x * vy + y * vx) / v, np.nan)
    return along, cross


def check_offsets_table(t, rep, eps=TRACK_EPS, sep_gate=TRACK_SEP_GATE_ARCSEC, sep_rtol=TRACK_SEP_RTOL):
    """ephOffsetAlongTrack/CrossTrack against the contract's formula on the
    file's own ephOffsetRa/Dec and ephRateRa/Dec; their NULL rule; and
    along^2 + cross^2 against ephOffsetRa^2 + ephOffsetDec^2 and
    ephOffset^2."""
    keys = to_np(t["obsid"])[0] if "obsid" in t.column_names else np.arange(len(t))
    ra, ra_valid, _ = to_np(t["ephRa"])
    orbit = ra_valid & ~np.isnan(ra)
    x, y, vx, vy = (to_np(t[c])[0].astype(np.float64) for c in TRACK_INPUTS)
    with np.errstate(invalid="ignore"):
        zero_rate = orbit & (np.hypot(vx, vy) == 0)
    want_a, want_c = expected_track_offsets(x, y, vx, vy)
    expect = orbit & np.isfinite(want_a) & np.isfinite(want_c)
    n_inputs_missing = int(np.sum(orbit & ~zero_rate & ~expect))

    got, present, nan_cells = {}, {}, 0
    for c in TRACK_COLUMNS:
        v, valid, _ = to_np(t[c])
        got[c] = v.astype(np.float64)
        present[c] = valid & ~np.isnan(v)
        nan_cells += int(np.sum(valid & np.isnan(v)))

    # the NULL rule
    bad = []
    for c in TRACK_COLUMNS:
        stray = present[c] & ~expect
        missing = expect & ~present[c]
        for what, m in (("set without an orbit", stray & ~orbit),
                        ("set where ephRate is 0", stray & zero_rate),
                        ("set where an input is missing", stray & orbit & ~zero_rate),
                        ("missing with an orbit and a rate", missing)):
            if m.any():
                bad.append(f"{c}: {int(m.sum()):,} {what} ({_examples(keys, m, got[c], k=3)})")
    rep.check("along/cross-track NULL exactly where no orbit or ephRate is 0", not bad,
              "; ".join(bad) or f"{int(expect.sum()):,} set; NULL: {int((~orbit).sum()):,} without an orbit, "
              f"{int(zero_rate.sum()):,} with ephRate 0" +
              (f", {n_inputs_missing:,} with an input missing" if n_inputs_missing else ""))
    if nan_cells:
        rep.info(f"along/cross-track missing values written as NaN, not NULL: {nan_cells:,} cells")

    # the values
    both = expect & present[TRACK_COLUMNS[0]] & present[TRACK_COLUMNS[1]]
    off = np.hypot(x, y)
    tol = eps * np.finfo(np.float32).eps * off
    bad, worst = [], 0.0
    for c, want in zip(TRACK_COLUMNS, (want_a, want_c)):
        d = np.abs(got[c] - want)
        with np.errstate(divide="ignore", invalid="ignore"):
            r = np.where(both, d / np.where(off > 0, off, np.inf), 0.0)
        worst = max(worst, float(np.nanmax(r)) if len(r) else 0.0)
        m = both & (d > tol)
        if m.any():
            bad.append(f"{c}: {int(m.sum()):,} rows ({_examples(keys, m, got[c], want, k=3)})")
    eps32 = float(np.finfo(np.float32).eps)
    rep.check(f"along/cross-track equal the contract formula (<= {eps} float32 eps x |offset|)", not bad,
              "; ".join(bad) or f"{int(both.sum()):,} rows; worst |d|/|offset| = {worst / eps32:.3g} eps")

    # along^2 + cross^2: a rotation of (ephOffsetRa, ephOffsetDec) ...
    h = np.hypot(got[TRACK_COLUMNS[0]], got[TRACK_COLUMNS[1]])
    m = both & (np.abs(h - off) > 2 * tol)
    rep.check("along^2 + cross^2 == ephOffsetRa^2 + ephOffsetDec^2", not m.any(),
              f"{int(m.sum()):,} rows ({_examples(keys, m, h, off, k=3)})" if m.any() else
              f"{int(both.sum()):,} rows")
    # ... and ~ ephOffset^2 (the great-circle separation)
    sep = to_np(t["ephOffset"])[0].astype(np.float64)
    with np.errstate(divide="ignore", invalid="ignore"):
        rel = np.abs(h - sep) / sep
    gated = both & np.isfinite(sep) & (sep > 0) & (sep <= sep_gate)
    m = gated & ~(rel <= sep_rtol)
    qs = (50, 99, 99.99, 100)
    for name, sel in ((f"<= {sep_gate:g}\"", gated), (f"> {sep_gate:g}\"", both & (sep > sep_gate))):
        if sel.any():
            rep.info(f"|sqrt(along^2 + cross^2) - ephOffset| / ephOffset, ephOffset {name} "
                     f"({int(sel.sum()):,} rows): " +
                     "  ".join(f"p{q}={v:.3g}" for q, v in zip(qs, np.nanpercentile(rel[sel], qs))))
    rep.check(f"along^2 + cross^2 ~ ephOffset^2 (rel. {sep_rtol:g} on the root, ephOffset <= {sep_gate:g}\")",
              not m.any(), f"{int(m.sum()):,} of {int(gated.sum()):,} rows outside"
              + (f" ({_examples(keys, m, h, sep, k=3)})" if m.any() else ""))
    return rep


def check_offsets(path, rep=None, **kw):
    """The along/cross-track checks alone (only the columns they need)."""
    rep = rep or Report(f"SSSource along/cross-track offsets: {path}")
    have = pq.read_schema(path).names
    need = ["obsid", "ephRa", "ephOffset", *TRACK_INPUTS, *TRACK_COLUMNS]
    miss = [c for c in need if c not in have]
    if miss:
        rep.check("columns needed by the offsets check present", False, f"missing {miss}")
        return rep
    t = pq.read_table(path, columns=need)
    rep.info(f"{len(t):,} rows")
    return check_offsets_table(t, rep, **kw)


def _measurement_id(t):
    """The measurement's id: diaSourceId or sourceId, whichever is set."""
    did, dv, _ = to_np(t["diaSourceId"])
    sid, sv, _ = to_np(t["sourceId"])
    return np.where(dv, did, sid), dv | sv


def check_primary(t, rep):
    """Exactly one primary row per measurement (processing, id)."""
    mid, valid = _measurement_id(t)
    proc = to_np(t["processing"])[0]
    prim, pv, _ = to_np(t["primary"])
    df = pd.DataFrame({"p": proc[valid], "id": mid[valid], "prim": (prim & pv)[valid]})
    g = df.groupby(["p", "id"], sort=False)["prim"].sum()
    bad = g[g != 1]
    rep.check("one primary row per measurement (processing, id)", bad.empty,
              f"{len(g):,} measurements, {int((~(prim & pv)).sum()):,} non-primary rows" +
              (f"; {len(bad):,} measurements with {sorted(set(bad.tolist()))} primary rows, "
               f"e.g. {list(bad.index[:3])}" if not bad.empty else ""))


# --------------------------------------------------------------------------
# 2. copied: against dia_sources.parquet
# --------------------------------------------------------------------------

#: Block-1 columns copied from dia_sources (status comes from obs_sbn).
COPIED_BLOCK1 = ("trksub", "trkid", "submission_id", "primary", "matchMethod")
#: dia_sources' names of the view's id and parentId.
DIA_ID, DIA_PARENT = "diaSourceId", "parentId"


def _diff(name, keys, mism, a, e):
    """'name: n rows (examples)' for a failing column."""
    return f"{name}: {int(mism.sum()):,} rows ({_examples(keys, mism, to_np(a)[0], to_np(e)[0], k=3)})"


def compare_columns(rep, ss, src, columns, keys, label, src_missing="fail"):
    """Compare ``ss[c]`` against ``src[c]`` (aligned) for each column;
    returns the number of failing columns."""
    bad, absent, n_rows = [], [], 0
    for c in columns:
        if c not in src.column_names or c not in ss.column_names:
            absent.append(c)
            continue
        a, e = ss[c], src[c]
        mism = compare(a, e)
        if mism.any():
            n_rows += int(mism.sum())
            bad.append(_diff(c, keys, mism, a, e))
    detail = f"{len(columns) - len(absent)} columns x {len(keys):,} rows"
    if bad:
        detail += f"; {len(bad)} columns differ:\n" + "\n".join(bad)
    rep.check(label, not bad, detail)
    if absent:
        msg = f"absent from SSSource or the source: {absent}"
        if src_missing == "fail":
            rep.check(f"{label}: source has every column", False, msg)
        else:
            rep.info(msg)
    return len(bad)


def check_id_values(rep, ss, measured_on, src_id, src_parent, keys, label):
    """The split ids against the source's id/parentId, by measuredOn."""
    bad = []
    for kind, (idc, parent) in ID_SPLIT.items():
        mine = measured_on == kind
        for col, srccol in ((idc, src_id), (parent, src_parent)):
            want = pc.if_else(pa.array(mine), _flat(srccol).cast(pa.int64()), pa.nulls(len(mine), pa.int64()))
            mism = compare(ss[col], want)
            if mism.any():
                bad.append(_diff(col, keys, mism, ss[col], want))
    rep.check(label, not bad, "; ".join(bad) or
              ", ".join(f"{k}: id->{a}, parent->{b}" for k, (a, b) in ID_SPLIT.items()))


class _Columns:
    """A Parquet file's columns, read one at a time on access and
    ``take``n by ``rows`` (keeps memory to a column, not a table)."""

    def __init__(self, path, rows):
        self.pf, self.rows = pq.ParquetFile(path), pa.array(rows)
        self.column_names = self.pf.schema_arrow.names

    def __getitem__(self, c):
        return self.pf.read(columns=[c])[c].take(self.rows)


def check_copied(sssource, dia_sources, schema=DEFAULT_SCHEMA, rep=None):
    rep = rep or Report(f"SSSource copied columns: {sssource} vs {dia_sources}")
    names = [c["name"] for c in schema_columns(schema)]
    b = blocks(names)
    block3 = [c for c in b[3] if c not in ID_COLUMNS]

    ss_obsid = to_np(pq.read_table(sssource, columns=["obsid"])["obsid"])[0]
    dia_obsid = to_np(pq.read_table(dia_sources, columns=["obsid"])["obsid"])[0]
    idx = _obsid_index(dia_obsid, ss_obsid)
    n_extra = int(np.sum(idx < 0))
    n_unused = len(dia_obsid) - len(np.unique(idx[idx >= 0]))
    rep.check("same obsid set as dia_sources",
              n_extra == 0 and n_unused == 0 and len(ss_obsid) == len(dia_obsid),
              f"SSSource {len(ss_obsid):,} rows, dia_sources {len(dia_obsid):,}; "
              f"{n_extra:,} not in dia_sources, "
              f"{n_unused:,} dia_sources rows without an SSSource row")
    keep = idx >= 0
    ss = _Columns(sssource, np.flatnonzero(keep))
    src = _Columns(dia_sources, idx[keep])
    keys = ss_obsid[keep]

    b1 = [c for c in COPIED_BLOCK1 if c in src.column_names]
    if "matchMethod" not in src.column_names:
        rep.info("dia_sources has no matchMethod (pre-WP1 extractor): not compared")
    compare_columns(rep, ss, src, b1, keys, "block 1 (copied) equal")
    compare_columns(rep, ss, src, block3, keys, "block 3 (measuredOn, processing, processingTable) equal")
    compare_columns(rep, ss, src, b[4], keys, "block 4 equal (exact, float64->float32 after the cast)",
                    src_missing="fail")
    on = to_np(ss["measuredOn"])[0]
    check_id_values(rep, ss, on, src[DIA_ID], src[DIA_PARENT], keys,
                    "id split values (diaSourceId/parentId -> the measuredOn pair)")
    return rep


# --------------------------------------------------------------------------
# 3. clickhouse: re-fetch a stratified sample from ssp.SubmittableSources
# --------------------------------------------------------------------------

def stratified_sample(strata, n, rng):
    """Row indices: up to ``n`` in all, as equal as possible per stratum
    (small strata are taken whole, the rest shared among the larger)."""
    strata = np.asarray(strata, dtype=object)
    groups = {g: np.flatnonzero(strata == g) for g in pd.unique(strata)}
    left, out = n, []
    for k, (g, rows) in enumerate(sorted(groups.items(), key=lambda x: len(x[1]))):
        q = min(len(rows), left // (len(groups) - k))
        out.append(rng.choice(rows, q, replace=False) if q < len(rows) else rows)
        left -= q
    return np.sort(np.concatenate(out)) if out else np.zeros(0, int)


def ch_fetch(keys, host=None, port=CH_PORT, database=CH_DATABASE, user=None, workers=CH_MAX_WORKERS,
             chunk=CH_CHUNK):
    """``{processing: ids}`` -> the view rows, one query per (processing,
    chunk of ids), at most ``workers`` (<= 4) at once. Read-only."""
    import io

    import clickhouse_connect

    from ssp.export.submittable import bypass_proxy, credentials

    if not 1 <= workers <= CH_MAX_WORKERS:
        raise ValueError(f"workers must be 1..{CH_MAX_WORKERS} (the server is shared)")
    host = host or current_host()
    user, password = credentials(host, port, database, user)
    bypass_proxy(host)
    tasks = [(p, np.asarray(ids[k:k + chunk], dtype=np.int64))
             for p, ids in keys.items() for k in range(0, len(ids), chunk)]

    def run(task):
        p, ids = task
        client = clickhouse_connect.get_client(
            host=host, port=port, database=database, username=user, password=password,
            settings={"readonly": 1, "max_execution_time": 600,
                      "cancel_http_readonly_queries_on_client_close": 1})
        try:
            sql = (f"SELECT * FROM {database}.{CH_VIEW} WHERE processing = {{p:String}} "
                   f"AND id IN ({','.join(map(str, ids.tolist()))}) FORMAT Parquet")
            return pq.read_table(io.BytesIO(client.raw_query(sql, parameters={"p": p})))
        finally:
            client.close()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        parts = [t for t in pool.map(run, tasks) if len(t)]
    return pa.concat_tables(parts, promote_options="permissive") if parts else None


def check_clickhouse(sssource, n=10_000, seed=1, fetch=ch_fetch, schema=DEFAULT_SCHEMA, rep=None):
    rep = rep or Report(f"SSSource vs ClickHouse {CH_DATABASE}.{CH_VIEW}: {sssource}")
    if n > CH_MAX_ROWS:
        raise SystemExit(f"--n at most {CH_MAX_ROWS:,} (the server is shared)")
    names = [c["name"] for c in schema_columns(schema)]
    b = blocks(names)
    block3 = [c for c in b[3] if c not in ID_COLUMNS and c != "processing"]
    ss_all = _read(sssource, ["obsid", "processing", *block3, *b[4], *ID_COLUMNS])
    proc_all = to_np(ss_all["processing"])[0]
    rows = stratified_sample(proc_all, n, np.random.default_rng(seed))
    ss = ss_all.take(pa.array(rows))
    del ss_all
    proc = proc_all[rows]
    mid, mvalid = _measurement_id(ss)
    rep.info(f"sample: {len(rows):,} rows (seed {seed}): {_counts(proc)}")
    rep.check("sampled rows have a measurement id", mvalid.all(), f"{int((~mvalid).sum()):,} without")

    keys = {p: np.unique(mid[(proc == p) & mvalid]) for p in pd.unique(proc)}
    t0 = time.time()
    view = fetch(keys)
    rep.info(f"fetched {0 if view is None else len(view):,} view rows in {time.time() - t0:.1f} s "
             f"({sum(len(v) for v in keys.values()):,} distinct (processing, id))")
    if view is None:
        rep.check("every sampled (processing, id) found in the view", False, "no rows returned")
        return rep
    vk = pd.MultiIndex.from_arrays([to_np(view["processing"])[0], to_np(view["id"])[0]])
    rep.check("(processing, id) unique in the view", vk.is_unique,
              f"{int(vk.duplicated().sum()):,} duplicates" if not vk.is_unique else "")
    vk = vk[~vk.duplicated()]
    view = view.take(pa.array(np.flatnonzero(~pd.MultiIndex.from_arrays(
        [to_np(view["processing"])[0], to_np(view["id"])[0]]).duplicated())))
    idx = vk.get_indexer(pd.MultiIndex.from_arrays([proc, mid]))
    found = (idx >= 0) & mvalid
    obsid = to_np(ss["obsid"])[0]
    rep.check("every sampled (processing, id) found in the view", found.all(),
              f"{int((~found).sum()):,} missing" + (f", e.g. {list(zip(proc[~found][:3], mid[~found][:3]))}"
                                                    if not found.all() else ""))
    ss = ss.filter(pa.array(found))
    src = view.take(pa.array(idx[found]))
    keys_ = obsid[found]
    dropped = [c for c in VIEW_DROPPED if c in src.column_names]
    rep.info(f"view columns not carried (VIEW_DROPPED): {dropped}")
    compare_columns(rep, ss, src, block3, keys_, "block 3 (measuredOn, processingTable) equal")
    compare_columns(rep, ss, src, b[4], keys_, "block 4 equal (exact, float64->float32 after the cast)")
    on = to_np(src["measuredOn"])[0]
    check_id_values(rep, ss, on, src["id"], src["parentId"], keys_,
                    "id split values (view id/parentId -> the measuredOn pair)")
    return rep


# --------------------------------------------------------------------------
# 4. regression: against today's SSSource
# --------------------------------------------------------------------------

def regression_columns(names):
    return [c for c in names
            if (c.startswith(REGRESSION_PREFIXES) or c in REGRESSION_EXACT) and c not in REGRESSION_EXCLUDED]


def check_regression(new, ref, rep=None):
    rep = rep or Report(f"SSSource regression: {new} vs {ref}")
    ref_names = pq.read_schema(ref).names
    new_names = pq.read_schema(new).names
    cols = regression_columns(ref_names)
    missing = [c for c in cols if c not in new_names]
    rep.check("every reference ephemeris/geometry column present", not missing,
              f"{len(cols)} columns" + (f"; missing {missing}" if missing else ""))
    cols = [c for c in cols if c in new_names]

    extra_ref = [c for c in ("designation", "ssObjectId", "primary", "processing", "submission_id", "trksub",
                             "trkid", "diaSourceId") if c in ref_names]
    rt = pq.read_table(ref, columns=["obsid", *cols, *extra_ref])
    nt = _read(new, ["obsid", *cols, "designation", "ssObjectId", "primary", "processing", "submission_id",
                     "trksub", "trkid", *ID_COLUMNS, "measuredOn"])
    r_obsid, n_obsid = to_np(rt["obsid"])[0], to_np(nt["obsid"])[0]
    idx = _obsid_index(r_obsid, n_obsid)
    n_new_only = int(np.sum(idx < 0))
    n_ref_only = len(r_obsid) - len(np.unique(idx[idx >= 0]))
    rep.check("same row set (obsid)", not (n_new_only or n_ref_only) and len(rt) == len(nt),
              f"new {len(nt):,}, reference {len(rt):,}; "
              f"{n_new_only:,} new-only, {n_ref_only:,} reference-only")
    keep = idx >= 0
    nt = nt.filter(pa.array(keep))
    rt = rt.take(pa.array(idx[keep]))
    keys = n_obsid[keep]

    bad, type_bad, repr_diff = [], [], 0
    for c in cols:
        if nt.schema.field(c).type != rt.schema.field(c).type:
            type_bad.append(f"{c}: {nt.schema.field(c).type} vs {rt.schema.field(c).type}")
            continue
        mism, rd = bitwise_mismatch(nt[c], rt[c])
        repr_diff += rd
        if mism.any():
            bad.append(_diff(c, keys, mism, nt[c], rt[c]))
    rep.check("ephemeris/geometry column types unchanged", not type_bad, "; ".join(type_bad))
    rep.check("ephemeris/geometry columns bitwise equal", not bad,
              f"{len(cols)} columns x {len(keys):,} rows"
              + (f"; {len(bad)} differ:\n" + "\n".join(bad) if bad else ""))
    if repr_diff:
        rep.info(f"NULL vs NaN (both missing, counted equal): {repr_diff:,} cells")

    # designation: "" and NULL both mean "none"
    nd, nv, _ = to_np(nt["designation"])
    rd_, rv, _ = to_np(rt["designation"])
    nd = np.where(nv & (nd != ""), nd, None)
    rd_ = np.where(rv & (rd_ != ""), rd_, None)
    mism = pd.Series(nd).fillna("\0").to_numpy() != pd.Series(rd_).fillna("\0").to_numpy()
    rep.check("designation equal ('' == NULL)", not mism.any(),
              f"{int(mism.sum()):,} differ ({_examples(keys, mism, nd, rd_)})" if mism.any() else "")

    # ssObjectId: the reference's where non-zero and with an orbit, else NULL
    if "ssObjectId" in rt.column_names:
        rs, rsv, _ = to_np(rt["ssObjectId"])
        ns, nsv, _ = to_np(nt["ssObjectId"])
        if "ephRa" in rt.column_names:
            era, erv, _ = to_np(rt["ephRa"])
            orbit = erv & ~np.isnan(era)
        else:
            orbit = np.ones(len(rs), dtype=bool)
        expect = rsv & (rs != 0) & orbit
        mism = (nsv != expect) | (expect & (ns != rs))
        rep.check("ssObjectId: reference's where non-zero with an orbit, else NULL", not mism.any(),
                  f"{int(expect.sum()):,} set, {int((~expect).sum()):,} NULL" +
                  (f"; {int(mism.sum()):,} differ ({_examples(keys, mism, np.where(nsv, ns, None), rs)})"
                   if mism.any() else ""))

    # other columns the reference carries
    other = [c for c in ("primary", "processing", "submission_id", "trksub", "trkid") if c in rt.column_names]
    compare_columns(rep, nt, rt, other, keys, f"other reference columns equal ({', '.join(other)})",
                    src_missing="info")
    if "diaSourceId" in rt.column_names and "measuredOn" in nt.column_names:
        mid, _ = _measurement_id(nt)
        rid, rv, _ = to_np(rt["diaSourceId"])
        mism = ~rv | (mid != rid)
        rep.check("reference diaSourceId == the row's diaSourceId/sourceId", not mism.any(),
                  f"{int(mism.sum()):,} differ ({_examples(keys, mism, mid, rid)})" if mism.any() else "")
    return rep


# --------------------------------------------------------------------------
# 5. ellipse: against NearbySSO
# --------------------------------------------------------------------------

def identical_orbits(orbits_a, orbits_b, designations):
    """``{designation: True/False}`` for designations in both snapshots:
    True if every ORBIT_IDENTITY column is equal (NULL == NULL). Designations
    with several rows in a snapshot count as not identical."""
    des = sorted(set(designations))
    tabs = []
    for path in (orbits_a, orbits_b):
        have = pq.read_schema(path).names
        cols = [ORBIT_KEY, *[c for c in ORBIT_IDENTITY if c in have]]
        t = pq.read_table(path, columns=cols, filters=[(ORBIT_KEY, "in", des)]).to_pandas()
        tabs.append(t)
    common = [c for c in tabs[0].columns if c in tabs[1].columns]
    a, b = (t[common] for t in tabs)
    dup = set(a[ORBIT_KEY][a[ORBIT_KEY].duplicated()]) | set(b[ORBIT_KEY][b[ORBIT_KEY].duplicated()])
    a = a.drop_duplicates(ORBIT_KEY).set_index(ORBIT_KEY)
    b = b.drop_duplicates(ORBIT_KEY).set_index(ORBIT_KEY)
    both = a.index.intersection(b.index)
    a, b = a.loc[both], b.loc[both]
    same = np.ones(len(both), dtype=bool)
    for c in a.columns:
        x, y = a[c].to_numpy(), b[c].to_numpy()
        nx, ny = pd.isna(x), pd.isna(y)
        eq = np.where(nx | ny, nx & ny, x == y)
        same &= eq.astype(bool)
    return {d: bool(s) and d not in dup for d, s in zip(both, same)}


def ellipse_metrics(a, b):
    """Per-row disagreement of ellipses ``a`` and ``b`` (each a tuple of
    raErr, decErr, cov arrays): relative raErr, relative decErr, and the
    absolute difference of the correlation coefficients."""
    ar, ad, ac = (np.asarray(x, dtype=np.float64) for x in a)
    br, bd, bc = (np.asarray(x, dtype=np.float64) for x in b)
    with np.errstate(divide="ignore", invalid="ignore"):
        rel_ra = np.abs(ar - br) / np.abs(br)
        rel_dec = np.abs(ad - bd) / np.abs(bd)
        drho = np.abs(ac / (ar * ad) - bc / (br * bd))
    return rel_ra, rel_dec, drho


def check_ellipse(sssource, nearbysso, orbits_a, orbits_b, processing="AP-DS", rtol=1e-2, rho_tol=1e-2,
                  rep=None, worst=10):
    rep = rep or Report(f"SSSource ellipse vs NearbySSO: {sssource} vs {nearbysso}")
    ss = _read(sssource, ["obsid", "diaSourceId", "designation", "processing", "midpointMjdTai",
                          *ELLIPSE_COLUMNS])
    # filter in Arrow: a nullable int64 column goes to pandas as float64,
    # which loses ids > 2^53
    ss = ss.filter(pc.is_valid(ss["diaSourceId"])).to_pandas()
    ss = ss[ss["designation"].notna() & (ss["designation"] != "")]
    if processing and processing != "all":
        ss = ss[ss["processing"] == processing]
    nss = pq.read_table(nearbysso, columns=["diaSourceId", "designation", *ELLIPSE_COLUMNS])
    nss = nss.filter(pc.is_valid(nss["diaSourceId"])).to_pandas()
    rep.info(f"SSSource rows with a diaSourceId and designation ({processing or 'all'}): {len(ss):,}; "
             f"NearbySSO rows: {len(nss):,}")
    j = ss.merge(nss, on=["diaSourceId", "designation"], suffixes=("", "_nss"))
    rep.info(f"present in both (diaSourceId and designation): {len(j):,} rows, "
             f"{j['designation'].nunique():,} objects")
    same = identical_orbits(orbits_a, orbits_b, j["designation"])
    is_same = j["designation"].map(same)
    rep.info(f"orbits: {sum(same.values()):,} identical, {sum(not v for v in same.values()):,} changed, "
             f"{j['designation'].nunique() - len(same):,} missing from a snapshot")
    j = j[is_same.fillna(False).astype(bool)]
    rep.check("rows to compare (identical orbits)", len(j) > 0, f"{len(j):,} rows")
    if not len(j):
        return rep

    a = [j[c].to_numpy(dtype=np.float64) for c in ELLIPSE_COLUMNS]
    b = [j[c + "_nss"].to_numpy(dtype=np.float64) for c in ELLIPSE_COLUMNS]
    pa_, pb = np.isfinite(a[0]), np.isfinite(b[0])
    rep.check("ellipse present in SSSource wherever NearbySSO has one", not np.any(pb & ~pa_),
              f"both {int(np.sum(pa_ & pb)):,}; NearbySSO only {int(np.sum(pb & ~pa_)):,}; "
              f"SSSource only {int(np.sum(pa_ & ~pb)):,}; neither {int(np.sum(~pa_ & ~pb)):,}")
    both = pa_ & pb
    if not both.any():
        return rep
    rel_ra, rel_dec, drho = ellipse_metrics([x[both] for x in a], [x[both] for x in b])
    qs = (50, 90, 99, 99.9, 100)
    for name, v in (("|d raErr|/raErr", rel_ra), ("|d decErr|/decErr", rel_dec), ("|d rho|", drho)):
        q = np.nanpercentile(v, qs)
        rep.info(f"{name:18s} " + "  ".join(f"p{p}={x:.3g}" for p, x in zip(qs, q)) +
                 f"  (> 1e-6: {int(np.sum(v > 1e-6)):,}, > 1e-3: {int(np.sum(v > 1e-3)):,})")
    score = np.fmax(np.fmax(rel_ra / rtol, rel_dec / rtol), drho / rho_tol)
    score = np.where(np.isnan(score), np.inf, score)
    jb = j[both].assign(rel_ra=rel_ra, rel_dec=rel_dec, drho=drho, score=score)
    w = jb.sort_values("score", ascending=False).head(worst)
    rep.info("worst cases (diaSourceId, designation, "
             "SSSource raErr/decErr/cov vs NearbySSO's [deg], metrics):")
    for r in w.itertuples():
        rep.info(f"  {r.diaSourceId} {r.designation!s:14s} "
                 f"{r.ephRaErr:.4g}/{r.ephDecErr:.4g}/{r.ephRa_ephDec_Cov:.3g} vs "
                 f"{r.ephRaErr_nss:.4g}/{r.ephDecErr_nss:.4g}/{r.ephRa_ephDec_Cov_nss:.3g}  "
                 f"rel_ra={r.rel_ra:.3g} rel_dec={r.rel_dec:.3g} drho={r.drho:.3g}")
    n_bad = int(np.sum(score > 1))
    rep.check(f"ellipses agree (rel. err <= {rtol:g}, |d rho| <= {rho_tol:g})", n_bad == 0,
              f"{int(both.sum()):,} compared, {n_bad:,} outside")
    return rep


# --------------------------------------------------------------------------
# 6. counts: against obs_sbn
# --------------------------------------------------------------------------

def check_counts(sssource, obs_sbn, dia_sources=None, station="X05", rep=None):
    rep = rep or Report(f"SSSource counts: {sssource} vs {obs_sbn}")
    ss = _read(sssource, ["obsid", "status", "ssObjectId", "designation", "primary", "processing",
                          "measuredOn", "matchMethod"])
    ob = pq.read_table(obs_sbn, columns=["obsid", "stn", "status"])
    stn = to_np(ob["stn"])[0]
    x05 = ob.filter(pa.array(stn == station))
    rep.info(f"obs_sbn: {len(ob):,} rows, {len(x05):,} {station}; SSSource: {len(ss):,} rows")
    ss_obsid, x_obsid = to_np(ss["obsid"])[0], to_np(x05["obsid"])[0]
    idx = _obsid_index(x_obsid, ss_obsid)
    n_not_x05 = int(np.sum(idx < 0))
    rep.check(f"every SSSource row is an obs_sbn {station} row", n_not_x05 == 0,
              f"{n_not_x05:,} are not" + (f", e.g. {list(ss_obsid[idx < 0][:3])}" if n_not_x05 else ""))

    if dia_sources:
        dia_obsid = to_np(pq.read_table(dia_sources, columns=["obsid"])["obsid"])[0]
        di = _obsid_index(x_obsid, dia_obsid)
        rep.info(f"dia_sources: {len(dia_obsid):,} rows; {station} rows it did not resolve: "
                 f"{len(x_obsid) - int(np.sum(di >= 0)):,}")
        ss_set, dia_set = set(ss_obsid), set(dia_obsid[di >= 0])
        rep.check(f"rows == the {station} obs_sbn rows dia_sources resolved",
                  ss_set == dia_set and len(ss_obsid) == len(dia_set),
                  f"{len(dia_set):,} expected, {len(ss_obsid):,} rows; {len(dia_set - ss_set):,} missing, "
                  f"{len(ss_set - dia_set):,} extra")
    else:
        n_left = len(x_obsid) - len(set(idx[idx >= 0]))
        rep.info(f"(no --dia-sources) {station} rows without an SSSource row: {n_left:,}")

    keep = idx >= 0
    st, sv, _ = to_np(ss["status"])
    want = to_np(x05["status"])[0][idx[keep]]
    mism = (st[keep] != want) | ~sv[keep]
    rep.check("status agrees with obs_sbn", not mism.any(),
              f"{int(mism.sum()):,} differ ({_examples(ss_obsid[keep], mism, st[keep], want)})"
              if mism.any() else "")

    sid, has_sid, _ = to_np(ss["ssObjectId"])
    designated = _designated(ss)
    isI = sv & (st == "I")
    prim, pv, _ = to_np(ss["primary"])
    n7 = designated & ~has_sid
    rep.info(f"status: {_counts(st)}")
    n_obs_I = int(np.sum(to_np(x05["status"])[0] == "I"))
    rep.info(f"I rows: {int(isI.sum()):,} (obs_sbn {station} I rows: {n_obs_I:,})")
    des7 = pd.Series(to_np(ss["designation"])[0][n7]).value_counts()
    rep.info(f"#7 rows (designated, no SSObject): {int(n7.sum()):,} rows of {len(des7):,} objects"
             + (": " + ", ".join(f"{d} ({c})" for d, c in des7.head(20).items()) if len(des7) else ""))
    rep.info(f"non-primary rows: {int(np.sum(~(prim & pv))):,}")
    rep.info(f"rows with an ssObjectId: {int(has_sid.sum()):,} ({len(np.unique(sid[has_sid])):,} objects)")
    for c in ("processing", "measuredOn", "matchMethod"):
        if c in ss.column_names:
            rep.info(f"per {c}: {_counts(to_np(ss[c])[0])}")
    rep.check("status 'I' rows undesignated and without ssObjectId",
              not np.any(isI & (designated | has_sid)),
              f"{int(np.sum(isI & designated)):,} designated, {int(np.sum(isI & has_sid)):,} with ssObjectId")
    return rep


# --------------------------------------------------------------------------
# 7. ssobject-permutation: SSObject must not depend on SSSource's row order
# --------------------------------------------------------------------------

def default_ssobject_cmd():
    """``ssp-build-ssobject`` next to this interpreter, else on PATH."""
    exe = os.path.join(os.path.dirname(sys.executable), "ssp-build-ssobject")
    return [exe] if os.path.exists(exe) else ["ssp-build-ssobject"]


def sssource_subset(sssource, max_objects=None, seed=0):
    """SSSource, or the rows of ``max_objects`` objects drawn at random
    (``seed``) from those with an ssObjectId."""
    if not max_objects:
        return pq.read_table(sssource)
    sid = pc.unique(pq.read_table(sssource, columns=["ssObjectId"])["ssObjectId"].drop_null()).to_numpy()
    if max_objects < len(sid):
        sid = np.sort(np.random.default_rng(seed).choice(sid, max_objects, replace=False))
    return pq.read_table(sssource, filters=[("ssObjectId", "in", sid.tolist())])


def _sha256(path):
    import hashlib
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 24), b""):
            h.update(b)
    return h.hexdigest()


def diff_tables(a, b, key="ssObjectId"):
    """Lines describing how two SSObject tables differ: schema, rows, and
    per column (after aligning on ``key``, if both have it) the rows that
    differ bitwise (floats: NaN == NULL == NaN)."""
    out = []
    if a.schema != b.schema:
        out.append(f"schemas differ: {a.schema.names == b.schema.names and 'types' or 'names'}")
    if len(a) != len(b):
        out.append(f"rows: {len(a):,} vs {len(b):,}")
        return out
    if key in a.column_names and key in b.column_names:
        ka, kb = to_np(a[key])[0], to_np(b[key])[0]
        if not np.array_equal(ka, kb):
            out.append(f"row order differs ({int(np.sum(ka != kb)):,} rows out of place); "
                       f"compared after sorting by {key}")
            a = a.take(pa.array(np.argsort(ka, kind="stable")))
            b = b.take(pa.array(np.argsort(kb, kind="stable")))
            if not np.array_equal(np.sort(ka), np.sort(kb)):
                out.append(f"different {key} sets")
                return out
    keys = to_np(a[key])[0] if key in a.column_names else np.arange(len(a))
    rows_any = np.zeros(len(a), dtype=bool)
    for c in a.column_names:
        if c not in b.column_names:
            continue
        try:
            mism, _ = bitwise_mismatch(a[c], b[c])
        except TypeError as e:
            out.append(f"{c}: {e}")
            continue
        if mism.any():
            rows_any |= mism
            x, _, kx = to_np(a[c])
            y, _, _ = to_np(b[c])
            extra = ""
            if kx == "f":
                with np.errstate(invalid="ignore"):
                    d = np.abs(x[mism].astype(np.float64) - y[mism])
                    r = d / np.abs(y[mism].astype(np.float64))
                if np.isfinite(d).any():
                    extra = f", max |d| {np.nanmax(d):.3g}, max |d|/|x| {np.nanmax(r):.3g}"
            out.append(f"{c}: {int(mism.sum()):,} rows{extra} (e.g. {_examples(keys, mism, x, y, k=3)})")
    if rows_any.any():
        out.append(f"{int(rows_any.sum()):,} of {len(a):,} objects differ in some column")
    return out


def permutation(sid, seed, shuffle_objects=False):
    """Row order of a permuted SSSource copy: the rows of each object (and
    the NULL-ssObjectId rows, as one group) shuffled among themselves, the
    groups kept together, in their order in the file (the builder requires
    SSSource grouped by ssObjectId) or, with ``shuffle_objects``, shuffled
    too. ``sid``: the ssObjectId column (Arrow)."""
    v, valid, _ = to_np(sid)
    group = pd.factorize(pd.Series(np.where(valid, v, -1)), sort=False)[0]
    rng = np.random.default_rng(seed)
    if shuffle_objects:
        group = rng.permutation(group.max() + 1)[group] if len(group) else group
    return np.lexsort((rng.random(len(group)), group))


def check_ssobject_permutation(sssource, dia, mpcorb, seeds=(1, 2), max_objects=None, object_seed=0,
                               workdir=None, cmd=None, extra_args=(), include_original=False,
                               shuffle_objects=False, permute_dia=False, rep=None):
    """Run ssp-build-ssobject (a black box, via its CLI) on permuted copies
    of SSSource (one per seed: see ``permutation``; also the file's own
    order with ``include_original``) and require byte-identical outputs.
    ``dia`` (None for none) is passed to the builder between SSSource and
    the orbits, as its older 3-argument form had it; today's builder
    ignores it. With ``permute_dia``, the DiaSource file is also given a
    random row order per seed (restricted to the SSSource rows' obsids)."""
    import shlex
    import subprocess
    import tempfile

    rep = rep or Report(f"SSObject permutation invariance: {sssource}")
    if permute_dia and dia is None:
        raise ValueError("--permute-dia needs a DiaSource file")
    cmd = shlex.split(cmd) if isinstance(cmd, str) else list(cmd or default_ssobject_cmd())
    tmp = None
    if workdir is None:
        tmp = tempfile.TemporaryDirectory(prefix="ssobject-perm-")
        workdir = tmp.name
    os.makedirs(workdir, exist_ok=True)
    try:
        t = sssource_subset(sssource, max_objects, object_seed)
        n_obj = len(pc.unique(t["ssObjectId"].drop_null())) if "ssObjectId" in t.column_names else 0
        rep.info(f"SSSource: {len(t):,} rows, {n_obj:,} objects"
                 + (f" (a random {max_objects:,}, seed {object_seed})" if max_objects else " (all)"))
        rep.info("permutation: rows within each object" + (", and the objects' order" if shuffle_objects
                                                            else " (objects kept in the file's order)"))
        dia_t = None
        if permute_dia:
            dia_t = pq.read_table(dia)
            if max_objects:
                keep = pc.is_in(dia_t["obsid"], value_set=pc.unique(t["obsid"]))
                dia_t = dia_t.filter(keep)
            rep.info(f"DiaSource: {len(dia_t):,} rows, permuted per seed")
        rep.info(f"builder: {shlex.join(cmd)} SSSOURCE {dia + ' ' if dia else ''}{mpcorb} --output OUT "
                 f"{shlex.join(extra_args)}")
        runs = [("original", None)] if include_original else []
        runs += [(f"seed {s}", s) for s in seeds]
        outs = []
        for name, s in runs:
            tag = "original" if s is None else f"seed{s}"
            src = os.path.join(workdir, f"sssource.{tag}.parquet")
            out = os.path.join(workdir, f"ssobject.{tag}.parquet")
            perm = np.arange(len(t)) if s is None else permutation(t["ssObjectId"], s, shuffle_objects)
            pq.write_table(t.take(pa.array(perm)), src, compression="zstd")
            dia_src = dia
            if dia_t is not None:
                dia_src = os.path.join(workdir, f"dia_sources.{tag}.parquet")
                dperm = (np.arange(len(dia_t)) if s is None
                         else np.random.default_rng([s, 1]).permutation(len(dia_t)))
                pq.write_table(dia_t.take(pa.array(dperm)), dia_src, compression="zstd")
            t0 = time.time()
            with open(os.path.join(workdir, f"ssobject.{tag}.log"), "w") as log:
                r = subprocess.run([*cmd, src, *([dia_src] if dia_src else []), mpcorb, "--output", out,
                                    *extra_args],
                                   stdout=log, stderr=subprocess.STDOUT)
            ok = r.returncode == 0 and os.path.exists(out)
            rep.check(f"builder succeeded ({name})", ok,
                      f"{time.time() - t0:.1f} s" + ("" if ok else f"; exit {r.returncode}, "
                                                     f"log {log.name}"))
            if ok:
                outs.append((name, out))
                os.remove(src)
                if dia_src and dia_src != dia:
                    os.remove(dia_src)
        if len(outs) < 2:
            rep.check("at least two outputs to compare", False, f"{len(outs)}")
            return rep
        (n0, ref), sha0 = outs[0], _sha256(outs[0][1])
        rep.info(f"{n0}: {os.path.getsize(ref):,} bytes, sha256 {sha0[:16]}")
        ta = None
        for name, out in outs[1:]:
            sha = _sha256(out)
            same = sha == sha0
            detail = f"{os.path.getsize(out):,} bytes, sha256 {sha[:16]}"
            rep.check(f"SSObject byte-identical ({name} vs {n0})", same, detail)
            if not same:
                ta = ta if ta is not None else pq.read_table(ref)
                diff = diff_tables(pq.read_table(out), ta)
                rep.info("\n".join(diff) or "tables equal (only the bytes differ)")
        return rep
    finally:
        if tmp is not None:
            tmp.cleanup()


# --------------------------------------------------------------------------
# mock: a widened SSSource faked from today's (development)
# --------------------------------------------------------------------------

FAULTS = {
    "order": "swap two columns",
    "type": "write detector as int32",
    "null": "a NULL ra",
    "dup_obsid": "a duplicated obsid",
    "unsorted": "two rows swapped",
    "match_method": "a matchMethod 'id'",
    "id_split": "a difference row's id in sourceId",
    "ssobjectid": "an orbit row with NULL ssObjectId",
    "ellipse_null": "an ellipse where ephRa is NULL",
    "value": "one psfFlux off by 1 float32 ulp",
    "eph_bit": "one ephRa with its last bit flipped",
    "status": "one status changed",
    "drop_row": "one row dropped",
    "ellipse_value": "one NearbySSO-matched ephRaErr scaled by 1.1",
    "along_value": "one ephOffsetAlongTrack scaled by 1 + 1e-5",
    "cross_sign": "one ephOffsetCrossTrack with its sign flipped",
    "track_swap": "along and cross swapped on one row",
    "track_null": "one ephOffsetAlongTrack NULL where it has a value",
    "track_no_orbit": "an ephOffsetCrossTrack where ephRa is NULL",
    "rate_zero": "one row's ephRateRa/Dec set to 0, along/cross kept",
}


def add_track_offsets(data):
    """The along/cross-track offsets of a table (or dict of columns) with
    TRACK_INPUTS and ephRa, per the contract: float32 Arrow arrays, NULL
    without an orbit or where ephRate is 0."""
    ra, rv, _ = to_np(data["ephRa"])
    x, y, vx, vy = (to_np(data[c])[0] for c in TRACK_INPUTS)
    along, cross = expected_track_offsets(x, y, vx, vy)
    out = []
    for v in (along, cross):
        miss = ~rv | np.isnan(ra) | np.isnan(v)
        out.append(pa.array(np.where(miss, 0, v).astype(np.float32), mask=miss))
    return out


def match_method_from_dia(match, obssubid):
    """The pre-WP1 stand-in for matchMethod: 'id' rows of an -A/-B obsSubID
    are obssubid_trail, other 'id' rows obssubid, the rest position."""
    trail = pd.Series(obssubid).fillna("").str.match(r".*-[AB]$").to_numpy()
    out = np.where(np.asarray(match) == "id", np.where(trail, "obssubid_trail", "obssubid"), "position")
    return out.astype(object)


def build_mock(ref, dia_sources, obs_sbn, out, nearbysso=None, faults=(), schema=DEFAULT_SCHEMA, seed=2):
    """Write a widened SSSource made from today's SSSource (blocks 2, 6),
    dia_sources (blocks 1, 3, 4) and obs_sbn (status), following the design.
    The ellipse comes from NearbySSO where it has the row, else made up."""
    cols = schema_columns(schema)
    names = [c["name"] for c in cols]
    b = blocks(names)
    rt = pq.read_table(ref)
    dia = pq.read_table(dia_sources)
    ob = pq.read_table(obs_sbn, columns=["obsid", "status"])
    d_obsid = to_np(dia["obsid"])[0]
    r_idx = _obsid_index(to_np(rt["obsid"])[0], d_obsid)
    o_idx = _obsid_index(to_np(ob["obsid"])[0], d_obsid)
    assert (r_idx >= 0).all() and (o_idx >= 0).all()
    rt = rt.take(pa.array(r_idx))
    n = len(dia)

    on = to_np(dia["measuredOn"])[0]
    data = {}
    for c in b[1]:
        if c == "status":
            data[c] = ob["status"].take(pa.array(o_idx))
        elif c == "matchMethod":
            data[c] = pa.array(match_method_from_dia(to_np(dia["match"])[0], to_np(dia["obssubid"])[0]))
        else:
            data[c] = dia[c]
    era, erv, _ = to_np(rt["ephRa"])
    orbit = erv & ~np.isnan(era)
    rs, rsv, _ = to_np(rt["ssObjectId"])
    data["ssObjectId"] = pa.array(np.where(rsv & (rs != 0) & orbit, rs, 0), mask=~(rsv & (rs != 0) & orbit))
    rd, rdv, _ = to_np(rt["designation"])
    data["designation"] = pa.array(np.where(rdv & (rd != ""), rd, None), pa.string())
    for c in b[3]:
        if c in ID_COLUMNS:
            continue
        data[c] = dia[c]
    for kind, (idc, parent) in ID_SPLIT.items():
        mine = pa.array(on == kind)
        data[idc] = pc.if_else(mine, dia[DIA_ID].combine_chunks(), pa.nulls(n, pa.int64()))
        data[parent] = pc.if_else(mine, dia[DIA_PARENT].combine_chunks(), pa.nulls(n, pa.int64()))
    for c in b[4]:
        data[c] = dia[c]
    for c in b[6]:
        if c not in ELLIPSE_COLUMNS and c not in TRACK_COLUMNS:
            data[c] = rt[c]
    # the along/cross-track offsets, per the contract
    along, cross = add_track_offsets(data)
    data[TRACK_COLUMNS[0]], data[TRACK_COLUMNS[1]] = along, cross

    # the ellipse: NearbySSO's where it has the row, else made up;
    # NULL without an orbit
    ell = np.full((3, n), np.nan)
    rng = np.random.default_rng(seed)
    s = rng.uniform(5e-6, 3e-5, size=(2, n))
    ell[0], ell[1] = s
    ell[2] = rng.uniform(-0.5, 0.5, n) * s[0] * s[1]
    hit = np.zeros(n, dtype=bool)
    if nearbysso:
        nss = pq.read_table(nearbysso, columns=["diaSourceId", "designation", *ELLIPSE_COLUMNS]).to_pandas()
        key = pd.MultiIndex.from_frame(nss[["diaSourceId", "designation"]])
        did, dv, _ = to_np(data["diaSourceId"])
        q = pd.MultiIndex.from_arrays([np.where(dv, did, -1), to_np(data["designation"])[0]])
        k = key.get_indexer(q)
        hit = (k >= 0) & (to_np(data["processing"])[0] == "AP-DS")
        for i, c in enumerate(ELLIPSE_COLUMNS):
            ell[i, hit] = nss[c].to_numpy()[k[hit]]
    ell[:, ~orbit] = np.nan
    for i, c in enumerate(ELLIPSE_COLUMNS):
        data[c] = pa.array(ell[i].astype(np.float32), mask=np.isnan(ell[i]))

    fields, arrays = [], []
    for c in cols:
        typ = felis_arrow_type(c["datatype"], c["name"] in SSSOURCE_DICTIONARY)
        if "type" in faults and c["name"] == "detector":
            typ = pa.int32()
        arr = _flat(data[c["name"]])
        arr = arr.cast(typ.value_type).dictionary_encode() if pa.types.is_dictionary(typ) else \
            arr.cast(typ, safe=not (pa.types.is_floating(typ) and pa.types.is_floating(arr.type)))
        fields.append(pa.field(c["name"], arr.type, nullable=c.get("nullable", True)))
        arrays.append(arr)
    t = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
    sid, sv, _ = to_np(t["ssObjectId"])
    obsid_rank = pc.rank(_flat(t["obsid"]), sort_keys="ascending").to_numpy()
    order = np.lexsort((obsid_rank, to_np(t["midpointMjdTai"])[0], np.where(sv, sid, 0), ~sv))
    t = t.take(pa.array(order))
    t = apply_faults(t, faults, from_nss=(hit & orbit)[order])
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    pq.write_table(t, out, compression="zstd")
    return t


def _set(t, name, arr):
    nullable = t.schema.field(name).nullable or arr.null_count > 0
    return t.set_column(t.column_names.index(name), pa.field(name, arr.type, nullable=nullable), arr)


def _replace(t, name, i, value):
    """Column ``name`` with row ``i`` set to ``value`` (None = NULL)."""
    col = _flat(t[name])
    typ = t.schema.field(name).type
    vals = col.to_pylist()
    vals[i] = value
    arr = pa.array(vals, col.type)
    if pa.types.is_dictionary(typ):
        arr = arr.dictionary_encode()
    return _set(t, name, arr)


def apply_faults(t, faults, from_nss=None):
    """Inject ``faults`` (names from FAULTS) into a widened SSSource table;
    ``from_nss`` marks the rows whose ellipse came from NearbySSO."""
    for f in faults:
        if f not in FAULTS:
            raise ValueError(f"unknown fault {f}; known: {sorted(FAULTS)}")
    orbit = np.flatnonzero(~np.isnan(to_np(t["ephRa"])[0]))
    no_orbit = np.flatnonzero(np.isnan(to_np(t["ephRa"])[0]))
    diff = np.flatnonzero(to_np(t["measuredOn"])[0] == "difference")
    if "order" in faults:
        names = t.column_names
        i, j = names.index("ra"), names.index("dec")
        names[i], names[j] = names[j], names[i]
        t = t.select(names)
    if "null" in faults:
        t = _replace(t, "ra", 3, None)
    if "dup_obsid" in faults:
        t = _replace(t, "obsid", 5, to_np(t["obsid"])[0][4])
    if "unsorted" in faults:
        idx = np.arange(len(t))
        idx[[10, 11]] = idx[[11, 10]]
        t = t.take(pa.array(idx))
    if "match_method" in faults:
        t = _replace(t, "matchMethod", 7, "id")
    if "id_split" in faults:
        i = int(diff[1])
        t = _replace(t, "sourceId", i, to_np(t["diaSourceId"])[0][i].item())
        t = _replace(t, "diaSourceId", i, None)
    if "ssobjectid" in faults:
        t = _replace(t, "ssObjectId", int(orbit[-1]), None)
    if "ellipse_null" in faults:
        t = _replace(t, "ephRaErr", int(no_orbit[0]), 1e-5)
    if "value" in faults:
        i = 20
        v = np.float32(to_np(t["psfFlux"])[0][i])
        t = _replace(t, "psfFlux", i, float(np.nextafter(v, np.float32(np.inf))))
    if "eph_bit" in faults:
        i = int(orbit[30])
        v = np.array([to_np(t["ephRa"])[0][i]], dtype=np.float64)
        v.view(np.uint64)[0] ^= 1
        t = _replace(t, "ephRa", i, float(v[0]))
    if "status" in faults:
        st = to_np(t["status"])[0]
        i = int(np.flatnonzero(st == "p")[0])
        t = _replace(t, "status", i, "P")
    if "drop_row" in faults:
        t = t.take(pa.array(np.r_[0:40, 41:len(t)]))
    along = to_np(t["ephOffsetAlongTrack"])[0] if "ephOffsetAlongTrack" in t.column_names else None
    cross = to_np(t["ephOffsetCrossTrack"])[0] if "ephOffsetCrossTrack" in t.column_names else None
    if along is not None:
        # rows whose offsets are well away from 0 (a fault there is visible)
        big = np.flatnonzero((np.abs(along) > 0.01) & (np.abs(cross) > 0.01) &
                             (np.abs(np.abs(along) - np.abs(cross)) > 0.01))
    if "along_value" in faults:
        i = int(big[50])
        t = _replace(t, "ephOffsetAlongTrack", i, float(np.float32(along[i] * (1 + 1e-5))))
    if "cross_sign" in faults:
        i = int(big[60])
        t = _replace(t, "ephOffsetCrossTrack", i, float(-cross[i]))
    if "track_swap" in faults:
        i = int(big[70])
        t = _replace(t, "ephOffsetAlongTrack", i, float(cross[i]))
        t = _replace(t, "ephOffsetCrossTrack", i, float(along[i]))
    if "track_null" in faults:
        t = _replace(t, "ephOffsetAlongTrack", int(big[80]), None)
    if "track_no_orbit" in faults:
        t = _replace(t, "ephOffsetCrossTrack", int(no_orbit[1]), 0.5)
    if "rate_zero" in faults:
        i = int(big[90])
        t = _replace(t, "ephRateRa", i, 0.0)
        t = _replace(t, "ephRateDec", i, 0.0)
    if "ellipse_value" in faults:
        ra_err = to_np(t["ephRaErr"])[0]
        proc = to_np(t["processing"])[0]
        ok = from_nss if from_nss is not None else np.ones(len(t), dtype=bool)
        cand = np.flatnonzero(~np.isnan(ra_err) & (proc == "AP-DS") & ok)
        i = int(cand[len(cand) // 2])
        t = _replace(t, "ephRaErr", i, float(ra_err[i] * 1.1))
    return t


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main(argv=None):
    ap = argparse.ArgumentParser(prog="python -m bench.sssource_validate",
                                 description="Black-box validation of the widened SSSource table.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def add(name, help_):
        p = sub.add_parser(name, help=help_)
        p.add_argument("--out", help="also write the report to this file")
        return p

    p = add("conformance", "schema and content rules, from sso_base.yaml")
    p.add_argument("sssource")
    p.add_argument("--schema", default=DEFAULT_SCHEMA)
    p = add("copied", "blocks 1/3/4 against dia_sources.parquet")
    p.add_argument("sssource")
    p.add_argument("dia_sources")
    p.add_argument("--schema", default=DEFAULT_SCHEMA)
    p = add("clickhouse", "a stratified sample re-fetched from ssp.SubmittableSources")
    p.add_argument("sssource")
    p.add_argument("--n", type=int, default=10_000, help=f"sample size (<= {CH_MAX_ROWS:,})")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--workers", type=int, default=CH_MAX_WORKERS,
                   help=f"concurrent queries (<= {CH_MAX_WORKERS})")
    p.add_argument("--host", default=None, help="(default: ~/.clickhouse.host)")
    p.add_argument("--port", type=int, default=CH_PORT)
    p.add_argument("--user", default=None)
    p.add_argument("--schema", default=DEFAULT_SCHEMA)
    p = add("offsets", "along/cross-track offsets against the contract's formula")
    p.add_argument("sssource")
    p.add_argument("--eps", type=float, default=TRACK_EPS, help="tolerance in float32 eps x |offset|")
    p.add_argument("--sep-gate", type=float, default=TRACK_SEP_GATE_ARCSEC,
                   help="gate along^2+cross^2 ~ ephOffset^2 on rows with ephOffset <= this [arcsec]")
    p.add_argument("--sep-rtol", type=float, default=TRACK_SEP_RTOL)
    p = add("regression", "ephemeris/geometry bitwise equal to today's SSSource")
    p.add_argument("new")
    p.add_argument("ref")
    p = add("ellipse", "the error ellipse against NearbySSO")
    p.add_argument("sssource")
    p.add_argument("nearbysso")
    p.add_argument("--orbits-a", required=True, help="mpc_orbits the SSSource was built from")
    p.add_argument("--orbits-b", required=True, help="mpc_orbits the NearbySSO was built from")
    p.add_argument("--processing", default="AP-DS", help="SSSource processing to compare ('all' for any)")
    p.add_argument("--rtol", type=float, default=1e-2)
    p.add_argument("--rho-tol", type=float, default=1e-2)
    p = add("counts", "rows and status against obs_sbn")
    p.add_argument("sssource")
    p.add_argument("obs_sbn")
    p.add_argument("--dia-sources", default=None)
    p = add("ssobject-permutation", "ssp-build-ssobject on row-permuted SSSource copies: identical output")
    p.add_argument("sssource")
    p.add_argument("inputs", nargs="+", metavar="[DIA] MPCORB",
                   help="the MPC orbits; optionally a DiaSource file before them (passed to the builder, "
                        "which ignores it; needed for --permute-dia)")
    p.add_argument("--seeds", default="1,2", help="comma-separated permutation seeds (default: %(default)s)")
    p.add_argument("--include-original", action="store_true", help="also run on the file's own row order")
    p.add_argument("--permute-dia", action="store_true",
                   help="also permute the DiaSource file's rows (per seed)")
    p.add_argument("--shuffle-objects", action="store_true",
                   help="also shuffle the order of the objects (rows stay grouped by ssObjectId)")
    p.add_argument("--max-objects", type=int, default=None,
                   help="use the rows of this many random objects only (for speed)")
    p.add_argument("--object-seed", type=int, default=0, help="seed of the --max-objects draw")
    p.add_argument("--workdir", default=None,
                   help="keep the outputs and logs here (default: a temp dir)")
    p.add_argument("--builder", default=None, help="the builder command (default: ssp-build-ssobject)")
    p.add_argument("--workers", type=int, default=None, help="passed to the builder as --workers")
    p.add_argument("--builder-args", default="", help="more arguments for the builder (one string)")
    p = sub.add_parser("mock", help="(development) a widened SSSource faked from today's")
    p.add_argument("ref")
    p.add_argument("dia_sources")
    p.add_argument("obs_sbn")
    p.add_argument("out")
    p.add_argument("--nearbysso", default=None)
    p.add_argument("--fault", action="append", default=[], choices=sorted(FAULTS))
    p.add_argument("--schema", default=DEFAULT_SCHEMA)

    a = ap.parse_args(argv)
    if a.cmd == "conformance":
        rep = check_conformance(a.sssource, a.schema)
    elif a.cmd == "copied":
        rep = check_copied(a.sssource, a.dia_sources, a.schema)
    elif a.cmd == "clickhouse":
        if not 1 <= a.workers <= CH_MAX_WORKERS:
            ap.error(f"--workers must be 1..{CH_MAX_WORKERS} (the server is shared)")
        if not 1 <= a.n <= CH_MAX_ROWS:
            ap.error(f"--n must be 1..{CH_MAX_ROWS:,} (the server is shared)")

        def fetch(keys):
            return ch_fetch(keys, a.host, a.port, CH_DATABASE, a.user, a.workers)
        rep = check_clickhouse(a.sssource, a.n, a.seed, fetch, a.schema)
    elif a.cmd == "offsets":
        rep = check_offsets(a.sssource, eps=a.eps, sep_gate=a.sep_gate, sep_rtol=a.sep_rtol)
    elif a.cmd == "regression":
        rep = check_regression(a.new, a.ref)
    elif a.cmd == "ellipse":
        rep = check_ellipse(a.sssource, a.nearbysso, a.orbits_a, a.orbits_b, a.processing, a.rtol, a.rho_tol)
    elif a.cmd == "counts":
        rep = check_counts(a.sssource, a.obs_sbn, a.dia_sources)
    elif a.cmd == "ssobject-permutation":
        import shlex
        extra = shlex.split(a.builder_args) + (["--workers", str(a.workers)] if a.workers else [])
        seeds = [int(x) for x in a.seeds.split(",") if x.strip()]
        if len(a.inputs) > 2:
            ap.error("ssobject-permutation: expected SSSOURCE [DIA] MPCORB")
        dia, mpcorb = (a.inputs if len(a.inputs) == 2 else (None, a.inputs[0]))
        rep = check_ssobject_permutation(a.sssource, dia, mpcorb, seeds, a.max_objects,
                                         a.object_seed, a.workdir, a.builder, extra, a.include_original,
                                         a.shuffle_objects, a.permute_dia)
    elif a.cmd == "mock":
        t = build_mock(a.ref, a.dia_sources, a.obs_sbn, a.out, a.nearbysso, a.fault, a.schema)
        print(f"wrote {a.out}: {len(t):,} rows, {t.num_columns} columns; faults: {a.fault or 'none'}")
        return 0
    return rep.finish(a.out)


if __name__ == "__main__":
    sys.exit(main())
