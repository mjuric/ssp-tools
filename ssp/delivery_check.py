"""The delivery check: every delivered PPDB Solar System table against the
schema (ssp.delivery_contract, "the delivery check").

A black box: it reads only the Parquet files and the vendored Felis YAML
(ppdb.yaml, resolved into sso_base.yaml by delivery_schema()), never the
builders. Per table it checks

  * the columns: exactly the schema's, in its order;
  * the types: each Arrow type compatible with the Felis datatype;
  * NULLs: none in ``nullable: false`` columns;
  * the primary key (ppdb.yaml's ``primaryKey``): unique, NULL counting
    as a value, and non-NULL in its ``nullable: false`` columns (NearbySSO's
    key has a nullable diaSourceId, NULL on its unmatched rows);
  * char lengths: string values no longer than the column's ``length``;
  * NaN: none in any float column, for NAN_FREE_TABLES (a missing value
    is NULL; docs/design/elongation-ssobject-nulls.md). The MPC tables
    (mpc_orbits) are not checked: their NaN come from the MPC replica;
  * ASCII, only with ``text_checks`` (``--text-checks``; off by default,
    as it reads every string, about 20-40 s for mpc_orbits.mpc_orb_jsonb):
    every value of every string column ASCII (Felis char/text is VOTable
    char; docs/design/mpc-orbits-ascii.md). Off, the check is absent from
    the results.

SSObservation is delivered as parts plus a manifest and a sidecar
(ssp.ssobservation_contract, "The partitioned delivery"):
check_ssobservation_parts checks the partitioning, the manifest and the
sidecar, and check_table runs on every part. check_elongation checks its
``elongation`` against the apparent elongation from the delivered columns:
the angle between the observed direction (``ra``, ``dec``) and the
observer-to-Sun direction ``topo_* - helio_*``, both aberrated by
``helio_v* - topo_v*``, to within elongation_tolerance_arcsec (1" plus
the rounding of the delivered vectors, by their stored type, float32 or
float64, plus the Sun's motion over the light-time), on every row with
the vectors. check_offsets checks the tangent-plane offsets ``ephOffsetRa``
and ``ephOffsetDec`` against the great-circle ``ephOffset``
(offsets_tolerance_arcsec): it catches an RA difference not wrapped across
RA 0/360 (F-16 of docs/design/precision-doubles.md).

NearbySSO is checked further by check_nearbysso: its sidecar
(NearbySSO_internal.parquet), the NULL pattern of matched and unmatched
rows, the row order, the margin, the times and the visits, and no NaN in
the sidecar's float columns (ssp.nearbysso._contract, "Unmatched
predictions, detector placement and corrected times").

Columns are read one at a time (per part), to bound memory; everything is
Arrow.

    python -m ssp.delivery_check DELIVERY_DIR [--tables T ...]
                                 [--schema-dir D] [--text-checks]

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

#: The tables whose float columns may hold no NaN (a missing value is NULL;
#: docs/design/elongation-ssobject-nulls.md). Not the MPC tables: mpc_orbits
#: keeps the MPC replica's NaN (owner, 2026-11-17).
NAN_FREE_TABLES = ("SSObservation", "SSObject", "NearbySSO")

#: check_elongation's tolerance on each row (elongation_tolerance_arcsec):
#: ELONGATION_TOL_FLOOR_ARCSEC, plus ELONGATION_TOL_ROUNDING_FACTOR times the
#: rounding bound of the observer-to-Sun direction recomputed from the
#: delivered vectors, by the unit roundoff of their stored type (2**-24 for
#: float32, 2**-53 for float64: the same code checks deliveries of either),
#: plus the Sun's barycentric motion over both light-time legs: topo -
#: helio is Sun(t - tau - tau_refl) - observer(t), the Sun as the object
#: saw it (ssp.ephem_assist._apparent_sun), with tau the object-to-observer
#: and tau_refl the Sun-to-object light time, while ``elongation`` is to
#: the Sun at t. With float32 vectors ~1" for nearby objects, ~7" at 90 AU;
#: with float64 vectors (since docs/design/precision-doubles.md) ~1"
#: nearby, ~3" at 90 AU (docs/design/precision-doubles.md, Record).
ELONGATION_TOL_FLOOR_ARCSEC = 1.0
ELONGATION_TOL_ROUNDING_FACTOR = 2.0
#: The Sun's barycentric speed, an upper bound [AU/day] (it stays under
#: ~16 m/s), times the light-time per AU [day]: the Sun's displacement
#: over the light-time, per AU of light path (|topo| + |helio|: tau +
#: tau_refl).
_SUN_LT_AU_PER_AU = (0.016 * 86400 / 149597870.7) * (499.004784 / 86400)
#: Rows whose elongation differs from the angle to ``topo`` (the predicted
#: direction) by more than this [deg] are counted, for information.
ELONGATION_TOPO_INFO_DEG = 0.01
#: The columns check_elongation reads.
ELONGATION_COLUMNS = ("elongation", "ra", "dec",
                      "topo_x", "topo_y", "topo_z", "helio_x", "helio_y", "helio_z",
                      "topo_vx", "topo_vy", "topo_vz", "helio_vx", "helio_vy", "helio_vz")
#: The position columns, whose stored type sets the tolerance's rounding term.
ELONGATION_VECTOR_COLUMNS = ELONGATION_COLUMNS[3:9]
#: check_offsets: the tangent-plane regime, where hypot(ephOffsetRa,
#: ephOffsetDec) must agree with ephOffset (offsets_tolerance_arcsec): the
#: separation and the (wrapped) RA difference both at most this [deg].
#: Beyond it the tangent-plane offsets have no meaning, and only their
#: bounds are checked: |ephOffsetDec| <= ephOffset (a great circle is never
#: shorter than its declination change) and |ephOffsetRa| <= 180 deg x
#: |cos(ephDec)| (the RA difference wrapped to [-180, 180)).
OFFSETS_TANGENT_MAX_DEG = 1.0
#: check_offsets' allowance for the float32 storage of ephOffsetRa,
#: ephOffsetDec and ephOffset, in float32 unit roundoffs (2**-24) relative to
#: the separation: each value is rounded once (half an ulp relative), so
#: hypot(ephOffsetRa, ephOffsetDec) is off by <= 1 and ephOffset by <= 1;
#: twice that.
OFFSETS_TOL_ROUNDING_UNITS = 4.0
#: ... and its absolute floor [arcsec]: the float64 rounding of the RA/Dec
#: differences and of Vincenty's formula (~1e-15 rad x a few; it matters
#: only for separations of milliarcseconds and below).
OFFSETS_TOL_FLOOR_ARCSEC = 1e-9
#: The columns check_offsets reads.
OFFSETS_COLUMNS = ("ephOffsetRa", "ephOffsetDec", "ephOffset", "ephDec")
#: The speed of light [km/s] (SSObservation's velocities are in km/s).
C_KM_S = 299792.458


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


#: Rows per batch read by the ASCII check.
ASCII_BATCH_ROWS = 32_768


def _non_ascii_snippet(s, width=24):
    """``s`` around its first non-ASCII character, for a detail, with the
    non-ASCII characters written as Python escapes (so the report is ASCII)."""
    k = next(i for i, ch in enumerate(s) if not ch.isascii())
    out = (("..." if k > width else "") + s[max(0, k - width):k + width]
           + ("..." if k + width < len(s) else ""))
    return out.encode("ascii", "backslashreplace").decode("ascii")


def _check_ascii(pf, arrow):
    """CheckResult "ascii": every value of every string (string,
    large_string, dictionary<string>) column of ParquetFile ``pf`` is ASCII
    (Felis char/text is VOTable char: docs/design/mpc-orbits-ascii.md). Read
    in batches, to bound memory; for a dictionary column only the values of
    a batch whose dictionary has a non-ASCII entry are looked at."""
    cols = [f.name for f in arrow if _is_string(f.type)]
    if not cols:
        return CheckResult("ascii", True, "no string columns")
    counts = dict.fromkeys(cols, 0)
    examples = {c: [] for c in cols}
    for batch in pf.iter_batches(batch_size=ASCII_BATCH_ROWS, columns=cols, use_threads=True):
        for c, arr in zip(cols, batch.columns):
            if pa.types.is_dictionary(arr.type):
                if pc.all(pc.string_is_ascii(arr.dictionary)).as_py() is not False:
                    continue
                arr = arr.dictionary_decode()
            bad = pc.fill_null(pc.invert(pc.string_is_ascii(arr)), False)
            idx = pc.indices_nonzero(bad)
            if not len(idx):
                continue
            counts[c] += len(idx)
            if len(examples[c]) < N_EXAMPLES:
                examples[c] += [_non_ascii_snippet(s) for s in
                                arr.take(idx.slice(0, N_EXAMPLES - len(examples[c]))).to_pylist()]
    bad = [f"{c} ({counts[c]:,} values with non-ASCII characters; e.g. {' | '.join(examples[c])})"
           for c in cols if counts[c]]
    return CheckResult("ascii", not bad, "; ".join(bad) if bad
                       else f"{len(cols)} string columns ASCII")


def _fmt_names(names, n=8):
    s = ", ".join(names[:n])
    return s + (f", ... ({len(names)} in all)" if len(names) > n else "")


def check_table(table, parquet_path, schema_dir=SCHEMA_DIR, text_checks=False):
    """Check one delivered table's Parquet file against the schema; a list
    of CheckResult (name, ok, detail). ``text_checks``: also the ASCII
    check (_check_ascii), absent otherwise."""
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

    keys = primary_keys(schema_dir).get(table, [])
    res.append(_check_key(pf, keys, got, nrows,
                          nullable=[k for k in keys if spec.get(k, {}).get("nullable", True) is not False]))
    if text_checks:
        res.append(_check_ascii(pf, arrow))
    if table in NAN_FREE_TABLES:
        res.append(_check_nan(pf, arrow))
    return res


def _nan_counts(pf, columns):
    """{column: number of NaN} of ``columns`` of ParquetFile ``pf``, read a
    column and a row group at a time (to bound memory); only those with any."""
    out = {}
    for c in columns:
        n = 0
        for rg in range(pf.num_row_groups):
            col = pf.read_row_group(rg, columns=[c])[c]
            n += pc.sum(pc.cast(pc.is_nan(col), pa.int64())).as_py() or 0
            del col
        if n:
            out[c] = n
    return out


def _check_nan(pf, arrow):
    """CheckResult "no NaN": no NaN in any float column of ParquetFile
    ``pf`` (a missing value is NULL)."""
    cols = [f.name for f in arrow if pa.types.is_floating(f.type)]
    bad = _nan_counts(pf, cols)
    return CheckResult("no NaN", not bad,
                       "NaN in " + "; ".join(f"{c} ({n:,})" for c, n in bad.items()) if bad
                       else f"no NaN in the {len(cols)} float columns")


def _radec_unit(ra_deg, dec_deg):
    ra, dec = np.radians(ra_deg), np.radians(dec_deg)
    return np.stack([np.cos(dec) * np.cos(ra), np.cos(dec) * np.sin(ra), np.sin(dec)], axis=1)


def _angle_deg(a, b):
    """The angle [deg] between the rows of ``a`` and ``b`` (N, 3), robust
    near 0 and 180: atan2(|a x b|, a . b)."""
    return np.degrees(np.arctan2(np.linalg.norm(np.cross(a, b), axis=1), np.einsum("ij,ij->i", a, b)))


def _aberrate(n, beta):
    """Directions ``n`` (N, 3) aberrated to first order in v/c by ``beta``
    (N, 3) = v / c: u' = u + beta - (u . beta) u, normalised, u = n / |n|
    (the rule of ssp.ephem_assist._phase_angle_deg)."""
    n = n / np.linalg.norm(n, axis=1, keepdims=True)
    m = n + beta - np.einsum("ij,ij->i", n, beta)[:, None] * n
    return m / np.linalg.norm(m, axis=1, keepdims=True)


def elongation_from_vectors(topo, helio, ra=None, dec=None, topo_v=None, helio_v=None):
    """The elongation [deg] from topocentric and heliocentric position
    vectors (N, 3), in float64: the angle between the target's direction and
    the observer-to-Sun direction, ``topo - helio``. The target's direction
    is (``ra``, ``dec``) [deg] if given, else ``topo``. With the velocities
    ``topo_v`` and ``helio_v`` [km/s] (N, 3), both directions are aberrated
    by the observatory's velocity relative to the Sun, ``helio_v - topo_v``:
    the apparent elongation, as SSObservation's (and Horizons' S-O-T)."""
    topo = np.asarray(topo, dtype=np.float64)
    sun = topo - np.asarray(helio, dtype=np.float64)
    target = topo if ra is None else _radec_unit(np.asarray(ra, np.float64), np.asarray(dec, np.float64))
    if topo_v is not None:
        beta = (np.asarray(helio_v, np.float64) - np.asarray(topo_v, np.float64)) / C_KM_S
        target, sun = _aberrate(target, beta), _aberrate(sun, beta)
    return _angle_deg(target, sun)


def unit_roundoff(dtype):
    """The unit roundoff of float type ``dtype`` (a NumPy dtype or an Arrow
    type): 2**-24 for float32, 2**-53 for float64."""
    if isinstance(dtype, pa.DataType):
        dtype = dtype.to_pandas_dtype()
    return float(np.finfo(dtype).eps) / 2


def vector_unit_roundoff(schema):
    """The unit roundoff of the stored topo_x/y/z and helio_x/y/z columns of
    Arrow ``schema`` (unit_roundoff; the largest, if their types differ).
    A column that is not floating point (which the type check also fails)
    raises ValueError; check_delivery reports that as an elongation FAIL."""
    u = 0.0
    for c in ELONGATION_VECTOR_COLUMNS:
        u = max(u, unit_roundoff(schema.field(c).type))
    return u


def elongation_tolerance_arcsec(topo, helio, roundoff):
    """check_elongation's tolerance [arcsec] on each row of the (N, 3)
    vectors ``topo`` and ``helio`` [AU], stored with unit roundoff
    ``roundoff`` (unit_roundoff of their type): the floor, the rounding of
    topo - helio, and the Sun's displacement over the light path |topo| +
    |helio| (see ELONGATION_TOL_FLOOR_ARCSEC)."""
    nt = np.linalg.norm(topo, axis=1)
    nh = np.linalg.norm(helio, axis=1)
    ns = np.linalg.norm(topo - helio, axis=1)
    rnd = roundoff * (nt + nh)           # |rounding error| of topo - helio
    lt = _SUN_LT_AU_PER_AU * (nt + nh)   # the Sun's displacement over tau + tau_refl
    rad = (ELONGATION_TOL_ROUNDING_FACTOR * rnd + lt) / ns
    return ELONGATION_TOL_FLOOR_ARCSEC + np.degrees(rad) * 3600


def check_elongation(files):
    """CheckResult "elongation": in every SSObservation part of ``files``,
    on every row with non-NULL topo_* and helio_* (positions and
    velocities), ``elongation`` within the row's tolerance
    (elongation_tolerance_arcsec) of the apparent elongation from the
    delivered columns: the angle between the observed
    direction (``ra``, ``dec``, which ``elongation`` is of) and the
    observer-to-Sun direction ``topo - helio``, both aberrated by the
    observatory's velocity relative to the Sun, ``helio_v - topo_v``
    (elongation_from_vectors; a NULL elongation there fails). The
    observed direction, not ``topo``'s: a row whose orbit predicts its
    position badly (the observed-minus-predicted separation is over 0.01
    deg on ~100 of 8M rows in the 2026-11-16 delivery) would otherwise fail
    although its elongation is right; the detail reports how many rows
    differ from the same angle with ``topo``'s direction by more than
    ELONGATION_TOPO_INFO_DEG, for information. Read a part's row group at
    a time; reports the rows checked and the largest difference, also as a
    fraction of the row's tolerance, and the vectors' stored types (whose
    unit roundoff sets the tolerance's rounding term)."""
    n_checked, n_bad, n_topo, worst, worst_frac, examples = 0, 0, 0, 0.0, 0.0, []
    types = set()
    vec = ELONGATION_COLUMNS[3:]
    for f in files:
        pf = pq.ParquetFile(f)
        missing = [c for c in ELONGATION_COLUMNS if c not in pf.schema_arrow.names]
        if missing:
            return CheckResult("elongation", False, f"{Path(f).name} lacks {missing}")
        roundoff = vector_unit_roundoff(pf.schema_arrow)
        types.update(str(pf.schema_arrow.field(c).type) for c in ELONGATION_VECTOR_COLUMNS)
        offset = 0
        for rg in range(pf.num_row_groups):
            t = pf.read_row_group(rg, columns=list(ELONGATION_COLUMNS))
            first, offset = offset, offset + len(t)
            has = np.ones(len(t), bool)
            for c in vec:
                has &= _valid(t[c])
            if not has.any():
                continue
            topo, helio, topo_v, helio_v = (np.stack([_np(t[c], np.nan)[has] for c in vec[k:k + 3]], axis=1)
                                            for k in (0, 3, 6, 9))
            ra, dec = (_np(t[c], np.nan)[has] for c in ("ra", "dec"))
            want = elongation_from_vectors(topo, helio, ra, dec, topo_v, helio_v)
            got = _np(t["elongation"], np.nan)[has].astype(np.float64)
            d = np.abs(got - want)
            d[~np.isfinite(d)] = np.inf
            with np.errstate(invalid="ignore"):
                n_topo += int(np.sum(~(np.abs(got - elongation_from_vectors(
                    topo, helio, topo_v=topo_v, helio_v=helio_v)) <= ELONGATION_TOPO_INFO_DEG)))
            n_checked += len(d)
            tol = elongation_tolerance_arcsec(topo, helio, roundoff) / 3600
            # (a NaN vector component gives a NaN want, d inf, and a NaN
            # tolerance: bad, at fraction inf)
            with np.errstate(invalid="ignore"):
                frac = d / tol
            frac[np.isnan(frac)] = np.inf
            if len(d):
                worst = max(worst, float(d.max()))
                worst_frac = max(worst_frac, float(frac.max()))
            bad = ~(d <= tol)
            if bad.any():
                n_bad += int(bad.sum())
                rows = np.flatnonzero(has)
                for i in np.flatnonzero(bad)[:N_EXAMPLES - len(examples)]:
                    examples.append(f"{Path(f).name} row {first + rows[i]:,}: {got[i]:.6f} vs {want[i]:.6f} "
                                    f"(tolerance {tol[i] * 3600:.2f} arcsec)")
            del t
    typestr = f" ({'/'.join(sorted(types))})" if types else ""
    detail = (f"{n_checked:,} rows with topo/helio vectors{typestr}; "
              f"max |elongation - apparent from (ra, dec) "
              f"and the vectors| {worst:.3g} deg ({worst * 3600:.3g} arcsec), at most {worst_frac:.2f} of "
              f"the row's tolerance; {n_topo:,} rows differ from the angle to topo by more than "
              f"{ELONGATION_TOPO_INFO_DEG} deg (orbit residuals; not a failure)")
    if n_bad:
        return CheckResult("elongation", False, f"{n_bad:,} rows differ by more than their tolerance; "
                           + detail + "; e.g. " + "; ".join(examples))
    return CheckResult("elongation", True,
                       detail + f"; within the tolerance ({ELONGATION_TOL_FLOOR_ARCSEC:g} arcsec + "
                       f"{ELONGATION_TOL_ROUNDING_FACTOR:g}x the vectors' rounding bound, by their "
                       "stored type, + the Sun's motion over tau + tau_refl)")


def offsets_tolerance_arcsec(sep, off_dec, dec):
    """check_offsets' tolerance [arcsec] on |hypot(ephOffsetRa, ephOffsetDec)
    - ephOffset|, in the tangent-plane regime (OFFSETS_TANGENT_MAX_DEG), for
    rows of separation ``sep`` [arcsec], ``off_dec`` (ephOffsetDec) [arcsec]
    and predicted declination ``dec`` [deg].

    With xi = dRA cos(dec) and eta = dDec (dec the predicted one, as
    ephOffsetRa uses), the great-circle separation s satisfies
    s^2 = dRA^2 cos(dec) cos(dec + eta) + eta^2 + O(s^4), and
    cos(dec) cos(dec + eta) = cos^2(dec) (1 - tan(dec) eta - eta^2 / 2 + ...),
    so |hypot(xi, eta) - s| / s <= |tan(dec)| |eta| / 2 + O(s^2) (radians).
    The O(s^2) term is that of a separation along a parallel, xi^2
    tan^2(dec) / 24, plus the like for the curvature: bounded here by
    (1 + tan^2(dec)) s^2 / 8 (3x the 1/24 seen in a brute-force comparison
    with Vincenty over all declinations to 89.999 deg, separations and RA
    differences up to 1 deg). Plus the float32 storage of the three columns
    (OFFSETS_TOL_ROUNDING_UNITS) and the float64 floor
    (OFFSETS_TOL_FLOOR_ARCSEC)."""
    s = np.radians(np.asarray(sep, np.float64) / 3600)
    eta = np.radians(np.abs(np.asarray(off_dec, np.float64)) / 3600)
    t = np.tan(np.radians(np.asarray(dec, np.float64)))
    rel = OFFSETS_TOL_ROUNDING_UNITS * 2.0 ** -24 + np.abs(t) * eta / 2 + (1 + t * t) * s * s / 8
    return np.asarray(sep, np.float64) * rel + OFFSETS_TOL_FLOOR_ARCSEC


def check_offsets(files):
    """CheckResult "offsets": in every SSObservation part of ``files``, on
    every row with non-NULL ephOffsetRa, ephOffsetDec and ephOffset (and
    ephDec), the tangent-plane offsets against the great-circle separation:
    within OFFSETS_TANGENT_MAX_DEG (separation and RA difference),
    |hypot(ephOffsetRa, ephOffsetDec) - ephOffset| within
    offsets_tolerance_arcsec; beyond it, |ephOffsetDec| <= ephOffset and
    |ephOffsetRa| <= 180 deg x |cos(ephDec)| (each with the same float32 and
    float64 allowances). A NaN fails. An RA difference not wrapped across
    RA 0/360 gives |ephOffsetRa| ~ 360 deg x cos(dec) at a small separation,
    and fails either way. Reports the rows checked, the worst fraction of
    the tolerance (or of the bound) and its separation, and the rows beyond
    the tangent-plane regime."""
    n_checked, n_bad, n_far, worst, worst_sep, examples = 0, 0, 0, 0.0, 0.0, []
    lim = OFFSETS_TANGENT_MAX_DEG * 3600
    slack = 1 + OFFSETS_TOL_ROUNDING_UNITS * 2.0 ** -24
    for f in files:
        pf = pq.ParquetFile(f)
        missing = [c for c in OFFSETS_COLUMNS if c not in pf.schema_arrow.names]
        if missing:
            return CheckResult("offsets", False, f"{Path(f).name} lacks {missing}")
        offset = 0
        for rg in range(pf.num_row_groups):
            t = pf.read_row_group(rg, columns=list(OFFSETS_COLUMNS))
            first, offset = offset, offset + len(t)
            has = np.ones(len(t), bool)
            for c in OFFSETS_COLUMNS:
                has &= _valid(t[c])
            if not has.any():
                continue
            xi, eta, sep, dec = (_np(t[c], np.nan)[has].astype(np.float64) for c in OFFSETS_COLUMNS)
            cosd = np.abs(np.cos(np.radians(dec)))
            with np.errstate(invalid="ignore", divide="ignore"):
                tangent = (sep <= lim) & (np.abs(xi) <= lim * cosd)
                far = ~tangent
                frac = np.full(len(sep), np.inf)
                # the tangent-plane regime
                tol = offsets_tolerance_arcsec(sep, eta, dec)
                frac[tangent] = (np.abs(np.hypot(xi, eta) - sep) / tol)[tangent]
                # beyond it: the bounds
                f_dec = np.abs(eta) / (sep * slack + OFFSETS_TOL_FLOOR_ARCSEC)
                f_ra = np.abs(xi) / (180 * 3600 * cosd * slack + OFFSETS_TOL_FLOOR_ARCSEC)
                frac[far] = np.maximum(f_dec, f_ra)[far]
            frac[np.isnan(frac)] = np.inf
            n_checked += len(sep)
            n_far += int(far.sum())
            if len(frac):
                i = int(np.argmax(frac))
                if frac[i] >= worst:
                    worst, worst_sep = float(frac[i]), float(sep[i])
            bad = ~(frac <= 1.0)
            if bad.any():
                n_bad += int(bad.sum())
                rows = np.flatnonzero(has)
                for i in np.flatnonzero(bad)[:N_EXAMPLES - len(examples)]:
                    examples.append(f"{Path(f).name} row {first + rows[i]:,}: ephOffsetRa {xi[i]:.6g}, "
                                    f"ephOffsetDec {eta[i]:.6g}, ephOffset {sep[i]:.6g} arcsec, "
                                    f"ephDec {dec[i]:.6g}")
            del t
    detail = (f"{n_checked:,} rows with the offsets; at most {worst:.3g} of the tolerance (at ephOffset "
              f"{worst_sep:.4g} arcsec); {n_far:,} rows beyond the tangent-plane regime "
              f"(> {OFFSETS_TANGENT_MAX_DEG:g} deg; bounds only)")
    if n_bad:
        return CheckResult("offsets", False, f"{n_bad:,} rows outside; " + detail + "; e.g. "
                           + "; ".join(examples))
    return CheckResult("offsets", True, detail + "; hypot(ephOffsetRa, ephOffsetDec) vs ephOffset within "
                       "the tangent-plane and float32 allowance")


def _check_key(pf, keys, got, nrows, nullable=()):
    """The key ``keys`` unique (NULL counting as a value, as BigQuery
    compares it), and without NULLs but in its ``nullable`` columns."""
    if not keys:
        return CheckResult("primary key", False, "the schema gives no primaryKey")
    label = ", ".join(keys)
    absent = [k for k in keys if k not in got]
    if absent:
        return CheckResult("primary key", False, f"({label}): column(s) {absent} not in the file")
    t = pf.read(columns=keys)
    t = pa.table({k: _decoded(t[k]) for k in keys})
    problems = []
    nulls = {k: t[k].null_count for k in keys if t[k].null_count and k not in nullable}
    if nulls:
        problems.append("NULLs in " + ", ".join(f"{k} ({n:,})" for k, n in nulls.items()))
        valid = pc.is_valid(t[next(iter(nulls))])
        for k in nulls:
            valid = pc.and_(valid, pc.is_valid(t[k]))
        t = t.filter(valid)
    # (group_by takes NULL as one key value)
    counts = t.group_by(keys).aggregate([([], "count_all")])
    ndup = len(t) - len(counts)
    if ndup:
        dup = counts.filter(pc.greater(counts["count_all"], 1))
        ex = [tuple(r[k] for k in keys) if len(keys) > 1 else r[keys[0]]
              for r in dup.slice(0, N_EXAMPLES).to_pylist()]
        problems.append(f"{ndup:,} duplicate rows over {len(dup):,} key values, e.g. {ex}")
    if problems:
        return CheckResult("primary key", False, f"({label}): " + "; ".join(problems))
    allowed = [k for k in keys if k in nullable and t[k].null_count]
    if allowed:
        return CheckResult("primary key", True, f"({label}): {nrows:,} unique; NULL only in nullable "
                           + ", ".join(f"{k} ({t[k].null_count:,})" for k in allowed))
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


# --------------------------------------------------------------------------
# NearbySSO: its sidecar, NULL pattern, row order, margin and times
# (ssp.nearbysso._contract, "Unmatched predictions, detector placement and
# corrected times"; docs/design/nearbysso-unmatched.md)
# --------------------------------------------------------------------------

#: NearbySSO columns read by check_nearbysso.
NEARBYSSO_CHECK_COLUMNS = ("diaSourceId", "visit", "detector", "designation", "ephOffset",
                           "diaDistanceRank", "ephRateRa", "ephRateDec")
#: The sidecar's placement columns: NULL together (no WCS).
SIDECAR_PLACEMENT = ("eph_x", "eph_y", "ephDetectorEdgeDist")
#: Slack [arcsec] on the margin for the float32 ephDetectorEdgeDist and its
#: computation: float32 resolves ~2e-6" at 30", the distance itself is a
#: float64 great-circle angle; 1e-4" is ~50 float32 steps.
MARGIN_FLOAT_TOL_ARCSEC = 1e-4
#: The margin cut is made at the visit's time (design, "Approach" 2) and
#: the row then moves to its corrected time, ephDetectorEdgeDist following
#: (7). The move is at most |rate| |dt|, with |dt| bounded by
#: MAX_CORRECTION_S (3 s: shifts beyond are hung readouts, flagged degraded
#: by the extract) plus SHUTTER_GRADIENT_MAX_S (0.3 s, the per-pixel part):
#: so the margin check allows |rate| * MARGIN_MOVE_DT_S on top of the
#: float slack. (On the 2026-10-09 inputs no unmatched row is beyond 30".)
MARGIN_MOVE_DT_S = 3.3
#: LSSTCam's detector IDs (189 detectors; ComCam's 0..8 are within).
DETECTOR_RANGE = (0, 188)
#: The earliest plausible day_obs (Rubin's first on-sky data were in 2024).
DAY_OBS_MIN = 20240101
#: TAI - UTC [s] (since 2017-01-01; no leap second announced since), for
#: the night (day_obs: the date at UTC-12h) of a TAI MJD.
TAI_MINUS_UTC_S = 37.0
_MJD0 = np.datetime64("1858-11-17", "D")


def _np(col, fill=None):
    """A ChunkedArray as NumPy (decoded; NULLs filled with ``fill``)."""
    col = _decoded(col)
    if fill is not None:
        col = pc.fill_null(col, fill)
    return col.to_numpy()


def _valid(col):
    return np.asarray(pc.is_valid(col).to_numpy(zero_copy_only=False), dtype=bool)


def _ex(idx, *cols, n=N_EXAMPLES):
    """Example rows ``idx`` (an index array) as 'row k (a, b, ...)'."""
    out = []
    for i in np.asarray(idx)[:n].tolist():
        out.append(f"row {i:,} (" + ", ".join(repr(c[i]) for c in cols) + ")")
    return "; ".join(out)


def day_obs_mjd(day_obs):
    """The MJD of each day_obs date (YYYYMMDD int), or -1 where it is not
    a valid calendar date."""
    day_obs = np.asarray(day_obs, dtype=np.int64)
    out = np.full(len(day_obs), -1, np.int64)
    for k, d in enumerate(day_obs.tolist()):
        try:
            s = f"{d:08d}"
            out[k] = (np.datetime64(f"{s[:4]}-{s[4:6]}-{s[6:]}", "D") - _MJD0).astype(np.int64)
        except ValueError:
            pass
    return out


def tai_night_mjd(mjd_tai):
    """The day_obs of TAI MJDs as the MJD of that date: the UTC date at
    UTC-12h."""
    return np.floor(np.asarray(mjd_tai, np.float64) - TAI_MINUS_UTC_S / 86400 - 0.5).astype(np.int64)


def check_nearbysso(delivery_dir):
    """NearbySSO's own rules beyond the schema (check_table): the sidecar
    (NEARBYSSO_SIDECAR_FILE next to NearbySSO.parquet: rows, key, columns,
    types and NULLs), the NULL pattern of matched and unmatched rows, the
    row order, the 30" margin of the unmatched rows, the rows' times and
    their visits. A list of CheckResult; anything unreadable is a FAIL.

    Reads the key, detector, the NULL-pattern and rate columns of
    NearbySSO and the whole sidecar (~100 B/row)."""
    from ssp.export.submittable import SHUTTER_GRADIENT_MAX_S
    from ssp.nearbysso._contract import (
        NEARBYSSO_KEY,
        NEARBYSSO_NONNULL,
        NEARBYSSO_SIDECAR_DTYPE,
        NEARBYSSO_SIDECAR_FILE,
        NEARBYSSO_SIDECAR_NONNULL,
        NEARBYSSO_UNMATCHED_NULL,
        PLACEMENT_MARGIN_ARCSEC,
    )

    d = Path(delivery_dir)
    res = []
    try:
        nf = pq.ParquetFile(d / "NearbySSO.parquet")
        have = [c for c in NEARBYSSO_CHECK_COLUMNS if c in nf.schema_arrow.names]
        missing = [c for c in NEARBYSSO_CHECK_COLUMNS if c not in have]
        if missing:
            return [CheckResult("nearbysso", False, f"NearbySSO lacks {missing}: its rules not checked")]
        n = nf.read(columns=have)
    except Exception as e:
        return [CheckResult("nearbysso", False, f"cannot read NearbySSO ({type(e).__name__}: {e})")]
    nrows = len(n)

    # ---- the sidecar: there, row for row, its columns and types ----------
    f = d / NEARBYSSO_SIDECAR_FILE
    if not f.exists():
        return [CheckResult("sidecar", False, f"{f}: missing")]
    try:
        sf = pq.ParquetFile(f)
        s = sf.read()
    except Exception as e:
        return [CheckResult("sidecar", False, f"{f}: cannot read ({type(e).__name__}: {e})")]
    if len(s) != nrows:
        return [CheckResult("sidecar", False, f"{f.name}: {len(s):,} rows, NearbySSO {nrows:,}")]
    res.append(CheckResult("sidecar", True, f"{f.name}: {len(s):,} rows, as NearbySSO"))

    want = list(NEARBYSSO_SIDECAR_DTYPE.names)
    names = s.column_names
    probs = []
    if names != want:
        probs.append(f"columns {names}, want {want}")
    for c in want:
        if c in names and names.count(c) == 1:
            t = s.schema.field(c).type
            if not _np_dtype_ok(NEARBYSSO_SIDECAR_DTYPE[c].str, t):
                probs.append(f"{c}: {t}, want {NEARBYSSO_SIDECAR_DTYPE[c].str}")
    res.append(CheckResult("sidecar columns", not probs, "; ".join(probs) or
                           f"{len(want)} columns, in the contract's order and types"))
    if probs:       # (the checks below need the contract's columns and types)
        return res
    sc = {c: s[c] for c in want}

    # NULLs: never in the non-null columns; the placement NULL together;
    # values present are finite
    probs = []
    for c in want:
        if c in NEARBYSSO_SIDECAR_NONNULL and sc[c].null_count:
            probs.append(f"{c}: {sc[c].null_count:,} NULLs")
    pv = [_valid(sc[c]) for c in SIDECAR_PLACEMENT]
    placed = pv[0] & pv[1] & pv[2]
    partial = (pv[0] | pv[1] | pv[2]) & ~placed
    if partial.any():
        probs.append(f"{SIDECAR_PLACEMENT} not NULL together in {int(partial.sum()):,} rows, e.g. "
                     + _ex(np.flatnonzero(partial), *[_np(sc[c]) for c in SIDECAR_PLACEMENT]))
    for c in (*SIDECAR_PLACEMENT, "midpointMjdTai"):
        v = _np(sc[c], np.nan).astype(np.float64)
        bad = _valid(sc[c]) & ~np.isfinite(v)
        if bad.any():
            probs.append(f"{c}: {int(bad.sum()):,} non-finite values")
    res.append(CheckResult("sidecar nulls", not probs, "; ".join(probs) or
                           f"none in {sorted(NEARBYSSO_SIDECAR_NONNULL)}; placement NULL together "
                           f"({int((~placed).sum()):,} rows), finite"))
    fl = [c for c in want if pa.types.is_floating(s.schema.field(c).type)]
    nan = {c: int(pc.sum(pc.cast(pc.is_nan(sc[c]), pa.int64())).as_py() or 0) for c in fl}
    nan = {c: k for c, k in nan.items() if k}
    res.append(CheckResult("sidecar no NaN", not nan,
                           "NaN in " + "; ".join(f"{c} ({k:,})" for c, k in nan.items()) if nan
                           else f"no NaN in the {len(fl)} float columns"))

    # the key, row for row (NULL == NULL)
    probs = []
    for k in NEARBYSSO_KEY:
        a, b = _decoded(n[k]), _decoded(sc[k])
        try:
            same = pc.fill_null(pc.equal(a, b.cast(a.type)), False)
        except Exception as e:
            probs.append(f"{k}: cannot compare ({type(e).__name__}: {e})")
            continue
        both_null = np.asarray(pc.and_(pc.is_null(a), pc.is_null(b)).to_numpy(zero_copy_only=False), bool)
        bad = ~(np.asarray(same.to_numpy(zero_copy_only=False), bool) | both_null)
        if bad.any():
            i = int(np.flatnonzero(bad)[0])
            probs.append(f"{k}: {int(bad.sum()):,} rows differ, first at row {i:,} "
                         f"({b[i].as_py()!r} vs NearbySSO {a[i].as_py()!r})")
    res.append(CheckResult("sidecar key", not probs, "; ".join(probs) or
                           f"{NEARBYSSO_KEY} equal to NearbySSO's, row for row"))

    # ---- NearbySSO: the NULL pattern ---------------------------------------
    matched = _valid(n["diaSourceId"])
    probs = []
    for c in NEARBYSSO_UNMATCHED_NULL[1:]:
        bad = _valid(n[c]) != matched
        if bad.any():
            probs.append(f"{c} NULL where diaSourceId is not, or set where it is NULL: {int(bad.sum()):,} "
                         f"rows, e.g. " + _ex(np.flatnonzero(bad), _np(n["diaSourceId"], -1), _np(n[c], -1)))
    for c in sorted(NEARBYSSO_NONNULL):
        if n[c].null_count:
            probs.append(f"{c}: {n[c].null_count:,} NULLs")
    rank = _np(n["diaDistanceRank"], 1)
    bad = matched & (rank < 1)
    if bad.any():
        probs.append(f"diaDistanceRank < 1 in {int(bad.sum()):,} matched rows")
    nm = int(matched.sum())
    res.append(CheckResult("null pattern", not probs, "; ".join(probs) or
                           f"{NEARBYSSO_UNMATCHED_NULL} NULL together: {nm:,} matched, {nrows - nm:,} "
                           f"unmatched rows; diaDistanceRank >= 1"))

    # ---- row order: matched by diaSourceId, then unmatched by (visit,
    # designation); the key unique (with designation, (visit, designation)
    # is unique among the unmatched rows, so strictly increasing)
    probs = []
    um = np.flatnonzero(~matched)
    if len(um) and um[0] != nm:
        probs.append(f"a matched row after an unmatched one (first unmatched row {int(um[0]):,}, "
                     f"{nm:,} matched rows)")
    did = _np(n["diaSourceId"], 0)[:nm]
    bad = np.flatnonzero(did[1:] < did[:-1]) + 1
    if len(bad) and matched[:nm].all():
        probs.append(f"matched rows not by diaSourceId: {len(bad):,} out of order, e.g. "
                     + _ex(bad, did))
    if not len(probs) and len(um) > 1:
        v = _np(n["visit"], 0)[nm:]
        des = _decoded(n["designation"]).slice(nm)
        des = des.combine_chunks() if isinstance(des, pa.ChunkedArray) else des
        des_le = np.asarray(pc.fill_null(pc.less_equal(des.slice(1), des.slice(0, len(des) - 1)), True)
                            .to_numpy(zero_copy_only=False), bool)
        bad = (v[1:] < v[:-1]) | ((v[1:] == v[:-1]) & des_le)
        if bad.any():
            i = np.flatnonzero(bad) + 1
            probs.append(f"unmatched rows not strictly by (visit, designation): {len(i):,} out of order, "
                         f"e.g. " + _ex(i + nm, _np(n["visit"]), _np(n["designation"])))
    res.append(CheckResult("row order", not probs, "; ".join(probs) or
                           "matched rows by diaSourceId, then unmatched rows by (visit, designation)"))

    # ---- the margin: unmatched rows placed, within 30" -------------------
    probs = []
    edge = _np(sc["ephDetectorEdgeDist"], np.nan).astype(np.float64)
    unplaced = ~matched & ~placed
    if unplaced.any():
        probs.append(f"{int(unplaced.sum()):,} unmatched rows without a placement, e.g. "
                     + _ex(np.flatnonzero(unplaced), _np(n["visit"]), _np(n["designation"])))
    rate = np.hypot(_np(n["ephRateRa"], np.nan), _np(n["ephRateDec"], np.nan)) * 3600 / 86400   # "/s
    tol = MARGIN_FLOAT_TOL_ARCSEC + np.nan_to_num(rate, nan=np.inf) * MARGIN_MOVE_DT_S
    with np.errstate(invalid="ignore"):
        beyond = ~matched & placed & (edge < -PLACEMENT_MARGIN_ARCSEC - tol)
        moved = ~matched & placed & (edge < -PLACEMENT_MARGIN_ARCSEC - MARGIN_FLOAT_TOL_ARCSEC) & ~beyond
    if beyond.any():
        probs.append(f"{int(beyond.sum()):,} unmatched rows beyond the margin, e.g. "
                     + _ex(np.flatnonzero(beyond), _np(n["visit"]), _np(n["designation"]), edge))
    lo = float(np.nanmin(edge[~matched])) if (~matched & placed).any() else None
    res.append(CheckResult("margin", not probs, "; ".join(probs) or
                           f"unmatched rows placed, ephDetectorEdgeDist >= -{PLACEMENT_MARGIN_ARCSEC:g}\" "
                           f"(min {lo if lo is None else round(lo, 5)}; {int(moved.sum()):,} beyond it only "
                           f"by their move to the corrected time)"))

    # ---- times ------------------------------------------------------------
    probs = []
    t = _np(sc["midpointMjdTai"], np.nan).astype(np.float64)
    flag = _np(sc["midpointMjdTai_flag"], False).astype(bool)
    visit = _np(n["visit"], 0).astype(np.int64)
    noflag = ~placed & ~flag
    if noflag.any():
        probs.append(f"{int(noflag.sum()):,} rows without a placement not flagged (no WCS, so no correction)"
                     ", e.g. " + _ex(np.flatnonzero(noflag), _np(n["diaSourceId"], -1), visit))
    # the night of the time is the visit's day_obs
    night = day_obs_mjd(np.unique(visit // 100000))
    vnight = night[np.searchsorted(np.unique(visit // 100000), visit // 100000)]
    with np.errstate(invalid="ignore"):
        wrong = (vnight >= 0) & np.isfinite(t) & (tai_night_mjd(np.nan_to_num(t)) != vnight)
    if wrong.any():
        probs.append(f"{int(wrong.sum()):,} rows whose midpointMjdTai is not in their visit's night, e.g. "
                     + _ex(np.flatnonzero(wrong), visit, t))
    # per visit, the flagged and the corrected rows apart: each set spans
    # at most the shutter's gradient across the focal plane, twice
    # SHUTTER_GRADIENT_MAX_S (|corrected - the visit's midpoint in the
    # table| <= 0.3 s). Flagged rows keep the visit's time, or (without a
    # correction table, or matched rows of a visit whose PPDB times are
    # corrected) their DiaSource's own, itself a header or corrected time.
    # (Flagged and corrected rows are not compared: they differ by the
    # visit's late-readout shift, which has no bound.)
    spread_max = 2 * SHUTTER_GRADIENT_MAX_S
    order = np.lexsort([t, flag, visit])
    vs, fs, ts = visit[order], flag[order], t[order]
    start = np.flatnonzero(np.r_[True, (vs[1:] != vs[:-1]) | (fs[1:] != fs[:-1])])
    stop = np.r_[start[1:], len(vs)]
    spread = (ts[stop - 1] - ts[start]) * 86400
    gv, gf = vs[start], fs[start]
    with np.errstate(invalid="ignore"):
        bad = ~(spread <= spread_max)
    for what, sel in (("flagged", bad & gf), ("corrected", bad & ~gf)):
        if sel.any():
            ex = zip(gv[sel][:N_EXAMPLES], spread[sel][:N_EXAMPLES])
            probs.append(f"{int(sel.sum()):,} visits whose {what} rows' times span more than "
                         f"{spread_max:g} s, e.g. " + "; ".join(f"visit {v} ({s:.3f} s)" for v, s in ex))
    smax = {k: float(np.max(spread[sel])) if sel.any() else 0.0 for k, sel in (("c", ~gf), ("f", gf))}
    res.append(CheckResult("times", not probs, "; ".join(probs) or
                           f"in their visit's night; {int(flag.sum()):,} flagged rows "
                           f"({int((~placed).sum()):,} without a placement); per visit, corrected times "
                           f"span <= {smax['c']:.3f} s, flagged <= {smax['f']:.6f} s "
                           f"(limit {spread_max:g} s)"))

    # ---- visits and detectors -------------------------------------------
    probs = []
    uday = np.unique(visit // 100000)
    bad_day = uday[(night < 0) | (uday < DAY_OBS_MIN)]
    if len(bad_day):
        probs.append(f"{len(bad_day):,} implausible day_obs (visit // 100000), "
                     f"e.g. {bad_day[:N_EXAMPLES].tolist()}")
    det = _np(n["detector"], -1)
    lo_det, hi_det = DETECTOR_RANGE
    bad = (det < lo_det) | (det > hi_det)
    if bad.any():
        probs.append(f"{int(bad.sum()):,} rows with a detector outside {lo_det}..{hi_det}, "
                     "e.g. " + _ex(np.flatnonzero(bad), visit, det))
    # a (visit, object) has matched rows or one unmatched row, never both
    if not probs:
        des = _decoded(n["designation"])
        um_pairs = pa.table({"visit": n["visit"].filter(pa.array(~matched)),
                             "designation": des.filter(pa.array(~matched))})
        m_pairs = pa.table({"visit": n["visit"].filter(pa.array(matched)),
                            "designation": des.filter(pa.array(matched))})
        both = um_pairs.join(m_pairs.group_by(["visit", "designation"]).aggregate([]),
                             ["visit", "designation"], join_type="inner")
        if len(both):
            probs.append(f"{len(both):,} (visit, designation) with both matched and unmatched rows, e.g. "
                         + "; ".join(str(tuple(r.values())) for r in both.slice(0, N_EXAMPLES).to_pylist()))
    span = f"{int(uday.min())}..{int(uday.max())}" if len(uday) else "-"
    res.append(CheckResult("visits", not probs, "; ".join(probs) or
                           f"{len(np.unique(visit)):,} visits on {len(uday):,} nights ({span}); detectors in "
                           f"{lo_det}..{hi_det}; no prediction both matched and unmatched"))
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


def check_delivery(delivery_dir, schema_dir=SCHEMA_DIR, tables=DELIVERY_TABLES, text_checks=False):
    """{Table: [CheckResult, ...]}: each table's DELIVERY_DIR/<Table>.parquet,
    except SSObservation, checked through its manifest
    (check_ssobservation_parts) with check_table on every part (results
    named '<check> [<part file>]'); a missing file is a FAIL.
    ``text_checks``: check_table's ASCII check too."""
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
                    rs = check_table(table, f, schema_dir, text_checks)
                except Exception as e:
                    rs = [CheckResult("file", False, f"{f}: cannot check ({type(e).__name__}: {e})")]
                for r in rs:
                    res.append(CheckResult(f"{r.name} [{f.name}]", r.ok, r.detail))
            if files:
                try:
                    res.append(check_elongation(files))
                except Exception as e:      # a safety net: a FAIL, never a crash
                    res.append(CheckResult("elongation", False, f"cannot check ({type(e).__name__}: {e})"))
                try:
                    res.append(check_offsets(files))
                except Exception as e:      # a safety net: a FAIL, never a crash
                    res.append(CheckResult("offsets", False, f"cannot check ({type(e).__name__}: {e})"))
            out[table] = res
            continue
        path = Path(delivery_dir) / f"{table}.parquet"
        if not path.exists():
            out[table] = [CheckResult("file", False, f"{path}: missing")]
        else:
            out[table] = check_table(table, path, schema_dir, text_checks)
            if table == "NearbySSO" and out[table][0].ok:
                try:
                    out[table].extend(check_nearbysso(delivery_dir))
                except Exception as e:      # a safety net: a FAIL, never a crash
                    out[table].append(CheckResult("nearbysso", False,
                                                  f"cannot check ({type(e).__name__}: {e})"))
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
    ap.add_argument("--text-checks", action="store_true",
                    help="also check that every string value is ASCII (reads every string column: "
                         "about 20-40 s more, nearly all for mpc_orbits.mpc_orb_jsonb; default: off)")
    args = ap.parse_args(argv)
    unknown = [t for t in args.tables if t not in DELIVERY_TABLES]
    if unknown:
        ap.error(f"unknown tables {unknown}; known: {', '.join(DELIVERY_TABLES)}")

    results, timings = {}, {}
    for table in args.tables:
        t0 = time.perf_counter()
        results.update(check_delivery(args.delivery_dir, args.schema_dir, [table], args.text_checks))
        timings[table] = time.perf_counter() - t0
    print(f"delivery check: {args.delivery_dir} (schema: {args.schema_dir})")
    print(format_report(results, timings))
    return 0 if all(r.ok for rs in results.values() for r in rs) else 1


if __name__ == "__main__":
    sys.exit(main())
