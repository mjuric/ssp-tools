"""Build the SSSource table (``ssp-build-sssource``, or ``python -m
ssp.sssource``): the widened SSSource of docs/design/sssource-widened.md.

One row per row of ``dia_sources.parquet`` (extract-submitted-sources), i.e.
per resolved X05 ``obs_sbn`` row, with exactly the columns of
``ssp.sssource_contract.SSSourceDtype`` in six blocks: the obs_sbn link
columns, the identification (ssObjectId, designation), the measurement's
metadata and identifiers, the measurement itself (copied from
dia_sources.parquet and cast to the schema's types), and the ephemeris and
geometry columns, computed per object with ASSIST from mpc_orbits.
"""

import argparse
import contextlib
import io
import os
import sys
import time

from astropy.coordinates import (
    SkyCoord,
    HeliocentricEclipticIAU76,
)
from astropy.time import Time
import astropy.units as u
from functools import partial
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from . import util
from .photfit import hg_V_mag
from .ephem_assist import MJD_J2000, compute_ephemerides_one, open_ephem
from .nearbysso import propagate as _propagate
# (a module attribute, so tests can substitute it)
from . import sssource_ellipse as _ellipse
from .sssource_contract import (
    ELLIPSE_COLUMNS, ID_SPLIT, MATCH_METHODS, SSSOURCE_DICTIONARY, SSSOURCE_NONNULL, SSSOURCE_SORT,
    VIEW_DROPPED, SSSourceDtype,
)


# --------------------------------------------------------------------------
# The column blocks of SSSourceDtype (see docs/design/sssource-widened.md)
# --------------------------------------------------------------------------
_NAMES = SSSourceDtype.names

#: Block 1 columns copied from dia_sources.parquet (status comes from
#: obs_sbn, matchMethod from dia_sources.parquet or _derive_match_method).
LINK_COLUMNS = ("obsid", "trksub", "trkid", "submission_id", "primary")
#: Block 3 columns copied from dia_sources.parquet (the ids are split, see
#: ID_SPLIT).
MEASURED_ON_COLUMNS = ("measuredOn", "processing", "processingTable")
#: Block 4: the measurement, copied from dia_sources.parquet.
MEASUREMENT_COLUMNS = _NAMES[_NAMES.index("visit"):_NAMES.index("glint_trail") + 1]
#: Block 6: the ephemeris and geometry, computed here.
EPHEMERIS_COLUMNS = _NAMES[_NAMES.index("eclLambda"):]
#: Block 6 columns that are measured (from the observed position and time),
#: not orbit-derived: they are filled for rows without an orbit too.
MEASURED_EPH_COLUMNS = ("elongation", "eclLambda", "eclBeta", "galLon", "galLat")

#: dia_sources.parquet columns not carried into SSSource: the view's query
#: helpers, the view's ``parentId`` (split into parentDiaSourceId /
#: parentSourceId), and extract-submitted-sources' match diagnostics (which
#: stay in dia_sources.parquet).
DIA_DROPPED = VIEW_DROPPED + ("parentId", "obssubid", "match",
                              "sep_mas", "dt_ms", "dmag", "band_ok", "n_pass", "ambiguous")


# The SSSource fields compute_sssource_entry fills in (and nothing else):
# what a parallel worker returns to the parent. Keep in sync with it
# (tests/test_sssource_parallel.py checks).
EPH_FIELDS = [
    "ephRateRa", "ephRateDec", "ephRate",
    "ephRa", "ephDec", "ephOffsetDec", "ephOffsetRa", "ephOffset",
    "ephOffsetAlongTrack", "ephOffsetCrossTrack",
    "helio_x", "helio_y", "helio_z", "helioRange",
    "helio_vx", "helio_vy", "helio_vz", "helio_vtot", "helioRangeRate",
    "topo_x", "topo_y", "topo_z", "topoRange",
    "topo_vx", "topo_vy", "topo_vz", "topo_vtot", "topoRangeRate",
    "phaseAngle", "ephVmag",
    *ELLIPSE_COLUMNS,
]

#: The working array the ephemerides are computed in, one row per SSSource
#: row: the object's key and designation, and the block-6 columns, with the
#: SSSourceDtype types (as today's SSSource, so the values are bitwise the
#: same).
WORK_DTYPE = np.dtype([("ssObjectId", "<i8"), ("designation", SSSourceDtype["designation"])]
                      + [(c, SSSourceDtype[c]) for c in EPHEMERIS_COLUMNS])


def along_cross_track(off_ra, off_dec, rate_ra, rate_dec):
    """The offset (``off_ra``, which includes cos(dec), and ``off_dec``)
    resolved along and across the predicted direction of motion (the rates
    ``rate_ra``, which includes cos(dec), and ``rate_dec``), as pipe_tasks'
    ssoAssociation computes them (see ssp.sssource_contract, block 6)::

        along = (off_ra * rate_ra + off_dec * rate_dec) / rate
        cross = (-off_ra * rate_dec + off_dec * rate_ra) / rate

    with rate = hypot(rate_ra, rate_dec). The result is in the offsets'
    units (independent of the rates'). Positive ``along`` is ahead of the
    prediction; positive ``cross`` is to the left of the motion (the
    motion rotated by +90 degrees, i.e. from +RA towards +Dec). Both are
    NaN where the rate is 0 or not finite (no orbit). Computed in float64.
    """
    off_ra, off_dec, rate_ra, rate_dec = (np.asarray(x, dtype=np.float64)
                                          for x in (off_ra, off_dec, rate_ra, rate_dec))
    rate = np.hypot(rate_ra, rate_dec)
    ok = np.isfinite(rate) & (rate > 0)
    safe = np.where(ok, rate, 1.0)
    along = np.where(ok, (off_ra * rate_ra + off_dec * rate_dec) / safe, np.nan)
    cross = np.where(ok, (-off_ra * rate_dec + off_dec * rate_ra) / safe, np.nan)
    return along, cross


def compute_sssource_entry(sss, assoc, mpcorb, dia, ephem, covs=None):
    """Fill the ephemeris-derived SSSource columns (EPH_FIELDS) for one
    object.

    ``mpcorb`` must be indexed by unpacked_primary_provisional_designation;
    ``assoc`` is a structured array holding, per observation, its row in
    ``dia`` (dia_index) and the observer's barycentric state (obs_pos [AU],
    obs_vel [km/s], each of shape (3,)); ``dia`` is a structured array of
    midpointMjdTai, ra and dec. ``covs`` maps designations to their orbits
    with covariances (from ssp.sssource_ellipse.load_orbit_covariances);
    objects not in it, or all if it is None, get a NaN error ellipse.
    """

    # extract only the subset of observations related to this object
    dia = dia[assoc["dia_index"]]

    # just verify we didn't screw up something
    assert np.all(sss["ssObjectId"] == sss["ssObjectId"][0])
    assert len(dia) == len(sss)

    provID = sss["designation"][0]
    ephTimes = Time(dia["midpointMjdTai"], format="mjd", scale="tai")
    e = compute_ephemerides_one(
        provID,
        ephTimes,
        None,
        ephem,
        row=mpcorb.loc[provID],
        obs_pos=assoc["obs_pos"].T,
        obs_vel=assoc["obs_vel"].T,
    )

    sss["ephRateRa"] = e.mu_lon
    sss["ephRateDec"] = e.mu_lat
    sss["ephRate"] = e.mu_total

    # Heliocentric and topocentric vectors are at light-emission time, per
    # the SSSource schema, following JPL Horizons conventions (see
    # ssp.ephem_assist.EphResult).
    # (RA wrapped to [0, 360), bitwise as SkyCoord would)
    sss["ephRa"] = util.wrap_ra_deg(e.ra_deg)
    sss["ephDec"] = e.dec_deg

    sss["ephOffsetDec"] = (dia["dec"] - sss["ephDec"]) * 3600
    sss["ephOffsetRa"] = (dia["ra"] - sss["ephRa"]) * np.cos(np.deg2rad(sss["ephDec"])) * 3600
    sss["ephOffset"] = util.sky_separation_arcsec(sss["ephRa"], sss["ephDec"], dia["ra"], dia["dec"])
    # along/cross-track [arcsec]: from the float64 offsets and rates (not
    # the float32-stored rates); NaN (NULL) where the rate is 0
    sss["ephOffsetAlongTrack"], sss["ephOffsetCrossTrack"] = along_cross_track(
        sss["ephOffsetRa"], sss["ephOffsetDec"], e.mu_lon, e.mu_lat)

    # Compute heliocentric position components
    sss["helio_x"] = e.helio_pos[0]
    sss["helio_y"] = e.helio_pos[1]
    sss["helio_z"] = e.helio_pos[2]
    sss["helioRange"] = np.sqrt(sss["helio_x"] ** 2 + sss["helio_y"] ** 2 + sss["helio_z"] ** 2)

    # Compute heliocentric velocity components
    sss["helio_vx"] = e.helio_vel[0]
    sss["helio_vy"] = e.helio_vel[1]
    sss["helio_vz"] = e.helio_vel[2]
    sss["helio_vtot"] = np.sqrt(sss["helio_vx"] ** 2 + sss["helio_vy"] ** 2 + sss["helio_vz"] ** 2)

    # Compute heliocentric radial velocity: dot product of velocity
    # and unit position vector
    sss["helioRangeRate"] = (
        sss["helio_vx"] * sss["helio_x"] + sss["helio_vy"] * sss["helio_y"] + sss["helio_vz"] * sss["helio_z"]
    ) / sss["helioRange"]

    # Compute topocentric position components
    sss["topo_x"] = e.topo_pos[0]
    sss["topo_y"] = e.topo_pos[1]
    sss["topo_z"] = e.topo_pos[2]
    sss["topoRange"] = np.sqrt(sss["topo_x"] ** 2 + sss["topo_y"] ** 2 + sss["topo_z"] ** 2)

    # Compute topocentric velocity components
    sss["topo_vx"] = e.topo_vel[0]
    sss["topo_vy"] = e.topo_vel[1]
    sss["topo_vz"] = e.topo_vel[2]
    sss["topo_vtot"] = np.sqrt(sss["topo_vx"] ** 2 + sss["topo_vy"] ** 2 + sss["topo_vz"] ** 2)

    # Compute topocentric radial velocity: dot product of velocity
    # and unit position vector
    sss["topoRangeRate"] = (
        sss["topo_vx"] * sss["topo_x"] + sss["topo_vy"] * sss["topo_y"] + sss["topo_vz"] * sss["topo_z"]
    ) / sss["topoRange"]

    sss["phaseAngle"] = e.phase_angle

    sss["ephVmag"] = hg_V_mag(e.H, e.G, sss["helioRange"], sss["topoRange"], e.phase_angle)

    # The predicted position's error ellipse, at the observation times (TDB
    # days since J2000, as compute_ephemerides_one integrates in; astropy
    # caches ephTimes.tdb), from the same observer positions, along the
    # precise pass's float64 line of sight (e.topo_pos, not the float32
    # topo_x/y/z: see ssp.sssource_ellipse.ephemeris_ellipse).
    orbit = covs.get(provID) if covs is not None else None
    if orbit is None:
        for c in ELLIPSE_COLUMNS:
            sss[c] = np.nan
    else:
        t_assist = ephTimes.tdb.mjd - MJD_J2000
        ellipse = _ellipse.ephemeris_ellipse(orbit, t_assist, assoc["obs_pos"], e.topo_pos.T, ephem)
        for c, v in zip(ELLIPSE_COLUMNS, ellipse):
            sss[c] = v

    max_sep = np.max(sss["ephOffset"])
    med_sep = np.median(sss["ephOffset"])
    print(f"{provID}: max/median separation: {max_sep:.4f}, {med_sep:.4f} arcsec")


#
# Parallel ephemerides (--workers N > 1)
#
# Workers are forked, and read their inputs from this module-level dict,
# filled in by the parent just before it creates the pool, so the large
# arrays are inherited through fork, never pickled. Each worker opens its
# own ASSIST ephemeris (the parent's C-level object isn't shared across the
# fork). Tasks are ranges of groups (objects); results are EPH_FIELDS
# arrays for their rows.
#
_PARALLEL = {}
_EPHEM = None   # one ASSIST ephemeris per worker process, opened lazily


def _sssource_chunk(g0, g1):
    """Worker: compute the EPH_FIELDS of the rows of groups [g0, g1);
    returns them, and the number of the ellipse's coarse propagations the
    step cap stopped (counted per process, see ssp.nearbysso.propagate)."""
    global _EPHEM
    if _EPHEM is None:
        _EPHEM = open_ephem()
    _propagate.STEP_CAP_STOPS = 0
    sss, obs_state = _PARALLEL["sss"], _PARALLEL["obs_state"]
    idx_start, idx_end = _PARALLEL["idx_start"], _PARALLEL["idx_end"]
    r0, r1 = idx_start[g0], idx_end[g1 - 1]   # (groups are in row order)

    # A private array holding only what compute_sssource_entry reads and
    # EPH_FIELDS: writing any other field fails here, rather than being
    # silently lost.
    keys = ["ssObjectId", "designation"]
    out = np.zeros(r1 - r0, dtype=[(f, sss.dtype[f]) for f in keys + EPH_FIELDS])
    for f in keys:
        out[f] = sss[f][r0:r1]

    # (the per-object lines go out in one write per chunk, so lines from
    # different workers don't mix)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        for g in range(g0, g1):
            s, e = idx_start[g] - r0, idx_end[g] - r0
            compute_sssource_entry(out[s:e], obs_state[r0 + s:r0 + e],
                                   _PARALLEL["mpcorb"], _PARALLEL["dia_eph"], _EPHEM,
                                   covs=_PARALLEL["covs"])
    sys.stdout.write(buf.getvalue())
    sys.stdout.flush()

    res = np.empty(len(out), dtype=[(f, out.dtype[f]) for f in EPH_FIELDS])
    for f in EPH_FIELDS:
        res[f] = out[f]
    return res, int(_propagate.STEP_CAP_STOPS)


def compute_ephemerides(sss, obs_state, dia_eph, mpcorb, workers=1, chunk_factor=8, covs=None):
    """Fill the EPH_FIELDS of ``sss`` with compute_sssource_entry, per
    object (``sss`` grouped by ssObjectId; ``obs_state`` its rows' observer
    states and DiaSource rows in ``dia_eph``; ``covs`` the orbit
    covariances for the error ellipse, see compute_sssource_entry).

    With ``workers`` > 1, the objects are split into about ``chunk_factor *
    workers`` chunks, balanced by observation count, and computed in a
    forked process pool (serially where fork is unavailable). The result
    is identical.

    Returns the number of the ellipse's coarse propagations stopped by the
    step cap.
    """
    if workers <= 1 or util.fork_context() is None or len(sss) == 0:
        # JPL planet and ASSIST asteroid ephemeris files, from the
        # SSP_ASSIST_PLANETS and SSP_ASSIST_ASTEROIDS environment variables.
        ephem = open_ephem()
        _propagate.STEP_CAP_STOPS = 0
        util.group_by(
            [sss, obs_state], "ssObjectId",
            partial(compute_sssource_entry, mpcorb=mpcorb, dia=dia_eph, ephem=ephem, covs=covs),
        )
        return int(_propagate.STEP_CAP_STOPS)

    # Group boundaries as util.group_by computes them, taken in row order
    # so that each chunk of groups is a contiguous range of rows.
    keys = sss["ssObjectId"]
    if not util.values_grouped(keys):
        raise ValueError("Key 'ssObjectId' is not properly grouped.")
    _, idx_start, counts = np.unique(keys, return_index=True, return_counts=True)
    order = np.argsort(idx_start)
    idx_start, counts = idx_start[order], counts[order]
    idx_end = idx_start + counts
    # contiguous runs of groups, balanced by observation count
    chunks = util.balanced_chunks(counts, chunk_factor * workers)
    print(f"Computing ephemerides of {len(counts):,} objects in {len(chunks)} chunks "
          f"on {workers} workers...", flush=True)
    _PARALLEL.update(sss=sss, obs_state=obs_state, dia_eph=dia_eph, mpcorb=mpcorb, covs=covs,
                     idx_start=idx_start, idx_end=idx_end)
    try:
        results = util.run_chunks(_sssource_chunk, chunks, workers, "ephemerides",
                                  weights=[int(counts[g0:g1].sum()) for g0, g1 in chunks])
    finally:
        _PARALLEL.clear()
    for (g0, g1), (res, _) in zip(chunks, results):
        r0, r1 = idx_start[g0], idx_end[g1 - 1]
        for f in EPH_FIELDS:
            sss[f][r0:r1] = res[f]
    return sum(stops for _, stops in results)


# --------------------------------------------------------------------------
# Writing sssource.parquet: casts and checks to the contract's types
# --------------------------------------------------------------------------

def arrow_type(name):
    """The Arrow type of SSSource column ``name``, from SSSourceDtype:
    ``U<n>`` is a string (dictionary-encoded for SSSOURCE_DICTIONARY), the
    rest the NumPy type's equivalent."""
    dt = SSSourceDtype[name]
    if dt.kind == "U":
        return pa.dictionary(pa.int32(), pa.string()) if name in SSSOURCE_DICTIONARY else pa.string()
    return pa.from_numpy_dtype(dt)


def sssource_schema():
    """The Arrow schema of sssource.parquet: SSSourceDtype's columns, in
    order, with their arrow_type, non-null exactly for SSSOURCE_NONNULL."""
    return pa.schema([pa.field(n, arrow_type(n), nullable=n not in SSSOURCE_NONNULL) for n in _NAMES])


def cast_column(name, arr):
    """Cast ``arr`` (an Arrow array, chunked or not, or a NumPy array) to
    SSSource column ``name``'s type, and check it.

    Raises ValueError if a narrowing integer cast would overflow, a finite
    float64 would overflow float32, a string is longer than the column's
    ``char`` length, or a non-null column (SSSOURCE_NONNULL) has a NULL.
    float64 -> float32 rounding is expected, not an error.
    """
    dt = SSSourceDtype[name]
    target = arrow_type(name)
    if not isinstance(arr, (pa.Array, pa.ChunkedArray)):
        arr = pa.array(arr)
    if pa.types.is_dictionary(arr.type) and arr.type != target:
        arr = arr.cast(arr.type.value_type)
    value_type = target.value_type if pa.types.is_dictionary(target) else target

    if arr.type != target and arr.type != value_type:
        try:
            if pa.types.is_floating(value_type):
                out = pc.cast(arr, value_type, safe=False)
                if pa.types.is_floating(arr.type) and pc.any(
                        pc.and_kleene(pc.is_finite(arr), pc.invert(pc.is_finite(out)))).as_py():
                    raise ValueError(f"values overflow {value_type}")
            else:
                # (safe: raises on integer overflow and float truncation)
                out = pc.cast(arr, value_type, safe=True)
        except (pa.ArrowInvalid, ValueError) as e:
            raise ValueError(f"SSSource column {name!r}: cannot cast {arr.type} to {value_type}: {e}") from e
        arr = out

    if dt.kind == "U" and arr.type == value_type and len(arr) and arr.null_count < len(arr):
        maxlen = pc.max(pc.utf8_length(arr)).as_py()
        if maxlen > dt.itemsize // 4:
            raise ValueError(f"SSSource column {name!r}: a value of {maxlen} characters "
                             f"is longer than the column's {dt.itemsize // 4}")

    if name in SSSOURCE_NONNULL and arr.null_count:
        raise ValueError(f"SSSource column {name!r} is non-null, but has {arr.null_count:,} NULL values")

    if arr.type != target:
        arr = pc.dictionary_encode(arr)
        if arr.type != target:   # (the index type)
            arr = arr.cast(target)
    if isinstance(arr, pa.ChunkedArray):
        arr = arr.combine_chunks()
    return arr


def sssource_table(columns, cast=True):
    """The SSSource table from ``columns`` (name -> array, every column of
    SSSourceDtype and nothing else), in schema order, each cast and checked
    with cast_column; with ``cast=False`` the columns must already be
    cast_column's output (their types are checked, not their values)."""
    names = set(columns)
    if names != set(_NAMES):
        raise ValueError(f"SSSource columns: missing {sorted(set(_NAMES) - names)}, "
                         f"unexpected {sorted(names - set(_NAMES))}")
    schema = sssource_schema()
    if not cast:
        bad = [n for n in _NAMES if columns[n].type != schema.field(n).type]
        if bad:
            raise ValueError(f"SSSource columns not cast (see cast_column): {bad}")
        return pa.Table.from_arrays([columns[n] for n in _NAMES], schema=schema)
    return pa.Table.from_arrays([cast_column(n, columns[n]) for n in _NAMES], schema=schema)


def sort_indices(ssObjectId, midpointMjdTai, obsid):
    """The row order of sssource.parquet (SSSOURCE_SORT): ascending, NULL
    ssObjectId last."""
    keys = pa.table(dict(zip(SSSOURCE_SORT, (ssObjectId, midpointMjdTai, obsid))))
    # (NULLs last by an explicit key: where null_placement goes differs
    # across pyarrow versions)
    keys = keys.append_column("_null", pc.is_null(keys[SSSOURCE_SORT[0]]))
    return pc.sort_indices(keys, sort_keys=[(k, "ascending") for k in ("_null", *SSSOURCE_SORT)]).to_numpy()


def write_sssource(table, path):
    """Write the SSSource ``table`` (from sssource_table, rows already in
    sort_indices order) to ``path``, zstd-compressed."""
    if table.schema != sssource_schema():
        raise ValueError("not an SSSource table (see sssource_table)")
    pq.write_table(table, path, compression="zstd")


def _derive_match_method(match, obssubid):
    """matchMethod for a dia_sources.parquet that predates it, from the
    extractor's ``match`` (``id``/``position``) and the obs_sbn
    ``obssubid``: ``position`` where match is position; ``obssubid_trail``
    where an id match's obssubid ends in -A or -B (one of a trail pair: the
    extractor resolves an -A/-B row by id only as a pair, a lone one by
    position); else ``obssubid``. A documented fallback, for older files."""
    obssubid = pc.utf8_trim_whitespace(obssubid)
    trail = pc.fill_null(pc.or_(pc.ends_with(obssubid, "-A"), pc.ends_with(obssubid, "-B")), False)
    is_id = pc.equal(match, "id")
    return pc.if_else(pc.equal(match, "position"), "position",
                      pc.if_else(pc.and_(is_id, trail), "obssubid_trail",
                                 pc.if_else(is_id, "obssubid", pa.scalar(None, pa.string()))))


def _check_values(name, arr, allowed):
    """Raise ValueError if ``arr`` has a non-null value not in ``allowed``."""
    bad = pc.filter(arr, pc.invert(pc.fill_null(pc.is_in(arr, pa.array(allowed)), True)))
    if len(bad):
        raise ValueError(f"{name}: unexpected values {pc.unique(bad).to_pylist()[:10]} "
                         f"(expected one of {list(allowed)})")


def _split_ids(measuredOn, id_, parent_id):
    """The block-3 identifier columns (ID_SPLIT): the view's ``id`` and
    ``parentId`` go to the pair matching ``measuredOn``; the other pair is
    NULL."""
    _check_values("measuredOn", measuredOn, list(ID_SPLIT))
    out = {}
    for kind, (id_col, parent_col) in ID_SPLIT.items():
        sel = pc.fill_null(pc.equal(measuredOn, kind), False)
        out[id_col] = pc.if_else(sel, id_, pa.scalar(None, id_.type))
        out[parent_col] = pc.if_else(sel, parent_id, pa.scalar(None, parent_id.type))
    return out


def _dia_read_columns(dia_present):
    """The dia_sources.parquet columns the build copies (block 1, 3 and 4),
    and whether matchMethod is among them; raises ValueError if some are
    missing."""
    has_match_method = "matchMethod" in dia_present
    need = (list(LINK_COLUMNS) + list(MEASURED_ON_COLUMNS) + ["diaSourceId", "parentId"]
            + list(MEASUREMENT_COLUMNS) + (["matchMethod"] if has_match_method else ["match", "obssubid"]))
    missing = [c for c in need + ["sep_mas", "dt_ms"] if c not in dia_present]
    if missing:
        raise ValueError(f"dia_sources.parquet lacks {missing}: SSSource is built from the output of "
                         "extract-submitted-sources")
    return need, has_match_method


# --------------------------------------------------------------------------
# The build
# --------------------------------------------------------------------------

def build_sssource(input_dir, output_dir, max_objects=None, dia_sample_frac=1.0, seed=42,
                   workers=1, chunk_factor=8):
    """Build ``{output_dir}/sssource.parquet`` from the dia_sources (from
    extract-submitted-sources), MPC observation (obs_sbn), identification
    and orbit tables in ``input_dir``.

    ``max_objects`` and ``dia_sample_frac`` subsample the inputs, for
    testing. ``workers`` > 1 computes the ephemerides in that many forked
    processes (see compute_ephemerides); the output is identical.
    """
    t_start = time.perf_counter()
    dia_path = f"{input_dir}/dia_sources.parquet"
    dia_file = pq.ParquetFile(dia_path)
    dia_present = dia_file.schema_arrow.names
    copy_columns, has_match_method = _dia_read_columns(set(dia_present))
    unexpected = sorted(set(dia_present) - set(_NAMES) - set(DIA_DROPPED) - set(copy_columns))
    if unexpected:
        print(f"WARNING: dia_sources.parquet columns not in SSSource, dropped: {unexpected}",
              file=sys.stderr)

    # Read only the columns linking and the ephemerides need here; the rest
    # are copied column by column at the end (the file has ~150).
    dia = pd.read_parquet(
        dia_path, engine="pyarrow", dtype_backend="pyarrow",
        columns=["obsid", "ra", "dec", "midpointMjdTai", "sep_mas", "dt_ms"],
    ).reset_index(drop=True)
    if not dia["obsid"].is_unique:
        raise ValueError("dia_sources.parquet: obsid is not unique")
    dia["file_row"] = np.arange(len(dia))     # (its row in dia_sources.parquet)
    n_dia = len(dia)
    if dia_sample_frac < 1.0:
        # Testing aid: drop some DIA sources and shuffle the rest, to
        # exercise the association logic with missing / unsorted indices.
        dia = dia.sample(frac=dia_sample_frac, random_state=seed).reset_index(drop=True)

    # Likewise only the obs_sbn columns used below (the dump has ~90).
    det_path = f"{input_dir}/obs_sbn.parquet"
    det_columns = ["obsid", "status", "provid", "permid"]
    det = pd.read_parquet(det_path, engine="pyarrow", dtype_backend="pyarrow",
                          columns=det_columns).reset_index(drop=True)
    # verify types didn't get mangled somewhere along the way
    # from the database to here
    for col in det_columns:
        assert det[col].dtype == "string[pyarrow]", (col, det[col].dtype)
    if not det["obsid"].is_unique:
        raise ValueError("obs_sbn.parquet: obsid is not unique")

    if max_objects is not None:
        # Testing aid: keep only a random subset of objects.
        sampled_provids = det["provid"].drop_duplicates().sample(max_objects, random_state=seed)
        det = det[det["provid"].isin(sampled_provids)].reset_index(drop=True)
    print(f"{len(det):,} MPC observations")

    # The association side table: from extract-submitted-sources, dia has
    # one row per obs_sbn row (obsid is unique), so each obs_sbn row gets
    # one SSSource row; a source claimed by several (both endpoints of a
    # trail, or repeated submissions) has one of them marked primary.
    assoc = (
        dia[["obsid", "file_row"]]
        .reset_index()
        .merge(det.add_prefix("mpc_"), left_on="obsid", right_on="mpc_obsid", how="inner")
    )
    assoc.rename(columns={"index": "dia_index"}, inplace=True)
    if max_objects is None and len(assoc) != len(dia):
        raise ValueError(f"{len(dia) - len(assoc):,} dia_sources.parquet rows have no obs_sbn row "
                         "(by obsid): are they from the same obs_sbn?")

    # extract-submitted-sources already verified each match against the PSF
    # *or trail* centroid and the midpoint of -A/-B endpoint pairs; check
    # its recorded offsets against the usual tolerances.
    util.assoc_validate_recorded(dia, assoc)

    # obs_sbn also holds observations of unidentified tracklets (status
    # 'I', no provid nor permid). They are in SSSource too -- they were
    # sent to and accepted by the MPC -- with a NULL ssObjectId and
    # designation, and NULL orbit-derived columns. Set them aside while
    # resolving the designations of the rest.
    undesignated = assoc["mpc_provid"].isna() & assoc["mpc_permid"].isna()
    # (status 'I', the Isolated Tracklet File, is unidentified by definition)
    n_bad = int(((assoc["mpc_status"] == "I").fillna(False) & ~undesignated).sum())
    if n_bad:
        raise ValueError(f"obs_sbn: {n_bad:,} status 'I' rows have a provid or permid")
    und = assoc[undesignated].reset_index(drop=True)
    assoc = assoc[~undesignated].reset_index(drop=True)

    totalNumObs = len(assoc)

    numid = pd.read_parquet(
        f"{input_dir}/numbered_identifications.parquet",
        engine="pyarrow",
        columns=["permid", "unpacked_primary_provisional_designation"],
        dtype_backend="pyarrow",
    ).reset_index(drop=True)
    curid = pd.read_parquet(
        f"{input_dir}/current_identifications.parquet",
        engine="pyarrow",
        dtype_backend="pyarrow",
        columns=[
            "unpacked_primary_provisional_designation",
            "unpacked_secondary_provisional_designation",
            "packed_primary_provisional_designation",
        ],
    ).reset_index(drop=True)

    # First step: some numbered objects in `obs_sbn` don't have their
    # provID set. Restore it.
    df = assoc[["mpc_provid", "mpc_permid"]].merge(numid, left_on="mpc_permid", right_on="permid", how="left")
    assert len(df) == len(assoc)

    assoc["mpc_provid"] = assoc["mpc_provid"].where(
        assoc["mpc_provid"].notna(), df["unpacked_primary_provisional_designation"]
    )

    assert not assoc["mpc_provid"].isna().any()
    assert len(assoc) == totalNumObs

    # Second step: update provisional designations with the primary ones.

    df = assoc[["mpc_provid"]].merge(
        curid, left_on="mpc_provid", right_on="unpacked_secondary_provisional_designation", how="inner"
    )
    # (the assignments below align on the index: a missing designation
    # would silently shift every later row)
    assert len(df) == len(assoc), (
        f"{assoc['mpc_provid'].nunique() - df['mpc_provid'].nunique():,} designations "
        f"({len(assoc) - len(df):,} observations) missing from current_identifications"
    )
    assoc["mpc_provid"] = df["unpacked_primary_provisional_designation"]
    assoc["mpc_packed"] = df["packed_primary_provisional_designation"]

    assert len(assoc) == totalNumObs

    mpc_orbits_path = f"{input_dir}/mpc_orbits.parquet"
    mpcorb = pd.read_parquet(
        mpc_orbits_path,
        engine="pyarrow",
        dtype_backend="pyarrow",
        columns=[
            "unpacked_primary_provisional_designation",
            "packed_primary_provisional_designation",
            "a",
            "q",
            "e",
            "i",
            "node",
            "argperi",
            "peri_time",
            "mean_anomaly",
            "epoch_mjd",
            "h",
            "g",
        ],
    ).set_index("unpacked_primary_provisional_designation", drop=False, verify_integrity=True)

    # Rows without an orbit to compute ephemerides from: the undesignated
    # ones and designated objects missing from mpc_orbits (issue #7). They
    # have no SSObject row, so their ssObjectId is NULL.
    assoc["no_orbit"] = ~assoc["mpc_provid"].isin(mpcorb.index)
    missing = assoc.loc[assoc["no_orbit"], "mpc_provid"]
    print(f"{len(und):,} observations of undesignated objects; {len(missing):,} observations of "
          f"{missing.nunique():,} designated objects without an orbit: {sorted(missing.unique())[:10]}")
    und["no_orbit"] = True
    assoc = pd.concat([assoc, und], ignore_index=True)
    totalNumObs = len(assoc)

    # sort the association table by object, those without an orbit last
    # (the order the ephemerides are computed in)
    assoc.sort_values(["no_orbit", "mpc_provid"], inplace=True)
    no_orbit = assoc["no_orbit"].to_numpy(dtype=bool)
    n_orbit = int(np.sum(~no_orbit))
    assert not no_orbit[:n_orbit].any() and no_orbit[n_orbit:].all()

    # (checked here, as the working array's designation is fixed-width)
    designation = cast_column("designation", pa.array(assoc["mpc_provid"], type=pa.string()))

    #
    # The working array: the object's key and the ephemeris columns
    #
    sss = np.zeros(totalNumObs, dtype=WORK_DTYPE)
    sss["ssObjectId"][:n_orbit] = util.packed_ascii_to_uint64_le(assoc["mpc_packed"].iloc[:n_orbit])
    sss["designation"] = assoc["mpc_provid"].fillna("")

    df = dia[["ra", "dec", "midpointMjdTai"]].iloc[assoc["dia_index"]]
    ra, dec, t = (
        df["ra"].to_numpy(),
        df["dec"].to_numpy(),
        Time(df["midpointMjdTai"].to_numpy(), format="mjd", scale="tai"),
    )

    sss["elongation"] = util.solar_elongation_ndarray(ra, dec, t)

    # Observer barycentric state for every observation, carried per row of
    # assoc so compute_sssource_entry gets its object's slice. It is
    # computed once per unique time (all sources from a visit share one
    # midpointMjdTai) in one vectorized call: the computation costs ~65 us
    # per time plus a large fixed overhead per call.
    tu, inv = np.unique(t.tai.mjd, return_inverse=True)
    robs, vobs = util.observatory_barycentric_posvel("X05", Time(tu, format="mjd", scale="tai"))
    # (a numpy structured array rather than columns of assoc, as slicing a
    # DataFrame per object cost more than the rest of the bookkeeping)
    obs_state = np.zeros(totalNumObs, dtype=[
        ("dia_index", np.int64), ("obs_pos", np.float64, 3), ("obs_vel", np.float64, 3)])
    obs_state["dia_index"] = assoc["dia_index"].to_numpy()
    obs_state["obs_pos"] = robs.to_value(u.au)[:, inv].T
    obs_state["obs_vel"] = vobs.to_value(u.km / u.s)[:, inv].T

    # FIXME: verify these coordinate transforms replicate IAU76 at JPL
    p = SkyCoord(ra=ra * u.deg, dec=dec * u.deg, distance=1 * u.au, frame="hcrs")
    ecl = p.transform_to(HeliocentricEclipticIAU76)
    p = SkyCoord(ra=ra * u.deg, dec=dec * u.deg, distance=1 * u.au, frame="icrs")
    gal = p.transform_to("galactic")

    sss["eclLambda"] = ecl.lon
    sss["eclBeta"] = ecl.lat
    sss["galLon"] = gal.l
    sss["galLat"] = gal.b

    # compute_sssource_entry takes DiaSource rows per object; give it only
    # the columns it uses, as a numpy structured array (taking rows of all
    # ~85 pyarrow-backed columns, or even of a DataFrame, dominated the
    # per-object cost).
    eph_columns = ("midpointMjdTai", "ra", "dec")
    dia_eph = np.zeros(len(dia), dtype=[(c, np.float64) for c in eph_columns])
    for c in eph_columns:
        dia_eph[c] = dia[c].to_numpy()
    del dia, df, p, ecl, gal

    print(f"[{time.perf_counter() - t_start:.1f} s] linked and set up", flush=True)
    # The orbits' covariances, for the error ellipse, of the objects present.
    covs = _ellipse.load_orbit_covariances(mpc_orbits_path, np.unique(sss["designation"][:n_orbit]),
                                           open_ephem())

    print(f"[{time.perf_counter() - t_start:.1f} s] orbit covariances loaded", flush=True)
    # ephemerides for the objects with orbits (the first n_orbit rows);
    # every orbit-derived column of the rest is NaN (NULL when written).
    t_eph = time.perf_counter()
    step_cap_stops = compute_ephemerides(sss[:n_orbit], obs_state[:n_orbit], dia_eph, mpcorb,
                                         workers=workers, chunk_factor=chunk_factor, covs=covs)
    t_eph = time.perf_counter() - t_eph
    n_ell = int(np.sum(np.isnan(sss["ephRaErr"][:n_orbit])))
    print(f"Ephemerides in {t_eph:.1f} s; error ellipse NULL on {n_ell:,} of {n_orbit:,} rows with an "
          f"orbit; {step_cap_stops:,} ellipse propagations stopped by the step cap.", flush=True)
    for name in EPHEMERIS_COLUMNS:
        if sss.dtype[name].kind == "f" and name not in MEASURED_EPH_COLUMNS:
            sss[name][n_orbit:] = np.nan
    del obs_state, mpcorb, covs

    #
    # Assemble the SSSource columns, in the output's row order
    #
    order = sort_indices(pa.array(sss["ssObjectId"], mask=no_orbit),
                         dia_eph["midpointMjdTai"][assoc["dia_index"].to_numpy()],
                         pa.array(assoc["obsid"], type=pa.string()))
    src = pa.array(assoc["file_row"].to_numpy()[order])   # (rows of dia_sources.parquet)
    sss, no_orbit = sss[order], no_orbit[order]
    columns = {
        "status": cast_column("status", pa.array(assoc["mpc_status"], type=pa.string()).take(order)),
        "ssObjectId": cast_column("ssObjectId", pa.array(sss["ssObjectId"], mask=no_orbit)),
        "designation": designation.take(order),
    }
    del assoc, designation

    # Block 6: NaN is NULL (a computed column with no value). The rest is
    # as today's SSSource, bitwise.
    for name in EPHEMERIS_COLUMNS:
        v = sss[name]
        mask = np.isnan(v) if v.dtype.kind == "f" else None
        columns[name] = cast_column(name, pa.array(v, mask=mask if mask is not None and mask.any() else None))
    del sss

    # Blocks 1, 3 and 4, copied from dia_sources.parquet (a few columns at
    # a time, to bound the memory)
    pending = {}
    for k in range(0, len(copy_columns), 16):
        group = copy_columns[k:k + 16]
        tbl = dia_file.read(columns=group, use_threads=True)
        for name in group:
            arr = tbl.column(name).take(src)
            if name in ("diaSourceId", "parentId", "match", "obssubid", "measuredOn"):
                pending[name] = arr
            if name in SSSourceDtype.names:
                columns[name] = cast_column(name, arr)
        del tbl
    if has_match_method:
        _check_values("matchMethod", columns["matchMethod"], MATCH_METHODS)
    else:
        print("dia_sources.parquet has no matchMethod (it predates it); "
              "derived it from match and obssubid")
        columns["matchMethod"] = cast_column(
            "matchMethod", _derive_match_method(pending["match"], pending["obssubid"]))
    for name, arr in _split_ids(pending["measuredOn"], pending["diaSourceId"], pending["parentId"]).items():
        columns[name] = cast_column(name, arr)
    del pending

    table = sssource_table(columns, cast=False)   # (each was cast above)
    del columns
    print(f"[{time.perf_counter() - t_start:.1f} s] assembled", flush=True)
    n_null = table["ssObjectId"].null_count
    n_undesignated = table["designation"].null_count
    n_objects = len(pc.unique(table["ssObjectId"].drop_null()))
    print(f"{n_objects:,} unique objects with {table.num_rows:,} total observations "
          f"({n_null:,} with a NULL ssObjectId: {n_undesignated:,} undesignated, "
          f"{n_null - n_undesignated:,} designated without an orbit).")
    if max_objects is None and dia_sample_frac >= 1.0:
        assert table.num_rows == n_dia

    path = f"{output_dir}/sssource.parquet"
    write_sssource(table, path)
    print(f"Wrote {path}: {table.num_rows:,} rows, {table.num_columns} columns "
          f"(ephemerides {t_eph:.1f} s, total {time.perf_counter() - t_start:.1f} s).")


def main():
    parser = argparse.ArgumentParser(
        prog="ssp-build-sssource",
        description="Build the SSSource table from the submitted-source and MPC Parquet files",
        epilog=(
            "Reads dia_sources (from extract-submitted-sources), obs_sbn, "
            "numbered_identifications, current_identifications and mpc_orbits "
            ".parquet files from the input directory and writes sssource.parquet "
            "(one row per dia_sources row, with the columns of the PPDB SSSource "
            "table) to the output directory. The ASSIST ephemeris files are taken "
            "from the SSP_ASSIST_PLANETS and SSP_ASSIST_ASTEROIDS environment variables."
        ),
    )
    parser.add_argument("--input-dir", default="./analysis/inputs",
                        help="Input directory (default: %(default)s)")
    parser.add_argument("--output-dir", default="./analysis/outputs",
                        help="Output directory (default: %(default)s)")
    parser.add_argument(
        "--max-objects", type=int, default=None,
        help="Process only this many randomly chosen objects (default: all)",
    )
    parser.add_argument(
        "--dia-sample-frac", type=float, default=1.0,
        help="Randomly keep this fraction of DIA sources, shuffled (default: %(default)s)",
    )
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for subsampling (default: %(default)s)")
    parser.add_argument(
        "--reraise", action="store_true",
        help="Re-raise exceptions instead of exiting gracefully (for debugging)",
    )
    parser.add_argument(
        "--workers", type=int, default=min(64, os.cpu_count() or 1),
        help=(
            "Number of worker processes for the per-object ephemerides "
            "(default: min(64, number of CPUs)). 1 runs serially, with no "
            "process pool. The output does not depend on it."
        ),
    )
    parser.add_argument(
        "--chunk-factor", type=int, default=8,
        help=(
            "With --workers > 1, split the work into about this many "
            "chunks per worker, to balance the load (default: %(default)s)."
        ),
    )
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers must be at least 1")
    if args.chunk_factor < 1:
        parser.error("--chunk-factor must be at least 1")

    try:
        build_sssource(
            args.input_dir, args.output_dir,
            max_objects=args.max_objects, dia_sample_frac=args.dia_sample_frac, seed=args.seed,
            workers=args.workers, chunk_factor=args.chunk_factor,
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.reraise:
            raise
        sys.exit(1)


if __name__ == "__main__":
    main()
