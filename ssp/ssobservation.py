"""Build the SSObservation table (``ssp-build-ssobservation``, or ``python -m
ssp.ssobservation``): SSObservation of docs/design/sssource-widened.md.

One row per row of ``dia_sources.parquet`` (extract-submitted-sources), i.e.
per resolved X05 ``obs_sbn`` row, with exactly the columns of
``ssp.ssobservation_contract.SSObservationDtype`` in six blocks: the obs_sbn
link columns, the identification (ssObjectId, designation), the measurement's
metadata and identifiers, the measurement itself (copied from
dia_sources.parquet and cast to the schema's types), and the ephemeris and
geometry columns, computed per object with ASSIST from mpc_orbits.
"""

import argparse
import contextlib
import datetime
import hashlib
import io
import json
import os
import subprocess
import sys
import time
from pathlib import Path

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

from . import nongrav as _nongrav
from . import util
from .photfit import hg_V_mag
from .ephem_assist import (MJD_J2000, compute_ephemerides_one, open_ephem, tail_position_angles,
                           tail_position_angles_f32)
from .nearbysso import propagate as _propagate
# (a module attribute, so tests can substitute it)
from . import ssobservation_ellipse as _ellipse
from .delivery_contract import SHUTTER_INPUT_COLUMNS
from .ssobservation_contract import (
    ELLIPSE_COLUMNS, ID_SPLIT, MANIFEST_FORMAT_VERSION, MATCH_METHODS, PART_FILE_FORMAT, PART_GLOB,
    PART_ROWS_DEFAULT, SHUTTER_INTERNAL, SIDECAR_FILE, SIDECAR_KEY, SSOBSERVATION_DICTIONARY,
    SSOBSERVATION_INTERNAL_DEFAULT, SSOBSERVATION_INTERNAL_DTYPE, SSOBSERVATION_INTERNAL_NONNULL,
    SSOBSERVATION_MANIFEST_FILE, SSOBSERVATION_NONNULL, SSOBSERVATION_SORT, VIEW_DROPPED,
    SSObservationDtype,
)


# --------------------------------------------------------------------------
# The column blocks of SSObservationDtype (see docs/design/sssource-widened.md)
# --------------------------------------------------------------------------
_NAMES = SSObservationDtype.names

#: Block 1 columns copied from dia_sources.parquet (status comes from
#: obs_sbn, matchMethod from dia_sources.parquet or _derive_match_method).
LINK_COLUMNS = ("obsid", "trksub", "trkid", "submission_id", "primary")
#: Block 3 columns copied from dia_sources.parquet (the ids are split, see
#: ID_SPLIT).
MEASURED_ON_COLUMNS = ("measuredOn", "processing", "processingTable")
#: Block 4: the measurement, copied from dia_sources.parquet.
MEASUREMENT_COLUMNS = _NAMES[_NAMES.index("visit"):_NAMES.index("glint_trail") + 1]
#: The shutter-correction flags (docs/design/shutter-timing.md), copied
#: with the corrected midpointMjdTai when dia_sources.parquet has the
#: correction (all of SHUTTER_INPUT_COLUMNS). A dia_sources.parquet without
#: any of them predates the correction: its times are the visits'
#: midpoints, so the flags are written True / False.
SHUTTER_FLAGS = ("midpointMjdTai_flag", "midpointMjdTai_flag_degraded")
#: With the correction, the observer state and the Sun are evaluated once
#: per visit time (midpointMjdTaiVisit) and shifted to each row's
#: midpointMjdTai (see observer_states); rows whose two times differ by more
#: than this [s] are evaluated at their own time instead.
MAX_SHIFT_S = 1.0
#: observer_states' finite-difference step for the observer's acceleration [s].
OBSERVER_SHIFT_STEP_S = 0.25
#: Block 6: the ephemeris and geometry, computed here.
EPHEMERIS_COLUMNS = _NAMES[_NAMES.index("eclLambda"):]
#: Block 6 columns that are measured (from the observed position and time),
#: not orbit-derived: they are filled for rows without an orbit too.
MEASURED_EPH_COLUMNS = ("elongation", "eclLambda", "eclBeta", "galLon", "galLat")

#: dia_sources.parquet columns not carried into SSObservation: the view's query
#: helpers, the view's ``parentId`` (split into parentDiaSourceId /
#: parentSourceId), extract-submitted-sources' match diagnostics (which
#: stay in dia_sources.parquet), and the shutter correction's internal
#: columns (SHUTTER_INTERNAL: midpointMjdTaiVisit, obstime_basis). All are
#: dropped without a warning.
DIA_DROPPED = VIEW_DROPPED + ("parentId", "obssubid", "match", "sep_mas", "dt_ms", "dmag", "band_ok",
                              "n_pass", "ambiguous") + SHUTTER_INTERNAL


# The SSObservation fields compute_ssobservation_entry fills in (and nothing
# else): what a parallel worker returns to the parent. Keep in sync with it
# (tests/test_ssobservation_parallel.py checks).
EPH_FIELDS = [
    "ephRateRa", "ephRateDec", "ephRate",
    "ephAntiSunPA", "ephAntiMotionPA",
    "ephRa", "ephDec", "ephOffsetDec", "ephOffsetRa", "ephOffset",
    "ephOffsetAlongTrack", "ephOffsetCrossTrack",
    "helio_x", "helio_y", "helio_z", "helioRange",
    "helio_vx", "helio_vy", "helio_vz", "helio_vtot", "helioRangeRate",
    "topo_x", "topo_y", "topo_z", "topoRange",
    "topo_vx", "topo_vy", "topo_vz", "topo_vtot", "topoRangeRate",
    "phaseAngle", "ephVmag",
    *ELLIPSE_COLUMNS,
]

#: The working array the ephemerides are computed in, one row per SSObservation
#: row: the object's key and designation, and the block-6 columns, with the
#: SSObservationDtype types (as today's SSObservation, so the values are
#: bitwise the same).
WORK_DTYPE = np.dtype([("ssObjectId", "<i8"), ("designation", SSObservationDtype["designation"])]
                      + [(c, SSObservationDtype[c]) for c in EPHEMERIS_COLUMNS])


def along_cross_track(off_ra, off_dec, rate_ra, rate_dec):
    """The offset (``off_ra``, which includes cos(dec), and ``off_dec``)
    resolved along and across the predicted direction of motion (the rates
    ``rate_ra``, which includes cos(dec), and ``rate_dec``), as pipe_tasks'
    ssoAssociation computes them (see ssp.ssobservation_contract, block 6)::

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


# Rows with a non-gravitational fit are selected by
# ssp.nearbysso.orbits.nongrav_marks (the non_gravs flag, or a CAR coefficient
# after vz; on the 2026-10-01 catalog both mark the same 638 orbits).


def load_nongravs(mpc_orbits_path, designations):
    """``({designation: ssp.nongrav.NonGrav}, n_errors)`` for the
    ``designations`` whose mpc_orbits row has a non-gravitational fit.

    Only the designation and mpc_orb_jsonb columns are streamed, one thread
    per row group; rows are filtered by designation first and then by the
    JSON text (the ``non_gravs: true`` flag, or a CAR coefficient beyond vz),
    so only the few non-grav orbits' JSON is parsed. A row where the flag and
    the coefficients disagree is parsed anyway, with a warning. A ValueError
    from ``ssp.nongrav.nongrav_params`` is printed and counted in
    ``n_errors``; such an orbit, and one whose JSON parses to
    ``ssp.nongrav.NONE``, is left out (integrated with gravity only).

    A designation on several rows (none in the catalog; build_ssobservation
    rejects them in mpc_orbits anyway) gets the last of its rows with a
    parseable fit: a later unparseable or gravity-only row doesn't remove it.
    """
    from concurrent.futures import ThreadPoolExecutor

    from .nearbysso.orbits import nongrav_marks

    key = "unpacked_primary_provisional_designation"
    d = pc.unique(pa.array([str(x) for x in designations if x is not None and str(x) != ""],
                           type=pa.string()))
    if len(d) == 0:
        return {}, 0

    def scan(rg):
        f = pq.ParquetFile(mpc_orbits_path)
        out = []
        for b in f.iter_batches(batch_size=20_000, row_groups=[rg],
                                columns=[key, "mpc_orb_jsonb"], use_threads=False):
            b = b.filter(pc.is_in(b.column(key), value_set=d))
            if not b.num_rows:
                continue
            j = b.column("mpc_orb_jsonb")
            flag, coef = (pa.array(x) for x in nongrav_marks(j))
            b = b.append_column("flag", flag).append_column("coef", coef)
            b = b.filter(pc.or_(flag, coef))
            if b.num_rows:
                out += zip(*(b.column(c).to_pylist() for c in (key, "mpc_orb_jsonb", "flag", "coef")))
        return out

    pf = pq.ParquetFile(mpc_orbits_path)
    if "mpc_orb_jsonb" not in pf.schema_arrow.names:
        print("WARNING: mpc_orbits has no mpc_orb_jsonb column; every orbit is integrated with "
              "gravity only", file=sys.stderr)
        return {}, 0
    nrg = pf.num_row_groups
    with ThreadPoolExecutor(max_workers=max(1, min(nrg, 16))) as ex:
        rows = [r for part in ex.map(scan, range(nrg)) for r in part]   # (in file order)

    nongravs, n_errors = {}, 0
    for des, j, flag, coef in rows:
        if flag != coef:
            print(f"WARNING: {des}: mpc_orb_jsonb non_gravs flag {'true' if flag else 'not true'} but "
                  f"{'' if coef else 'no '}CAR coefficients beyond vz", file=sys.stderr)
        try:
            ng = _nongrav.nongrav_params(j)
        except ValueError as exc:
            kept = "an earlier row's fit kept" if des in nongravs else "integrated with gravity only"
            print(f"WARNING: {des}: {exc}; {kept}", file=sys.stderr)
            n_errors += 1
            continue
        if ng.model:
            nongravs[des] = ng
    return nongravs, n_errors


def compute_ssobservation_entry(sss, assoc, mpcorb, dia, ephem, covs=None, nongravs=None):
    """Fill the ephemeris-derived SSObservation columns (EPH_FIELDS) for one
    object.

    ``mpcorb`` must be indexed by unpacked_primary_provisional_designation;
    ``assoc`` is a structured array holding, per observation, its row in
    ``dia`` (dia_index) and the observer's barycentric state (obs_pos [AU],
    obs_vel [km/s], each of shape (3,)); ``dia`` is a structured array of
    midpointMjdTai, ra and dec. ``covs`` maps designations to their orbits
    with covariances (from ssp.ssobservation_ellipse.load_orbit_covariances);
    objects not in it, or all if it is None, get a NaN error ellipse.
    ``nongravs`` maps designations to their ``ssp.nongrav.NonGrav`` (from
    load_nongravs); the others, or all if it is None, are integrated with
    gravity only.
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
        nongrav=nongravs.get(provID, _nongrav.NONE) if nongravs is not None else _nongrav.NONE,
    )

    sss["ephRateRa"] = e.mu_lon
    sss["ephRateDec"] = e.mu_lat
    sss["ephRate"] = e.mu_total

    # The tail position angles, from the float64 light-emission-time vectors
    # (as NearbySSO computes them; docs/design/tail-angles.md)
    anti_sun, anti_motion = tail_position_angles(e.helio_pos, e.helio_vel, e.topo_pos)
    sss["ephAntiSunPA"] = tail_position_angles_f32(anti_sun)
    sss["ephAntiMotionPA"] = tail_position_angles_f32(anti_motion)

    # Heliocentric and topocentric vectors are at light-emission time, per
    # the SSObservation schema, following JPL Horizons conventions (see
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
    # topo_x/y/z: see ssp.ssobservation_ellipse.ephemeris_ellipse).
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


def _ssobservation_chunk(g0, g1):
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

    # A private array holding only what compute_ssobservation_entry reads and
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
            compute_ssobservation_entry(out[s:e], obs_state[r0 + s:r0 + e],
                                   _PARALLEL["mpcorb"], _PARALLEL["dia_eph"], _EPHEM,
                                   covs=_PARALLEL["covs"], nongravs=_PARALLEL["nongravs"])
    sys.stdout.write(buf.getvalue())
    sys.stdout.flush()

    res = np.empty(len(out), dtype=[(f, out.dtype[f]) for f in EPH_FIELDS])
    for f in EPH_FIELDS:
        res[f] = out[f]
    return res, int(_propagate.STEP_CAP_STOPS)


def compute_ephemerides(sss, obs_state, dia_eph, mpcorb, workers=1, chunk_factor=8, covs=None,
                        nongravs=None):
    """Fill the EPH_FIELDS of ``sss`` with compute_ssobservation_entry, per
    object (``sss`` grouped by ssObjectId; ``obs_state`` its rows' observer
    states and DiaSource rows in ``dia_eph``; ``covs`` the orbit
    covariances for the error ellipse, and ``nongravs`` their non-gravitational
    parameters, see compute_ssobservation_entry).

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
            partial(compute_ssobservation_entry, mpcorb=mpcorb, dia=dia_eph, ephem=ephem, covs=covs,
                    nongravs=nongravs),
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
                     nongravs=nongravs, idx_start=idx_start, idx_end=idx_end)
    try:
        results = util.run_chunks(_ssobservation_chunk, chunks, workers, "ephemerides",
                                  weights=[int(counts[g0:g1].sum()) for g0, g1 in chunks])
    finally:
        _PARALLEL.clear()
    for (g0, g1), (res, _) in zip(chunks, results):
        r0, r1 = idx_start[g0], idx_end[g1 - 1]
        for f in EPH_FIELDS:
            sss[f][r0:r1] = res[f]
    return sum(stops for _, stops in results)


# --------------------------------------------------------------------------
# Writing SSObservation: casts and checks to the contract's types
# --------------------------------------------------------------------------

def column_dtype(name):
    """The NumPy type of column ``name``: SSObservationDtype's, or for a
    column that may be internal, SSOBSERVATION_INTERNAL_DTYPE's."""
    if name in _NAMES:
        return SSObservationDtype[name]
    return np.dtype(SSOBSERVATION_INTERNAL_DTYPE[name])


def arrow_type(name):
    """The Arrow type of column ``name`` (column_dtype): ``U<n>`` is a
    string (dictionary-encoded for SSOBSERVATION_DICTIONARY), the rest the
    NumPy type's equivalent."""
    dt = column_dtype(name)
    if dt.kind == "U":
        return pa.dictionary(pa.int32(), pa.string()) if name in SSOBSERVATION_DICTIONARY else pa.string()
    return pa.from_numpy_dtype(dt)


def ssobservation_schema():
    """The Arrow schema of the SSObservation (each part):
    SSObservationDtype's columns, in order, with their arrow_type, non-null
    exactly for SSOBSERVATION_NONNULL."""
    return pa.schema([pa.field(n, arrow_type(n), nullable=n not in SSOBSERVATION_NONNULL) for n in _NAMES])


def cast_column(name, arr):
    """Cast ``arr`` (an Arrow array, chunked or not, or a NumPy array) to
    SSObservation column ``name``'s type, and check it.

    Raises ValueError if a narrowing integer cast would overflow, a finite
    float64 would overflow float32, a string is longer than the column's
    ``char`` length, or a non-null column (SSOBSERVATION_NONNULL) has a NULL.
    float64 -> float32 rounding is expected, not an error.
    """
    dt = column_dtype(name)
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
            raise ValueError(f"SSObservation column {name!r}: cannot cast {arr.type} to {value_type}: "
                             f"{e}") from e
        arr = out

    if dt.kind == "U" and arr.type == value_type and len(arr) and arr.null_count < len(arr):
        maxlen = pc.max(pc.utf8_length(arr)).as_py()
        if maxlen > dt.itemsize // 4:
            raise ValueError(f"SSObservation column {name!r}: a value of {maxlen} characters "
                             f"is longer than the column's {dt.itemsize // 4}")

    if (name in SSOBSERVATION_NONNULL or name in SSOBSERVATION_INTERNAL_NONNULL) and arr.null_count:
        raise ValueError(f"SSObservation column {name!r} is non-null, but has {arr.null_count:,} NULL values")

    if arr.type != target:
        arr = pc.dictionary_encode(arr)
        if arr.type != target:   # (the index type)
            arr = arr.cast(target)
    if isinstance(arr, pa.ChunkedArray):
        arr = arr.combine_chunks()
    return arr


def ssobservation_table(columns, cast=True):
    """The SSObservation table from ``columns`` (name -> array, every column of
    SSObservationDtype and nothing else), in schema order, each cast and
    checked with cast_column; with ``cast=False`` the columns must already be
    cast_column's output (their types are checked, not their values)."""
    names = set(columns)
    if names != set(_NAMES):
        raise ValueError(f"SSObservation columns: missing {sorted(set(_NAMES) - names)}, "
                         f"unexpected {sorted(names - set(_NAMES))}")
    schema = ssobservation_schema()
    if not cast:
        bad = [n for n in _NAMES if columns[n].type != schema.field(n).type]
        if bad:
            raise ValueError(f"SSObservation columns not cast (see cast_column): {bad}")
        return pa.Table.from_arrays([columns[n] for n in _NAMES], schema=schema)
    return pa.Table.from_arrays([cast_column(n, columns[n]) for n in _NAMES], schema=schema)


def sort_indices(ssObjectId, midpointMjdTai, obsid):
    """The row order of the SSObservation (SSOBSERVATION_SORT): ascending,
    NULL ssObjectId last."""
    keys = pa.table(dict(zip(SSOBSERVATION_SORT, (ssObjectId, midpointMjdTai, obsid))))
    # (NULLs last by an explicit key: where null_placement goes differs
    # across pyarrow versions)
    keys = keys.append_column("_null", pc.is_null(keys[SSOBSERVATION_SORT[0]]))
    return pc.sort_indices(keys,
                           sort_keys=[(k, "ascending") for k in ("_null", *SSOBSERVATION_SORT)]).to_numpy()


def write_ssobservation(table, path):
    """Write the SSObservation ``table`` (from ssobservation_table, rows
    already in sort_indices order), or a part of it, to ``path``,
    zstd-compressed."""
    if table.schema != ssobservation_schema():
        raise ValueError("not an SSObservation table (see ssobservation_table)")
    pq.write_table(table, path, compression="zstd")


# --------------------------------------------------------------------------
# The internal columns, the parts and the manifest
# (ssp.ssobservation_contract, "Internal columns and the sidecar" and "The
# partitioned delivery")
# --------------------------------------------------------------------------

#: The repository root, when running from a source tree (for the manifest's
#: schema md5 and commit).
REPO_ROOT = Path(__file__).resolve().parent.parent
#: The schema copy ssp/schema_ppdb.py is generated from, relative to REPO_ROOT.
SCHEMA_FILE = Path("tests/data/sdm_schemas/sso_base.yaml")
SCHEMA_SOURCE = "lsst/sdm_schemas tickets/DM-55375"


def check_internal_columns(internal_columns):
    """The configured internal columns as a tuple, checked: each in
    SSOBSERVATION_INTERNAL_DTYPE and not in SSObservationDtype, none
    repeated. Raises ValueError otherwise."""
    if isinstance(internal_columns, str):
        raise ValueError("internal_columns is a sequence of column names, not a string "
                         "(see parse_internal_columns)")
    cols = tuple(internal_columns)
    delivered = [c for c in cols if c in _NAMES]
    if delivered:
        raise ValueError(f"internal columns {delivered} are in the delivered SSObservation schema: "
                         "a column is either delivered or internal, not both")
    unknown = [c for c in cols if c not in SSOBSERVATION_INTERNAL_DTYPE]
    if unknown:
        raise ValueError(f"internal columns {unknown} are not columns the build can make internal "
                         f"(one of {list(SSOBSERVATION_INTERNAL_DTYPE)})")
    dup = sorted({c for c in cols if cols.count(c) > 1})
    if dup:
        raise ValueError(f"internal columns {dup} given more than once")
    return cols


def parse_internal_columns(text):
    """``--internal-columns``: comma-separated names; an empty string is
    none. Not checked (see check_internal_columns)."""
    return tuple(c.strip() for c in text.split(",")) if text.strip() else ()


def sidecar_schema(internal_columns):
    """The sidecar's Arrow schema: obsid, then ``internal_columns`` in that
    order, all non-null."""
    return pa.schema([pa.field(SIDECAR_KEY, arrow_type(SIDECAR_KEY), nullable=False)]
                     + [pa.field(c, arrow_type(c), nullable=False) for c in internal_columns])


def part_bounds(ssObjectId, part_rows):
    """The parts' row ranges [(start, end), ...] of a table in
    SSOBSERVATION_SORT order whose ssObjectId column is ``ssObjectId`` (an
    Arrow array, NULLs last), as the contract cuts them: a ranged part
    closes at the first object boundary at or after ``part_rows`` rows; the
    NULL rows follow in parts of ``part_rows`` rows; an empty table is one
    empty part."""
    if part_rows < 1:
        raise ValueError(f"part_rows must be at least 1, not {part_rows}")
    n = len(ssObjectId)
    if n == 0:
        return [(0, 0)]
    n_null = ssObjectId.null_count
    n_ranged = n - n_null
    if n_null and ssObjectId.slice(0, n_ranged).null_count:
        raise ValueError("ssObjectId: NULL rows are not last")
    ids = ssObjectId.slice(0, n_ranged).to_numpy(zero_copy_only=False)
    # (the row after each object's last row, ascending; ends with n_ranged)
    ends = np.append(np.flatnonzero(ids[1:] != ids[:-1]) + 1, n_ranged)
    bounds = []
    start = 0
    while start < n_ranged:
        end = int(ends[np.searchsorted(ends, start + part_rows, side="left")]) \
            if start + part_rows < n_ranged else n_ranged
        bounds.append((start, end))
        start = end
    bounds += [(s, min(s + part_rows, n)) for s in range(n_ranged, n, part_rows)]
    return bounds


def _md5(path, bufsize=1 << 24):
    h = hashlib.md5()
    with open(path, "rb") as f:
        while chunk := f.read(bufsize):
            h.update(chunk)
    return h.hexdigest()


def _file_entry(path):
    return {"bytes": os.path.getsize(path), "md5": _md5(path)}


def source_commit():
    """``git rev-parse HEAD`` of the source tree this module runs from, or
    None where it isn't a git checkout."""
    try:
        r = subprocess.run(["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"], capture_output=True,
                           text=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return None
    sha = r.stdout.strip()
    return sha if r.returncode == 0 and sha else None


def schema_md5():
    """The md5 of the sso_base.yaml copy (SCHEMA_FILE) in the source tree,
    or None where it isn't there."""
    p = REPO_ROOT / SCHEMA_FILE
    return _md5(p) if p.is_file() else None


def write_partitioned(table, sidecar, output_dir, part_rows=PART_ROWS_DEFAULT):
    """Write the SSObservation ``table`` (rows in SSOBSERVATION_SORT order)
    to ``output_dir`` as parts (part_bounds), then the ``sidecar`` (obsid
    and the internal columns, same rows), then the manifest, last. Earlier
    parts and manifest there are removed first. Returns the manifest."""
    if sidecar.num_rows != table.num_rows:
        raise ValueError(f"sidecar has {sidecar.num_rows:,} rows, SSObservation {table.num_rows:,}")
    out = Path(output_dir)
    # (the manifest first: a run that fails part way leaves no manifest)
    for p in [out / SSOBSERVATION_MANIFEST_FILE, *sorted(out.glob(PART_GLOB))]:
        p.unlink(missing_ok=True)

    ids = table["ssObjectId"].combine_chunks()
    parts = []
    for k, (s, e) in enumerate(part_bounds(ids, part_rows)):
        name = PART_FILE_FORMAT.format(k)
        write_ssobservation(table.slice(s, e - s), out / name)
        sl = ids.slice(s, e - s)
        null = bool(e > s and sl.null_count)
        parts.append({"file": name, "rows": e - s,
                      "ssObjectId_min": None if null or e == s else sl[0].as_py(),
                      "ssObjectId_max": None if null or e == s else sl[-1].as_py(),
                      "null_ssObjectId": null, **_file_entry(out / name)})

    pq.write_table(sidecar, out / SIDECAR_FILE, compression="zstd")
    manifest = {
        "table": "SSObservation",
        "format_version": MANIFEST_FORMAT_VERSION,
        "schema": {"source": SCHEMA_SOURCE, "file": SCHEMA_FILE.name, "md5": schema_md5()},
        "ssp_tools_commit": source_commit(),
        "created_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "partition_key": "ssObjectId",
        "sort": list(SSOBSERVATION_SORT),
        "part_rows": part_rows,
        "rows": table.num_rows,
        "parts": parts,
        "sidecar": {"file": SIDECAR_FILE, "key": SIDECAR_KEY,
                    "columns": [c for c in sidecar.column_names if c != SIDECAR_KEY],
                    "rows": sidecar.num_rows, **_file_entry(out / SIDECAR_FILE)},
    }
    tmp = out / (SSOBSERVATION_MANIFEST_FILE + ".tmp")
    tmp.write_text(json.dumps(manifest, indent=2) + "\n")
    tmp.replace(out / SSOBSERVATION_MANIFEST_FILE)
    return manifest


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


def shutter_corrected(dia_present):
    """Whether a dia_sources.parquet with columns ``dia_present`` has
    shutter-corrected times: True with all of SHUTTER_INPUT_COLUMNS, False
    with none (it predates the correction); raises ValueError for a
    partial set."""
    have = [c for c in SHUTTER_INPUT_COLUMNS if c in dia_present]
    if have and len(have) != len(SHUTTER_INPUT_COLUMNS):
        raise ValueError(f"dia_sources.parquet has the shutter-correction columns {have} but lacks "
                         f"{[c for c in SHUTTER_INPUT_COLUMNS if c not in dia_present]}: all of "
                         f"{SHUTTER_INPUT_COLUMNS} or none")
    return bool(have)


def _dia_read_columns(dia_present):
    """The dia_sources.parquet columns the build copies (block 1, 3 and 4),
    whether matchMethod is among them, and whether the times are
    shutter-corrected (shutter_corrected); raises ValueError if some are
    missing."""
    has_match_method = "matchMethod" in dia_present
    corrected = shutter_corrected(dia_present)
    need = (list(LINK_COLUMNS) + list(MEASURED_ON_COLUMNS) + ["diaSourceId", "parentId"]
            + [c for c in MEASUREMENT_COLUMNS if corrected or c not in SHUTTER_FLAGS]
            + (["midpointMjdTai_flag_degraded"] if corrected else [])
            + (["matchMethod"] if has_match_method else ["match", "obssubid"]))
    missing = [c for c in need + ["sep_mas", "dt_ms"] if c not in dia_present]
    if missing:
        raise ValueError(f"dia_sources.parquet lacks {missing}: SSObservation is built from the output of "
                         "extract-submitted-sources")
    return need, has_match_method, corrected


def _unique_observer_states(t_mjd_tai):
    """The X05 barycentric position and velocity (astropy Quantities, each
    (3, M)) at the M unique TAI MJDs of ``t_mjd_tai``, and the inverse index
    (N,) back to ``t_mjd_tai``: one vectorized call (the computation costs
    ~65 us per time plus a large fixed overhead per call)."""
    tu, inv = np.unique(t_mjd_tai, return_inverse=True)
    robs, vobs = util.observatory_barycentric_posvel("X05", Time(tu, format="mjd", scale="tai"))
    return tu, robs, vobs, inv


def observer_states(t_mjd_tai, t_visit=None):
    """The observer's (X05) barycentric ICRF state at each TAI MJD of
    ``t_mjd_tai``: (obs_pos [AU], obs_vel [km/s]), each of shape (N, 3).

    Without ``t_visit``, it is computed exactly, once per unique time. With
    ``t_visit`` (each row's visit midpoint, from which its time differs by
    the shutter-motion correction, dt, |dt| <= 0.24 s), it is computed once
    per unique visit time, and at that time + OBSERVER_SHIFT_STEP_S, and
    shifted by dt: position r + v dt + a dt^2 / 2 and velocity v + a dt,
    with the acceleration a from the two velocities. (Exact evaluation per
    source costs ~0.2 ms per unique time: ~25 min and GBs for a day's 8M
    sources.) The shift is accurate to well under a micro-arcsecond as seen
    from any solar-system distance (see tests/test_ssobservation_shutter.py).
    Rows with dt == 0 get exactly their visit time's state; rows with |dt|
    > MAX_SHIFT_S, or a non-finite dt, are computed exactly at their own
    time.
    """
    t = np.asarray(t_mjd_tai, dtype=np.float64)
    if t_visit is None:
        _, robs, vobs, inv = _unique_observer_states(t)
        return robs.to_value(u.au)[:, inv].T, vobs.to_value(u.km / u.s)[:, inv].T

    pos = np.empty((len(t), 3))
    vel = np.empty((len(t), 3))
    tv = np.asarray(t_visit, dtype=np.float64)
    dt_s = (t - tv) * 86400.0
    shift = np.abs(dt_s) <= MAX_SHIFT_S       # (False for NaN)
    if shift.any():
        tu, robs, vobs, inv = _unique_observer_states(tv[shift])
        h_day = OBSERVER_SHIFT_STEP_S / 86400.0
        _, vobs1 = util.observatory_barycentric_posvel(
            "X05", Time(tu, np.full(len(tu), h_day), format="mjd", scale="tai"))
        r0 = robs.to_value(u.au)                                        # AU
        v0 = vobs.to_value(u.au / u.day)                                # AU/day
        acc = (vobs1.to_value(u.au / u.day) - v0) / h_day               # AU/day^2
        v0_kms = vobs.to_value(u.km / u.s)
        acc_kms = (vobs1.to_value(u.km / u.s) - v0_kms) / OBSERVER_SHIFT_STEP_S   # km/s^2
        d = dt_s[shift]
        dd = d / 86400.0
        pos[shift] = (r0[:, inv] + v0[:, inv] * dd + 0.5 * acc[:, inv] * (dd * dd)).T
        vel[shift] = (v0_kms[:, inv] + acc_kms[:, inv] * d).T
    if not shift.all():
        _, robs, vobs, inv = _unique_observer_states(t[~shift])
        pos[~shift] = robs.to_value(u.au)[:, inv].T
        vel[~shift] = vobs.to_value(u.km / u.s)[:, inv].T
    return pos, vel


# --------------------------------------------------------------------------
# The build
# --------------------------------------------------------------------------

def build_ssobservation(input_dir, output_dir, max_objects=None, dia_sample_frac=1.0, seed=42,
                        workers=1, chunk_factor=8, part_rows=PART_ROWS_DEFAULT,
                        internal_columns=SSOBSERVATION_INTERNAL_DEFAULT):
    """Build the SSObservation in ``output_dir`` from the dia_sources (from
    extract-submitted-sources), MPC observation (obs_sbn), identification
    and orbit tables in ``input_dir``: its parts (PART_FILE_FORMAT, cut
    every ``part_rows`` rows at object boundaries), the sidecar of the
    ``internal_columns`` (SIDECAR_FILE) and the manifest
    (SSOBSERVATION_MANIFEST_FILE); see write_partitioned. Returns the
    manifest.

    ``internal_columns`` (each in SSOBSERVATION_INTERNAL_DTYPE, none in
    SSObservationDtype) go to the sidecar, in that order; the other
    columns of SSOBSERVATION_INTERNAL_DTYPE aren't produced.

    ``max_objects`` and ``dia_sample_frac`` subsample the inputs, for
    testing. ``workers`` > 1 computes the ephemerides in that many forked
    processes (see compute_ephemerides); the output is identical.
    """
    t_start = time.perf_counter()
    internal_columns = check_internal_columns(internal_columns)
    if not isinstance(part_rows, (int, np.integer)) or isinstance(part_rows, bool) or part_rows < 1:
        raise ValueError(f"part_rows must be an integer of at least 1, not {part_rows!r}")
    part_rows = int(part_rows)
    dia_path = f"{input_dir}/dia_sources.parquet"
    dia_file = pq.ParquetFile(dia_path)
    dia_present = dia_file.schema_arrow.names
    copy_columns, has_match_method, corrected = _dia_read_columns(set(dia_present))
    unexpected = sorted(set(dia_present) - set(_NAMES) - set(DIA_DROPPED) - set(copy_columns))
    # (the columns that may be internal are produced only when configured)
    copy_columns = [c for c in copy_columns if c not in SSOBSERVATION_INTERNAL_DTYPE or c in internal_columns]
    if "matchMethod" not in internal_columns:
        copy_columns = [c for c in copy_columns if c not in ("match", "obssubid")]
    if unexpected:
        print(f"WARNING: dia_sources.parquet columns not in SSObservation, dropped: {unexpected}",
              file=sys.stderr)

    # Read only the columns linking and the ephemerides need here; the rest
    # are copied column by column at the end (the file has ~150).
    # Every ephemeris column is computed at the row's midpointMjdTai (with the
    # correction, the shutter-corrected time); midpointMjdTaiVisit only
    # anchors the shift of the observer state and the Sun (observer_states).
    dia = pd.read_parquet(
        dia_path, engine="pyarrow", dtype_backend="pyarrow",
        columns=["obsid", "ra", "dec", "midpointMjdTai", "sep_mas", "dt_ms"]
        + (["midpointMjdTaiVisit"] if corrected else []),
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
    # one SSObservation row; a source claimed by several (both endpoints of a
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
    # 'I', no provid nor permid). They are in SSObservation too -- they were
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

    df = dia[["ra", "dec", "midpointMjdTai"] + (["midpointMjdTaiVisit"] if corrected else [])
             ].iloc[assoc["dia_index"]]
    ra, dec, t = (
        df["ra"].to_numpy(),
        df["dec"].to_numpy(),
        Time(df["midpointMjdTai"].to_numpy(), format="mjd", scale="tai"),
    )
    t_visit = (df["midpointMjdTaiVisit"].to_numpy(dtype=np.float64, na_value=np.nan)
               if corrected else None)

    if t_visit is None:
        sss["elongation"] = util.solar_elongation_ndarray(ra, dec, t)
    else:
        # the Sun once per visit time, shifted (as observer_states)
        dt_s = (t.tai.mjd - t_visit) * 86400.0
        shift = np.abs(dt_s) <= MAX_SHIFT_S
        if shift.any():
            sss["elongation"][shift] = util.solar_elongation_ndarray(
                ra[shift], dec[shift], Time(t_visit[shift], format="mjd", scale="tai"), dt_s=dt_s[shift])
        if not shift.all():
            sss["elongation"][~shift] = util.solar_elongation_ndarray(ra[~shift], dec[~shift], t[~shift])
        print(f"Shutter-corrected times: |midpointMjdTai - midpointMjdTaiVisit| up to "
              f"{np.nanmax(np.abs(dt_s), initial=0.0):.3f} s; {int(np.sum(~shift)):,} rows beyond "
              f"{MAX_SHIFT_S} s (or without a visit time) evaluated at their own time", flush=True)

    # Observer barycentric state for every observation, carried per row of
    # assoc so compute_ssobservation_entry gets its object's slice: once per
    # unique time (all sources from a visit share one midpointMjdTai), or,
    # with shutter-corrected times, once per visit time and shifted.
    # (a numpy structured array rather than columns of assoc, as slicing a
    # DataFrame per object cost more than the rest of the bookkeeping)
    obs_state = np.zeros(totalNumObs, dtype=[
        ("dia_index", np.int64), ("obs_pos", np.float64, 3), ("obs_vel", np.float64, 3)])
    obs_state["dia_index"] = assoc["dia_index"].to_numpy()
    obs_state["obs_pos"], obs_state["obs_vel"] = observer_states(t.tai.mjd, t_visit)

    # FIXME: verify these coordinate transforms replicate IAU76 at JPL
    p = SkyCoord(ra=ra * u.deg, dec=dec * u.deg, distance=1 * u.au, frame="hcrs")
    ecl = p.transform_to(HeliocentricEclipticIAU76)
    p = SkyCoord(ra=ra * u.deg, dec=dec * u.deg, distance=1 * u.au, frame="icrs")
    gal = p.transform_to("galactic")

    sss["eclLambda"] = ecl.lon
    sss["eclBeta"] = ecl.lat
    sss["galLon"] = gal.l
    sss["galLat"] = gal.b

    # compute_ssobservation_entry takes DiaSource rows per object; give it only
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
    # The non-gravitational parameters of the objects present that have them.
    nongravs, n_ng_errors = load_nongravs(mpc_orbits_path, np.unique(sss["designation"][:n_orbit]))
    n_ng = {m: sum(ng.model == m for ng in nongravs.values()) for m in ("comet", "yarkovsky")}
    print(f"[{time.perf_counter() - t_start:.1f} s] non-gravitational parameters: "
          f"{n_ng['comet']:,} comets, {n_ng['yarkovsky']:,} Yarkovsky asteroids; "
          f"{n_ng_errors:,} unparseable (integrated with gravity only)", flush=True)
    # ephemerides for the objects with orbits (the first n_orbit rows);
    # every orbit-derived column of the rest is NaN (NULL when written).
    t_eph = time.perf_counter()
    step_cap_stops = compute_ephemerides(sss[:n_orbit], obs_state[:n_orbit], dia_eph, mpcorb,
                                         workers=workers, chunk_factor=chunk_factor, covs=covs,
                                         nongravs=nongravs)
    t_eph = time.perf_counter() - t_eph
    n_ell = int(np.sum(np.isnan(sss["ephRaErr"][:n_orbit])))
    print(f"Ephemerides in {t_eph:.1f} s; error ellipse NULL on {n_ell:,} of {n_orbit:,} rows with an "
          f"orbit; {step_cap_stops:,} ellipse propagations stopped by the step cap.", flush=True)
    for name in EPHEMERIS_COLUMNS:
        if sss.dtype[name].kind == "f" and name not in MEASURED_EPH_COLUMNS:
            sss[name][n_orbit:] = np.nan
    del obs_state, mpcorb, covs, nongravs

    #
    # Assemble the SSObservation columns, in the output's row order
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
    # as today's SSObservation, bitwise.
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
            if name in SSObservationDtype.names or name in SSOBSERVATION_INTERNAL_DTYPE:
                columns[name] = cast_column(name, arr)
        del tbl
    if "matchMethod" not in internal_columns:
        pass
    elif has_match_method:
        _check_values("matchMethod", columns["matchMethod"], MATCH_METHODS)
    else:
        print("dia_sources.parquet has no matchMethod (it predates it); "
              "derived it from match and obssubid")
        columns["matchMethod"] = cast_column(
            "matchMethod", _derive_match_method(pending["match"], pending["obssubid"]))
    if not corrected:
        print("dia_sources.parquet has no shutter-corrected times (it predates them): "
              "midpointMjdTai is the visit midpoint, midpointMjdTai_flag set on every row")
        n = len(src)
        columns[SHUTTER_FLAGS[0]] = cast_column(SHUTTER_FLAGS[0], pa.array(np.ones(n, dtype=bool)))
        if SHUTTER_FLAGS[1] in internal_columns:
            columns[SHUTTER_FLAGS[1]] = cast_column(SHUTTER_FLAGS[1], pa.array(np.zeros(n, dtype=bool)))
    for name, arr in _split_ids(pending["measuredOn"], pending["diaSourceId"], pending["parentId"]).items():
        columns[name] = cast_column(name, arr)
    del pending

    # The internal columns are not in the delivered table: they go to the
    # sidecar, with obsid, in the same row order.
    sidecar = pa.Table.from_arrays([columns[SIDECAR_KEY], *(columns.pop(c) for c in internal_columns)],
                                   schema=sidecar_schema(internal_columns))
    table = ssobservation_table(columns, cast=False)   # (each was cast above)
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

    manifest = write_partitioned(table, sidecar, output_dir, part_rows=part_rows)
    n_null_parts = sum(p["null_ssObjectId"] for p in manifest["parts"])
    print(f"Wrote {len(manifest['parts'])} SSObservation parts ({n_null_parts} of NULL ssObjectId), "
          f"{SIDECAR_FILE} ({', '.join(internal_columns) or 'obsid only'}) and "
          f"{SSOBSERVATION_MANIFEST_FILE} to {output_dir}: {table.num_rows:,} rows, "
          f"{table.num_columns} columns (ephemerides {t_eph:.1f} s, "
          f"total {time.perf_counter() - t_start:.1f} s).")
    return manifest


def main():
    parser = argparse.ArgumentParser(
        prog="ssp-build-ssobservation",
        description="Build the SSObservation table from the submitted-source and MPC Parquet files",
        epilog=(
            "Reads dia_sources (from extract-submitted-sources), obs_sbn, "
            "numbered_identifications, current_identifications and mpc_orbits "
            ".parquet files from the input directory and writes the SSObservation "
            "(one row per dia_sources row, with the columns of the PPDB SSObservation "
            "table) to the output directory as parts, SSObservation.partNNNN.parquet, "
            "partitioned by ssObjectId, with " + SSOBSERVATION_MANIFEST_FILE + " and the sidecar of "
            "internal columns, " + SIDECAR_FILE + ". The ASSIST ephemeris files are taken "
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
    parser.add_argument(
        "--part-rows", type=int, default=PART_ROWS_DEFAULT,
        help=("Close a part at the first object boundary at or after this many rows; NULL-ssObjectId "
              "rows are cut every this many rows (default: %(default)s)"),
    )
    parser.add_argument(
        "--internal-columns", default=",".join(SSOBSERVATION_INTERNAL_DEFAULT),
        help=("Comma-separated columns written to the sidecar instead of the delivered table, from "
              f"{', '.join(SSOBSERVATION_INTERNAL_DTYPE)}; an empty string means none "
              "(default: %(default)s)"),
    )
    args = parser.parse_args()
    if args.part_rows < 1:
        parser.error("--part-rows must be at least 1")
    if args.workers < 1:
        parser.error("--workers must be at least 1")
    if args.chunk_factor < 1:
        parser.error("--chunk-factor must be at least 1")

    try:
        build_ssobservation(
            args.input_dir, args.output_dir,
            max_objects=args.max_objects, dia_sample_frac=args.dia_sample_frac, seed=args.seed,
            workers=args.workers, chunk_factor=args.chunk_factor, part_rows=args.part_rows,
            internal_columns=parse_internal_columns(args.internal_columns),
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.reraise:
            raise
        sys.exit(1)


if __name__ == "__main__":
    main()
