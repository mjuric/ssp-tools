"""Build the SSObject table from the (widened) SSSource and mpc_orbits.

Per object and band, an H/G12 fit of SSSource's photometry (``fit_band``):
a band's slope fit fails (``{band}_slope_fit_failed``) when the free G12
ends at a bound, the fit isn't invertible, it uses fewer than 3 points, or
its points span less than 2 deg in phase angle; H is then refit at a
fiducial G12 (0.5). See ``compute_ssobject`` for the details. Every value
is a function of the set of an object's SSSource rows, whatever their order.
"""
import pandas as pd
import numpy as np
from functools import partial
from . import photfit
from . import util
from . import schema
from .moid import MOIDSolver, earth_orbit
import argparse
import os
import sys

def nJy_to_mag(f_njy):
    """
    Convert flux density in nanoJanskys (nJy) to AB magnitude.

    Parameters
    ----------
    f_njy : float or array-like
        Flux density in nanoJanskys.

    Returns
    -------
    float or array-like
        AB magnitude corresponding to the input flux density.
    """
    return 31.4 - 2.5 * np.log10(f_njy)

def nJy_err_to_mag_err(f_njy, f_err_njy):
    """
    Convert flux error in nanoJanskys to magnitude error.

    Parameters
    ----------
    f_njy : float
        Flux in nanoJanskys.
    f_err_njy : float
        Flux error in nanoJanskys.

    Returns
    -------
    float
        Magnitude error.
    """
    return 1.085736 * (f_err_njy / f_njy)

FIT_COLUMNS = ["psfMag", "psfMagErr", "phaseAngle", "topoRange", "helioRange"]

# The only SSSource (widened, ssp.schema_ppdb.SSSourceDtype) columns
# compute_ssobject uses. The photometry is SSSource's own: band, and the
# float32 psfFlux and psfFluxErr, converted to magnitudes in float64.
SSS_COLUMNS = ["ssObjectId", "designation", "primary", "ephRa",
               "midpointMjdTai", "band", "psfFlux", "psfFluxErr", "extendedness",
               "phaseAngle", "topoRange", "helioRange"]


def _entry_columns(sss):
    """The SSSource columns that compute_ssobject_entry reads, as numpy
    arrays, converted once for the whole table rather than per object
    (slicing and reducing the pyarrow-backed frame per object cost about a
    third of the per-object time). The magnitudes are computed in float64
    from SSSource's float32 fluxes."""
    flux = sss["psfFlux"].to_numpy(dtype=np.float64, na_value=np.nan)
    flux_err = sss["psfFluxErr"].to_numpy(dtype=np.float64, na_value=np.nan)
    with np.errstate(divide="ignore", invalid="ignore"):
        cols = {"psfMag": nJy_to_mag(flux), "psfMagErr": nJy_err_to_mag_err(flux, flux_err)}
    for c in ("phaseAngle", "topoRange", "helioRange"):
        cols[c] = np.asarray(sss[c])
    cols["band"] = np.asarray(sss["band"].astype(str))
    cols["ssObjectId"] = sss["ssObjectId"].to_numpy()
    cols["designation"] = sss["designation"].to_numpy()
    cols["midpointMjdTai"] = sss["midpointMjdTai"].to_numpy(dtype=float, na_value=np.nan)
    cols["extendedness"] = sss["extendedness"].to_numpy(dtype=float, na_value=np.nan)
    return cols


# The slope (G12) fit of a band fails (``{band}_slope_fit_failed``) when
# any of these holds; fit_band returns them as a bit mask.
FAIL_BOUND = 1        # the free G12 ends at a bound (within G12_BOUND_TOL of 0 or 1)
FAIL_SINGULAR = 2     # J^T J singular: no finite result (other than FAIL_FEW), or a
                      # non-finite G12Err with G12 inside (0, 1)
FAIL_FEW = 4          # fewer than MIN_SLOPE_OBS usable points, or used (after clipping)
FAIL_SPAN = 8         # the points used span less than minPhaseSpan in phase angle
FAILURES = {"bound": FAIL_BOUND, "singular": FAIL_SINGULAR, "few": FAIL_FEW, "span": FAIL_SPAN}

#: A free G12 this close to 0 or 1 is at the bound: ~10x the bounded
#: search's tolerance (its minima at a bound land within ~1e-6 of it).
G12_BOUND_TOL = 1e-5
#: The fewest points (after clipping) a slope fit may use.
MIN_SLOPE_OBS = 3
#: Fits clip outliers only if more than this many usable points are left
#: (the free fit's condition; the fiducial-G12 fallback uses it too).
CLIP_MIN_OBS = 3
#: The fiducial G12 of a failed slope fit (DP2's fixed value).
FIDUCIAL_G12 = 0.5
#: The smallest phase-angle span [deg] of the points a slope fit uses.
MIN_PHASE_SPAN = 2.0


def fit_band(
    mag, magSigma, phaseAngle, tdist, rdist, fixedG12=None,
    magSigmaFloor=0.0, nSigmaClip=None, fiducialG12=None,
    minPhaseSpan=MIN_PHASE_SPAN,
):
    """The H/G12 fit of one band, as the SSObject columns (without the
    band prefix; "Cov" is the H-G12 covariance), plus ``failures``, the
    FAIL_* mask.

    With a free G12 (``fixedG12`` None), the slope fit fails when any
    FAIL_* rule holds. A band with fewer than MIN_SLOPE_OBS observations
    gets no free fit at all (it would fail FAIL_FEW). On failure, H is
    refit with G12 fixed at ``fiducialG12`` (default FIDUCIAL_G12),
    with the same error floor and clipping (clipping only with more than
    CLIP_MIN_OBS usable points, as the free fit); G12 is stored as that
    value, G12Err and Cov are NaN, and HErr, nObsUsed and Chi2 are the
    fixed-G12 fit's. If clipping leaves one point, H, HErr come from it
    (nObsUsed 1, Chi2 NaN). Only if no point is usable or none survives
    clipping are H, HErr and G12 NaN (nObsUsed 0). slope_fit_failed is
    set in either case. (NaN is how SSObject stores NULL.)

    With ``fixedG12`` set, G12 isn't fit and these rules don't apply:
    the fit is the fixed-G12 one, and slope_fit_failed is set only if
    it fails.

    Every input is taken as a set (photfit.fitHG12 orders them
    canonically; the phase span is max - min), so the result doesn't
    depend on their order.
    """
    if fiducialG12 is None:
        fiducialG12 = FIDUCIAL_G12 if fixedG12 is None else fixedG12
    kw = dict(magSigmaFloor=magSigmaFloor, nSigmaClip=nSigmaClip)
    phaseAngle = np.asarray(phaseAngle)
    failures = 0
    if fixedG12 is None:
        res = None
        if len(mag) >= MIN_SLOPE_OBS:
            det = {}
            res = photfit.fitHG12(mag, magSigma, phaseAngle, tdist, rdist, _details=det, **kw)
        if res is None:
            failures = FAIL_FEW
        elif not (np.isfinite(res.H) and np.isfinite(res.G12) and np.isfinite(res.H_err)
                  and (np.isfinite(res.G12_err) or res.G12 in (0., 1.))):
            # (fewer than MIN_SLOPE_OBS usable points, or clipped to fewer:
            # fitHG12 returns no result, and that is FAIL_FEW. A G12_err
            # is NaN by design only at a bound.)
            few = (det["nusable"] < MIN_SLOPE_OBS
                   or ("keep" in det and det["keep"].sum() < MIN_SLOPE_OBS))
            failures = FAIL_FEW if few else FAIL_SINGULAR
        else:
            if min(res.G12, 1. - res.G12) <= G12_BOUND_TOL:
                failures |= FAIL_BOUND
            if res.nobs < MIN_SLOPE_OBS:
                failures |= FAIL_FEW
            pa = phaseAngle[det["used"]]
            if np.nanmax(pa) - np.nanmin(pa) < minPhaseSpan:
                failures |= FAIL_SPAN
        G = fiducialG12
    else:
        G = fixedG12

    if fixedG12 is not None or failures:
        # (clipped under the same condition as the free fit, more than 3
        # usable points, so it never uses fewer points than that would)
        res = photfit.fitHG12(mag, magSigma, phaseAngle, tdist, rdist, fixedG12=G,
                              clipMinObs=CLIP_MIN_OBS if fixedG12 is None else None, **kw)
        nDof = res.nobs - 1
        failed = bool(failures) or not np.isfinite(res.H)
        out = dict(H=res.H, HErr=res.H_err, G12=G if np.isfinite(res.H) else np.nan,
                   G12Err=np.nan, Cov=np.nan)
    else:
        nDof = res.nobs - 2
        failed = False
        out = dict(H=res.H, HErr=res.H_err, G12=res.G12, G12Err=res.G12_err, Cov=res.HG_cov)
    if res.nobs == 0:
        out["H"] = out["HErr"] = np.nan
    # chi2dof is per degree of freedom of the points the fit used (after
    # clipping), so scale back by those, not by all of the band's points.
    with np.errstate(invalid="ignore"):
        out.update(Chi2=res.chi2dof * nDof, nObsUsed=res.nobs, slope_fit_failed=failed,
                   failures=failures)
    return out


def compute_ssobject_entry(
    row, sss, fixedG12=None, magSigmaFloor=0.0, nSigmaClip=None,
    fiducialG12=None, minPhaseSpan=MIN_PHASE_SPAN,
):
    """Fill the SSObject ``row`` of one object. ``sss`` maps each column of
    ``_entry_columns`` to that object's rows (numpy arrays), in any order:
    every value computed here is a function of the set of rows, bitwise
    (``photfit.fitHG12`` sorts its inputs canonically)."""
    # just verify we didn't screw up something
    assert np.all(sss["ssObjectId"] == sss["ssObjectId"][0])

    # Metadata columns
    row["ssObjectId"] = sss["ssObjectId"][0]
    row["firstObservationMjdTai"] = np.nanmin(sss["midpointMjdTai"])

    if "discoverySubmissionDate" in row.dtype.names: # DP2 does not have this field
        # FIXME: here I arbitrarily guess we discover everything 7 days
        # after first obsv. we should really pull this out of the obs_sbn tbl.
        row["discoverySubmissionDate"] = row["firstObservationMjdTai"] + 7.
    row["arc"] = np.ptp(sss["midpointMjdTai"])
    row["designation"] = sss["designation"][0]

    # observation counts
    row["nObs"] = len(sss["ssObjectId"])

    # (selecting bands on numpy arrays is much cheaper than filtering the
    # pyarrow-backed frame six times per object)
    bandCol = sss["band"]
    fitCols = {col: sss[col] for col in FIT_COLUMNS}

    # per band entries
    for band in "ugrizy":
        inBand = bandCol == band
        df = {col: arr[inBand] for col, arr in fitCols.items()}

        # set defaults for this band (equivalents of NULL)
        row[f'{band}_Chi2'] = np.nan
        row[f'{band}_G12'] = np.nan
        row[f'{band}_G12Err'] = np.nan
        row[f'{band}_H'] = np.nan
        row[f'{band}_H_{band}_G12_Cov'] = np.nan
        row[f'{band}_HErr'] = np.nan
        row[f'{band}_nObsUsed'] = 0
        row[f'{band}_phaseAngleMin'] = np.nan
        row[f'{band}_phaseAngleMax'] = np.nan

        nBandObs = len(df["phaseAngle"])
        row[f"{band}_nObs"] = nBandObs
        if nBandObs > 0:
            # nanmin/nanmax: skip nulls, like the pandas reductions
            paMin, paMax = np.nanmin(df["phaseAngle"]), np.nanmax(df["phaseAngle"])
            row[f"{band}_phaseAngleMin"] = paMin
            row[f"{band}_phaseAngleMax"] = paMax

            fit = fit_band(
                df["psfMag"], df["psfMagErr"],
                df["phaseAngle"], df["topoRange"], df["helioRange"],
                fixedG12=fixedG12, magSigmaFloor=magSigmaFloor,
                nSigmaClip=nSigmaClip, fiducialG12=fiducialG12,
                minPhaseSpan=minPhaseSpan,
            )
            for col, val in fit.items():
                if col != "failures":
                    row[f"{band}_{col}" if col != "Cov" else f"{band}_H_{band}_G12_Cov"] = val

    # Extendedness (null for DiaSources that lack it -> NaN)
    ext = sss["extendedness"]
    ext = ext[~np.isnan(ext)]
    row["extendednessMin"] = ext.min() if len(ext) else np.nan
    row["extendednessMax"] = ext.max() if len(ext) else np.nan
    row["extendednessMedian"] = np.median(ext) if len(ext) else np.nan

#
# Parallel build (--workers N > 1)
#
# Workers are forked, and read their inputs from this module-level dict,
# filled in by the parent just before it creates the pool. The (large)
# joined SSSource frame is thus inherited through fork, never pickled.
# Tasks are (start, end) index ranges; results are small numpy arrays.
#
_PARALLEL = {}
_MOID_SOLVER = None   # one MOIDSolver per worker process, created lazily

MOID_COLUMNS = [
    "MOIDEarth", "MOIDEarthDeltaV", "MOIDEarthEclipticLongitude",
    "MOIDEarthTrueAnomaly", "MOIDEarthTrueAnomalyObject",
]

def _entry(callback, row, cols, start, end):
    """Call ``callback`` for the object in rows [start, end) of ``cols``."""
    callback(row, {c: a[start:end] for c, a in cols.items()})

def _ssobject_chunk(g0, g1):
    """Worker: compute SSObject rows for groups [g0, g1)."""
    sss = _PARALLEL["sss"]
    idx_start, idx_end = _PARALLEL["idx_start"], _PARALLEL["idx_end"]
    callback = _PARALLEL["callback"]
    out = np.zeros(g1 - g0, dtype=schema.SSObjectDtype)
    for k, g in enumerate(range(g0, g1)):
        _entry(callback, out[k], sss, idx_start[g], idx_end[g])
    return out

def _moid_chunk(j0, j1):
    """Worker: compute the MOID columns for matched orbits [j0, j1)."""
    global _MOID_SOLVER
    if _MOID_SOLVER is None:
        _MOID_SOLVER = MOIDSolver()
    solver = _MOID_SOLVER
    a, e, i, node, argperi, epoch_mjd = _PARALLEL["elements"]
    res = np.full((len(MOID_COLUMNS), j1 - j0), np.nan, dtype=np.float64)
    for k, j in enumerate(range(j0, j1)):
        earth = earth_orbit(epoch_mjd[j])
        res[:, k] = solver.compute(earth, (a[j], e[j], i[j], node[j], argperi[j]))
    return res

def compute_ssobject(
    sss, mpcorb, fixedG12=None, magSigmaFloor=0.05,
    nSigmaClip=10.0, workers=1, chunk_factor=8,
    fiducialG12=None, minPhaseSpan=MIN_PHASE_SPAN,
):
    """
    Compute solar system object properties from SSSource and MPC orbit
    data.

    This function takes a pre-grouped (widened) SSSource table, computes
    per-object quantities from its photometry and geometry, and
    calculates additional orbital parameters like Tisserand J and Minimum
    Orbit Intersection Distance (MOID) with Earth for matching objects.

    Parameters
    ----------
    sss : pandas.DataFrame
        SSSource table, pre-grouped by 'ssObjectId' (the order of each
        object's rows doesn't matter). Must have the ``SSS_COLUMNS``.
    mpcorb : pandas.DataFrame
        MPC orbit data with columns like
        'unpacked_primary_provisional_designation', 'q', 'e', 'i',
        'node', 'argperi'.
    fixedG12, magSigmaFloor, nSigmaClip, fiducialG12, minPhaseSpan
        The per-band H/G12 fits, as ``fit_band``. A band's slope fit
        fails (``{band}_slope_fit_failed``, "G12 fit failed in {band}
        band. G12 contains a fiducial value used to fit H.") when:

        1. the free G12 ends at a bound, within G12_BOUND_TOL (1e-5) of
           0 or 1;
        2. the fit isn't invertible (J^T J singular): no finite result,
           or no finite G12Err with G12 inside (0, 1);
        3. fewer than MIN_SLOPE_OBS (3) points are usable, or used after
           clipping;
        4. the points it uses span less than ``minPhaseSpan`` (default
           2 deg) in phase angle.

        H is then refit with G12 fixed at ``fiducialG12`` (default
        ``fixedG12`` if set, else 0.5), clipping as the free fit does
        (only with more than 3 usable points); G12 is stored as that value,
        G12Err and the H-G12 covariance are NaN, and HErr, nObsUsed and
        Chi2 come from the fixed-G12 fit; one point left after clipping
        gives H from that point (nObsUsed 1, Chi2 NaN). Only if no point
        is usable, or none survives clipping, are H, HErr and G12 NaN
        (nObsUsed 0); the flag is set either way. (SSObject stores NULL
        as NaN.) With
        ``fixedG12`` set G12 isn't fit, and the flag means the fixed fit
        failed. Each fit is a function of the set of its band's points,
        whatever their order.
    workers : int
        Number of worker processes for the per-object and MOID stages.
        1 (the default) runs everything serially in this process. More
        than 1 forks a process pool (falling back to serial where the
        fork start method is unavailable); the result is identical.
    chunk_factor : int
        With workers > 1, split each stage into about
        ``chunk_factor * workers`` chunks, to balance the load.

    Returns
    -------
    numpy.ndarray
        Array of ssObject records with dtype schema.ssObjectDtype,
        containing computed properties for each unique ssObjectId,
        including magnitudes, orbital elements, Tisserand J, and
        MOID-related values.

    Raises
    ------
    AssertionError
        If 'sss' is not pre-grouped by 'ssObjectId'.

    Notes
    -----
    - The function assumes 'sss' is large and avoids internal
      sorting/copying for efficiency.
    - Tisserand J and MOID are computed only for objects matching
      designations in 'mpcorb'.
    - MOID computation uses a MOIDSolver for each matched object.
    """

    missing = [c for c in SSS_COLUMNS if c not in sss.columns]
    if missing:
        raise ValueError(f"SSSource lacks the columns {missing} (SSObject needs the widened SSSource)")

    # Sources without an orbit get no SSObject: unmatched ones (a NULL
    # ssObjectId in the widened SSSource; 0 in older files), and any other
    # with NULL/NaN ephemerides (older files: designated objects with no
    # mpc_orbits row). (np.isnan as well, as pyarrow-backed isna() misses NaN)
    oid = sss["ssObjectId"]
    unmatched = oid.isna().to_numpy(dtype=bool) | (oid.fillna(0) == 0).to_numpy(dtype=bool)
    no_orbit = unmatched | np.isnan(sss["ephRa"].to_numpy(dtype=float, na_value=np.nan))
    if no_orbit.any():
        print(f"Skipping {no_orbit.sum():,} SSSource rows without an orbit "
              f"({unmatched.sum():,} unmatched)")
        sss = sss[~no_orbit]

    # A source claimed by several obs_sbn rows (both endpoints of a trail,
    # repeated submissions) has several SSSource rows; count it once.
    sss = sss[sss["primary"].to_numpy(dtype=bool)]

    # assert that sss is pre-grouped by ssObjectId
    assert util.values_grouped(sss["ssObjectId"]), (
        "SSSource table must be pre-grouped by ssObjectId. "
        "An easy way to do this is to sort by ssObjectId before calling compute_ssobject(). "
        "The grouping is required for correct per-object computations, and since SSSource is "
        "typically large and we want to avoid copies, it's not done internally."
    )

    # Pre-create the empty array
    totalNumObjects = np.unique(sss["ssObjectId"].to_numpy()).size
    obj = np.zeros(totalNumObjects, dtype=schema.SSObjectDtype)

    # compute per-object quantities
    callback = partial(
        compute_ssobject_entry, fixedG12=fixedG12,
        magSigmaFloor=magSigmaFloor, nSigmaClip=nSigmaClip,
        fiducialG12=fiducialG12, minPhaseSpan=minPhaseSpan,
    )
    parallel = workers > 1 and util.fork_context() is not None
    cols = _entry_columns(sss)
    # Group boundaries as util.group_by computes them, so the rows come out
    # in the same (ascending ssObjectId) order.
    keys = cols["ssObjectId"]
    if totalNumObjects and not util.values_grouped(keys):
        raise ValueError("Key 'ssObjectId' is not properly grouped.")
    _, idx_start, counts = np.unique(keys, return_index=True, return_counts=True)
    idx_end = idx_start + counts
    if not parallel:
        for k in range(totalNumObjects):
            _entry(callback, obj[k], cols, idx_start[k], idx_end[k])
            if k % 10_000 == 0:
                print(f"[objects] {k:,}/{totalNumObjects:,}", flush=True)
    elif totalNumObjects:
        # contiguous runs of groups, balanced by observation count
        chunks = util.balanced_chunks(counts, chunk_factor * workers)
        print(f"Computing {totalNumObjects:,} objects in {len(chunks)} chunks "
              f"on {workers} workers...")
        _PARALLEL.update(sss=cols, idx_start=idx_start, idx_end=idx_end, callback=callback)
        try:
            results = util.run_chunks(_ssobject_chunk, chunks, workers, "objects",
                                  weights=[int(counts[g0:g1].sum()) for g0, g1 in chunks])
        finally:
            _PARALLEL.clear()
        for (g0, g1), res in zip(chunks, results):
            obj[g0:g1] = res
        del results

    #
    # compute columns that can be efficiently computed in a vector fashon
    #
    # Tisserand J

    if mpcorb is not None:
        # inner join by provisional designation. We allow for some objects
        # to be missing from mpcorb (this should not happen often, but it
        # did in DP1).
        # FIXME: at some point require that no objects are missing. I _think_
        # that shouldn't happen in normal operations.
        oidx, midx = util.argjoin(obj["designation"].astype("U"),
                             mpcorb["unpacked_primary_provisional_designation"].to_numpy().astype("U")
                            )
        assert np.all(mpcorb["unpacked_primary_provisional_designation"].take(midx) ==
                    obj["designation"][oidx].astype("U"))
        q, e, i, node, argperi, epoch_mjd = util.unpack(
            mpcorb["q e i node argperi epoch_mjd".split()].take(midx)
        )
        a = q / (1. - e)
        obj["tisserand_J"][oidx] = util.tisserand_jupiter(a, e, i)

        # MOID computation
        if parallel and len(oidx):
            n = len(oidx)
            chunks = util.balanced_chunks(np.ones(n), chunk_factor * workers)
            _PARALLEL.update(elements=(a, e, i, node, argperi, epoch_mjd))
            try:
                results = util.run_chunks(_moid_chunk, chunks, workers, "MOID")
            finally:
                _PARALLEL.clear()
            moid = np.concatenate(results, axis=1)
            # The serial loop writes in order, so if an object matched
            # several orbits its last one wins; do the same.
            _, last = np.unique(oidx[::-1], return_index=True)
            last = n - 1 - last
            for col, vals in zip(MOID_COLUMNS, moid):
                obj[col][oidx[last]] = vals[last]
        else:
            solver = MOIDSolver()
            for i, el_obj in enumerate(zip(a, e, i, node, argperi)):
                earth = earth_orbit(epoch_mjd[i])
                (moid, deltaV, eclon, trueEarth, trueObject) = solver.compute(earth, el_obj)
                row = obj[oidx[i]]
                row["MOIDEarth"] = moid
                row["MOIDEarthDeltaV"] = deltaV
                row["MOIDEarthEclipticLongitude"] = eclon
                row["MOIDEarthTrueAnomaly"] = trueEarth
                row["MOIDEarthTrueAnomalyObject"] = trueObject

    return obj

def main():
    """
    CLI entry point for building SSObject table from SSSource,
    DiaSource, and MPC orbit data.
    """
    parser = argparse.ArgumentParser(
        description="Build SSObject table from SSSource and MPC orbit Parquet files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  ssp-build-ssobject sssource.parquet mpc_orbits.parquet --output ssobject.parquet

The photometry comes from SSSource. The older form, with dia_sources.parquet
between the two, is still accepted; that file is not read.
        """
    )

    parser.add_argument(
        "sssource_parquet",
        help="Path to the (widened) SSSource Parquet file"
    )
    parser.add_argument(
        "inputs", nargs="+", metavar="mpcorb_parquet",
        help="Path to MPC orbits Parquet file (an extra dia_sources.parquet "
             "before it, as in the older form, is ignored)"
    )
    parser.add_argument(
        "--output", "-o",
        required=True,
        help="Path to output SSObject Parquet file"
    )
    parser.add_argument(
        "--reraise",
        action="store_true",
        help="Re-raise exceptions instead of exiting gracefully (for debugging)"
    )
    parser.add_argument(
        "--hg12FixedG12",
        type=float,
        default=None,
        help=(
            "If set, fix the G12 slope parameter to this value and "
            "only fit H. If unset, both H and G12 are fit."
        ),
    )
    parser.add_argument(
        "--hg12MagSigmaFloor",
        type=float,
        default=0.05,
        help=(
            "Systematic magnitude error floor (mag) added in quadrature "
            "to measurement errors before HG12 fitting (default: 0.05, "
            "as in DP2; 0 for none)."
        ),
    )
    parser.add_argument(
        "--hg12NSigmaClip",
        type=float,
        default=10.0,
        help=(
            "Reject outliers beyond this many sigma after an initial "
            "robust (soft_l1) fit, then refit the rest (default: 10, as "
            "in DP2; inf to keep every point)."
        ),
    )

    parser.add_argument(
        "--hg12FiducialG12",
        type=float,
        default=None,
        help=(
            "G12 of the fixed-G12 refit of H where a band's slope fit "
            "fails (default: --hg12FixedG12 if set, else 0.5, as in DP2)."
        ),
    )
    parser.add_argument(
        "--hg12MinPhaseSpan",
        type=float,
        default=MIN_PHASE_SPAN,
        help=(
            "A slope fit whose points span less than this phase angle "
            f"(deg) fails (default: {MIN_PHASE_SPAN})."
        ),
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=min(64, os.cpu_count() or 1),
        help=(
            "Number of worker processes for the per-object fits and the "
            "MOIDs (default: min(64, number of CPUs)). 1 runs serially, "
            "with no process pool. The output does not depend on it."
        ),
    )

    parser.add_argument(
        "--chunk-factor",
        type=int,
        default=8,
        help=(
            "With --workers > 1, split the work into about this many "
            "chunks per worker, to balance the load (default: 8)."
        ),
    )

    args = parser.parse_args()
    if len(args.inputs) > 2:
        parser.error("expected: sssource_parquet [diasource_parquet] mpcorb_parquet")
    if len(args.inputs) == 2:
        print(f"Note: {args.inputs[0]} is not read; the photometry comes from SSSource.")
    args.mpcorb_parquet = args.inputs[-1]
    if args.workers < 1:
        parser.error("--workers must be at least 1")
    if args.chunk_factor < 1:
        parser.error("--chunk-factor must be at least 1")

    try:
        # Load SSSource: only the columns compute_ssobject uses (the
        # widened SSSource has ~180)
        print(f"Loading SSSource from {args.sssource_parquet}...")
        sss = pd.read_parquet(args.sssource_parquet, engine="pyarrow", dtype_backend="pyarrow",
                              columns=SSS_COLUMNS).reset_index(drop=True)
        num = len(sss)
        print(f"Loaded {num:,} SSSource rows")

        # Load MPC orbits
        mpcorb_columns = [
            "unpacked_primary_provisional_designation", "a", "q", "e", "i",
            "node", "argperi", "peri_time", "mean_anomaly", "epoch_mjd", "h", "g"
        ]
        print(f"Loading MPC orbits from {args.mpcorb_parquet}...")
        mpcorb = pd.read_parquet(args.mpcorb_parquet, engine="pyarrow",
                                 dtype_backend="pyarrow", columns=mpcorb_columns
                                 ).reset_index(drop=True)
        print(f"Loaded {len(mpcorb):,} MPC orbit rows")

        # Compute SSObject
        print("Computing SSObject data...")
        obj = compute_ssobject(
            sss, mpcorb,
            fixedG12=args.hg12FixedG12,
            magSigmaFloor=args.hg12MagSigmaFloor,
            nSigmaClip=args.hg12NSigmaClip,
            fiducialG12=args.hg12FiducialG12,
            minPhaseSpan=args.hg12MinPhaseSpan,
            workers=args.workers,
            chunk_factor=args.chunk_factor,
        )

        # Save result
        print(f"Saving {len(obj):,} SSObject rows to {args.output}...")
        util.struct_to_parquet(obj, args.output)

        print(f"Success! Created SSObject with {len(obj):,} objects")
        print(f"Row size: {obj.dtype.itemsize:,} bytes, Total size: {obj.nbytes:,} bytes")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.reraise:
            raise
        sys.exit(1)

if __name__ == "__main__":
    input_dir = "./analysis/inputs"
    output_dir = "./analysis/outputs"

    #
    # Loads
    #

    # load SSSource
    sss = pd.read_parquet(f'{output_dir}/sssource.parquet',
                          engine="pyarrow", dtype_backend="pyarrow",
                          columns=SSS_COLUMNS).reset_index(drop=True)

    # Load mpcorb
    mpcorb = pd.read_parquet(f'{input_dir}/mpc_orbits.parquet',
                             engine="pyarrow", dtype_backend="pyarrow",
                             columns=[
                                 "unpacked_primary_provisional_designation",
                                 "a", "q", "e", "i", "node", "argperi",
                                 "peri_time", "mean_anomaly", "epoch_mjd", "h", "g"
                             ]).reset_index(drop=True)

    #
    # Business logic
    #
    obj = compute_ssobject(sss, mpcorb)

    #
    # Save
    #
    util.struct_to_parquet(obj, f"{output_dir}/ssobject.parquet")

    print(f"row_length={obj.dtype.itemsize:,} bytes, rows={len(obj):,}, {obj.nbytes:,} bytes total")
