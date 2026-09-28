"""The contract between the NearbySSO work packages.

This module fixes the data types and the function signatures (with their
exact semantics) that the work packages implement and consume, so that they
can be built in parallel. See docs/design/nearbysso.md. Work packages must
not change this file; a needed change goes to the integrating session.

Conventions used throughout:

- **Frames:** barycentric ICRF (equatorial), AU and AU/day, unless stated.
- **"ASSIST time"** is days since J2000 TDB, i.e. ``JD_TDB - ephem.jd_ref``
  (``ephem.jd_ref`` is 2451545.0). Observation times arrive as TAI MJD and
  are converted with astropy.
- **Angles** are in degrees at interfaces, **separations** in arcsec, and
  **on-sky rates** in deg/day with ``ephRateRa`` including the cos(dec)
  factor, as in SSSource.
- **Error ellipses** use the DiaSource convention: ``raErr`` (the RA error
  *on the sky*, i.e. including cos(dec)), ``decErr`` (deg), and
  ``ra_dec_Cov`` (deg^2).
"""

from __future__ import annotations

from typing import NamedTuple

import numpy as np

# --------------------------------------------------------------------------
# Constants (owner decisions, 2026-09-28)
# --------------------------------------------------------------------------

#: A DiaSource is "near" a prediction within this separation.
MATCH_RADIUS_ARCSEC = 5.0

#: A prediction is eligible only while the 1-sigma semi-major axis of its
#: on-sky error ellipse is at most this.
SIGMA_MAX_ARCSEC = 10.0

#: Observatory code of the observer.
OBSCODE = "X05"

# --------------------------------------------------------------------------
# WP1: orbits
# --------------------------------------------------------------------------

#: One row per eligible orbit, as returned by ``orbits.load_orbits``.
ORBIT_DTYPE = np.dtype([
    ("designation", "U16"),      # unpacked primary provisional designation
    ("packed", "U8"),            # packed primary provisional designation
    # the elements, exactly as in mpc_orbits, so that the precise pass
    # (ssp.ephem_assist.compute_ephemerides_one, via its ``row=``) sees the
    # same orbit as the coarse pass
    ("q", "f8"), ("e", "f8"), ("i", "f8"), ("node", "f8"), ("argperi", "f8"),
    ("peri_time", "f8"), ("epoch_mjd", "f8"), ("h", "f8"), ("g", "f8"),
    # barycentric ICRF state and covariance at epoch
    ("epoch", "f8"),             # ASSIST time of the epoch (epoch_mjd is TT)
    ("state0", "f8", (6,)),      # x, y, z [AU], vx, vy, vz [AU/day]; from
                                 # ssp.ephem_assist.elements_row_to_bary_icrf
    ("cov0", "f8", (6, 6)),      # covariance of state0 (same units and frame)
    ("has_cov", "?"),            # False: no usable covariance (missing, or
                                 # its 6x6 block not positive definite)
    ("normalized_rms", "f8"),    # from mpc_orbits, for the run report
])

# orbits.load_orbits(path, with_filter=True) -> np.ndarray[ORBIT_DTYPE]
#   Reads mpc_orbits Parquet. The filter keeps orbits that aren't comets
#   (designation without '/', packed not starting with '_'), have all of
#   q, e, i, node, argperi and peri_time, and an arc filtered exactly as
#   lsst-gen-ephemcache's get-mpcorb.py does: on the JSON
#   orbit_fit_statistics.arc_length_total *text*, NOT IN ('0 days',
#   '1 days', '2 days'). (The Parquet arc_length_total column is NULL for
#   ~0.5M mostly multi-opposition orbits whose JSON arc is a year range,
#   so a numeric filter on it would wrongly drop them.) It
#   returns rows sorted by designation, and prints a one-line summary (rows
#   read, kept, has_cov false and why).

# --------------------------------------------------------------------------
# WP3: DiaSources and visits
# --------------------------------------------------------------------------

#: The DiaSource columns read from any input catalog.
DIA_COLUMNS = ["diaSourceId", "visit", "midpointMjdTai", "ra", "dec"]

#: One row per visit, as returned by ``visits.build_visits``.
VISIT_DTYPE = np.dtype([
    ("visit", "i8"),
    ("night", "i8"),             # day_obs, the visit id // 100000
    ("t_tai_mjd", "f8"),         # the visit's midpointMjdTai (all sources share it)
    ("t", "f8"),                 # ASSIST time (TDB) of t_tai_mjd
    ("center", "f8", (3,)),      # unit vector: the normalized mean of its sources
    ("radius", "f8"),            # [rad] angle from center enclosing every source
    ("obs_pos", "f8", (3,)),     # X05 barycentric ICRF position [AU] at t
    ("obs_vel", "f8", (3,)),     # X05 barycentric ICRF velocity [km/s] at t
    ("dia_start", "i8"),         # this visit's rows are dia[dia_start:dia_end]
    ("dia_end", "i8"),           #   of the visit-sorted DiaSource arrays
])

# visits.read_dia(path, t_lo_mjd=None, t_hi_mjd=None) -> dict[str, np.ndarray]
#   The DIA_COLUMNS of the rows with t_lo <= midpointMjdTai < t_hi, sorted
#   by (visit, diaSourceId). For the time slices; reads only those columns.
# visits.build_visits(dia) -> np.ndarray[VISIT_DTYPE], sorted by visit.
# visits.DiaIndex(dia, visits): per-visit HEALPix (order 15) index of the
#   DiaSources, built once, cheap to share through fork.
#   .match(visit_idx, ra, dec, radius_arcsec) -> (pred, dia_row, sep_arcsec)
#       For arrays of predictions (visit_idx[k], ra[k], dec[k]): every
#       DiaSource of that visit within radius_arcsec, as three flat arrays.
#       `pred` is the prediction index k of each match, `dia_row` indexes
#       the visit-sorted dia arrays, and separations use
#       ssp.util.sky_separation_arcsec.
# visits.VisitIndex(visits): per-night spatial index of visit centres.
#   .candidates(track, margin_arcsec) -> np.ndarray of visit indices
#       The visits where the orbit in `track` (a CoarseTrack) may fall
#       within the visit's field: for each night k with
#       track.sigma_major[k] <= SIGMA_MAX_ARCSEC, the visits of that night
#       whose centre is within radius + |rate| * |t - track.t[k]| +
#       margin_arcsec of the track position extrapolated linearly to the
#       visit time. It must never miss a visit the object is actually in,
#       which the WP5 brute-force check verifies.
#
#       The linear extrapolation within a night is not accurate for nearby
#       objects: the topocentric track curves with the diurnal parallax
#       (amplitude ~R_earth/delta; hundreds to thousands of arcsec for NEOs
#       within ~0.01 AU), and the rate itself changes. So the tolerance per
#       visit is at least
#           radius + |rate|*|dt| + D + 0.5*|drate/dt|*dt^2 + margin_arcsec
#       where drate/dt is estimated from the rate change between adjacent
#       nightly samples (conservatively, the larger of the two sides), and
#       the diurnal-parallax term is
#           D = A * max(2, |exp(i*W*dt) - 1 - i*W*dt|) * (1 + A),
#           A = arcsin(R_earth / delta_eff)
#       with W the sidereal rotation rate and delta_eff = delta reduced by
#       |d delta/dt| * |dt|. (The exact worst case of a rotating parallax
#       vector against its linear extrapolation; the 2 covers |dt| <= 0.338 d,
#       i.e. sampling at VisitIndex.night_t.)

# --------------------------------------------------------------------------
# WP2: propagation and uncertainty
# --------------------------------------------------------------------------


class CoarseTrack(NamedTuple):
    """One orbit, sampled once per night (``propagate.coarse``).

    Each array has one entry per requested night (shape (K,), or (K, 3)).
    Positions are *topocentric* (from X05) and geometric (no light-time
    correction), which is enough to select candidates; the precise pass
    computes the published values.
    """
    t: np.ndarray            # (K,) ASSIST time of each sample
    ra: np.ndarray           # (K,) [deg]
    dec: np.ndarray          # (K,) [deg]
    rate_ra: np.ndarray      # (K,) [deg/day], cos(dec) included
    rate_dec: np.ndarray     # (K,) [deg/day]
    ra_err: np.ndarray       # (K,) [deg] 1-sigma, on the sky (cos(dec) included)
    dec_err: np.ndarray      # (K,) [deg]
    ra_dec_cov: np.ndarray   # (K,) [deg^2]
    sigma_major: np.ndarray  # (K,) [arcsec] 1-sigma semi-major axis of the ellipse
    ok: np.ndarray           # (K,) bool: False where the integration failed
    delta: np.ndarray        # (K,) [AU] topocentric distance (sizes the diurnal-
                             # parallax term of the candidate margin)
    cpos: np.ndarray | None = None  # (K, 3, 3) [AU^2] barycentric position block
                             # of C(t) (NaN where not ok or not has_cov), for
                             # ellipse_at's precise projection


# propagate.coarse(orbit, t, obs_pos, ephem) -> CoarseTrack
#   orbit: one ORBIT_DTYPE row. t: (K,) ASSIST times; obs_pos: (K, 3) X05
#   barycentric positions [AU] at t. ephem: an open ASSIST ephemeris.
#
#   One ASSIST simulation for this orbit alone, integrated from its epoch
#   through the sorted t, with six variational particles created with
#   add_variation(testparticle=0) and seeded on the unit state axes, and
#   sim.ri_ias15.adaptive_mode = 2 set after attaching ASSIST. At each t:
#   the state, Phi(t) = d state(t) / d state0, C(t) = Phi cov0 Phi^T, and
#   the projection of C(t)'s position block onto the topocentric tangent
#   plane (the Jacobian of (RA*cos(dec), Dec) with respect to the object's
#   position, for the observer at obs_pos). Orbits with has_cov False get an
#   infinite sigma_major (so they're never eligible) and NaN ellipses, but
#   positions and rates as usual.
#
# propagate.ellipse_at(track, t, topo_pos=None)
#       -> (ra_err, dec_err, ra_dec_cov, sigma_major)
#   The error ellipse at arbitrary ASSIST times t (inside the sampled span).
#   With topo_pos ((N, 3) [AU], object - observer at each t, e.g. the
#   precise pass's EphResult.topo_pos.T), it linearly interpolates
#   track.cpos between samples (a convex combination, so it stays positive
#   semidefinite) and projects it on the tangent plane of topo_pos; this is
#   what the published ephRaErr/ephDecErr/ephRa_ephDec_Cov use. (Interpolating
#   the sky components instead is ~20% off for NEOs within ~0.01 AU, whose
#   line of sight turns within a night, and loses size near the poles.)
#   Without topo_pos, it interpolates the sky components of the samples.
#   Non-finite t, or a non-PSD result, gives NaN errors and an infinite
#   sigma_major (never eligible).

# --------------------------------------------------------------------------
# WP4: the output
# --------------------------------------------------------------------------

#: One row per DiaSource with an eligible prediction within the radius,
#: nearest only. The draft NearbySSO schema (sdm_schemas
#: u/mjuric/ppdb-sso-ng) plus the error ellipse.
NEARBYSSO_DTYPE = np.dtype([
    ("diaSourceId", "i8"),
    ("ssObjectId", "i8"),        # written null where the object has no SSObject row
    ("designation", "U16"),
    ("ephRa", "f8"),
    ("ephDec", "f8"),
    ("ephOffset", "f4"),         # [arcsec]
    ("ephVmag", "f4"),
    ("ephRateRa", "f4"),         # [deg/day]
    ("ephRateDec", "f4"),
    ("ephRaErr", "f4"),          # [deg]
    ("ephDecErr", "f4"),
    ("ephRa_ephDec_Cov", "f4"),  # [deg^2]
])
