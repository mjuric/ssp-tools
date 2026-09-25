"""ASSIST-based ephemeris generation, drop-in alternative to ssp.ephem.

Uses local `mpcorb` osculating elements (no Horizons fetch) and ASSIST's
n-body propagator (REBOUND IAS15 + JPL DE441 perturbers + GR + relativistic
Sun) for state propagation. RA/Dec is then computed from observer geometry
with second-order Taylor light-time correction.

Targets: agreement with JPL Horizons "astrometric ICRF RA/Dec"
(extra_prec=YES) at <1 mas RMS great-circle separation when given the
same osculating elements.

Heavy dependencies (`rebound`, `assist`) are imported lazily so simply
importing the package does not require a JPL planet ephemeris file on disk.
"""

from __future__ import annotations

from collections import namedtuple
from typing import Optional

import numpy as np
import pandas as pd
from astropy.coordinates import SkyCoord
from astropy.time import Time
import astropy.units as u

from . import util


# Gauss's gravitational constant: k = 0.01720209895 AU^(3/2) day^(-1) M_sun^(-1/2)
# GM_sun in AU^3 / day^2 = k^2.
GM_SUN = 0.01720209895 ** 2

# Speed of light in AU / day (IAU 2012, derived from c and AU).
C_AU_PER_DAY = 173.144632674240

# Obliquity defining the J2000 ecliptic frame of MPC/JPL osculating elements:
# IAU76/80 value 84381.448", the same one Horizons assumes for ECLIP=J2000.
# (The IAU 2006 value, 84381.406", would rotate positions by up to 42 mas.)
OBLIQUITY_J2000 = np.deg2rad(84381.448 / 3600.0)
_COS_EPS = np.cos(OBLIQUITY_J2000)
_SIN_EPS = np.sin(OBLIQUITY_J2000)

# J2000.0 epoch as MJD; ASSIST counts days from this instant in TDB.
MJD_J2000 = 51544.5


EphResult = namedtuple(
    "EphResult",
    [
        # Per-epoch arrays, all shape (N,) unless noted.
        "ra_deg",        # astrometric ICRF, light-time corrected
        "dec_deg",
        "xx",            # (3, N) barycentric ICRF position [AU]
        "vv",            # (3, N) barycentric ICRF velocity [km/s]
        "obs",           # (3, N) observer barycentric ICRF position [AU]
        "mu_lon",        # cos(dec)·dRA/dt   [deg/day]
        "mu_lat",        # dDec/dt           [deg/day]
        "mu_total",      # great-circle rate [deg/day]
        "H",             # absolute mag (scalar)
        "G",             # slope param  (scalar)
    ],
)


# ---------------------------------------------------------------------------
# Pure-numpy Kepler solver and element → state conversion
# ---------------------------------------------------------------------------

def solve_kepler(M: np.ndarray, e: float, tol: float = 1e-14, max_iter: int = 50) -> np.ndarray:
    """Solve Kepler's equation E - e sin E = M.

    Uses Newton-Raphson with a robust initial guess. ``M`` may be scalar or
    an ndarray; ``e`` is a scalar in [0, 1). Returns E in radians, same shape
    as ``M``.
    """
    M = np.atleast_1d(np.asarray(M, dtype=np.float64))
    M = np.mod(M + np.pi, 2 * np.pi) - np.pi  # wrap to [-pi, pi]
    # Initial guess (Danby 1988, good even for high e)
    E = M + e * np.sin(M)
    for _ in range(max_iter):
        f = E - e * np.sin(E) - M
        fp = 1.0 - e * np.cos(E)
        dE = -f / fp
        E = E + dE
        if np.all(np.abs(dE) < tol):
            break
    return E


def kepler_to_helio_ecliptic(
    a: float, e: float, inc_rad: float,
    Omega_rad: float, omega_rad: float, M_rad: float,
    mu: float = GM_SUN,
) -> tuple[np.ndarray, np.ndarray]:
    """Convert classical orbital elements to heliocentric J2000 *ecliptic* state.

    Returns (X, V) where X is position in AU and V is velocity in AU/day,
    both shape (3,).
    """
    E = solve_kepler(np.array([M_rad]), e)[0]
    cosE, sinE = np.cos(E), np.sin(E)
    # True anomaly via half-angle formula (numerically stable)
    nu = 2.0 * np.arctan2(np.sqrt(1.0 + e) * np.sin(E / 2.0),
                          np.sqrt(1.0 - e) * np.cos(E / 2.0))
    cosnu, sinnu = np.cos(nu), np.sin(nu)

    p = a * (1.0 - e * e)
    r = a * (1.0 - e * cosE)

    # Perifocal frame (P, Q, W) where P is toward perihelion.
    x_pf = r * cosnu
    y_pf = r * sinnu

    # Velocity in perifocal frame: v = sqrt(mu/p) * (-sin(nu), e + cos(nu)).
    fac = np.sqrt(mu / p)
    vx_pf = -fac * sinnu
    vy_pf = fac * (e + cosnu)

    # Rotation perifocal → J2000 ecliptic: R3(-Omega) R1(-i) R3(-omega)
    cosO, sinO = np.cos(Omega_rad), np.sin(Omega_rad)
    cosw, sinw = np.cos(omega_rad), np.sin(omega_rad)
    cosi, sini = np.cos(inc_rad), np.sin(inc_rad)

    # Direct multiplication of the three rotations
    R = np.array([
        [cosO * cosw - sinO * sinw * cosi, -cosO * sinw - sinO * cosw * cosi,  sinO * sini],
        [sinO * cosw + cosO * sinw * cosi, -sinO * sinw + cosO * cosw * cosi, -cosO * sini],
        [sinw * sini,                       cosw * sini,                       cosi       ],
    ])

    pos_pf = np.array([x_pf, y_pf, 0.0])
    vel_pf = np.array([vx_pf, vy_pf, 0.0])

    return R @ pos_pf, R @ vel_pf


def ecliptic_to_equatorial(v: np.ndarray) -> np.ndarray:
    """Rotate a 3-vector from J2000 ecliptic to J2000 equatorial (ICRF).

    Works on shape (3,) or (3, N) arrays.
    """
    R = np.array([
        [1.0,    0.0,        0.0],
        [0.0,  _COS_EPS, -_SIN_EPS],
        [0.0,  _SIN_EPS,  _COS_EPS],
    ])
    return R @ v


def elements_row_to_bary_icrf(row, sun_pos_au, sun_vel_au_day):
    """Take one mpcorb row (heliocentric ecliptic mean elements) plus the Sun's
    barycentric ICRF state at the same epoch. Return barycentric ICRF (X, V).
    """
    a = float(row["a"])
    e = float(row["e"])
    inc = np.deg2rad(float(row["i"]))
    Om = np.deg2rad(float(row["node"]))
    om = np.deg2rad(float(row["argperi"]))
    M = np.deg2rad(float(row["mean_anomaly"]))

    X_hel_ecl, V_hel_ecl = kepler_to_helio_ecliptic(a, e, inc, Om, om, M)
    X_hel = ecliptic_to_equatorial(X_hel_ecl)
    V_hel = ecliptic_to_equatorial(V_hel_ecl)

    return X_hel + sun_pos_au, V_hel + sun_vel_au_day


# ---------------------------------------------------------------------------
# ASSIST integration
# ---------------------------------------------------------------------------

def _open_ephem(planets_path: Optional[str], asteroids_path: Optional[str]):
    """Lazy import of assist + open Ephem. Caller is responsible for keeping
    the returned object alive for the duration of a benchmark/run.
    """
    import assist  # noqa: F401  (lazy)
    return assist.Ephem(planets_path=planets_path, asteroids_path=asteroids_path)


def _propagate_one(
    state_X_au, state_V_au_day, t_epoch_assist, t_targets_assist, ephem
):
    """Integrate one test particle with ASSIST through a sorted list of times.

    Parameters
    ----------
    state_X_au, state_V_au_day : (3,) ndarray
        Barycentric ICRF state at ``t_epoch_assist``.
    t_epoch_assist : float
        Initial time in JD-TDB-since-J2000 (i.e. MJD_TDB - 51544.5).
    t_targets_assist : (N,) ndarray
        Target times (same scale). Need not be sorted; we sort, integrate
        monotonically, and unsort on the way out.
    ephem : assist.Ephem
        Already-loaded ephemeris.

    Returns
    -------
    X : (3, N) ndarray, barycentric ICRF position [AU]
    V : (3, N) ndarray, barycentric ICRF velocity [AU/day]
    """
    import rebound
    import assist as _assist

    sim = rebound.Simulation()
    sim.t = float(t_epoch_assist)
    ax = _assist.Extras(sim, ephem)  # noqa: F841 (sim holds reference)
    sim.add(
        x=float(state_X_au[0]), y=float(state_X_au[1]), z=float(state_X_au[2]),
        vx=float(state_V_au_day[0]), vy=float(state_V_au_day[1]), vz=float(state_V_au_day[2]),
    )

    # Sort target times so we always integrate monotonically (forward or back).
    t_targets = np.asarray(t_targets_assist, dtype=np.float64)
    order = np.argsort(t_targets)
    t_sorted = t_targets[order]

    n = len(t_sorted)
    X_out = np.empty((3, n), dtype=np.float64)
    V_out = np.empty((3, n), dtype=np.float64)

    for i, t in enumerate(t_sorted):
        ax.integrate_or_interpolate(float(t))
        # Re-fetch every time: integrate_or_interpolate swaps the particle
        # array for an interpolated copy, so a cached sim.particles[0] would
        # keep reading the end-of-step state instead of the state at t.
        p = sim.particles[0]
        X_out[:, i] = (p.x, p.y, p.z)
        V_out[:, i] = (p.vx, p.vy, p.vz)

    # Restore original ordering
    inv = np.empty_like(order)
    inv[order] = np.arange(n)
    return X_out[:, inv], V_out[:, inv]


# ---------------------------------------------------------------------------
# Light-time correction (analytic 2nd-order Taylor)
# ---------------------------------------------------------------------------

def _light_time_correct(X_t, V_t, sun_pos_t, r_obs_t, n_iter: int = 3):
    """Compute the apparent observer→target vector with light-time correction.

    The astrometric observer-target vector is X(t - dt_lt) - r_obs(t), where
    dt_lt = |X(t - dt_lt) - r_obs(t)| / c. We Taylor-expand X about t to
    second order using the heliocentric Kepler acceleration; the Sun
    dominates over planetary perturbations on the ~500 s light-time scale to
    well below microarcsec.

    All inputs may be (3,) or (3, N).
    """
    X_hel = X_t - sun_pos_t
    r_hel = np.sqrt(np.sum(X_hel * X_hel, axis=0))
    a_t = -GM_SUN * X_hel / r_hel ** 3   # AU / day^2

    dt_lt = np.zeros_like(r_hel)
    for _ in range(n_iter):
        X_em = X_t - dt_lt * V_t + 0.5 * dt_lt ** 2 * a_t
        rho = X_em - r_obs_t
        dt_lt = np.sqrt(np.sum(rho * rho, axis=0)) / C_AU_PER_DAY
    X_em = X_t - dt_lt * V_t + 0.5 * dt_lt ** 2 * a_t
    return X_em - r_obs_t   # observer → target, light-time corrected


def _vector_to_radec(rho):
    """ICRF unit vector to RA, Dec in degrees. ``rho`` is shape (3,) or (3, N)."""
    r = np.sqrt(np.sum(rho * rho, axis=0))
    ra = np.degrees(np.arctan2(rho[1], rho[0])) % 360.0
    dec = np.degrees(np.arcsin(rho[2] / r))
    return ra, dec


# ---------------------------------------------------------------------------
# Public APIs
# ---------------------------------------------------------------------------

def compute_ephemerides_one(
    provID: str,
    ephTimes: Time,
    mpcorb: pd.DataFrame,
    ephem,
    observer_code: str = "X05",
    rate_dt_seconds: float = 60.0,
) -> EphResult:
    """ASSIST-based replacement for ``ssp.ephem._aux_compute_ephemerides``.

    Like the original it computes per-epoch quantities for *one* object, but
    using local mpcorb elements (no Horizons fetch). All times go through
    astropy so TAI/UTC/TDB are handled correctly: the asteroid epoch in
    mpcorb is treated as TT-MJD (MPC convention) and converted to TDB for
    integration; observation times come in as TAI-MJD and are converted to
    TDB.
    """
    row = (
        mpcorb.query(
            "unpacked_primary_provisional_designation == @provID",
            engine="python",
        ).iloc[0]
    )
    H = float(row["h"])
    G = float(row["g"])
    epoch_tt_mjd = float(row["epoch_mjd"])

    # Time scales --------------------------------------------------------
    # MPC element epochs are in TT (Terrestrial Time).
    epoch_tdb_mjd = Time(epoch_tt_mjd, format="mjd", scale="tt").tdb.mjd
    # Observation times come in as TAI MJD.
    t_tdb_mjd = ephTimes.tdb.mjd
    t_assist = t_tdb_mjd - MJD_J2000
    t0_assist = epoch_tdb_mjd - MJD_J2000

    # Initial state ------------------------------------------------------
    # ASSIST gives us the Sun's barycentric state at epoch directly.
    sun = ephem.get_particle("Sun", t0_assist)
    sun_pos_epoch = np.array([sun.x, sun.y, sun.z])
    sun_vel_epoch = np.array([sun.vx, sun.vy, sun.vz])

    X0_bary, V0_bary = elements_row_to_bary_icrf(row, sun_pos_epoch, sun_vel_epoch)

    # Propagate ----------------------------------------------------------
    X, V = _propagate_one(X0_bary, V0_bary, t0_assist, t_assist, ephem)

    # Sun barycentric position at each observation time (for light-time
    # acceleration term and for caller's helio* columns).
    sun_pos = np.empty((3, len(t_assist)))
    for k, t in enumerate(t_assist):
        s = ephem.get_particle("Sun", float(t))
        sun_pos[:, k] = (s.x, s.y, s.z)

    # Observer barycentric ICRF state at each obs time -------------------
    r_obs_q, v_obs_q = util.observatory_barycentric_posvel(observer_code, ephTimes)
    r_obs = r_obs_q.to(u.au).value          # (3, N)
    v_obs = v_obs_q.to(u.km / u.s).value    # (3, N)

    # Apparent astrometric ICRF positions --------------------------------
    rho = _light_time_correct(X, V, sun_pos, r_obs)
    ra_deg, dec_deg = _vector_to_radec(rho)

    # Rates of motion via central difference at +- dt/2 ------------------
    dt_day = rate_dt_seconds / 86400.0
    t2_assist = t_assist + dt_day
    X2, V2 = _propagate_one(X0_bary, V0_bary, t0_assist, t2_assist, ephem)
    sun_pos2 = np.empty_like(sun_pos)
    for k, t in enumerate(t2_assist):
        s = ephem.get_particle("Sun", float(t))
        sun_pos2[:, k] = (s.x, s.y, s.z)
    # Observer position at t + dt
    ephTimes2 = ephTimes + (rate_dt_seconds * u.s)
    r_obs2_q, _ = util.observatory_barycentric_posvel(observer_code, ephTimes2)
    r_obs2 = r_obs2_q.to(u.au).value
    rho2 = _light_time_correct(X2, V2, sun_pos2, r_obs2)
    ra2, dec2 = _vector_to_radec(rho2)

    # Wrap RA differences across 0/360.
    dra = ((ra2 - ra_deg + 540.0) % 360.0) - 180.0
    cos_dec = np.cos(np.deg2rad(dec_deg))
    mu_lon = (dra * cos_dec) / dt_day              # deg/day
    mu_lat = (dec2 - dec_deg) / dt_day             # deg/day
    # Great-circle separation between (ra1,dec1) and (ra2,dec2)
    s1 = SkyCoord(ra=ra_deg * u.deg, dec=dec_deg * u.deg, frame="icrs")
    s2 = SkyCoord(ra=ra2 * u.deg, dec=dec2 * u.deg, frame="icrs")
    mu_total = s1.separation(s2).to(u.deg).value / dt_day

    # Convert object velocity to km/s for caller compatibility.
    vv_km_s = (V * u.au / u.day).to(u.km / u.s).value

    return EphResult(
        ra_deg=ra_deg,
        dec_deg=dec_deg,
        xx=X,
        vv=vv_km_s,
        obs=r_obs,
        mu_lon=mu_lon,
        mu_lat=mu_lat,
        mu_total=mu_total,
        H=H,
        G=G,
    )


def compute_ephemerides_batch(
    schedule: dict,
    mpcorb: pd.DataFrame,
    planets_path: Optional[str] = None,
    asteroids_path: Optional[str] = None,
    observer_code: str = "X05",
    rate_dt_seconds: float = 60.0,
) -> dict:
    """Batched form. ``schedule`` maps provID → astropy.Time array (TAI MJD).

    Loads the ASSIST ephemeris exactly once and reuses it across every
    object. Returns ``{provID: EphResult}``.
    """
    ephem = _open_ephem(planets_path, asteroids_path)
    out = {}
    for provID, eph_times in schedule.items():
        out[provID] = compute_ephemerides_one(
            provID, eph_times, mpcorb, ephem,
            observer_code=observer_code,
            rate_dt_seconds=rate_dt_seconds,
        )
    return out
