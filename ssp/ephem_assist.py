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

ASSIST's own perturbers (Pluto and the 16 asteroids of sb441-n16; see
``self_perturber``) can't be integrated as test particles: the particle
sits on its own point mass and is slung away. They're detected from the
state at epoch. The 16 asteroids are integrated with ASSIST's asteroid
forces off (as JPL leaves the target out of its perturbers; ~1-3 km per
year of force-model error); Pluto's state is taken from the planet
ephemeris instead (DE440's Pluto-system barycentre, ~0.06" from its MPC
orbit). See docs/design/nearbysso.md, "ASSIST's own perturbers".

The integrator runs IAS15 with ``adaptive_mode = 2`` at ``epsilon = 1e-11``
(``PRECISE_ADAPTIVE_MODE``, ``PRECISE_EPSILON``): within 1 mas of a converged
reference everywhere measured, and up to ~2,800x faster for close-approach
NEOs than ASSIST's default step control.
"""

from __future__ import annotations

import os
from collections import namedtuple
from typing import Optional

import numpy as np
import pandas as pd
from astropy.time import Time
import astropy.units as u

from . import nongrav as _nongrav
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

# IAS15 step control for _propagate_one (set after attaching ASSIST, which
# resets it). Measured 2026-09-28 on 250 orbits over a year of candidate
# times, against mode 2 at epsilon 1e-15: ASSIST's default control is off by
# up to 7.3 mas in deep NEO encounters (and takes up to 3 M steps, ~130 s);
# plain mode 2 (epsilon 1e-9) by up to 120 mas on 0.01-0.03 AU passes; mode 2
# at 1e-11 by at most 0.93 mas, at 50-550 steps. See docs/design/nearbysso.md,
# "Precise-pass step control".
PRECISE_ADAPTIVE_MODE = 2
PRECISE_EPSILON = 1e-11

# ASSIST body id of the Sun. Passing the integer to Ephem.get_particle
# skips its per-call name lookup, which dominated its cost.
ASSIST_SUN = 0

# ASSIST's own perturbers that are also small bodies with MPC orbits: Pluto
# (10, from the planet file: DE440's Pluto-system barycentre, the point
# MPC's orbit refers to) and the 16 sb441-n16 asteroids (11-26).
ASSIST_PLUTO = 10
ASSIST_PERTURBER_IDS = tuple(range(10, 27))

# A state within both of these of one of those bodies at epoch is that
# body: the MPC states are 11-112 km (Pluto 1,533 km) and 0.001-0.0045 m/s
# from theirs; the nearest other orbit in the 2026-09-26 snapshot is
# 876,000 km and 5.6 km/s from one. The velocity is what separates them (a
# false Pluto would get Pluto's positions).
SELF_PERTURBER_MAX_AU = 1e-4                               # ~15,000 km
SELF_PERTURBER_MAX_AU_DAY = 1e-3 * 86400.0 / 149597870.7   # 1 m/s


EphResult = namedtuple(
    "EphResult",
    [
        # Per-epoch arrays, all shape (N,) unless noted.
        "ra_deg",        # astrometric ICRF, light-time corrected
        "dec_deg",
        "xx",            # (3, N) geometric barycentric ICRF position at observation time [AU]
        "vv",            # (3, N) geometric barycentric ICRF velocity at observation time [km/s]
        "obs",           # (3, N) observer barycentric ICRF position [AU]
        "mu_lon",        # cos(dec)·dRA/dt   [deg/day]
        "mu_lat",        # dDec/dt           [deg/day]
        "mu_total",      # great-circle rate [deg/day]
        "H",             # absolute mag (scalar)
        "G",             # slope param  (scalar)
        # Light-emission-time geometry, following the JPL Horizons observer
        # table conventions (quantities 19, 20, 24): the object is evaluated
        # at emission time t - tau; the topocentric vector is relative to the
        # observer at observation time t (Horizons "delta"); the heliocentric
        # vector is relative to the *apparent* Sun, i.e. the Sun at the time
        # the light reflected at emission left it (Horizons "r", S-T-O).
        "helio_pos",     # (3, N) object - apparent Sun [AU]
        "helio_vel",     # (3, N) [km/s]
        "topo_pos",      # (3, N) object - observer [AU]; points at (ra_deg, dec_deg)
        "topo_vel",      # (3, N) [km/s]
        "light_time",    # (N,) down-leg light time tau [day]
        "phase_angle",   # (N,) Sun-target-observer phase angle [deg], Horizons "phi"
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
    # Initial guess (Danby 1988), converges even for e -> 1 at small M
    E = M + 0.85 * e * np.sign(np.sin(M))
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

    R = _perifocal_to_ecliptic(inc_rad, Omega_rad, omega_rad)
    pos_pf = np.array([x_pf, y_pf, 0.0])
    vel_pf = np.array([vx_pf, vy_pf, 0.0])

    return R @ pos_pf, R @ vel_pf


def _perifocal_to_ecliptic(inc_rad: float, Omega_rad: float, omega_rad: float) -> np.ndarray:
    """Rotation perifocal (P toward perihelion) → J2000 ecliptic:
    R3(-Omega) R1(-i) R3(-omega)."""
    cosO, sinO = np.cos(Omega_rad), np.sin(Omega_rad)
    cosw, sinw = np.cos(omega_rad), np.sin(omega_rad)
    cosi, sini = np.cos(inc_rad), np.sin(inc_rad)

    # Direct multiplication of the three rotations
    return np.array([
        [cosO * cosw - sinO * sinw * cosi, -cosO * sinw - sinO * cosw * cosi,  sinO * sini],
        [sinO * cosw + cosO * sinw * cosi, -sinO * sinw + cosO * cosw * cosi, -cosO * sini],
        [sinw * sini,                       cosw * sini,                       cosi       ],
    ])


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


def solve_kepler_hyperbolic(M: np.ndarray, e: float, tol: float = 1e-14, max_iter: int = 100) -> np.ndarray:
    """Solve the hyperbolic Kepler equation e sinh H - H = M for e > 1.

    Newton from arcsinh(M / e). That start fails for e -> 1 at small |M|
    (e cosh H - 1 ~ e - 1, so the first step overflows): then it restarts
    from sign(M) ln(2 |M| / e + 1.8) (Danby's start, from which Newton
    converges monotonically), and returns NaN if that fails too. Results
    that converge from the first start are unchanged (the same fallback as
    ssp.nearbysso.orbits._solve_kepler_hyperbolic_vec)."""
    M = np.atleast_1d(np.asarray(M, dtype=np.float64))
    H, ok = _newton_hyperbolic(np.arcsinh(M / e), M, e, tol, max_iter)
    if not ok:
        H, ok = _newton_hyperbolic(np.sign(M) * np.log(2.0 * np.abs(M) / e + 1.8), M, e, tol, max_iter)
        if not ok:
            H = np.full_like(M, np.nan)
    return H


def _newton_hyperbolic(H, M, e, tol, max_iter):
    """Newton iterations for solve_kepler_hyperbolic: (H, converged)."""
    with np.errstate(over="ignore", invalid="ignore"):
        for _ in range(max_iter):
            f = e * np.sinh(H) - H - M
            fp = e * np.cosh(H) - 1.0
            dH = -f / fp
            H = H + dH
            if np.all(np.abs(dH) < tol * np.maximum(1.0, np.abs(H))):
                return H, True
        # (steps between tol and 1e-10 are rounding cycles: converged)
        return H, bool(np.all(np.abs(dH) < 1e-10 * np.maximum(1.0, np.abs(H))))


def cometary_to_helio_ecliptic(
    q: float, e: float, inc_rad: float,
    Omega_rad: float, omega_rad: float, dt_peri_days: float,
    mu: float = GM_SUN,
) -> tuple[np.ndarray, np.ndarray]:
    """Heliocentric J2000 *ecliptic* state from cometary elements.

    Uses (q, e, time since perihelion), the canonical MPC/Horizons element
    set, so it works for every orbit with e != 1 (elliptic and hyperbolic).
    Returns (X [AU], V [AU/day]), both shape (3,).
    """
    if abs(1.0 - e) < 1e-10:
        raise ValueError(f"parabolic orbit (e={e!r}) not supported")
    a = q / (1.0 - e)               # negative for hyperbolic orbits
    n = np.sqrt(mu / abs(a) ** 3)   # mean motion [rad/day]
    M = n * dt_peri_days
    if e < 1.0:
        E = solve_kepler(np.array([M]), e)[0]
        cosE, sinE = np.cos(E), np.sin(E)
        Edot = n / (1.0 - e * cosE)
        b = a * np.sqrt(1.0 - e * e)
        pos_pf = np.array([a * (cosE - e), b * sinE, 0.0])
        vel_pf = np.array([-a * sinE * Edot, b * cosE * Edot, 0.0])
    else:
        H = solve_kepler_hyperbolic(np.array([M]), e)[0]
        coshH, sinhH = np.cosh(H), np.sinh(H)
        Hdot = n / (e * coshH - 1.0)
        aa = -a
        b = aa * np.sqrt(e * e - 1.0)
        pos_pf = np.array([aa * (e - coshH), b * sinhH, 0.0])
        vel_pf = np.array([-aa * sinhH * Hdot, b * coshH * Hdot, 0.0])

    R = _perifocal_to_ecliptic(inc_rad, Omega_rad, omega_rad)
    return R @ pos_pf, R @ vel_pf


def elements_row_to_bary_icrf(row, sun_pos_au, sun_vel_au_day):
    """Take one mpcorb row (heliocentric ecliptic osculating elements) plus
    the Sun's barycentric ICRF state at the same epoch. Return barycentric
    ICRF (X, V).

    Uses the canonical cometary elements (q, e, i, node, argperi,
    peri_time), which are what the MPC orbit JSON (`COM` block) stores. The
    derived `a` and `mean_anomaly` columns of mpc_orbits are NaN for ~54% of
    objects and inconsistent with (q, e, peri_time) for tens of thousands
    more, so they are not used. `peri_time` and `epoch_mjd` are both TT MJD.
    """
    q = float(row["q"])
    e = float(row["e"])
    inc = np.deg2rad(float(row["i"]))
    Om = np.deg2rad(float(row["node"]))
    om = np.deg2rad(float(row["argperi"]))
    dt_peri = float(row["epoch_mjd"]) - float(row["peri_time"])

    X_hel_ecl, V_hel_ecl = cometary_to_helio_ecliptic(q, e, inc, Om, om, dt_peri)
    X_hel = ecliptic_to_equatorial(X_hel_ecl)
    V_hel = ecliptic_to_equatorial(V_hel_ecl)

    return X_hel + sun_pos_au, V_hel + sun_vel_au_day


# ---------------------------------------------------------------------------
# ASSIST integration
# ---------------------------------------------------------------------------

def open_ephem(planets_path: Optional[str] = None, asteroids_path: Optional[str] = None):
    """Lazy import of assist + open Ephem. Caller is responsible for keeping
    the returned object alive for the duration of a benchmark/run.

    Paths default to the ``SSP_ASSIST_PLANETS`` and ``SSP_ASSIST_ASTEROIDS``
    environment variables (the JPL DE440/441 planet file and the ASSIST
    sb441-n16 asteroid file).
    """
    import assist  # noqa: F401  (lazy)
    planets_path = planets_path or os.environ.get("SSP_ASSIST_PLANETS")
    asteroids_path = asteroids_path or os.environ.get("SSP_ASSIST_ASTEROIDS")
    return assist.Ephem(planets_path=planets_path, asteroids_path=asteroids_path)


_open_ephem = open_ephem


# The last self_perturber answer: the coarse and the precise pass of one
# orbit ask about the same state (per process, so safe under fork).
_SELF_PERTURBER_CACHE = {"ephem": None, "key": None, "body": None}


def _body_velocity(body, t, ephem, p=None):
    """Barycentric velocity [AU/day] of ASSIST body ``body`` at t: the
    ephemeris's, or (NaN for the sb441 asteroids) a central difference of
    positions, h = 0.01 d."""
    p = p if p is not None else ephem.get_particle(body, t)
    v = np.array([p.vx, p.vy, p.vz])
    if not np.all(np.isfinite(v)):
        a = ephem.get_particle(body, t + 0.01)
        c = ephem.get_particle(body, t - 0.01)
        v = (np.array([a.x, a.y, a.z]) - np.array([c.x, c.y, c.z])) / 0.02
    return v


def self_perturber(state_X_au, state_V_au_day, t_epoch_assist, ephem):
    """The ASSIST body id (10-26) whose own state a barycentric state at
    epoch is (within SELF_PERTURBER_MAX_AU and SELF_PERTURBER_MAX_AU_DAY),
    or None for an ordinary orbit.

    Integrated with all of ASSIST's forces, such an object feels its own
    point mass from tens of km away. See the module docstring for what the
    propagators do with them. Costs a few us: the perturbers' positions are
    cached per epoch (17 ephemeris lookups for a new one), and the last
    answer is cached.
    """
    X = np.asarray(state_X_au, dtype=np.float64).reshape(-1)[:3]
    V = np.asarray(state_V_au_day, dtype=np.float64).reshape(-1)[:3]
    t = float(t_epoch_assist)
    key = (t, X.tobytes(), V.tobytes())
    c = _SELF_PERTURBER_CACHE
    if c["ephem"] is ephem and c["key"] == key:
        return c["body"]
    body = None
    if np.all(np.isfinite(X)) and np.all(np.isfinite(V)) and np.isfinite(t):
        B = _perturber_positions(t, ephem)
        if B is not None:
            dr = B - X
            k = int(np.argmin(np.einsum("ij,ij->i", dr, dr)))
            if dr[k] @ dr[k] < SELF_PERTURBER_MAX_AU ** 2:
                b = ASSIST_PERTURBER_IDS[k]
                dv = V - _body_velocity(b, t, ephem)
                if dv @ dv < SELF_PERTURBER_MAX_AU_DAY ** 2:
                    body = b
    c.update(ephem=ephem, key=key, body=body)
    return body


# The perturbers' positions at recent epochs (MPC epochs are shared by
# many orbits, so this makes self_perturber a few microseconds).
_PERTURBER_POS_CACHE = {"ephem": None, "pos": {}}
_PERTURBER_POS_CACHE_MAX = 4096


def _perturber_positions(t, ephem):
    """(17, 3) positions of ASSIST_PERTURBER_IDS at t, or None outside the
    ephemeris; cached per epoch."""
    c = _PERTURBER_POS_CACHE
    if c["ephem"] is not ephem:
        c.update(ephem=ephem, pos={})
    pos = c["pos"]
    if t not in pos:
        try:
            ps = [ephem.get_particle(b, t) for b in ASSIST_PERTURBER_IDS]
            pos[t] = np.array([(p.x, p.y, p.z) for p in ps])
        except Exception:            # outside the ephemeris
            pos[t] = None
        if len(pos) > _PERTURBER_POS_CACHE_MAX:
            pos.pop(next(iter(pos)))
    return pos[t]


def forces_without_self(ax, body):
    """Set an ASSIST Extras' forces for integrating perturber ``body``
    (from self_perturber): asteroid perturbers off for bodies 11-26, the
    planets (Pluto among them) off for Pluto. No-op for None.

    (For Pluto this is only good for a plain integration: ASSIST's
    variational equations keep the planets' tidal terms even with PLANETS
    off, and blow up 1,500 km from Pluto.)"""
    if body is None:
        return
    drop = "PLANETS" if body == ASSIST_PLUTO else "ASTEROIDS"
    ax.forces = [f for f in ax.forces if f != drop]


def ephemeris_states(body, t_assist, ephem):
    """Barycentric ICRF (X, V) of ASSIST body ``body`` at times t_assist,
    each (3, N) [AU, AU/day]; velocities as in _body_velocity."""
    t = np.atleast_1d(np.asarray(t_assist, dtype=np.float64))
    X = np.empty((3, len(t)))
    V = np.empty((3, len(t)))
    for k, tk in enumerate(t.tolist()):
        p = ephem.get_particle(body, tk)
        X[:, k] = (p.x, p.y, p.z)
        V[:, k] = _body_velocity(body, tk, ephem, p)
    return X, V


_DETECT = object()


def _propagate_one(
    state_X_au, state_V_au_day, t_epoch_assist, t_targets_assist, ephem,
    perturber=_DETECT, integrate_pluto=False, nongrav=_nongrav.NONE,
):
    """Integrate one test particle with ASSIST through a sorted list of times.

    ASSIST's own perturbers (``self_perturber``; pass ``perturber`` to
    skip the detection) are handled as the module docstring says: bodies
    11-26 are integrated without asteroid forces, and Pluto's states come
    from the planet ephemeris (or, with ``integrate_pluto``, from an
    integration without the planets, e.g. to difference for Phi).

    ``nongrav`` (an ``ssp.nongrav.NonGrav``; ``ssp.nongrav.NONE``, the default,
    or None for gravity only) sets the non-gravitational acceleration with
    ``ssp.nongrav.apply`` right after ASSIST is attached (its
    NON_GRAVITATIONAL force is on by default and does nothing without
    particle_params). Gravity-only orbits never call it, so they integrate
    bitwise as before. The self-perturber paths ignore it (none of those
    bodies has non-gravitational parameters).

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

    if perturber is _DETECT:
        perturber = self_perturber(state_X_au, state_V_au_day, t_epoch_assist, ephem)
    if perturber == ASSIST_PLUTO and not integrate_pluto:
        # DE440's body 10 is the Pluto-system barycentre, which is what MPC's
        # 1930 BM orbit refers to: Horizons (2026-09-28, JD_TDB 2461200.5)
        # puts target 9 (the barycentre) 1,534 km and 0.002 m/s from the MPC
        # state, and 999 (Pluto itself) 2,334 km and 24.3 m/s from it.
        return ephemeris_states(ASSIST_PLUTO, t_targets_assist, ephem)

    sim = rebound.Simulation()
    sim.t = float(t_epoch_assist)
    ax = _assist.Extras(sim, ephem)  # noqa: F841 (sim holds reference)
    forces_without_self(ax, perturber)
    if perturber is None and nongrav is not None and nongrav.model:
        _nongrav.apply(ax, nongrav)
    # after attaching: ASSIST resets the step control (see PRECISE_EPSILON)
    sim.ri_ias15.adaptive_mode = PRECISE_ADAPTIVE_MODE
    sim.ri_ias15.epsilon = PRECISE_EPSILON
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

def _emission_state(X_t, V_t, sun_pos_t, r_obs_t, n_iter: int = 3):
    """Object state at light-emission time for an observer at ``r_obs_t``.

    The emission time is t - tau with tau = |X(t - tau) - r_obs(t)| / c. We
    Taylor-expand the state about t using the heliocentric Kepler
    acceleration: X(t - tau) = X - tau V + tau^2 a / 2 and
    V(t - tau) = V - tau a. The neglected terms (jerk, planetary
    accelerations) are < 1e-12 AU even for NEOs at lunar distance and
    TNOs with multi-hour light times.

    All inputs may be (3,) or (3, N). Returns (X_em, V_em, tau) with tau in
    days.
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
    V_em = V_t - dt_lt * a_t
    return X_em, V_em, dt_lt


def _light_time_correct(X_t, V_t, sun_pos_t, r_obs_t, n_iter: int = 3):
    """Compute the apparent observer→target vector with light-time correction.

    The astrometric observer-target vector is X(t - tau) - r_obs(t); see
    `_emission_state`. All inputs may be (3,) or (3, N).
    """
    X_em, _, _ = _emission_state(X_t, V_t, sun_pos_t, r_obs_t, n_iter)
    return X_em - r_obs_t   # observer → target, light-time corrected


def _apparent_sun(X_em, t_em_assist, ephem, n_iter: int = 3):
    """Barycentric Sun state as seen from the object at emission time.

    Solves t_refl = t_em - |X_em - X_sun(t_refl)| / c per epoch, i.e. the Sun
    at the time the light reflected by the object at ``t_em`` left the Sun
    (Horizons' "apparent" Sun for r, rdot and S-T-O). Returns
    (pos [AU], vel [AU/day]), each (3, N).
    """
    t_em = np.atleast_1d(np.asarray(t_em_assist, dtype=np.float64))
    pos = np.empty((3, len(t_em)))
    vel = np.empty((3, len(t_em)))
    for k, t in enumerate(t_em):
        t_refl = t
        for _ in range(n_iter):
            s = ephem.get_particle(ASSIST_SUN, float(t_refl))
            d = np.array([X_em[0, k] - s.x, X_em[1, k] - s.y, X_em[2, k] - s.z])
            t_refl = t - np.sqrt(d @ d) / C_AU_PER_DAY
        s = ephem.get_particle(ASSIST_SUN, float(t_refl))
        pos[:, k] = (s.x, s.y, s.z)
        vel[:, k] = (s.vx, s.vy, s.vz)
    return pos, vel


def _phase_angle_deg(helio_pos, topo_pos, helio_vel_au_day):
    """Phase angle at the target between the Sun and the observer [deg].

    Matches JPL Horizons' true phase angle "phi" (observer quantity 43): the
    direction to the observer is the astrometric one, and the direction to
    the Sun is where sunlight arrives from in the target's rest frame, i.e.
    aberrated (first order in v/c) by the target's heliocentric velocity.
    This differs from the purely geometric angle by up to v/c (~20").
    Verified against Horizons to Horizons' printed precision (0.2").
    """
    u_sun = -helio_pos / np.linalg.norm(helio_pos, axis=0)
    u_obs = -topo_pos / np.linalg.norm(topo_pos, axis=0)
    beta = helio_vel_au_day / C_AU_PER_DAY
    u_sun = u_sun + beta - np.sum(u_sun * beta, axis=0) * u_sun
    u_sun = u_sun / np.linalg.norm(u_sun, axis=0)
    cosph = np.clip(np.sum(u_sun * u_obs, axis=0), -1.0, 1.0)
    return np.degrees(np.arccos(cosph))


def _sky_rates(rho, V_em, V_obs, ltrate: bool = True):
    """On-sky rates of the astrometric position rho = X(t - tau) - O(t).

    d(rho)/dt = V_em (1 - dtau/dt) - V_obs, with dtau/dt = d|rho|/dt / c
    (``ltrate=False`` drops that factor, a ~1e-4 relative effect). The
    rate of the unit vector u is (rho' - (u.rho') u) / |rho|, projected on
    the local east (RA) and north (Dec) directions. Velocities in AU/day.

    Returns (mu_lon, mu_lat, mu_total) in deg/day, where mu_lon includes
    the cos(dec) factor.
    """
    d = np.sqrt(np.sum(rho * rho, axis=0))
    u_ = rho / d
    rel = V_em - V_obs
    if ltrate:
        # |rho|' (1 + u.V_em / c) = u.(V_em - V_obs)
        ddot = np.sum(u_ * rel, axis=0) / (1.0 + np.sum(u_ * V_em, axis=0) / C_AU_PER_DAY)
        rhodot = V_em * (1.0 - ddot / C_AU_PER_DAY) - V_obs
    else:
        rhodot = rel
    udot = (rhodot - np.sum(u_ * rhodot, axis=0) * u_) / d   # rad/day
    ra = np.arctan2(u_[1], u_[0])
    dec = np.arcsin(np.clip(u_[2], -1.0, 1.0))
    e_ra = np.array([-np.sin(ra), np.cos(ra), np.zeros_like(ra)])
    e_dec = np.array([-np.sin(dec) * np.cos(ra), -np.sin(dec) * np.sin(ra), np.cos(dec)])
    mu_lon = np.degrees(np.sum(udot * e_ra, axis=0))
    mu_lat = np.degrees(np.sum(udot * e_dec, axis=0))
    mu_total = np.degrees(np.sqrt(np.sum(udot * udot, axis=0)))
    return mu_lon, mu_lat, mu_total


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
    row=None,
    obs_pos: Optional[np.ndarray] = None,
    obs_vel: Optional[np.ndarray] = None,
    nongrav=_nongrav.NONE,
) -> EphResult:
    """Per-epoch ephemeris quantities for *one* object from its local mpcorb
    elements (no Horizons fetch), propagated with ASSIST.

    All times go through astropy so TAI/UTC/TDB are handled correctly: the
    asteroid epoch in mpcorb is treated as TT-MJD (MPC convention) and
    converted to TDB for integration; observation times come in as TAI-MJD
    and are converted to TDB.

    Pass the object's mpcorb row as ``row`` when calling this for many
    objects: otherwise it is looked up in ``mpcorb`` by ``provID`` with a
    full-table scan (~30 ms on the full 1.5M-row table), and ``mpcorb`` may
    be None when ``row`` is given.

    Likewise, ``obs_pos`` [AU] and ``obs_vel`` [km/s], each (3, N), can
    supply the observer's barycentric ICRF state at ``ephTimes``, e.g.
    sliced from one vectorized util.observatory_barycentric_posvel call over
    all observations; that call has a large fixed cost per invocation.

    ``nongrav`` (an ``ssp.nongrav.NonGrav``, e.g. from
    ``ssp.nongrav.nongrav_params`` of the object's mpc_orb_jsonb) adds its
    non-gravitational acceleration to the integration; None or
    ``ssp.nongrav.NONE`` integrates gravity only (see _propagate_one).
    """
    if row is None:
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
    sun = ephem.get_particle(ASSIST_SUN, t0_assist)
    sun_pos_epoch = np.array([sun.x, sun.y, sun.z])
    sun_vel_epoch = np.array([sun.vx, sun.vy, sun.vz])

    X0_bary, V0_bary = elements_row_to_bary_icrf(row, sun_pos_epoch, sun_vel_epoch)

    # Propagate ----------------------------------------------------------
    X, V = _propagate_one(X0_bary, V0_bary, t0_assist, t_assist, ephem, nongrav=nongrav)

    # Sun barycentric position at each observation time (for light-time
    # acceleration term and for caller's helio* columns).
    sun_pos = np.empty((3, len(t_assist)))
    for k, t in enumerate(t_assist):
        s = ephem.get_particle(ASSIST_SUN, float(t))
        sun_pos[:, k] = (s.x, s.y, s.z)

    # Observer barycentric ICRF state at each obs time -------------------
    if obs_pos is None or obs_vel is None:
        r_obs_q, v_obs_q = util.observatory_barycentric_posvel(observer_code, ephTimes)
        obs_pos = r_obs_q.to(u.au).value          # (3, N)
        obs_vel = v_obs_q.to(u.km / u.s).value    # (3, N)
    r_obs = np.asarray(obs_pos, dtype=np.float64)
    v_obs = np.asarray(obs_vel, dtype=np.float64)

    # Light-emission-time state and apparent astrometric ICRF positions ---
    X_em, V_em, tau = _emission_state(X, V, sun_pos, r_obs)
    rho = X_em - r_obs
    ra_deg, dec_deg = _vector_to_radec(rho)

    # Heliocentric/topocentric geometry at emission time (Horizons r/delta
    # conventions; see EphResult).
    sun_app_pos, sun_app_vel = _apparent_sun(X_em, t_assist - tau, ephem)
    au_day_to_km_s = (1.0 * u.au / u.day).to_value(u.km / u.s)
    helio_pos = X_em - sun_app_pos
    helio_vel = (V_em - sun_app_vel) * au_day_to_km_s
    topo_pos = rho
    topo_vel = V_em * au_day_to_km_s - v_obs
    phase_angle = _phase_angle_deg(helio_pos, topo_pos, V_em - sun_app_vel)

    # Rates of motion of the astrometric position, analytically --------
    mu_lon, mu_lat, mu_total = _sky_rates(rho, V_em, v_obs / au_day_to_km_s)

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
        helio_pos=helio_pos,
        helio_vel=helio_vel,
        topo_pos=topo_pos,
        topo_vel=topo_vel,
        light_time=tau,
        phase_angle=phase_angle,
    )


def compute_ephemerides_batch(
    schedule: dict,
    mpcorb: pd.DataFrame,
    planets_path: Optional[str] = None,
    asteroids_path: Optional[str] = None,
    observer_code: str = "X05",
    nongravs: Optional[dict] = None,
) -> dict:
    """Batched form. ``schedule`` maps provID → astropy.Time array (TAI MJD).
    ``nongravs`` optionally maps provIDs to their ``ssp.nongrav.NonGrav``
    (the others integrate gravity only).

    Loads the ASSIST ephemeris exactly once and reuses it across every
    object. Returns ``{provID: EphResult}``.
    """
    ephem = open_ephem(planets_path, asteroids_path)
    out = {}
    for provID, eph_times in schedule.items():
        out[provID] = compute_ephemerides_one(
            provID, eph_times, mpcorb, ephem,
            observer_code=observer_code,
            nongrav=_nongrav.NONE if nongravs is None else nongravs.get(provID, _nongrav.NONE),
        )
    return out
