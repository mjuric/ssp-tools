"""WP2: per-orbit ASSIST propagation with variational equations, and the
on-sky error ellipse. See ``_contract.CoarseTrack``.

Geometry and conventions (see also docs/design/nearbysso.md, "Uncertainty
propagation"):

- **Positions** are topocentric and *geometric*: the object's barycentric
  position at t minus the observer's at t, with no light-time correction.
  The precise pass computes the published, light-time-corrected values; the
  two directions differ by tau |V_perp| / Delta = |V_perp| / c, the
  object's (barycentric, transverse) motion during the light time: up to
  ~20" (tested to 1e-3 of that).
- **Rates** are the instantaneous rates of that geometric direction, from
  the object's topocentric velocity V(t) - v_obs(t). The observer velocity
  is not an input, so it is reconstructed as Earth's barycentric velocity
  (from the ASSIST ephemeris) plus the diurnal rotation of the site,
  omega x (obs_pos - r_earth), with omega along the ICRF z axis (the ~0.3
  deg precession tilt of the pole since J2000 changes it by < 1%, i.e.
  < 4 m/s). That is the true observer velocity to a few m/s, so the rates
  match the precise pass's to (v/c) |d rho/dt| / Delta + |a| / c (its
  light-time terms; ~1e-4 of the total angular speed, plus ~1e-4 deg/day).
  It is better than a finite difference of the nightly ``obs_pos``, which
  aliases the diurnal motion (samples fall at different times of night)
  with ~0.4 km/s errors, and than ignoring the observer's motion, which is
  wrong by the Earth's 30 km/s. If ``obs_pos`` is not within 1e-4 AU of the
  geocentre (i.e. not a site on the Earth), the rotation term is dropped.
  Note that the rates are exact but *instantaneous*: the topocentric track
  curves within a night, mostly through the diurnal parallax (amplitude
  R_earth / Delta), so a linear extrapolation over +-6 h is off by ~1-3"
  for the main belt and by up to degrees for NEOs within 0.01 AU.
- **The ellipse** projects the position block of C(t) = Phi C0 Phi^T onto
  the tangent plane at the topocentric direction u: with the local east and
  north unit vectors e_ra and e_dec, the Jacobian of (RA cos Dec, Dec) with
  respect to the object's barycentric position (observer fixed) is
  J = [e_ra; e_dec] / |rho|, and Sigma_sky = J C_pos J^T. Then
  ra_err = sqrt(Sigma_00), dec_err = sqrt(Sigma_11) [deg],
  ra_dec_cov = Sigma_01 [deg^2], and sigma_major = sqrt(largest eigenvalue)
  [arcsec].
- **Failures** (a non-finite state, a REBOUND/ASSIST error status or
  exception, e.g. outside the ephemeris range) mark the affected samples
  ``ok = False`` with NaN positions, rates and ellipses and an infinite
  sigma_major; coarse() never raises for one bad orbit. Once an integration
  fails, every later sample in that direction of time is marked failed.
  So do samples with a non-finite time or observer position. Orbits with
  an osculating perihelion below 0.02 AU at epoch aren't integrated at all
  (a plunge into the Sun would hang IAS15), and a step cap stops any other
  runaway integration (see _MAX_STEPS_BASE); ``STEP_CAP_STOPS`` counts
  those stops, for the run report.
- **ASSIST's own perturbers** (ephem_assist.self_perturber: Pluto and the
  16 sb441-n16 asteroids, which would sit on their own point mass): their
  states are exactly the precise pass's (ephem_assist._propagate_one: the
  16 asteroids integrated with ASSIST's asteroid forces off, Pluto from the
  planet ephemeris), and Phi comes from central differences of plain
  integrations with that force group off. (Not from variational
  particles: ASSIST's variational equations keep the perturbers' tidal
  terms even with their force group off, and blow up next to the body.)
- **Non-PSD sky covariances** (e.g. from a non-PSD cov0) are treated like
  missing ones: NaN errors, infinite sigma_major.
- **Orbits with has_cov False** get NaN ellipses and sigma_major = inf, but
  positions and rates as usual.
"""

from __future__ import annotations

import ctypes

import numpy as np

from .. import ephem_assist as ea
from ._contract import CoarseTrack

# ASSIST body id of the Earth (geocentre).
_ASSIST_EARTH = 3

# Earth's rotation rate [rad/day] (sidereal day = 0.99726958 d).
_OMEGA_EARTH = 2.0 * np.pi / 0.99726958

# obs_pos within this of the geocentre [AU] (~15000 km) is taken as a site
# on the Earth, and gets the diurnal rotation velocity.
_EARTH_SITE_MAX_AU = 1e-4

_RAD2DEG = 180.0 / np.pi

# Orbits whose osculating heliocentric perihelion distance at epoch is
# below this [AU] are not integrated (all samples ok=False): a plunge into
# the Sun drives IAS15's step to zero and would hang the integration.
_Q_MIN_AU = 0.02

# Backstop: a simulation that takes more steps than
# _MAX_STEPS_BASE + _MAX_STEPS_PER_YEAR * (years integrated) is stopped,
# and its remaining samples fail. Real orbits take ~20-65 steps per year
# (measured: at most 91 over 1.4 years, on 205 orbits including NEOs
# through 0.006 AU approaches); a step costs 0.1-0.7 ms (the most near the
# Sun), so this bounds a runaway orbit to ~1 s per year integrated. A step
# count (not wall time) keeps the output deterministic.
_MAX_STEPS_BASE = 1000
_MAX_STEPS_PER_YEAR = 300

#: Number of simulations stopped by the step cap in this process, for the
#: run report. Read it, and reset it with ``propagate.STEP_CAP_STOPS = 0``;
#: in forked workers each process counts its own, so a worker must return
#: its count to the parent.
STEP_CAP_STOPS = 0

# Relative tolerance of the PSD test of a sky covariance (see _ellipse).
_PSD_EPS = 1e-10


def _integrate(state0, epoch, t_seq, ephem, X, Phi, ok, idx):
    """One ASSIST simulation with six variational particles, from ``epoch``
    through ``t_seq`` (monotonic, in the direction away from epoch), which
    are the times ``t[idx]``. Fills ``X[idx]`` (6-vector states),
    ``Phi[idx]`` and ``ok[idx]``, up to the first failure."""
    import assist
    import rebound

    sim = rebound.Simulation()
    sim.t = float(epoch)
    sim.add(x=float(state0[0]), y=float(state0[1]), z=float(state0[2]),
            vx=float(state0[3]), vy=float(state0[4]), vz=float(state0[5]))
    # testparticle=0: the variations of particle 0 alone. REBOUND's default
    # (-1) makes ASSIST skip the variational accelerations, and Phi comes
    # out as the free-motion [[I, tI], [0, I]] with no error.
    for j in range(6):
        var = sim.add_variation(testparticle=0)
        vp = var.particles[0]
        vp.x = vp.y = vp.z = vp.vx = vp.vy = vp.vz = 0.0
        setattr(vp, ("x", "y", "z", "vx", "vy", "vz")[j], 1.0)
    ax = assist.Extras(sim, ephem)
    sim.ri_ias15.adaptive_mode = 2   # after attaching: ASSIST resets it

    years = abs(float(t_seq[-1]) - float(epoch)) / 365.25 if len(t_seq) else 0.0
    max_steps = int(_MAX_STEPS_BASE + _MAX_STEPS_PER_YEAR * years)

    capped = []

    def heartbeat(simp):             # the backstop; see _MAX_STEPS_BASE
        if simp.contents.steps_done > max_steps:
            if not capped:
                capped.append(True)
            simp.contents.stop()
    sim.heartbeat = heartbeat

    n = sim.N            # 7: the particle, then its six variations
    stride = ctypes.sizeof(rebound.Particle) // 8
    buf_t = ctypes.c_double * (n * stride)
    S = np.empty((len(idx), n, 6))
    m = 0                # samples done
    for t in t_seq:
        try:
            ax.integrate_or_interpolate(float(t))
        except Exception:   # one bad orbit must not stop a run
            break
        if sim._status > 0:  # REB_STATUS_GENERIC_ERROR, stopped, ...
            break
        # Read the particle array afresh each time: integrate_or_interpolate
        # swaps in an interpolated copy (see ephem_assist._propagate_one).
        # A Particle starts with x, y, z, vx, vy, vz; rows are particle 0,
        # then the six variational particles in the order they were added.
        # (serialize_particle_data would be simpler, but only copies the
        # real particles.)
        addr = ctypes.addressof(sim._particles.contents)
        S[m] = np.frombuffer(buf_t.from_address(addr)).reshape(n, stride)[:, :6]
        m += 1
    if capped:
        global STEP_CAP_STOPS
        STEP_CAP_STOPS += 1
    # Everything from the first non-finite sample on has failed.
    bad = ~np.all(np.isfinite(S[:m]), axis=(1, 2))
    if bad.any():
        m = int(np.argmax(bad))
    done = idx[:m]
    X[done] = S[:m, 0]
    Phi[done] = np.transpose(S[:m, 1:], (0, 2, 1))  # column k: d/d state0_k
    ok[done] = True


# Central-difference steps for a self-perturber's Phi [AU, AU/day]
_FD_STEP = np.array([1e-5] * 3 + [1e-7] * 3)


def _self_perturber_track(state0, epoch, t, ephem, body, X, Phi, ok):
    """States and Phi for one of ASSIST's own perturbers (``body``, from
    ephem_assist.self_perturber) at the finite times of t. The states are
    ephem_assist._propagate_one's (the precise pass's): for bodies 11-26 an
    integration without asteroid forces, for Pluto the planet ephemeris
    (the Pluto-system barycentre, which is the point MPC's orbit refers
    to). Phi is a central difference of plain integrations with the same
    force group off (for Pluto, without the planets: ~0.1" in a year from
    its own trajectory, which only matters to Phi at the 1e-3 level). 13
    plain integrations, ~10 ms per orbit-year; there are 17 such orbits."""
    good = np.flatnonzero(np.isfinite(t))
    if not len(good):
        return
    tg = t[good]
    kw = dict(perturber=body, integrate_pluto=True)
    try:
        Xs, Vs = ea._propagate_one(state0[:3], state0[3:], epoch, tg, ephem, perturber=body)
        P = np.empty((len(good), 6, 6))
        for j in range(6):
            d = np.zeros(6)
            d[j] = _FD_STEP[j]
            sp, sm = state0 + d, state0 - d
            Xp, Vp = ea._propagate_one(sp[:3], sp[3:], epoch, tg, ephem, **kw)
            Xm, Vm = ea._propagate_one(sm[:3], sm[3:], epoch, tg, ephem, **kw)
            P[:, :, j] = (np.concatenate([Xp, Vp]) - np.concatenate([Xm, Vm])).T / (2 * d[j])
    except Exception:       # e.g. outside the ephemeris
        return
    S = np.concatenate([Xs, Vs]).T
    fin = np.all(np.isfinite(P), axis=(1, 2)) & np.all(np.isfinite(S), axis=1)
    X[good[fin]] = S[fin]
    Phi[good[fin]] = P[fin]
    ok[good[fin]] = True


# The Earth's states for the last times asked for: every orbit of a run is
# sampled at the same times. (Per process, so safe under fork.)
_EARTH_CACHE = {"ephem": None, "t": None, "E": None}


def _earth_states(t, ephem):
    """(K, 6) barycentric Earth states at ASSIST times t, cached for the
    last (t, ephem). NaN where the ephemeris can't give one."""
    # (compare the ephem by identity, holding a reference: an id() could be
    # reused by a new object once the old one is freed)
    tb = t.tobytes()
    if _EARTH_CACHE["ephem"] is ephem and _EARTH_CACHE["t"] == tb:
        return _EARTH_CACHE["E"]
    get = ephem.get_particle
    fin = np.isfinite(t)   # (get_particle segfaults on a NaN time)
    E = np.full((len(t), 6), np.nan)
    try:
        E[fin] = np.array([(e.x, e.y, e.z, e.vx, e.vy, e.vz)
                           for e in (get(_ASSIST_EARTH, tk) for tk in t[fin].tolist())]
                          ).reshape(-1, 6)
    except Exception:            # a time outside the ephemeris
        for k in np.flatnonzero(fin):
            try:
                e = get(_ASSIST_EARTH, float(t[k]))
            except Exception:
                continue
            E[k] = (e.x, e.y, e.z, e.vx, e.vy, e.vz)
    E.setflags(write=False)
    _EARTH_CACHE.update(ephem=ephem, t=tb, E=E)
    return E


def _observer_velocity(t, obs_pos, ephem):
    """Barycentric velocity [AU/day] of an Earth-bound observer at obs_pos
    (K, 3), at ASSIST times t (K,). See the module docstring."""
    E = _earth_states(t, ephem)
    g = obs_pos - E[:, :3]
    site = np.sqrt(np.sum(g * g, axis=1)) < _EARTH_SITE_MAX_AU
    rot = _OMEGA_EARTH * np.stack([-g[:, 1], g[:, 0], np.zeros(len(t))], axis=1)
    return E[:, 3:] + np.where(site[:, None], rot, 0.0)


def _ra_deg(u):
    """RA [deg] in [0, 360) of unit vectors u (K, 3)."""
    ra = np.degrees(np.arctan2(u[:, 1], u[:, 0])) % 360.0
    ra[ra >= 360.0] = 0.0   # (-tiny) % 360 rounds to 360.0
    return ra


def _tangent_basis(u):
    """Local east and north unit vectors (K, 3) at unit directions u (K, 3)."""
    ra = np.arctan2(u[:, 1], u[:, 0])
    dec = np.arcsin(np.clip(u[:, 2], -1.0, 1.0))
    e_ra = np.stack([-np.sin(ra), np.cos(ra), np.zeros_like(ra)], axis=1)
    e_dec = np.stack([-np.sin(dec) * np.cos(ra), -np.sin(dec) * np.sin(ra),
                      np.cos(dec)], axis=1)
    return e_ra, e_dec


def _sky(cpos, rho):
    """Project position covariances cpos (K, 3, 3) [AU^2] onto the tangent
    planes of the topocentric vectors rho (K, 3) [AU]: the sky covariance
    components (s00, s01, s11) [deg^2] of (RA cos Dec, Dec)."""
    d = np.sqrt(np.sum(rho * rho, axis=1))
    with np.errstate(invalid="ignore", divide="ignore"):
        e_ra, e_dec = _tangent_basis(rho / d[:, None])
        J = np.stack([e_ra, e_dec], axis=1) * (_RAD2DEG / d)[:, None, None]
    S = J @ cpos @ np.transpose(J, (0, 2, 1))              # (K, 2, 2)
    return S[:, 0, 0], 0.5 * (S[:, 0, 1] + S[:, 1, 0]), S[:, 1, 1]


def _ellipse(s00, s01, s11):
    """(ra_err, dec_err, ra_dec_cov, sigma_major) from the sky covariance
    [deg^2]. Non-finite or non-PSD input (s00 < 0, s11 < 0, or
    s00 s11 - s01^2 < -eps s00 s11) gives NaN errors and an infinite
    sigma_major, so that it is never eligible."""
    s00 = np.asarray(s00, dtype=np.float64)
    s01 = np.asarray(s01, dtype=np.float64)
    s11 = np.asarray(s11, dtype=np.float64)
    with np.errstate(invalid="ignore", over="ignore"):
        good = (np.isfinite(s00) & np.isfinite(s01) & np.isfinite(s11)
                & (s00 >= 0) & (s11 >= 0)
                & (s00 * s11 - s01 * s01 >= -_PSD_EPS * s00 * s11))
        lam = 0.5 * (s00 + s11) + np.hypot(0.5 * (s00 - s11), s01)
        sigma = np.where(good, np.sqrt(np.maximum(lam, 0.0)) * 3600.0, np.inf)
        ra_err = np.where(good, np.sqrt(np.maximum(s00, 0.0)), np.nan)
        dec_err = np.where(good, np.sqrt(np.maximum(s11, 0.0)), np.nan)
        cov = np.where(good, s01, np.nan)
    return ra_err, dec_err, cov, sigma


def _perihelion(state0, epoch, ephem):
    """Osculating heliocentric perihelion distance [AU] of state0 at epoch
    (NaN if the Sun isn't available there)."""
    from ..ephem_assist import ASSIST_SUN, GM_SUN
    try:
        s = ephem.get_particle(ASSIST_SUN, float(epoch))
    except Exception:
        return np.nan
    r = state0[:3] - np.array([s.x, s.y, s.z])
    v = state0[3:] - np.array([s.vx, s.vy, s.vz])
    rn = np.sqrt(r @ r)
    if not rn > 0:
        return 0.0
    h = np.cross(r, v)
    e = np.cross(v, h) / GM_SUN - r / rn
    return float((h @ h) / GM_SUN / (1.0 + np.sqrt(e @ e)))


def coarse(orbit, t, obs_pos, ephem, _phi=None):
    """Sample one orbit at ASSIST times ``t`` (K,), observed from
    ``obs_pos`` (K, 3). Returns a CoarseTrack; see ``_contract`` and the
    module docstring. ``delta`` is the geometric topocentric distance
    |object - observer| [AU] (NaN where ``ok`` is False).

    ``_phi``, for tests: a dict, which gets ``state`` (K, 6) and ``phi``
    (K, 6, 6), Phi[k, i, j] = d state_i(t_k) / d state0_j (NaN where not
    ok).

    The times need not be sorted; samples on either side of the epoch are
    integrated by two simulations, each moving away from the epoch."""
    t = np.asarray(t, dtype=np.float64).reshape(-1)
    obs_pos = np.asarray(obs_pos, dtype=np.float64).reshape(-1, 3)
    K = len(t)
    if obs_pos.shape[0] != K:
        raise ValueError(f"obs_pos has {obs_pos.shape[0]} rows for {K} times")

    X = np.full((K, 6), np.nan)
    Phi = np.full((K, 6, 6), np.nan)
    ok = np.zeros(K, dtype=bool)

    state0 = np.asarray(orbit["state0"], dtype=np.float64)
    epoch = float(orbit["epoch"])
    if (K and np.all(np.isfinite(state0)) and np.isfinite(epoch)
            and _perihelion(state0, epoch, ephem) >= _Q_MIN_AU):
        order = np.argsort(t, kind="stable")
        ts = t[order]
        good_t = np.isfinite(ts)
        fwd = order[good_t & (ts >= epoch)]
        bwd = order[good_t & (ts < epoch)][::-1]
        # ASSIST's own perturbers: see the module docstring
        body = ea.self_perturber(state0[:3], state0[3:], epoch, ephem)
        if body is not None:
            _self_perturber_track(state0, epoch, t, ephem, body, X, Phi, ok)
        else:
            for idx in (fwd, bwd):
                if len(idx):
                    _integrate(state0, epoch, t[idx], ephem, X, Phi, ok, idx)
    # A sample without a finite observer position has no track.
    bad = ~np.all(np.isfinite(obs_pos), axis=1)
    ok &= ~bad
    X[bad] = np.nan
    Phi[bad] = np.nan
    if _phi is not None:
        _phi.update(state=X, phi=Phi)

    # Topocentric geometry ---------------------------------------------------
    rho = X[:, :3] - obs_pos
    d = np.sqrt(np.sum(rho * rho, axis=1))
    with np.errstate(invalid="ignore", divide="ignore"):
        u = rho / d[:, None]
    ra = _ra_deg(u)
    dec = np.degrees(np.arcsin(np.clip(u[:, 2], -1.0, 1.0)))
    e_ra, e_dec = _tangent_basis(u)

    v_obs = np.full((K, 3), np.nan)
    if ok.any():
        # all of t, so that the Earth states are shared with other orbits
        v_obs[ok] = _observer_velocity(t, obs_pos, ephem)[ok]
    rhodot = X[:, 3:] - v_obs
    with np.errstate(invalid="ignore", divide="ignore"):
        udot = (rhodot - np.sum(u * rhodot, axis=1)[:, None] * u) / d[:, None]
    rate_ra = np.sum(udot * e_ra, axis=1) * _RAD2DEG
    rate_dec = np.sum(udot * e_dec, axis=1) * _RAD2DEG

    # Uncertainty -------------------------------------------------------------
    if bool(orbit["has_cov"]):
        cov0 = np.asarray(orbit["cov0"], dtype=np.float64)
        cov = Phi @ cov0 @ np.transpose(Phi, (0, 2, 1))    # (K, 6, 6)
        cov = 0.5 * (cov + np.transpose(cov, (0, 2, 1)))
        s00, s01, s11 = _sky(cov[:, :3, :3], rho)
    else:
        cov = np.full((K, 6, 6), np.nan)
        s00 = s01 = s11 = np.full(K, np.nan)
    ra_err, dec_err, ra_dec_cov, sigma_major = _ellipse(s00, s01, s11)

    return CoarseTrack(
        t=t.copy(), ra=ra, dec=dec, rate_ra=rate_ra, rate_dec=rate_dec,
        ra_err=ra_err, dec_err=dec_err, ra_dec_cov=ra_dec_cov,
        sigma_major=sigma_major, ok=ok, delta=d, cov=cov,
    )


def ellipse_at(track, t, topo_pos=None):
    """The error ellipse at ASSIST times ``t``: (ra_err, dec_err,
    ra_dec_cov, sigma_major), shaped like ``t``.

    With ``topo_pos`` (t's shape + (3,), [AU], object - observer at each t,
    e.g. the precise pass's ``EphResult.topo_pos.T``), it propagates
    ``track.cov`` from each of the two bracketing samples k to t under free
    motion, giving the position block C_pp + tau (C_pv + C_vp) + tau^2 C_vv
    with tau = t - t_k, blends the two linearly in time, and projects the
    result on the tangent plane of ``topo_pos``. That follows C(t), which is
    quadratic in time over a day (interpolating the position block alone
    overestimates short-arc sigmas up to ~50x near their observed arc), and
    a line of sight that turns within a night (NEOs close to the Earth).
    Without ``topo_pos``, it interpolates the samples' sky covariance
    components (ra_err^2, ra_dec_cov, dec_err^2) instead.

    Either way it's a convex combination of PSD matrices (each free-motion
    propagation A C A^T is PSD), so it stays PSD.

    Samples with a non-finite time are ignored. Times outside the sampled
    span are **clamped** to the nearest sample (no extrapolation). A
    non-finite query time, a bracketing sample with weight and no
    covariance (failed, or has_cov False), or a non-PSD result give NaN
    errors and an infinite sigma_major; a time exactly on a good sample
    returns that sample.
    """
    t = np.asarray(t, dtype=np.float64)
    shape = t.shape
    t = t.reshape(-1)
    N = len(t)
    ts = np.asarray(track.t, dtype=np.float64)
    if topo_pos is not None:
        topo_pos = np.asarray(topo_pos, dtype=np.float64).reshape(N, 3)
        if track.cov is None:
            raise ValueError("ellipse_at: topo_pos needs a track with cov")
    # Samples with a non-finite time are dropped (sorted last, a NaN would
    # otherwise poison the clip for every query).
    keep = np.flatnonzero(np.isfinite(ts))
    if len(keep) == 0:
        nan = np.full(shape, np.nan)
        return nan, nan.copy(), nan.copy(), np.full(shape, np.inf)
    order = keep[np.argsort(ts[keep], kind="stable")]
    ts = ts[order]
    if topo_pos is not None:
        C = np.asarray(track.cov, dtype=np.float64)[order]
        # per sample: C_pp, C_pv + C_vp, C_vv, flattened (K, 27)
        vals = np.concatenate([C[:, :3, :3], C[:, :3, 3:] + C[:, 3:, :3], C[:, 3:, 3:]],
                              axis=1).reshape(len(ts), 27)
    else:
        vals = np.stack([np.asarray(track.ra_err)[order] ** 2,
                         np.asarray(track.ra_dec_cov)[order],
                         np.asarray(track.dec_err)[order] ** 2], axis=1)

    finite = np.isfinite(t)
    tc = np.clip(np.where(finite, t, ts[0]), ts[0], ts[-1])
    hi = np.clip(np.searchsorted(ts, tc, side="left"), 0, len(ts) - 1)
    lo = np.maximum(hi - 1, 0)
    span = ts[hi] - ts[lo]
    with np.errstate(invalid="ignore", divide="ignore"):
        w = np.where(span > 0, (tc - ts[lo]) / span, 1.0)[:, None]
    # Only include a neighbour that has weight, so that a NaN neighbour
    # doesn't poison an exact hit on a good sample.
    if topo_pos is not None:
        # free-motion position covariance from sample k at tau = t - t_k
        def free(k):
            tau = (tc - ts[k])[:, None]
            c = vals[k]
            return c[:, :9] + tau * c[:, 9:18] + tau * tau * c[:, 18:]
        v = (np.where(w < 1.0, (1.0 - w) * free(lo), 0.0)
             + np.where(w > 0.0, w * free(hi), 0.0))
    else:
        v = (np.where(w < 1.0, (1.0 - w) * vals[lo], 0.0)
             + np.where(w > 0.0, w * vals[hi], 0.0))
    v[~finite] = np.nan
    if topo_pos is not None:
        s00, s01, s11 = _sky(v.reshape(N, 3, 3), topo_pos)
    else:
        s00, s01, s11 = v[:, 0], v[:, 1], v[:, 2]
    out = _ellipse(s00, s01, s11)
    return tuple(x.reshape(shape) for x in out)
