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
- **Orbits with has_cov False** get NaN ellipses and sigma_major = inf, but
  positions and rates as usual.
"""

from __future__ import annotations

import ctypes

import numpy as np

from ._contract import CoarseTrack

# ASSIST body id of the Earth (geocentre).
_ASSIST_EARTH = 3

# Earth's rotation rate [rad/day] (sidereal day = 0.99726958 d).
_OMEGA_EARTH = 2.0 * np.pi / 0.99726958

# obs_pos within this of the geocentre [AU] (~15000 km) is taken as a site
# on the Earth, and gets the diurnal rotation velocity.
_EARTH_SITE_MAX_AU = 1e-4

_RAD2DEG = 180.0 / np.pi


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

    n = sim.N            # 7: the particle, then its six variations
    stride = ctypes.sizeof(rebound.Particle) // 8
    buf_t = ctypes.c_double * (n * stride)
    views = {}           # particle-array address -> numpy view of it
    S = np.empty((len(idx), n, 6))
    m = 0                # samples done
    for t in t_seq:
        try:
            ax.integrate_or_interpolate(float(t))
        except Exception:   # one bad orbit must not stop a run
            break
        if sim._status > 0:  # REB_STATUS_GENERIC_ERROR, escape, ...
            break
        # Read the particle array afresh each time: integrate_or_interpolate
        # swaps in an interpolated copy (see ephem_assist._propagate_one).
        # A Particle starts with x, y, z, vx, vy, vz; rows are particle 0,
        # then the six variational particles in the order they were added.
        # (serialize_particle_data would be simpler, but only copies the
        # real particles.)
        addr = ctypes.addressof(sim._particles.contents)
        v = views.get(addr)
        if v is None:
            v = views[addr] = np.frombuffer(buf_t.from_address(addr)).reshape(n, stride)[:, :6]
        S[m] = v
        m += 1
    # Everything from the first non-finite sample on has failed.
    bad = ~np.all(np.isfinite(S[:m]), axis=(1, 2))
    if bad.any():
        m = int(np.argmax(bad))
    done = idx[:m]
    X[done] = S[:m, 0]
    Phi[done] = np.transpose(S[:m, 1:], (0, 2, 1))  # column k: d/d state0_k
    ok[done] = True


def _observer_velocity(t, obs_pos, ephem):
    """Barycentric velocity [AU/day] of an Earth-bound observer at obs_pos
    (K, 3), at ASSIST times t (K,). See the module docstring."""
    get = ephem.get_particle
    E = np.array([(e.x, e.y, e.z, e.vx, e.vy, e.vz)
                  for e in (get(_ASSIST_EARTH, tk) for tk in t.tolist())]).reshape(-1, 6)
    g = obs_pos - E[:, :3]
    site = np.sqrt(np.sum(g * g, axis=1)) < _EARTH_SITE_MAX_AU
    rot = _OMEGA_EARTH * np.stack([-g[:, 1], g[:, 0], np.zeros(len(t))], axis=1)
    return E[:, 3:] + np.where(site[:, None], rot, 0.0)


def _tangent_basis(u):
    """Local east and north unit vectors (K, 3) at unit directions u (K, 3)."""
    ra = np.arctan2(u[:, 1], u[:, 0])
    dec = np.arcsin(np.clip(u[:, 2], -1.0, 1.0))
    e_ra = np.stack([-np.sin(ra), np.cos(ra), np.zeros_like(ra)], axis=1)
    e_dec = np.stack([-np.sin(dec) * np.cos(ra), -np.sin(dec) * np.sin(ra),
                      np.cos(dec)], axis=1)
    return e_ra, e_dec


def _ellipse(s00, s01, s11):
    """(ra_err, dec_err, ra_dec_cov, sigma_major) from the sky covariance
    [deg^2]. NaN input gives NaN errors and an infinite sigma_major."""
    s00 = np.asarray(s00, dtype=np.float64)
    s01 = np.asarray(s01, dtype=np.float64)
    s11 = np.asarray(s11, dtype=np.float64)
    with np.errstate(invalid="ignore"):
        lam = 0.5 * (s00 + s11) + np.hypot(0.5 * (s00 - s11), s01)
        sigma = np.sqrt(np.maximum(lam, 0.0)) * 3600.0
        ra_err = np.sqrt(np.maximum(s00, 0.0))
        dec_err = np.sqrt(np.maximum(s11, 0.0))
    ra_err = np.where(np.isnan(s00), np.nan, ra_err)
    dec_err = np.where(np.isnan(s11), np.nan, dec_err)
    sigma = np.where(np.isnan(lam), np.inf, sigma)
    return ra_err, dec_err, s01.copy(), sigma


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
    if K and np.all(np.isfinite(state0)) and np.isfinite(epoch):
        order = np.argsort(t, kind="stable")
        ts = t[order]
        fwd = order[ts >= epoch]
        bwd = order[ts < epoch][::-1]
        for idx in (fwd, bwd):
            if len(idx):
                _integrate(state0, epoch, t[idx], ephem, X, Phi, ok, idx)
    if _phi is not None:
        _phi.update(state=X, phi=Phi)

    # Topocentric geometry ---------------------------------------------------
    rho = X[:, :3] - obs_pos
    d = np.sqrt(np.sum(rho * rho, axis=1))
    with np.errstate(invalid="ignore", divide="ignore"):
        u = rho / d[:, None]
    ra = np.degrees(np.arctan2(u[:, 1], u[:, 0])) % 360.0
    dec = np.degrees(np.arcsin(np.clip(u[:, 2], -1.0, 1.0)))
    e_ra, e_dec = _tangent_basis(u)

    v_obs = np.full((K, 3), np.nan)
    if ok.any():
        v_obs[ok] = _observer_velocity(t[ok], obs_pos[ok], ephem)
    rhodot = X[:, 3:] - v_obs
    with np.errstate(invalid="ignore", divide="ignore"):
        udot = (rhodot - np.sum(u * rhodot, axis=1)[:, None] * u) / d[:, None]
    rate_ra = np.sum(udot * e_ra, axis=1) * _RAD2DEG
    rate_dec = np.sum(udot * e_dec, axis=1) * _RAD2DEG

    # Uncertainty -------------------------------------------------------------
    if bool(orbit["has_cov"]):
        cov0 = np.asarray(orbit["cov0"], dtype=np.float64)
        P = Phi[:, :3, :]                                  # (K, 3, 6)
        Cpos = P @ cov0 @ np.transpose(P, (0, 2, 1))       # (K, 3, 3)
        with np.errstate(invalid="ignore", divide="ignore"):
            J = np.stack([e_ra, e_dec], axis=1) * (_RAD2DEG / d)[:, None, None]
        S = J @ Cpos @ np.transpose(J, (0, 2, 1))          # (K, 2, 2) [deg^2]
        s00, s01, s11 = S[:, 0, 0], 0.5 * (S[:, 0, 1] + S[:, 1, 0]), S[:, 1, 1]
    else:
        s00 = s01 = s11 = np.full(K, np.nan)
    ra_err, dec_err, ra_dec_cov, sigma_major = _ellipse(s00, s01, s11)

    return CoarseTrack(
        t=t.copy(), ra=ra, dec=dec, rate_ra=rate_ra, rate_dec=rate_dec,
        ra_err=ra_err, dec_err=dec_err, ra_dec_cov=ra_dec_cov,
        sigma_major=sigma_major, ok=ok, delta=d,
    )


def ellipse_at(track, t):
    """The error ellipse at ASSIST times ``t``: (ra_err, dec_err,
    ra_dec_cov, sigma_major), shaped like ``t``.

    Linearly interpolates the sky covariance components (ra_err^2,
    ra_dec_cov, dec_err^2) between the two bracketing samples of ``track``
    (a convex combination of PSD matrices stays PSD), then derives the
    errors and sigma_major. Times outside the sampled span are **clamped**
    to the nearest sample (no extrapolation). If either bracketing sample
    has no ellipse (failed, or has_cov False), the result is NaN with an
    infinite sigma_major; a time exactly on a good sample returns it.
    """
    t = np.asarray(t, dtype=np.float64)
    shape = t.shape
    t = t.reshape(-1)
    ts = np.asarray(track.t, dtype=np.float64)
    if len(ts) == 0:
        nan = np.full(shape, np.nan)
        return nan, nan.copy(), nan.copy(), np.full(shape, np.inf)
    order = np.argsort(ts, kind="stable")
    ts = ts[order]
    comps = np.stack([np.asarray(track.ra_err)[order] ** 2,
                      np.asarray(track.ra_dec_cov)[order],
                      np.asarray(track.dec_err)[order] ** 2])   # (3, K)

    tc = np.clip(t, ts[0], ts[-1])
    hi = np.clip(np.searchsorted(ts, tc, side="left"), 0, len(ts) - 1)
    lo = np.maximum(hi - 1, 0)
    span = ts[hi] - ts[lo]
    with np.errstate(invalid="ignore", divide="ignore"):
        w = np.where(span > 0, (tc - ts[lo]) / span, 1.0)
    # Only include a neighbour that has weight, so that a NaN neighbour
    # doesn't poison an exact hit on a good sample.
    a = np.where(w < 1.0, (1.0 - w) * comps[:, lo], 0.0)
    b = np.where(w > 0.0, w * comps[:, hi], 0.0)
    s = a + b
    out = _ellipse(s[0], s[1], s[2])
    return tuple(x.reshape(shape) for x in out)
