"""WP2: ``ssp.nearbysso.propagate`` (coarse pass and error ellipses).

The ASSIST tests use a handful of real orbits from the 2026-09-26
``mpc_orbits`` snapshot (``tests/data/nearbysso_orbits.json``: the elements
and the 6x6 state block of ``mpc_orb_jsonb.CAR``), converted to
``ORBIT_DTYPE`` rows by a test-only helper (WP1 owns the real loader). They
are skipped unless ``SSP_ASSIST_PLANETS`` and ``SSP_ASSIST_ASTEROIDS`` are
set, and need no network.
"""

import json
import os
from pathlib import Path

import numpy as np
import pytest

from ssp.nearbysso import propagate
from ssp.nearbysso._contract import ORBIT_DTYPE, CoarseTrack

HAVE_ASSIST = bool(os.environ.get("SSP_ASSIST_PLANETS") and os.environ.get("SSP_ASSIST_ASTEROIDS"))
needs_assist = pytest.mark.skipif(not HAVE_ASSIST, reason="SSP_ASSIST_PLANETS / SSP_ASSIST_ASTEROIDS not set")

ORBITS_JSON = Path(__file__).parent / "data" / "nearbysso_orbits.json"

# The test orbits (see the module docstring), by role.
MB_LONG = "2007 VY347"      # main belt, 18-yr arc
MB_SHORT = "2026 DF62"      # main belt, 3-day arc
NEO_LONG = "2003 LN6"       # NEO, 23-yr arc
NEO_CA = "2025 FA22"        # NEO, 0.0057 AU from Earth on MJD 60936.5
NEO_SHORT = "2025 PM"       # NEO, 31-day arc, 0.0071 AU on MJD 60904.5

# X05 (Rubin) MPC parallax constants: longitude [deg], rho cos(phi'),
# rho sin(phi').
X05 = (289.74081, 0.864981, -0.500958)
AU_KM = 149597870.7
R_EARTH_AU = 6378.137 / AU_KM


# ---------------------------------------------------------------------------
# Test-only helpers
# ---------------------------------------------------------------------------

def load_orbit_rows(ephem):
    """{designation: ORBIT_DTYPE row} for the test orbits: state0 from
    elements_row_to_bary_icrf, cov0 from the CAR 6x6 block (heliocentric
    ecliptic J2000), rotated to equatorial."""
    from astropy.time import Time

    from ssp import ephem_assist as ea

    ce, se = np.cos(ea.OBLIQUITY_J2000), np.sin(ea.OBLIQUITY_J2000)
    R = np.array([[1, 0, 0], [0, ce, -se], [0, se, ce]])
    R6 = np.zeros((6, 6))
    R6[:3, :3] = R6[3:, 3:] = R
    out = {}
    for r in json.loads(ORBITS_JSON.read_text()):
        row = np.zeros((), dtype=ORBIT_DTYPE)
        row["designation"] = r["unpacked_primary_provisional_designation"]
        row["packed"] = r["packed_primary_provisional_designation"]
        for k in ("q", "e", "i", "node", "argperi", "peri_time", "epoch_mjd", "h", "g",
                  "normalized_rms"):
            row[k] = r[k]
        epoch = Time(r["epoch_mjd"], format="mjd", scale="tt").tdb.mjd - ea.MJD_J2000
        s = ephem.get_particle(ea.ASSIST_SUN, epoch)
        X, V = ea.elements_row_to_bary_icrf(r, np.array([s.x, s.y, s.z]),
                                            np.array([s.vx, s.vy, s.vz]))
        row["epoch"] = epoch
        row["state0"] = np.concatenate([X, V])
        C = R6 @ np.array(r["car_cov"]) @ R6.T
        row["cov0"] = C
        row["has_cov"] = bool(np.all(np.linalg.eigvalsh(C) > 0))
        out[str(row["designation"])] = row
    return out


def x05_state(t, ephem):
    """A simple X05 model: the ASSIST geocentre plus the site rotating
    about the ICRF z axis at the sidereal rate (with an approximate GMST).
    Returns barycentric (pos [AU], vel [AU/day]), each (K, 3). Good enough
    for tests that feed the same observer to both code paths."""
    t = np.atleast_1d(np.asarray(t, dtype=np.float64))
    lon, rc, rs = X05
    omega = 2 * np.pi / 0.99726958
    theta = 2 * np.pi * (0.7790572732640 + 1.00273781191135448 * t) + np.radians(lon)
    g = R_EARTH_AU * np.stack([rc * np.cos(theta), rc * np.sin(theta), np.full_like(t, rs)], axis=1)
    gv = omega * np.stack([-g[:, 1], g[:, 0], np.zeros_like(t)], axis=1)
    E = [ephem.get_particle(3, float(tk)) for tk in t]
    pos = np.array([[e.x, e.y, e.z] for e in E]) + g
    vel = np.array([[e.vx, e.vy, e.vz] for e in E]) + gv
    return pos, vel


def nights(mjd_lo, mjd_hi, step=1.0):
    """ASSIST times of nightly samples (~04:00 UTC, i.e. Chilean midnight)."""
    from ssp import ephem_assist as ea
    return np.arange(mjd_lo, mjd_hi, step) + 0.17 - ea.MJD_J2000


def plain_states(state0, epoch, t, ephem):
    """(K, 6) states from plain ASSIST integrations (both directions)."""
    from ssp import ephem_assist as ea
    X, V = ea._propagate_one(state0[:3], state0[3:], epoch, t, ephem)
    return np.concatenate([X, V]).T


def phi_variational(orbit, t, ephem):
    """(K, 6) states and (K, 6, 6) Phi(t), through coarse()'s _phi hook."""
    out = {}
    tr = propagate.coarse(orbit, t, np.zeros((len(t), 3)), ephem, _phi=out)
    assert tr.ok.all()
    return out["state"], out["phi"]


def sky_offsets(u0, u):
    """Gnomonic tangent-plane offsets [deg] of unit vectors u (N, 3) about
    u0 (3,), along (east, north)."""
    e_ra, e_dec = propagate._tangent_basis(u0[None, :])
    w = u @ u0
    return np.degrees(np.stack([(u @ e_ra[0]) / w, (u @ e_dec[0]) / w], axis=1))


@pytest.fixture(scope="module")
def ephem():
    from ssp.ephem_assist import open_ephem
    return open_ephem()


@pytest.fixture(scope="module")
def orbits(ephem):
    return load_orbit_rows(ephem)


# ---------------------------------------------------------------------------
# No-ASSIST tests: ellipse_at and the ellipse algebra
# ---------------------------------------------------------------------------

def _track(t, s00, s01, s11):
    ra_err, dec_err, cov, sig = propagate._ellipse(np.array(s00), np.array(s01), np.array(s11))
    K = len(t)
    z = np.zeros(K)
    return CoarseTrack(t=np.asarray(t, float), ra=z, dec=z, rate_ra=z, rate_dec=z, ra_err=ra_err,
                       dec_err=dec_err, ra_dec_cov=cov, sigma_major=sig, ok=np.ones(K, bool),
                       delta=np.ones(K))


def test_ellipse_algebra():
    # diag(4, 1) arcsec^2, rotated by 30 deg: sigma_major = 2 arcsec
    a = 1 / 3600
    c, s = np.cos(np.radians(30)), np.sin(np.radians(30))
    Rm = np.array([[c, -s], [s, c]])
    S = Rm @ np.diag([4 * a * a, a * a]) @ Rm.T
    ra_err, dec_err, cov, sig = propagate._ellipse(S[0, 0], S[0, 1], S[1, 1])
    assert np.isclose(sig, 2.0, rtol=1e-12)
    assert np.isclose(ra_err, np.sqrt(S[0, 0])) and np.isclose(dec_err, np.sqrt(S[1, 1]))
    assert np.isclose(cov, S[0, 1])
    _, _, _, sig = propagate._ellipse(np.nan, np.nan, np.nan)
    assert sig == np.inf


def test_ellipse_at_interpolates_covariance():
    t = np.array([0.0, 1.0, 3.0])
    s00 = np.array([1.0, 3.0, 3.0]) * 1e-8
    s01 = np.array([0.0, 1.0, -1.0]) * 1e-8
    s11 = np.array([2.0, 2.0, 4.0]) * 1e-8
    tr = _track(t, s00, s01, s11)
    tq = np.array([0.0, 0.25, 1.0, 2.0, 3.0])
    ra_err, dec_err, cov, sig = propagate.ellipse_at(tr, tq)
    e00 = np.interp(tq, t, s00)
    e01 = np.interp(tq, t, s01)
    e11 = np.interp(tq, t, s11)
    np.testing.assert_allclose(ra_err, np.sqrt(e00), rtol=1e-12)
    np.testing.assert_allclose(dec_err, np.sqrt(e11), rtol=1e-12)
    np.testing.assert_allclose(cov, e01, rtol=1e-12, atol=1e-24)
    lam = 0.5 * (e00 + e11) + np.hypot(0.5 * (e00 - e11), e01)
    np.testing.assert_allclose(sig, np.sqrt(lam) * 3600, rtol=1e-12)
    # at the samples, exactly the track's values
    np.testing.assert_allclose(propagate.ellipse_at(tr, t)[3], tr.sigma_major, rtol=1e-12)
    # interpolated ellipses stay PSD
    assert np.all(e00 * e11 - e01 ** 2 >= 0)


def test_ellipse_at_clamps_unsorted_and_shape():
    t = np.array([3.0, 0.0, 1.0])            # unsorted samples
    tr = _track(t, [3e-8, 1e-8, 2e-8], [0, 0, 0], [1e-8, 1e-8, 1e-8])
    ra_err, _, _, _ = propagate.ellipse_at(tr, np.array([[-5.0, 0.5], [2.0, 10.0]]))
    assert ra_err.shape == (2, 2)
    np.testing.assert_allclose(ra_err ** 2, [[1e-8, 1.5e-8], [2.5e-8, 3e-8]], rtol=1e-12)
    # scalar in, 0-d out
    assert np.shape(propagate.ellipse_at(tr, 0.5)[3]) == ()


def test_ellipse_at_missing_samples():
    nan = np.nan
    tr = _track([0.0, 1.0, 2.0], [1e-8, nan, 1e-8], [0, nan, 0], [1e-8, nan, 1e-8])
    ra_err, dec_err, cov, sig = propagate.ellipse_at(tr, np.array([0.0, 0.5, 1.0, 1.5, 2.0]))
    assert np.isfinite(sig[[0, 4]]).all()
    assert np.all(sig[1:4] == np.inf) and np.isnan(ra_err[1:4]).all()
    # a whole track without a covariance
    tr = _track([0.0, 1.0], [nan, nan], [nan, nan], [nan, nan])
    ra_err, dec_err, cov, sig = propagate.ellipse_at(tr, np.array([0.0, 0.3, 7.0]))
    assert np.all(sig == np.inf) and np.isnan(ra_err).all() and np.isnan(cov).all()


# ---------------------------------------------------------------------------
# ASSIST tests
# ---------------------------------------------------------------------------

@needs_assist
@pytest.mark.parametrize("name,span", [
    (MB_LONG, (60790, 61200)),
    (NEO_LONG, (60790, 61300)),
    (NEO_CA, (60790, 61100)),       # through the 0.0057 AU approach at 60936.5
])
def test_phi_matches_finite_differences(ephem, orbits, name, span):
    """Phi from the variational particles against central differences of
    plain integrations, at nightly samples on both sides of the epoch."""
    orbit = orbits[name]
    t = nights(*span, step=7.0)
    X, Phi = phi_variational(orbit, t, ephem)
    s0 = orbit["state0"].astype(float)
    # The same trajectory as a plain integration, to the integration error
    # (the variational particles change IAS15's step choices): measured
    # 1e-13 AU (main belt) to 5e-9 AU (0.17" at the close approach).
    np.testing.assert_allclose(X, plain_states(s0, orbit["epoch"], t, ephem), rtol=0, atol=2e-8)
    # Steps where the differences are neither round-off nor nonlinearity
    # dominated (1e-7/1e-9 leaves 1e-3 of noise in the off-diagonal blocks).
    h = np.array([1e-5] * 3 + [1e-7] * 3)
    fd = np.empty_like(Phi)
    for k in range(6):
        dp = np.zeros(6)
        dp[k] = h[k]
        fd[:, :, k] = (plain_states(s0 + dp, orbit["epoch"], t, ephem)
                       - plain_states(s0 - dp, orbit["epoch"], t, ephem)) / (2 * h[k])
    # Compare column blocks, relative to each block's size: the position
    # and velocity rows have different units.
    for rows in (slice(0, 3), slice(3, 6)):
        for cols in (slice(0, 3), slice(3, 6)):
            a, b = Phi[:, rows, cols], fd[:, rows, cols]
            err = np.linalg.norm(a - b, axis=(1, 2)) / np.linalg.norm(b, axis=(1, 2))
            assert err.max() < 1e-4, (name, rows, cols, err.max())
    # and Phi is not the free-motion matrix a wrong setup silently gives
    assert np.abs(Phi[-1, :3, :3] - np.eye(3)).max() > 1e-3


@needs_assist
def test_topocentric_matches_precise_pass(ephem, orbits):
    """coarse()'s geometric direction against compute_ephemerides_one's
    light-time-corrected one: they differ by the object's motion during the
    light time, |V_perp| / c (tens of arcsec). The rates differ by
    (v/c) |d rho/dt| / Delta (that offset in direction projects a little of
    the radial motion onto the sky) plus |a| / c (the velocity at emission
    time; ~1e-4 deg/day at 1 AU from the Sun)."""
    from astropy.time import Time

    from ssp import ephem_assist as ea

    for name in (MB_LONG, NEO_LONG, NEO_CA):
        orbit = orbits[name]
        t = nights(60790, 61100, step=3.0)
        obs_pos, obs_vel = x05_state(t, ephem)
        tr = propagate.coarse(orbit, t, obs_pos, ephem)
        assert tr.ok.all()
        times = Time(t + ea.MJD_J2000, format="mjd", scale="tdb").tai
        row = {k: orbit[k] for k in ("q", "e", "i", "node", "argperi", "peri_time", "epoch_mjd", "h", "g")}
        eph = ea.compute_ephemerides_one(None, times, None, ephem, row=row, obs_pos=obs_pos.T,
                                         obs_vel=obs_vel.T * AU_KM / 86400)
        u_c = np.stack([np.cos(np.radians(tr.dec)) * np.cos(np.radians(tr.ra)),
                        np.cos(np.radians(tr.dec)) * np.sin(np.radians(tr.ra)),
                        np.sin(np.radians(tr.dec))], axis=1)
        u_p = (eph.topo_pos / np.linalg.norm(eph.topo_pos, axis=0)).T
        sep = np.degrees(np.arctan2(np.linalg.norm(np.cross(u_c, u_p), axis=1),
                                    np.sum(u_c * u_p, axis=1))) * 3600
        # expected: the light-time displacement of the object, seen from
        # the observer: tau * V_perp / Delta = |V_perp| / c
        V = eph.vv.T / AU_KM * 86400                      # AU/day
        Vp = V - np.sum(V * u_p, axis=1)[:, None] * u_p
        expect = np.degrees(np.linalg.norm(Vp, axis=1) / ea.C_AU_PER_DAY) * 3600
        assert np.all(np.abs(sep - expect) < 1e-3 * expect + 1e-3), (name, np.abs(sep - expect).max())
        d = np.linalg.norm(eph.topo_pos, axis=0)
        vrel = np.linalg.norm(eph.topo_vel, axis=0) / AU_KM * 86400    # AU/day
        r_sun = np.linalg.norm(eph.helio_pos, axis=0)
        a_sun = ea.GM_SUN / r_sun ** 2
        bound = np.degrees(2 * (np.linalg.norm(V, axis=1) * vrel / d + a_sun) / ea.C_AU_PER_DAY)
        drate = np.hypot(tr.rate_ra - eph.mu_lon, tr.rate_dec - eph.mu_lat)
        assert np.all(drate < bound), (name, (drate / bound).max())
        assert np.all(drate < 1e-3 * np.degrees(vrel / d) + 2e-4), name
        assert np.all(np.isfinite(tr.sigma_major))
        # delta is the geometric |X(t) - O(t)| of the precise pass's state
        # (to the integration difference; see the Phi test), and exceeds
        # its light-time-corrected distance |X(t - tau) - O(t)| by the
        # radial motion during the light time, tau (u . V), to second
        # order in tau (~1e-8 AU, i.e. a few km)
        np.testing.assert_allclose(tr.delta, np.linalg.norm(eph.xx.T - obs_pos, axis=1),
                                   rtol=0, atol=2e-8)
        dd = tr.delta - d
        expect_d = eph.light_time * np.sum(u_p * V, axis=1)
        err = np.abs(dd - expect_d)
        assert np.all(err < 1e-3 * np.abs(expect_d) + 3e-8), (name, err.max())


@needs_assist
def test_no_covariance_and_failures(ephem, orbits):
    orbit = orbits[MB_LONG].copy()
    t = nights(60790, 60800)
    obs_pos, _ = x05_state(t, ephem)
    good = propagate.coarse(orbit, t, obs_pos, ephem)
    orbit["has_cov"] = False
    tr = propagate.coarse(orbit, t, obs_pos, ephem)
    assert tr.ok.all() and np.all(tr.sigma_major == np.inf)
    for f in ("ra_err", "dec_err", "ra_dec_cov"):
        assert np.isnan(getattr(tr, f)).all()
    for f in ("ra", "dec", "rate_ra", "rate_dec"):
        np.testing.assert_array_equal(getattr(tr, f), getattr(good, f))
    assert np.all(propagate.ellipse_at(tr, t[:3] + 0.5)[3] == np.inf)

    # a non-finite state: every sample fails, nothing raises
    orbit = orbits[MB_LONG].copy()
    orbit["state0"][0] = np.nan
    tr = propagate.coarse(orbit, t, obs_pos, ephem)
    assert not tr.ok.any() and np.all(tr.sigma_major == np.inf) and np.isnan(tr.ra).all()
    assert np.isnan(tr.delta).all()

    # beyond the ephemeris range (the DE440/441 file ends in 2650, at ASSIST
    # time ~237430): the samples inside are fine, those outside fail. (The
    # orbit's epoch is moved there, so as not to integrate for 600 years.)
    orbit = orbits[MB_LONG].copy()
    orbit["epoch"] = 237300.0
    tt = orbit["epoch"] + np.array([30.0, 10.0, 400.0, 900.0])
    op = np.tile(obs_pos[:1], (4, 1))
    tr = propagate.coarse(orbit, tt, op, ephem)
    assert tr.ok.tolist() == [True, True, False, False]
    assert np.isfinite(tr.delta[:2]).all() and np.isnan(tr.delta[2:]).all()
    assert np.all(tr.sigma_major[2:] == np.inf) and np.isfinite(tr.sigma_major[:2]).all()

    # no samples
    tr = propagate.coarse(orbits[MB_LONG], np.array([]), np.zeros((0, 3)), ephem)
    assert len(tr.t) == 0 and len(tr.sigma_major) == 0


@needs_assist
def test_coarse_order_and_ellipse_at(ephem, orbits):
    """Unsorted input times give the same samples, in the caller's order;
    ellipse_at at a sample reproduces it, and between nightly samples it's
    close to the directly computed ellipse."""
    orbit = orbits[NEO_LONG]
    t = nights(60790, 61300, step=1.0)
    obs_pos, _ = x05_state(t, ephem)
    tr = propagate.coarse(orbit, t, obs_pos, ephem)
    perm = np.random.default_rng(3).permutation(len(t))
    tp = propagate.coarse(orbit, t[perm], obs_pos[perm], ephem)
    for f in CoarseTrack._fields:
        np.testing.assert_allclose(getattr(tp, f), getattr(tr, f)[perm], rtol=1e-9, atol=1e-15)
    tm = t[:-1] + 0.5
    om, _ = x05_state(tm, ephem)
    direct = propagate.coarse(orbit, tm, om, ephem)
    interp = propagate.ellipse_at(tr, tm)
    np.testing.assert_allclose(interp[3], direct.sigma_major, rtol=0.02)


def _monte_carlo(orbit, t, obs_pos, ephem, n=500, seed=42):
    """Sky offsets [deg] (n, K, 2) of n states drawn from cov0, each
    integrated, about the nominal track's direction; and that track."""
    tr = propagate.coarse(orbit, t, obs_pos, ephem)
    s0 = orbit["state0"].astype(float)
    draws = np.random.default_rng(seed).multivariate_normal(s0, orbit["cov0"], size=n)
    u0 = np.stack([np.cos(np.radians(tr.dec)) * np.cos(np.radians(tr.ra)),
                   np.cos(np.radians(tr.dec)) * np.sin(np.radians(tr.ra)),
                   np.sin(np.radians(tr.dec))], axis=1)
    off = np.empty((n, len(t), 2))
    for i in range(n):
        rho = plain_states(draws[i], orbit["epoch"], t, ephem)[:, :3] - obs_pos
        u = rho / np.linalg.norm(rho, axis=1)[:, None]
        for k in range(len(t)):
            off[i, k] = sky_offsets(u0[k], u[k:k + 1])[0]
    return off, tr


@needs_assist
@pytest.mark.parametrize("name,linear", [
    # long arcs: linear everywhere
    (MB_LONG, lambda t: np.ones(len(t), bool)),
    (NEO_CA, lambda t: np.ones(len(t), bool)),
    # a 3-day arc in 2026 Feb (MJD ~61088-61091), with its MPC epoch 90 days
    # earlier: the linear ellipse pinches to < 1" at the observed arc, while
    # states drawn from the (Gaussian, epoch) cov0 spread by ~50" there. The
    # posterior at the epoch is really a curved banana, so there it's the
    # Monte Carlo from the Gaussian cov0 that is wrong. Agreement is checked
    # only more than 40 days from the arc. Where the ellipse is also
    # ~1 deg long (500:1), the bend of the line of variations adds up to
    # ~15% to its minor axis.
    (MB_SHORT, lambda t: np.abs(t + 51544.5 - 61090) > 40),
])
def test_ellipse_matches_monte_carlo(ephem, orbits, name, linear):
    """The linear on-sky ellipse against the scatter of 500 states drawn
    from cov0 and integrated: both principal axes within 10%, and a mean
    squared Mahalanobis distance of ~2 (the statistical error of 500 draws
    is ~3%); 25% for the minor axis and the Mahalanobis distance of
    ellipses longer than 0.5 deg."""
    orbit = orbits[name]
    t = nights(60790, 61300, step=30.0)
    obs_pos, _ = x05_state(t, ephem)
    off, tr = _monte_carlo(orbit, t, obs_pos, ephem)
    lin = linear(t)
    assert lin.sum() >= 10
    for k in np.flatnonzero(lin):
        S = np.array([[tr.ra_err[k] ** 2, tr.ra_dec_cov[k]], [tr.ra_dec_cov[k], tr.dec_err[k] ** 2]])
        ll = np.sqrt(np.linalg.eigvalsh(S))
        lm = np.sqrt(np.linalg.eigvalsh(np.cov(off[:, k].T)))
        tol = 0.10 if tr.sigma_major[k] < 1800 else 0.25
        assert abs(lm[1] / ll[1] - 1) < 0.10, (name, t[k], lm, ll)
        assert abs(lm[0] / ll[0] - 1) < tol, (name, t[k], lm, ll)
        assert np.isclose(ll[1] * 3600, tr.sigma_major[k], rtol=1e-9)
        d2 = np.einsum("ni,ij,nj->n", off[:, k], np.linalg.inv(S), off[:, k])
        assert abs(d2.mean() / 2 - 1) < max(tol, 0.15), (name, t[k], d2.mean() / 2)
    if not lin.all():
        # and it does break down at the observed arc, as described above
        k = np.argmin(np.abs(t + 51544.5 - 61090))
        assert np.sqrt(np.linalg.eigvalsh(np.cov(off[:, k].T)))[1] * 3600 > 5 * tr.sigma_major[k]
