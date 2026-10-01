"""WP3 of the widened SSSource: ``ssp.sssource_ellipse``.

The loader tests run offline on synthetic mpc_orbits rows (as
tests/test_nearbysso_orbits.py builds them). The ellipse tests use the
NearbySSO test orbits (tests/data/nearbysso_orbits.json and
nearbysso_perturbers.json) and are skipped unless SSP_ASSIST_PLANETS and
SSP_ASSIST_ASTEROIDS are set; they need no network.
"""

import numpy as np
import pyarrow.parquet as pq
import pytest
from test_ephem_perturbers import PERTURBERS_JSON
from test_nearbysso_orbits import FakeEphem, _synthetic_table
from test_nearbysso_propagate import (AU_KM, MB_LONG, MB_SHORT, NEO_CA, NEO_LONG, NEO_SHORT, load_orbit_rows,
                                      needs_assist, nights, x05_state)

from ssp import ephem_assist as ea
from ssp import sssource_ellipse as se
from ssp.nearbysso import orbits as O
from ssp.nearbysso import propagate
from ssp.nearbysso._contract import ORBIT_DTYPE, CoarseTrack


# ---------------------------------------------------------------------------
# load_orbit_covariances (offline)
# ---------------------------------------------------------------------------

def _rows_equal(a, b):
    for f in ORBIT_DTYPE.names:
        x, y = np.asarray(a[f]), np.asarray(b[f])
        if x.dtype.kind == "f":
            np.testing.assert_array_equal(x, y, err_msg=f)    # (NaN == NaN here)
        else:
            assert np.array_equal(x, y), f


@pytest.mark.parametrize("row_group_size", [None, 3])
def test_load_orbit_covariances_subset(tmp_path, row_group_size):
    """The requested, present designations, each exactly load_orbits(...,
    with_filter=False)'s row: comets, short arcs, rows without elements,
    covariance or JSON included."""
    table, _, _ = _synthetic_table(np.random.default_rng(1))
    path = tmp_path / "mpc_orbits.parquet"
    pq.write_table(table, path, row_group_size=row_group_size)
    ephem = FakeEphem()
    rows = O.load_orbits(path, with_filter=False, ephem=ephem, verbose=False)
    full = {str(r["designation"]): r for r in rows}

    want = ["C/2024 G7", "2002 CC", "2002 CE", "2001 BB", "1997 XX", "1998 YY", "2000 AA", "2025 OF623",
            "1994 UU", "2000 AA", "not there", "", None]
    got = se.load_orbit_covariances(path, want, ephem, verbose=False)
    present = {w for w in want if w in full}
    assert set(got) == present
    for d, r in got.items():
        assert r.dtype == ORBIT_DTYPE
        _rows_equal(r, full[d])
    assert not got["1998 YY"]["has_cov"] and not got["1997 XX"]["has_cov"] and got["C/2024 G7"]["has_cov"]
    assert np.isnan(got["2001 BB"]["state0"]).all()

    # a numpy array of designations works too; nothing found or asked: {}
    assert set(se.load_orbit_covariances(path, np.array(sorted(present)), ephem, verbose=False)) == present
    assert se.load_orbit_covariances(path, ["nope", "nope either"], ephem, verbose=False) == {}
    assert se.load_orbit_covariances(path, [], ephem, verbose=False) == {}


def test_load_orbit_covariances_prints_summary(tmp_path, capsys):
    table, _, _ = _synthetic_table(np.random.default_rng(1))
    path = tmp_path / "mpc_orbits.parquet"
    pq.write_table(table, path)
    se.load_orbit_covariances(path, ["2000 AA", "1999 ZZ"], FakeEphem())
    assert "load_orbits: 2 rows read, no filter; 2 kept" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# ephemeris_ellipse: no ASSIST needed
# ---------------------------------------------------------------------------

def _fake_track(t, cov):
    K = len(t)
    z = np.zeros(K)
    return CoarseTrack(t=np.asarray(t, float), ra=z, dec=z, rate_ra=z, rate_dec=z, ra_err=z, dec_err=z,
                       ra_dec_cov=z, sigma_major=z, ok=np.ones(K, bool), delta=np.ones(K), cov=cov)


def _orbit(has_cov=True):
    o = np.zeros((), dtype=ORBIT_DTYPE)
    o["has_cov"] = has_cov
    return o


def test_convention_ra_err_includes_cos_dec(monkeypatch):
    """With an isotropic position covariance sigma^2 I at distance d, the
    ellipse is round: ra_err = dec_err = sigma / d [rad], at any Dec. (An
    RA *coordinate* error would be sigma / (d cos Dec).)"""
    sigma, d = 1e-6, 2.0
    t = np.array([0.0, 1.0, 2.0])
    cov = np.zeros((3, 6, 6))
    cov[:, :3, :3] = sigma ** 2 * np.eye(3)
    cov[:, 3:, 3:] = 1e-12 * np.eye(3)
    monkeypatch.setattr(propagate, "coarse", lambda orbit, t, obs_pos, ephem: _fake_track(t, cov[:len(t)]))
    dec = np.radians([0.0, 60.0, -80.0])
    ra = np.radians([10.0, 200.0, 330.0])
    topo = d * np.stack([np.cos(dec) * np.cos(ra), np.cos(dec) * np.sin(ra), np.sin(dec)], axis=1)
    ra_err, dec_err, c = se.ephemeris_ellipse(_orbit(), t, np.zeros((3, 3)), topo, None)
    want = np.degrees(sigma / d)
    np.testing.assert_allclose(ra_err, want, rtol=1e-12)
    np.testing.assert_allclose(dec_err, want, rtol=1e-12)
    np.testing.assert_allclose(c, 0.0, atol=1e-12 * want ** 2)
    assert ra_err.dtype == dec_err.dtype == c.dtype == np.float64

    # an anisotropic one: the projection on east and north
    C = np.diag([1.0, 4.0, 9.0]) * sigma ** 2
    C[0, 2] = C[2, 0] = 2.0 * sigma ** 2
    cov[:, :3, :3] = C
    ra_err, dec_err, c = se.ephemeris_ellipse(_orbit(), t, np.zeros((3, 3)), topo, None)
    e_ra, e_dec = propagate._tangent_basis(topo / d)
    for k in range(3):
        J = np.degrees(np.stack([e_ra[k], e_dec[k]]) / d)
        S = J @ C @ J.T
        np.testing.assert_allclose([ra_err[k] ** 2, dec_err[k] ** 2, c[k]], [S[0, 0], S[1, 1], S[0, 1]],
                                   rtol=1e-10, atol=1e-14 * S[0, 0])


def test_no_covariance_is_nan(monkeypatch):
    called = []
    monkeypatch.setattr(propagate, "coarse", lambda *a, **k: called.append(1))
    out = se.ephemeris_ellipse(_orbit(has_cov=False), np.arange(4.0), np.ones((4, 3)), np.ones((4, 3)), None)
    assert all(np.isnan(x).all() and x.shape == (4,) for x in out)
    assert not called                   # not even propagated
    out = se.ephemeris_ellipse(_orbit(), np.zeros(0), np.zeros((0, 3)), np.zeros((0, 3)), None)
    assert all(x.shape == (0,) for x in out)


def test_exception_is_nan(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("ASSIST blew up")
    monkeypatch.setattr(propagate, "coarse", boom)
    out = se.ephemeris_ellipse(_orbit(), np.arange(3.0), np.ones((3, 3)), np.ones((3, 3)), None)
    assert all(np.isnan(x).all() for x in out)


def test_shape_mismatch_raises():
    with pytest.raises(ValueError):
        se.ephemeris_ellipse(_orbit(), np.arange(3.0), np.ones((2, 3)), np.ones((3, 3)), None)
    with pytest.raises(ValueError):
        se.ephemeris_ellipse(_orbit(), np.arange(3.0), np.ones((3, 3)), np.ones((3,)), None)


def test_samples_distinct_sorted_times(monkeypatch):
    """coarse gets each distinct finite time once, sorted, with a finite
    observer position; rows with a non-finite t or obs_pos are NaN."""
    seen = {}

    def fake(orbit, t, obs_pos, ephem):
        seen.update(t=np.array(t), obs=np.array(obs_pos))
        cov = np.zeros((len(t), 6, 6))
        cov[:, :3, :3] = 1e-12 * (1.0 + np.asarray(t))[:, None, None] * np.eye(3)
        return _fake_track(t, cov)

    monkeypatch.setattr(propagate, "coarse", fake)
    t = np.array([3.0, 1.0, 3.0, np.nan, 2.0, 1.0, 5.0])
    obs = np.tile([1.0, 0.0, 0.0], (7, 1))
    obs[6] = np.nan
    topo = np.tile([0.0, 1.0, 0.0], (7, 1))
    a, b, c = se.ephemeris_ellipse(_orbit(), t, obs, topo, None)
    np.testing.assert_array_equal(seen["t"], [1.0, 2.0, 3.0])
    assert np.isnan(a[[3, 6]]).all() and np.isfinite(a[[0, 1, 2, 4, 5]]).all()
    assert a[0] == a[2] and a[1] == a[5]
    want = np.degrees(1e-6) ** 2 * np.array([2.0, 3.0, 4.0])      # sigma^2 = 1e-12 (1 + t)
    np.testing.assert_allclose(a[[1, 4, 0]] ** 2, want, rtol=1e-12)


# ---------------------------------------------------------------------------
# ephemeris_ellipse with ASSIST
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def ephem():
    return ea.open_ephem()


@pytest.fixture(scope="module")
def orbits(ephem):
    return load_orbit_rows(ephem)


@pytest.fixture(scope="module")
def perturbers(ephem):
    return load_orbit_rows(ephem, PERTURBERS_JSON)


def _obs_times(m0, m1, per_night=6, seed=0):
    """Several observations per night, within +-3 h of nightly midnight."""
    rng = np.random.default_rng(seed)
    t = np.repeat(nights(m0, m1), per_night)
    return t + rng.uniform(-0.125, 0.125, t.size)


def _precise_topo(orbit, t, ephem):
    """(obs_pos, the precise pass's topo_pos), each (K, 3)."""
    from astropy.time import Time
    obs_pos, obs_vel = x05_state(t, ephem)
    times = Time(t + ea.MJD_J2000, format="mjd", scale="tdb")
    e = ea.compute_ephemerides_one(str(orbit["designation"]), times, None, ephem, row=orbit,
                                   obs_pos=obs_pos.T, obs_vel=obs_vel.T * AU_KM / 86400.0)
    return obs_pos, e.topo_pos.T


@needs_assist
@pytest.mark.parametrize("name", [MB_LONG, MB_SHORT, NEO_LONG, NEO_CA, NEO_SHORT])
def test_matches_coarse(ephem, orbits, name):
    """The same as coarse() at those times plus ellipse_at(topo_pos)."""
    orbit = orbits[name]
    t = _obs_times(60930, 60940) if name == NEO_CA else _obs_times(60990, 61010)
    obs_pos, topo = _precise_topo(orbit, t, ephem)
    got = se.ephemeris_ellipse(orbit, t, obs_pos, topo, ephem)
    order = np.argsort(t)
    tr = propagate.coarse(orbit, t[order], obs_pos[order], ephem)
    want = propagate.ellipse_at(tr, t, topo_pos=topo)
    for g, w in zip(got, want[:3]):
        assert np.isfinite(g).all()
        np.testing.assert_allclose(g, w, rtol=1e-12, atol=0)

    # with the geometric direction as topo_pos, it is coarse()'s own ellipse
    # at its samples (NearbySSO's convention; sigma_major from it agrees)
    st = {}
    tr = propagate.coarse(orbit, t, obs_pos, ephem, _phi=st)
    rho = st["state"][:, :3] - obs_pos
    ra_err, dec_err, cov = se.ephemeris_ellipse(orbit, t, obs_pos, rho, ephem)
    np.testing.assert_allclose(ra_err, tr.ra_err, rtol=1e-10)
    np.testing.assert_allclose(dec_err, tr.dec_err, rtol=1e-10)
    np.testing.assert_allclose(cov / (ra_err * dec_err), tr.ra_dec_cov / (tr.ra_err * tr.dec_err), atol=1e-9)
    smaj = propagate._ellipse(ra_err ** 2, cov, dec_err ** 2)[3]
    np.testing.assert_allclose(smaj, tr.sigma_major, rtol=1e-8)


@needs_assist
def test_monte_carlo_convention(ephem, orbits):
    """ra_err is the spread of (RA - RA0) cos Dec0, dec_err of Dec, and
    ra_dec_cov their covariance, for positions drawn from C_pp(t) and seen
    along topo_pos."""
    orbit = orbits[NEO_LONG]                    # at Dec ~ +36 deg then
    t = np.array([61220.17 - ea.MJD_J2000])
    obs_pos, topo = _precise_topo(orbit, t, ephem)
    ra_err, dec_err, cov = se.ephemeris_ellipse(orbit, t, obs_pos, topo, ephem)
    tr = propagate.coarse(orbit, t, obs_pos, ephem)
    rng = np.random.default_rng(3)
    X = topo[0] + rng.multivariate_normal(np.zeros(3), tr.cov[0, :3, :3], size=20000)
    ra = np.degrees(np.arctan2(X[:, 1], X[:, 0]))
    dec = np.degrees(np.arcsin(X[:, 2] / np.linalg.norm(X, axis=1)))
    ra0 = np.degrees(np.arctan2(topo[0, 1], topo[0, 0]))
    dec0 = np.degrees(np.arcsin(topo[0, 2] / np.linalg.norm(topo[0])))
    dra = ((ra - ra0 + 180) % 360 - 180) * np.cos(np.radians(dec0))
    S = np.cov(np.stack([dra, dec - dec0]))
    assert np.cos(np.radians(dec0)) < 0.9       # (so that cos Dec matters)
    np.testing.assert_allclose([ra_err[0], dec_err[0]], np.sqrt(np.diag(S)), rtol=0.03)
    np.testing.assert_allclose(cov[0] / (ra_err[0] * dec_err[0]), S[0, 1] / np.sqrt(S[0, 0] * S[1, 1]),
                               atol=0.03)


@needs_assist
def test_unsorted_duplicate_times(ephem, orbits):
    """Unsorted and repeated times give the values of the sorted, distinct
    ones, row by row; a NaN time is NaN alone."""
    orbit = orbits[NEO_LONG]
    t = _obs_times(60990, 61000, per_night=4)
    obs_pos, topo = _precise_topo(orbit, t, ephem)
    ref = se.ephemeris_ellipse(orbit, t, obs_pos, topo, ephem)

    rng = np.random.default_rng(7)
    idx = rng.permutation(np.concatenate([np.arange(t.size), rng.integers(0, t.size, 15)]))
    t2, o2, p2 = t[idx].copy(), obs_pos[idx].copy(), topo[idx].copy()
    t2[3] = np.nan
    got = se.ephemeris_ellipse(orbit, t2, o2, p2, ephem)
    for g, r in zip(got, ref):
        assert np.isnan(g[3])
        keep = np.arange(t2.size) != 3
        np.testing.assert_array_equal(g[keep], r[idx][keep])


@needs_assist
def test_self_perturber_ceres(ephem, perturbers):
    """Ceres (one of ASSIST's own perturbers): finite, milliarcsecond-level."""
    orbit = perturbers["A801 AA"]
    t = _obs_times(61190, 61200, per_night=3)
    obs_pos, topo = _precise_topo(orbit, t, ephem)
    ra_err, dec_err, cov = se.ephemeris_ellipse(orbit, t, obs_pos, topo, ephem)
    assert np.isfinite(ra_err).all() and np.isfinite(dec_err).all() and np.isfinite(cov).all()
    assert 0 < ra_err.max() * 3600 < 1.0 and 0 < dec_err.max() * 3600 < 1.0


@needs_assist
def test_bad_orbits_do_not_raise(ephem, orbits):
    t = _obs_times(60990, 60995, per_night=2)
    obs_pos, topo = _precise_topo(orbits[MB_LONG], t, ephem)

    def nan_all(o):
        out = se.ephemeris_ellipse(o, t, obs_pos, topo, ephem)
        return all(np.isnan(x).all() for x in out)

    o = orbits[MB_LONG].copy()
    o["state0"][:] = np.nan
    assert nan_all(o)
    o = orbits[MB_LONG].copy()
    o["epoch"] = np.nan
    assert nan_all(o)
    o = orbits[MB_LONG].copy()
    o["cov0"][0, 0] = -1.0          # not PSD
    assert nan_all(o)
    o = orbits[MB_LONG].copy()       # a Sun-plunger (q < 0.02 AU): not integrated
    o["state0"][3:] = 0.0
    assert nan_all(o)
    # beyond the ephemeris range: the samples there are NaN, the rest fine
    o = orbits[MB_LONG].copy()
    o["epoch"] = 237300.0
    tt = o["epoch"] + np.array([10.0, 30.0, 900.0])
    op = np.tile(obs_pos[:1], (3, 1))
    ra_err, _, _ = se.ephemeris_ellipse(o, tt, op, np.tile(topo[:1], (3, 1)), ephem)
    assert np.isfinite(ra_err[:2]).all() and np.isnan(ra_err[2])
