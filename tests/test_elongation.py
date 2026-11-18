"""ssp.util.solar_elongation_ndarray: the apparent (aberrated) solar
elongation, as Horizons' S-O-T: the angle between the target and the
observer-to-Sun direction, both aberrated by the observatory's velocity
(docs/design/elongation-ssobject-nulls.md).

The tests against JPL Horizons (tests/data/elongation/horizons_sot.csv) need
DE440 in the astropy cache and are skipped without it (they never download);
X05's location is given, not fetched from the MPC. The known-geometry tests
use stand-ins for the Sun and the observatory and always run."""

from pathlib import Path

import astropy.units as u
import numpy as np
import pandas as pd
import pytest
from astropy.coordinates import EarthLocation
from astropy.time import Time

from ssp import util

HORIZONS = Path(__file__).parent / "data" / "elongation" / "horizons_sot.csv"

def real_ephemeris_or_skip(monkeypatch):
    """The real Sun and observatory, with X05's location given (not fetched
    from the MPC) and DE440 only from the astropy cache."""
    from astropy.utils.data import conf
    monkeypatch.setattr(conf, "allow_internet", False)
    loc = EarthLocation.from_geodetic(-70.749417 * u.deg, -30.244639 * u.deg, 2663 * u.m)
    monkeypatch.setattr(util, "earthlocation_from_obscode", lambda code: loc)
    try:
        t = Time([60800.0], format="mjd", scale="tai")
        util.observatory_barycentric_posvel("X05", t)
        util.sun_barycentric_position(t)
    except Exception as e:      # (not cached)
        pytest.skip(f"DE440 not available offline: {e}")


def _unit(ra_deg, dec_deg):
    ra, dec = np.radians(ra_deg), np.radians(dec_deg)
    return np.stack([np.cos(dec) * np.cos(ra), np.cos(dec) * np.sin(ra), np.sin(dec)])


def _radec(v):
    v = v / np.linalg.norm(v, axis=0)
    return np.degrees(np.arctan2(v[1], v[0])) % 360, np.degrees(np.arcsin(v[2]))


def _angle_deg(a, b):
    return np.degrees(np.arctan2(np.linalg.norm(np.cross(a, b, axis=0), axis=0), np.sum(a * b, axis=0)))


# --------------------------------------------------------------------------
# Against JPL Horizons' S-O-T
# --------------------------------------------------------------------------

def test_horizons_sot(monkeypatch):
    """The apparent elongation, given Horizons' own RA and Dec, reproduces
    Horizons' S-O-T (the apparent, aberrated one) to within 0.001 deg (the
    observed residual is ~0.2", S-O-T's printed precision)."""
    real_ephemeris_or_skip(monkeypatch)
    d = pd.read_csv(HORIZONS)
    assert len(d) == 319
    t = Time(d["mjd_tai"].to_numpy(), format="mjd", scale="tai")
    elong = util.solar_elongation_ndarray(d["ra"].to_numpy(), d["dec"].to_numpy(), t)
    diff = elong - d["sot"].to_numpy()
    assert np.max(np.abs(diff)) < 0.001, np.max(np.abs(diff))

    # sanity: the geometric angle (no aberration) differs by up to 2 v/c
    # (~41"), and does differ
    r_obs, _ = util.observatory_barycentric_posvel("X05", t)
    s = util.sun_barycentric_position(t).to_value(u.au) - r_obs.to_value(u.au)
    geometric = _angle_deg(_unit(d["ra"].to_numpy(), d["dec"].to_numpy()), s)
    dg = np.abs(geometric - elong) * 3600
    assert 10 < np.max(dg) < 42, np.max(dg)


def test_horizons_sot_shifted(monkeypatch):
    """The same with the times given as a reference time and dt_s."""
    real_ephemeris_or_skip(monkeypatch)
    d = pd.read_csv(HORIZONS)
    t = d["mjd_tai"].to_numpy()
    dt = np.linspace(-0.24, 0.24, len(t))
    exact = util.solar_elongation_ndarray(d["ra"].to_numpy(), d["dec"].to_numpy(),
                                          Time(t, format="mjd", scale="tai"))
    fast = util.solar_elongation_ndarray(d["ra"].to_numpy(), d["dec"].to_numpy(),
                                         Time(t - dt / 86400, format="mjd", scale="tai"), dt_s=dt)
    assert np.max(np.abs(fast - exact)) * 3.6e9 < 1.0          # < 1 uas


# --------------------------------------------------------------------------
# Known geometry (stand-ins for the Sun and the observatory)
# --------------------------------------------------------------------------

def _stand_in_observatory(obscode, obstime):
    """At rest (no aberration): the known-geometry tests are of the Sun's
    direction; test_aberration gives the observatory a velocity."""
    ph = 2 * np.pi * (obstime.tai.mjd - 60800.0) / 365.25
    r = np.stack([np.cos(ph), 0.917 * np.sin(ph), 0.398 * np.sin(ph)])
    return r * u.au, 0 * r * u.au / u.day


def _stand_in_sun(obstime):
    """A Sun off the barycentre, moving, so its position matters."""
    m = np.atleast_1d(obstime.tai.mjd) - 60800.0
    return np.stack([0.005 + 1e-6 * m, -0.003 + 0 * m, 0.001 + 0 * m]) * u.au


@pytest.fixture
def stand_ins(monkeypatch):
    monkeypatch.setattr(util, "observatory_barycentric_posvel", _stand_in_observatory)
    monkeypatch.setattr(util, "sun_barycentric_position", _stand_in_sun)


def _sun_dir(t):
    r, _ = _stand_in_observatory("X05", t)
    return _stand_in_sun(t).to_value(u.au) - r.to_value(u.au)


def test_known_geometry(stand_ins):
    t = Time(60800.0 + np.repeat(np.arange(10) * 17.3, 3), format="mjd", scale="tai")
    s = _sun_dir(t)
    ra, dec = _radec(s)
    # a target at the Sun's own observer-centric direction: 0
    e = util.solar_elongation_ndarray(ra, dec, t)
    assert np.max(e) * 3.6e6 < 1.0                            # < 1 mas
    # opposite: 180
    ra2, dec2 = _radec(-s)
    e = util.solar_elongation_ndarray(ra2, dec2, t)
    assert np.max(np.abs(e - 180)) * 3.6e6 < 1.0
    # perpendicular (in the plane of the Sun and the pole): 90
    n = np.cross(s, np.array([0.0, 0.0, 1.0])[:, None], axis=0)
    ra3, dec3 = _radec(n)
    e = util.solar_elongation_ndarray(ra3, dec3, t)
    np.testing.assert_allclose(e, 90.0, rtol=0, atol=1e-9)
    # a known angle, 37 deg from the Sun
    k = np.cross(n, s, axis=0)
    k /= np.linalg.norm(k, axis=0)
    sn = s / np.linalg.norm(s, axis=0)
    v = np.cos(np.radians(37)) * sn + np.sin(np.radians(37)) * k
    ra4, dec4 = _radec(v)
    e = util.solar_elongation_ndarray(ra4, dec4, t)
    np.testing.assert_allclose(e, 37.0, rtol=0, atol=1e-9)


def test_uses_observer_not_barycentre(stand_ins):
    """The Sun's direction is from the observatory: a target in the
    barycentre-to-Sun direction is not at elongation 0."""
    t = Time(np.full(4, 60850.0), format="mjd", scale="tai")
    ra, dec = _radec(_stand_in_sun(t).to_value(u.au))
    e = util.solar_elongation_ndarray(ra, dec, t)
    assert np.min(e) > 10


def test_scalar_and_shape(stand_ins):
    t = Time(60810.0, format="mjd", scale="tai")
    ra, dec = _radec(_sun_dir(Time([60810.0], format="mjd", scale="tai")))
    e = util.solar_elongation_ndarray(ra[0], dec[0], t)
    assert np.ndim(e) == 0 and e * 3.6e6 < 1.0


def _fixed(monkeypatch, sun_dir, v_kms):
    """A Sun at ``sun_dir`` [AU] from an observatory at the origin moving at
    ``v_kms`` [km/s], at every time."""
    def obs(obscode, obstime):
        n = np.atleast_1d(obstime.tai.mjd).size
        v = np.repeat(np.asarray(v_kms, float)[:, None], n, axis=1)
        return np.zeros((3, n)) * u.au, (v * u.km / u.s).to(u.au / u.day)

    def sun(obstime):
        n = np.atleast_1d(obstime.tai.mjd).size
        return np.repeat(np.asarray(sun_dir, float)[:, None], n, axis=1) * u.au
    monkeypatch.setattr(util, "observatory_barycentric_posvel", obs)
    monkeypatch.setattr(util, "sun_barycentric_position", sun)


def test_aberration(monkeypatch):
    """Sun along +x, the observatory moving along +y at 30 km/s (beta = v /
    c): the Sun's apparent direction tilts towards +y by ~beta, the
    antisolar point's too, and a target along +y (the apex) not at all."""
    v = 30.0
    b = np.degrees(v / util.C_KM_S)
    _fixed(monkeypatch, [1.0, 0, 0], [0, v, 0])
    t = Time(np.full(3, 60800.0), format="mjd", scale="tai")
    e = util.solar_elongation_ndarray(np.array([0.0, 180.0, 90.0]), np.zeros(3), t)
    # (first order: exact to ~beta^2 ~ 1e-8 rad)
    np.testing.assert_allclose(e, [0.0, 180 - 2 * b, 90 - b], rtol=0, atol=1e-6)
    assert 180 - e[1] > 40 / 3600                      # (~41")
    # the velocity matters only through beta: at rest, geometric
    _fixed(monkeypatch, [1.0, 0, 0], [0, 0, 0])
    e = util.solar_elongation_ndarray(np.array([0.0, 180.0, 90.0]), np.zeros(3), t)
    np.testing.assert_allclose(e, [0.0, 180.0, 90.0], rtol=0, atol=1e-12)


def test_aberrate_rule():
    """util.aberrate is ephem_assist._phase_angle_deg's rule."""
    rng = np.random.default_rng(5)
    n = rng.normal(size=(3, 50)) * 3
    beta = rng.normal(size=(3, 50)) * 1e-4
    got = util.aberrate(n, beta)
    un = n / np.linalg.norm(n, axis=0)
    want = un + beta - np.sum(un * beta, axis=0) * un
    np.testing.assert_allclose(got, want / np.linalg.norm(want, axis=0), rtol=0, atol=1e-15)
    np.testing.assert_allclose(np.linalg.norm(got, axis=0), 1.0, rtol=0, atol=1e-15)


def test_broadcast_scalar_time_array_positions(stand_ins):
    """A scalar time with array positions, and an array of times with a
    scalar position, broadcast (as before the fix)."""
    t1 = Time(60810.0, format="mjd", scale="tai")
    ra, dec = np.array([10.0, 100.0, 250.0]), np.array([-5.0, 20.0, 60.0])
    e = util.solar_elongation_ndarray(ra, dec, t1)
    assert e.shape == (3,)
    each = [util.solar_elongation_ndarray(r, d, t1) for r, d in zip(ra, dec)]
    np.testing.assert_array_equal(e, each)
    # and with dt_s (of the time's shape)
    e2 = util.solar_elongation_ndarray(ra, dec, t1, dt_s=0.0)
    np.testing.assert_array_equal(e2, e)

    tt = Time(60810.0 + np.arange(4) * 3.0, format="mjd", scale="tai")
    e = util.solar_elongation_ndarray(33.0, -12.0, tt)
    assert e.shape == (4,)
    each = [util.solar_elongation_ndarray(33.0, -12.0, tt[i]) for i in range(4)]
    np.testing.assert_array_equal(e, each)
    e2 = util.solar_elongation_ndarray(33.0, -12.0, tt, dt_s=np.zeros(4))
    np.testing.assert_array_equal(e2, e)
