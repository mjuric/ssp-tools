"""ssp.util.solar_elongation_ndarray: the angle between the target and the
geometric observer-to-Sun direction (docs/design/elongation-ssobject-nulls.md).

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

#: The speed of light [km/s].
C_KMS = 299792.458


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
    """Horizons' S-O-T is the angle between the *apparent* (aberrated)
    target and Sun, while SSObservation's elongation is the geometric one
    (the owner's decision, 2026-11-17): they differ by up to ~41" (twice the
    annual aberration). So: (1) the geometric elongation is within 0.012 deg
    of S-O-T, and (2) aberrating both directions by the observatory's
    barycentric velocity, our observer-to-Sun vector reproduces S-O-T to
    the fixture's precision (0.001 deg), validating the Sun direction."""
    real_ephemeris_or_skip(monkeypatch)
    d = pd.read_csv(HORIZONS)
    assert len(d) == 319
    t = Time(d["mjd_tai"].to_numpy(), format="mjd", scale="tai")
    elong = util.solar_elongation_ndarray(d["ra"].to_numpy(), d["dec"].to_numpy(), t)
    diff = elong - d["sot"].to_numpy()
    assert np.max(np.abs(diff)) < 0.012, np.max(np.abs(diff))
    # (and the geometric elongation is not the apparent one)
    assert np.max(np.abs(diff)) > 0.005

    # the same vectors, aberrated (to first order, as v/c ~ 1e-4)
    r_obs, v_obs = util.observatory_barycentric_posvel("X05", t)
    s = util.sun_barycentric_position(t).to_value(u.au) - r_obs.to_value(u.au)
    beta = v_obs.to_value(u.km / u.s) / C_KMS
    p = _unit(d["ra"].to_numpy(), d["dec"].to_numpy())

    def aberrate(n):
        m = n / np.linalg.norm(n, axis=0) + beta
        return m / np.linalg.norm(m, axis=0)

    apparent = _angle_deg(aberrate(p), aberrate(s))
    assert np.max(np.abs(apparent - d["sot"].to_numpy())) < 0.001


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
    ph = 2 * np.pi * (obstime.tai.mjd - 60800.0) / 365.25
    r = np.stack([np.cos(ph), 0.917 * np.sin(ph), 0.398 * np.sin(ph)])
    return r * u.au, -r * 0.0172 * u.au / u.day


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
