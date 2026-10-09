"""util.wrap_ra_deg and util.sky_separation_arcsec, the numpy replacements for
SkyCoord in the SSObservation build, must be bitwise identical to astropy."""

import numpy as np
from astropy.coordinates import SkyCoord

from ssp import util


def skycoord_ra(ra):
    return SkyCoord(ra=ra, dec=np.zeros_like(ra), unit="deg", frame="icrs").ra.deg


def bitwise_equal(a, b):
    a, b = np.asarray(a, dtype=np.float64), np.asarray(b, dtype=np.float64)
    return a.shape == b.shape and np.array_equal(a.view(np.uint64), b.view(np.uint64))


def test_wrap_ra_edge_cases():
    edge = np.array([
        0.0, -0.0, 360.0, np.nextafter(360.0, 0), np.nextafter(360.0, 720), -1e-300, -1e-17, -360.0, 720.0,
        -720.5, 1e6 + 0.1, -1e6 - 0.1, 180.0, np.nan,
    ])
    assert bitwise_equal(util.wrap_ra_deg(edge), skycoord_ra(edge))
    # singly, too: astropy leaves in-range arrays untouched (-0.0 stays -0.0)
    for x in edge:
        assert bitwise_equal(util.wrap_ra_deg(np.array([x])), skycoord_ra(np.array([x]))), x
    inrange = np.array([-0.0, 0.0, 12.5, np.nextafter(360.0, 0), np.nan])
    assert bitwise_equal(util.wrap_ra_deg(inrange), skycoord_ra(inrange))


def test_wrap_ra_random():
    rng = np.random.default_rng(0)
    ra = np.concatenate([rng.uniform(-1000, 1000, 100_000), rng.uniform(0, 360, 1000)])
    assert bitwise_equal(util.wrap_ra_deg(ra), skycoord_ra(ra))
    # the input is not modified
    ra0 = ra.copy()
    util.wrap_ra_deg(ra)
    assert bitwise_equal(ra, ra0)


def random_pairs(rng, n):
    """Pairs of points: across the sky, near the poles, across the RA wrap,
    and at small (SSObservation-like) separations, some with RA outside
    [0, 360)."""
    ra1 = rng.uniform(0, 360, n)
    dec1 = np.degrees(np.arcsin(rng.uniform(-1, 1, n)))
    far = [ra1, dec1, rng.uniform(0, 360, n), np.degrees(np.arcsin(rng.uniform(-1, 1, n)))]

    pole = rng.choice([-1, 1], n) * (90 - rng.uniform(0, 1e-3, n))
    near_pole = [ra1, pole, rng.uniform(0, 360, n), np.clip(pole + rng.normal(0, 1e-4, n), -90, 90)]
    at_pole = [ra1, np.full(n, 90.0), rng.uniform(0, 360, n), 90 - rng.uniform(0, 1e-5, n)]

    ra_w = rng.uniform(-1e-3, 1e-3, n) % 360
    wrap = [ra_w, dec1, (ra_w + rng.normal(0, 1e-3, n)) % 360, dec1 + rng.normal(0, 1e-3, n)]
    unwrapped = [ra_w - 360 * rng.integers(-2, 3, n), dec1, ra_w + rng.normal(0, 1e-3, n), dec1]

    small = [ra1, dec1, ra1 + rng.normal(0, 1 / 3600, n), np.clip(dec1 + rng.normal(0, 1 / 3600, n), -90, 90)]
    same = [ra1, dec1, ra1, dec1]
    return [np.concatenate(c) for c in zip(far, near_pole, at_pole, wrap, unwrapped, small, same)]


def test_sky_separation_matches_skycoord():
    ra1, dec1, ra2, dec2 = random_pairs(np.random.default_rng(1), 50_000)
    expect = SkyCoord(ra=ra1, dec=dec1, unit="deg").separation(SkyCoord(ra=ra2, dec=dec2, unit="deg")).arcsec
    got = util.sky_separation_arcsec(ra1, dec1, ra2, dec2)
    assert np.max(np.abs(got - expect)) <= 1e-9
    assert bitwise_equal(got, expect)
