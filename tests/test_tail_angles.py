"""ssp.ephem_assist.tail_position_angles (docs/design/tail-angles.md; the
contract in ssp/ssobservation_contract.py, "Tail position angles"), on
synthetic geometry with known answers."""

import numpy as np
import pytest

from ssp.ephem_assist import tail_position_angles, tail_position_angles_f32


def _unit(ra, dec):
    a, d = np.radians(ra), np.radians(dec)
    return np.array([np.cos(d) * np.cos(a), np.cos(d) * np.sin(a), np.sin(d)])


def _basis(ra, dec):
    a, d = np.radians(ra), np.radians(dec)
    north = np.array([-np.sin(d) * np.cos(a), -np.sin(d) * np.sin(a), np.cos(d)])
    east = np.array([-np.sin(a), np.cos(a), 0.0 * a])
    return north, east


def _pa(w, topo, v=None):
    """(anti_sun, anti_motion) of one helio_pos w (and helio_vel v)."""
    v = np.zeros(3) if v is None else v
    s, m = tail_position_angles(np.asarray(w)[:, None], np.asarray(v)[:, None], np.asarray(topo)[:, None])
    assert s.shape == m.shape == (1,) and s.dtype == m.dtype == np.float64
    return s[0], m[0]


@pytest.mark.parametrize("ra, dec", [(30.0, 20.0), (200.0, -45.0), (0.0, 0.0), (123.4, 80.0),
                                     (359.9999999, 5.0), (1e-7, -5.0), (77.0, 89.9999), (300.0, -89.9999)])
def test_cardinal_directions(ra, dec):
    """North -> 0, east -> 90, south -> 180, west -> 270; for the anti-Sun
    vector, and (with the sign flipped) the heliocentric velocity."""
    topo = 2.7 * _unit(ra, dec)
    north, east = _basis(ra, dec)
    for w, want in ((north, 0.0), (east, 90.0), (-north, 180.0), (-east, 270.0),
                    ((north + east) / np.sqrt(2), 45.0), ((north - east) / np.sqrt(2), 315.0)):
        # (a component along the line of sight doesn't change the angle)
        s, m = _pa(3.1 * w + 0.7 * _unit(ra, dec), topo, -5.0 * w)
        for got in (s, m):
            assert 0.0 <= got < 360.0
            d = (got - want + 180.0) % 360.0 - 180.0
            assert abs(d) < 1e-6, (ra, dec, want, got)


def test_anti_motion_sign():
    """anti_motion_pa is the angle of -helio_vel: a velocity due north
    gives 180, due east 270."""
    topo = _unit(40.0, 10.0)
    north, east = _basis(40.0, 10.0)
    assert _pa(north, topo, north)[1] == pytest.approx(180.0, abs=1e-9)
    assert _pa(north, topo, east)[1] == pytest.approx(270.0, abs=1e-9)
    assert _pa(north, topo, -east)[1] == pytest.approx(90.0, abs=1e-9)
    # the anti-Sun angle is helio_pos's own
    assert _pa(north, topo, east)[0] == pytest.approx(0.0, abs=1e-9)


def test_exact_pole():
    """At the exact pole, the basis is the contract's at alpha = atan2(0, 0)
    = 0: north = (-1, 0, 0) at the north pole, (1, 0, 0) at the south one,
    east = (0, 1, 0)."""
    assert _pa([-1.0, 0, 0], [0, 0, 3.0])[0] == 0.0
    assert _pa([0, 1.0, 0], [0, 0, 3.0])[0] == pytest.approx(90.0, abs=1e-12)
    assert _pa([1.0, 0, 0], [0, 0, -3.0])[0] == 0.0
    assert _pa([0, 1.0, 0], [0, 0, -3.0])[0] == pytest.approx(90.0, abs=1e-12)


def test_ra_wrap():
    """Either side of RA 0, the same sky direction gives the same angle, and
    an angle just west of north is just below 360, never 360."""
    for ra in (359.9999999, 0.0, 1e-7):
        topo = _unit(ra, 12.0)
        north, east = _basis(ra, 12.0)
        assert _pa(east, topo)[0] == pytest.approx(90.0, abs=1e-9)
        s = _pa(north - 1e-9 * east, topo)[0]
        assert 359.99 < s < 360.0
    # a tiny negative angle: x % 360 would be 360.0
    topo = _unit(0.0, 0.0)
    s = _pa([0.0, -1e-300, 1.0], topo)[0]
    assert 0.0 <= s < 360.0


def test_against_astropy_position_angle():
    """Random geometry against astropy's position angle of a point a small
    step along the projected vector (ICRS)."""
    from astropy.coordinates import SkyCoord
    import astropy.units as u

    rng = np.random.default_rng(5)
    n = 300
    topo = rng.normal(size=(3, n)) * rng.uniform(0.1, 5, n)
    hp = rng.normal(size=(3, n)) * 2.5
    hv = rng.normal(size=(3, n)) * 20.0
    s, m = tail_position_angles(hp, hv, topo)
    uu = topo / np.linalg.norm(topo, axis=0)
    c0 = SkyCoord(*uu, representation_type="cartesian", frame="icrs")
    for w, got in ((hp, s), (-hv, m)):
        perp = w - np.sum(w * uu, axis=0) * uu
        p = uu + 1e-7 * perp / np.linalg.norm(perp, axis=0)
        c1 = SkyCoord(*p, representation_type="cartesian", frame="icrs")
        want = c0.position_angle(c1).to_value(u.deg)
        d = (got - want + 180.0) % 360.0 - 180.0
        assert np.abs(d).max() < 1e-5
        assert (got >= 0).all() and (got < 360).all()


def test_nan_rules():
    topo = np.array([[1.0, 1.0, 1.0, np.nan, 1.0],
                     [0.0, 0.0, 0.0, 0.0, 0.0],
                     [0.0, 0.0, 0.0, 0.0, 0.0]])
    hp = np.array([[2.0, 0.0, 0.0, 0.0, np.nan],      # along the line of sight
                   [0.0, 0.0, 1.0, 1.0, 1.0],         # (1: zero vector)
                   [0.0, 0.0, 0.0, 0.0, 0.0]])
    hv = np.array([[0.0, 0.0, -3.0, 0.0, 0.0],        # 2: hv along the line of sight
                   [1.0, 1.0, 0.0, 1.0, 1.0],
                   [0.0, 0.0, 0.0, 0.0, 0.0]])
    s, m = tail_position_angles(hp, hv, topo)
    assert np.isnan(s[:2]).all() and s[2] == pytest.approx(90.0)
    assert m[0] == pytest.approx(270.0) and m[1] == pytest.approx(270.0) and np.isnan(m[2])
    # a NaN in any input of a row makes both NaN
    assert np.isnan(s[3:]).all() and np.isnan(m[3:]).all()
    s, m = tail_position_angles(np.array([[0.0], [1.0], [0.0]]), np.array([[np.nan], [0.0], [0.0]]),
                                np.array([[1.0], [0.0], [0.0]]))
    assert np.isnan(s[0]) and np.isnan(m[0])


def test_empty_and_shape_errors():
    s, m = tail_position_angles(np.zeros((3, 0)), np.zeros((3, 0)), np.zeros((3, 0)))
    assert s.shape == m.shape == (0,)
    with pytest.raises(ValueError):
        tail_position_angles(np.zeros((2, 4)), np.zeros((2, 4)), np.zeros((2, 4)))


def test_f32():
    f = tail_position_angles_f32(np.array([0.0, 90.0, 359.99999999, 359.9, np.nan]))
    assert f.dtype == np.float32
    assert f[0] == 0 and f[1] == 90 and f[2] == 0 and f[3] == np.float32(359.9) and np.isnan(f[4])
