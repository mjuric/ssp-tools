"""ASSIST's own perturbers (Pluto and the 16 sb441-n16 asteroids) in both
passes: detected at epoch, then integrated without their own force group
(the asteroids) or taken from the planet ephemeris (Pluto). See
docs/design/nearbysso.md, "ASSIST's own perturbers".

The 17 orbits are from the 2026-09-26 mpc_orbits snapshot
(tests/data/nearbysso_perturbers.json). Needs SSP_ASSIST_*; no network.
"""

from pathlib import Path

import numpy as np
import pytest

from test_nearbysso_propagate import (AU_KM, load_orbit_rows, needs_assist, nights,
                                      x05_state)

PERTURBERS_JSON = Path(__file__).parent / "data" / "nearbysso_perturbers.json"

#: designation -> ASSIST body id
BODY = {"1930 BM": 10, "A868 WA": 11, "A801 AA": 12, "A861 EB": 13, "A903 KB": 14, "A851 OA": 15,
        "A854 RA": 16, "A858 CA": 17, "A849 GA": 18, "A910 TC": 19, "A847 PA": 20, "A804 RA": 21,
        "A802 FA": 22, "A852 FA": 23, "A866 KA": 24, "A866 LA": 25, "A807 FA": 26}

#: a few epochs: the DP2-DS and AP-DS fixture nights, and a later one
EPOCHS = (60796, 61095, 61400)

pytestmark = needs_assist


@pytest.fixture(scope="module")
def ephem():
    from ssp.ephem_assist import open_ephem
    return open_ephem()


@pytest.fixture(scope="module")
def perturbers(ephem):
    return load_orbit_rows(ephem, PERTURBERS_JSON)


@pytest.fixture(scope="module")
def ordinary(ephem):
    return load_orbit_rows(ephem)


def _times():
    return np.concatenate([nights(m, m + 3) for m in EPOCHS])


def _sep_arcsec(a, b):
    a = a / np.linalg.norm(a, axis=1)[:, None]
    b = b / np.linalg.norm(b, axis=1)[:, None]
    return np.degrees(np.arctan2(np.linalg.norm(np.cross(a, b), axis=1), np.sum(a * b, axis=1))) * 3600


def _body_pos(ephem, b, t):
    return np.array([[p.x, p.y, p.z] for p in (ephem.get_particle(b, float(tk)) for tk in t)])


def test_detection(ephem, perturbers, ordinary):
    from ssp import ephem_assist as ea
    assert sorted(perturbers) == sorted(BODY)
    for name, o in perturbers.items():
        assert ea.self_perturber(o["state0"][:3], o["state0"][3:], o["epoch"], ephem) == BODY[name], name
    for name, o in ordinary.items():
        assert ea.self_perturber(o["state0"][:3], o["state0"][3:], o["epoch"], ephem) is None, name
    # Ceres' own state, displaced: 1,000 km at 3 km/s relative isn't Ceres
    # (a false match would be harmless for Ceres, catastrophic for Pluto)
    o = perturbers["A801 AA"]
    ep = float(o["epoch"])
    X, V = ea.ephemeris_states(12, ep, ephem)
    X, V = X[:, 0], V[:, 0]
    dx = np.array([1000.0 / AU_KM, 0, 0])
    kms = 86400 / AU_KM                                   # 1 km/s in AU/day
    assert ea.self_perturber(X + dx, V + np.array([0, 3 * kms, 0]), ep, ephem) is None
    assert ea.self_perturber(X + dx, V + np.array([0, 5e-4 * kms, 0]), ep, ephem) == 12
    assert ea.self_perturber(X + dx, V + np.array([0, 2e-3 * kms, 0]), ep, ephem) is None   # 2 m/s
    assert ea.self_perturber(X + 1.01e-4 * np.array([1, 0, 0]), V, ep, ephem) is None      # 15,100 km
    assert ea.self_perturber(X + 0.99e-4 * np.array([1, 0, 0]), V, ep, ephem) == 12
    assert ea.self_perturber(np.full(3, np.nan), V, ep, ephem) is None
    # and Pluto, whose velocity comes from the ephemeris
    Xp, Vp = ea.ephemeris_states(10, ep, ephem)
    assert ea.self_perturber(Xp[:, 0] + dx, Vp[:, 0], ep, ephem) == 10
    assert ea.self_perturber(Xp[:, 0] + dx, Vp[:, 0] + np.array([0, 3 * kms, 0]), ep, ephem) is None


def test_precise_pass(ephem, perturbers):
    """compute_ephemerides_one (the SSSource path) is within 0.1" of the
    ephemeris body; it used to be 16-152 deg off (the particle was slung
    off its own point mass). Pluto's is the ephemeris itself: DE440's body
    10 is the Pluto-system barycentre, which is what MPC's 1930 BM orbit
    refers to (Horizons, 2026-09-28: 1,534 km and 0.002 m/s from target 9,
    the barycentre, vs 2,334 km and 24 m/s from 999, Pluto itself)."""
    from astropy.time import Time

    from ssp import ephem_assist as ea

    t = _times()
    obs_pos, obs_vel = x05_state(t, ephem)
    times = Time(t + ea.MJD_J2000, format="mjd", scale="tdb").tai
    for name, o in perturbers.items():
        row = {k: o[k] for k in ("q", "e", "i", "node", "argperi", "peri_time", "epoch_mjd", "h", "g")}
        eph = ea.compute_ephemerides_one(None, times, None, ephem, row=row, obs_pos=obs_pos.T,
                                         obs_vel=obs_vel.T * AU_KM / 86400)
        sep = _sep_arcsec(eph.xx.T - obs_pos, _body_pos(ephem, BODY[name], t) - obs_pos)
        assert sep.max() < 0.1, (name, sep.max())
        if BODY[name] == ea.ASSIST_PLUTO:
            assert sep.max() < 1e-6
        assert np.all(np.isfinite(eph.ra_deg)) and np.all(np.isfinite(eph.mu_lon))


def test_coarse_pass(ephem, perturbers):
    """coarse() is ok, within 0.1" of the ephemeris body, and has a small,
    finite sigma (these orbits' uncertainties are milliarcseconds)."""
    from ssp.nearbysso import propagate

    t = _times()
    obs_pos, _ = x05_state(t, ephem)
    for name, o in perturbers.items():
        n0 = propagate.STEP_CAP_STOPS
        tr = propagate.coarse(o, t, obs_pos, ephem)
        assert propagate.STEP_CAP_STOPS == n0, name
        assert tr.ok.all(), name
        u = np.stack([np.cos(np.radians(tr.dec)) * np.cos(np.radians(tr.ra)),
                      np.cos(np.radians(tr.dec)) * np.sin(np.radians(tr.ra)),
                      np.sin(np.radians(tr.dec))], axis=1)
        sep = _sep_arcsec(u, _body_pos(ephem, BODY[name], t) - obs_pos)
        assert sep.max() < 0.1, (name, sep.max())
        smax = tr.sigma_major.max()
        assert np.all(np.isfinite(tr.sigma_major)) and smax < 1.0, (name, smax)
        assert np.all(np.isfinite(tr.rate_ra)) and np.all(np.isfinite(tr.cov))


def test_ordinary_orbits_unchanged(ephem, ordinary, monkeypatch):
    """For an ordinary orbit the detection is the only difference: both
    passes are bitwise identical to running with it switched off."""
    from astropy.time import Time

    from ssp import ephem_assist as ea
    from ssp.nearbysso import propagate

    t = _times()
    obs_pos, obs_vel = x05_state(t, ephem)
    times = Time(t + ea.MJD_J2000, format="mjd", scale="tdb").tai

    def run():
        out = {}
        for name, o in ordinary.items():
            row = {k: o[k] for k in ("q", "e", "i", "node", "argperi", "peri_time", "epoch_mjd", "h", "g")}
            eph = ea.compute_ephemerides_one(None, times, None, ephem, row=row, obs_pos=obs_pos.T,
                                             obs_vel=obs_vel.T * AU_KM / 86400)
            out[name] = (propagate.coarse(o, t, obs_pos, ephem), eph)
        return out
    with_detection = run()
    monkeypatch.setattr(ea, "self_perturber", lambda *a, **k: None)
    without = run()
    for name in ordinary:
        for a, b in zip(with_detection[name][0], without[name][0]):
            np.testing.assert_array_equal(a, b)
        for a, b in zip(with_detection[name][1], without[name][1]):
            np.testing.assert_array_equal(a, b)
