"""The precise pass's IAS15 step control (ephem_assist.PRECISE_*): pinned
in every integration path of _propagate_one, and accurate through a
moderate close approach where plain adaptive_mode 2 is ~100 mas off.

2025 QD (2026-09-26 mpc_orbits snapshot, tests/data/ephem_close_approach.json)
passes 0.014 AU from the Earth on MJD 60899. Needs SSP_ASSIST_*; no network.
"""

from pathlib import Path

import numpy as np
import pytest

from test_nearbysso_propagate import load_orbit_rows, needs_assist, nights, x05_state

pytestmark = needs_assist

QD_JSON = Path(__file__).parent / "data" / "ephem_close_approach.json"
PERTURBERS_JSON = Path(__file__).parent / "data" / "nearbysso_perturbers.json"
MAS = 206264806.2


@pytest.fixture(scope="module")
def ephem():
    from ssp.ephem_assist import open_ephem
    return open_ephem()


def _capture(monkeypatch):
    """Record the simulations _propagate_one attaches ASSIST to."""
    import assist
    sims = []
    real = assist.Extras.__init__

    def init(self, sim, eph):
        real(self, sim, eph)
        sims.append(sim)
    monkeypatch.setattr(assist.Extras, "__init__", init)
    return sims


def test_step_control_pinned(ephem, monkeypatch):
    from ssp import ephem_assist as ea

    import rebound

    assert ea.PRECISE_ADAPTIVE_MODE == 2 and ea.PRECISE_EPSILON == 1e-11
    m2 = rebound.Simulation()
    m2.ri_ias15.adaptive_mode = 2        # REBOUND reports it by name ("prs23")
    mode2 = m2.ri_ias15.adaptive_mode
    ordinary = load_orbit_rows(ephem)["2007 VY347"]
    pert = load_orbit_rows(ephem, PERTURBERS_JSON)
    cases = [(ordinary, {}),                                            # the default forces
             (pert["A801 AA"], {}),                                    # Ceres: no asteroid forces
             (pert["1930 BM"], dict(perturber=10, integrate_pluto=True))]  # Pluto: no planets
    for o, kw in cases:
        sims = _capture(monkeypatch)
        s = o["state0"]
        ea._propagate_one(s[:3], s[3:], o["epoch"], np.array([o["epoch"] + 30.0]), ephem, **kw)
        assert len(sims) == 1
        assert sims[0].ri_ias15.adaptive_mode == mode2
        assert sims[0].ri_ias15.epsilon == 1e-11
        monkeypatch.undo()


def _run(o, t, ephem, mode, eps):
    import assist
    import rebound
    sim = rebound.Simulation()
    sim.t = float(o["epoch"])
    ax = assist.Extras(sim, ephem)
    s = o["state0"]
    sim.add(x=s[0], y=s[1], z=s[2], vx=s[3], vy=s[4], vz=s[5])
    sim.ri_ias15.adaptive_mode = mode
    sim.ri_ias15.epsilon = eps
    X = np.empty((len(t), 3))
    for k, tk in enumerate(t):
        ax.integrate_or_interpolate(float(tk))
        p = sim.particles[0]
        X[k] = (p.x, p.y, p.z)
    return X


def test_close_approach_accuracy(ephem):
    """2025 QD from its epoch (MJD 61200) back through its 0.014 AU pass:
    _propagate_one stays within 1 mas (topocentric, X05) of a mode-2,
    epsilon 1e-15 reference; plain mode 2 (epsilon 1e-9) is ~100 mas off."""
    from ssp import ephem_assist as ea

    o = load_orbit_rows(ephem, QD_JSON)["2025 QD"]
    t = np.sort(np.concatenate([nights(60790, 61200, 5.0), nights(60897, 60902, 1 / 24)]))
    obs, _ = x05_state(t, ephem)
    s = o["state0"]
    X, _ = ea._propagate_one(s[:3], s[3:], o["epoch"], t, ephem)
    ref = _run(o, t, ephem, 2, 1e-15)
    plain = _run(o, t, ephem, 2, 1e-9)

    def sep(a, b):
        a, b = a - obs, b - obs
        return np.arctan2(np.linalg.norm(np.cross(a, b), axis=1), np.sum(a * b, axis=1)) * MAS
    assert np.min(np.linalg.norm(ref - obs, axis=1)) < 0.015
    assert sep(X.T, ref).max() < 1.0
    assert sep(plain, ref).max() > 20.0
