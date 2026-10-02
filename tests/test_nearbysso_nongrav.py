"""WP N2: non-gravitational forces in NearbySSO's loader and coarse pass.

``tests/data/nearbysso_nongrav_orbits.json`` holds five ``mpc_orbits`` rows of the
2026-10-01 snapshot: the comets P/1991 T1 (145P; A1, A2) and P/2010 J5
(the catalog's largest A1), the Yarkovsky asteroids 2004 MN4 (Apophis) and
2018 CW2 (whose fitted A2 is exactly 0), and the gravity-only 1978 VN9.
The loader tests need no ASSIST; the propagation tests are skipped unless
``SSP_ASSIST_PLANETS`` and ``SSP_ASSIST_ASTEROIDS`` are set.
"""

import inspect
import json
import os
import tempfile
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ssp import ephem_assist as ea
from ssp import nongrav
from ssp.nearbysso import orbits as O
from ssp.nearbysso import propagate
from ssp.nearbysso._contract import ORBIT_DTYPE

HAVE_ASSIST = bool(os.environ.get("SSP_ASSIST_PLANETS") and os.environ.get("SSP_ASSIST_ASTEROIDS"))
needs_assist = pytest.mark.skipif(not HAVE_ASSIST, reason="SSP_ASSIST_PLANETS / SSP_ASSIST_ASTEROIDS not set")

DATA_JSON = Path(__file__).parent / "data" / "nearbysso_nongrav_orbits.json"
COMET, COMET_BIG, YARK, YARK_ZERO, GRAV = "P/1991 T1", "P/2010 J5", "2004 MN4", "2018 CW2", "1978 VN9"


_DATA = {}


def _data():
    """DATA_JSON as an mpc_orbits Parquet file (written once per session)."""
    if "path" not in _DATA:
        _DATA["dir"] = tempfile.TemporaryDirectory()
        path = Path(_DATA["dir"].name) / "nongrav_orbits.parquet"
        pq.write_table(pa.Table.from_pylist(json.loads(DATA_JSON.read_text())), path)
        _DATA["path"] = path
    return _DATA["path"]


class FakeSun:
    def get_particle(self, body, t):
        assert body == ea.ASSIST_SUN
        return SimpleNamespace(x=1e-3, y=2e-3, z=-1e-4, vx=1e-6, vy=-2e-6, vz=1e-8)


def _json_by_name():
    t = pq.read_table(_data())
    names = t["unpacked_primary_provisional_designation"].to_pylist()
    return dict(zip(names, t["mpc_orb_jsonb"].to_pylist()))


def _load(ephem, path=None, **kw):
    out = O.load_orbits(path or _data(), with_filter=False, ephem=ephem, verbose=False, **kw)
    return {str(r["designation"]): r for r in out}, out


# ---------------------------------------------------------------------------
# load_orbits
# ---------------------------------------------------------------------------

def test_load_nongrav_fields():
    stats = {}
    rows, out = _load(FakeSun(), stats=stats)
    assert stats["nongrav"] == dict(comet=2, yarkovsky=2, unparsed=0, cov_missing=0, cov_not_psd=0,
                                    cov_clipped=0)
    js = _json_by_name()
    for name, model in [(COMET, "comet"), (COMET_BIG, "comet"), (YARK, "yarkovsky"),
                        (YARK_ZERO, "yarkovsky")]:
        r = rows[name]
        ng = nongrav.nongrav_params(js[name])
        assert r["ng_model"] == model and r["has_cov"]
        np.testing.assert_array_equal(r["ng_A"], ng.A)
        np.testing.assert_array_equal(r["ng_fitted"], ng.fitted)
        sel = np.concatenate([np.arange(6), 6 + np.flatnonzero(ng.fitted)])
        cf = r["cov_full"]
        # the state block is cov0, the cross terms rotated on their state side
        np.testing.assert_array_equal(cf[:6, :6], r["cov0"])
        np.testing.assert_array_equal(cf[:6, 6:][:, ng.fitted], O.R6 @ ng.cov[:6, 6:])
        np.testing.assert_array_equal(cf[6:, :6][ng.fitted], (O.R6 @ ng.cov[:6, 6:]).T)
        np.testing.assert_array_equal(cf[np.ix_(sel[6:], sel[6:])], ng.cov[6:, 6:])
        # unfitted A's: zero rows and columns
        un = 6 + np.flatnonzero(~ng.fitted)
        assert np.all(cf[un] == 0) and np.all(cf[:, un] == 0)
        np.testing.assert_array_equal(cf, cf.T)
        # cov0 is the rotated CAR state block, as before
        np.testing.assert_allclose(r["cov0"], O.R6 @ ng.cov[:6, :6] @ O.R6.T, rtol=1e-12)
    np.testing.assert_allclose(rows[YARK]["ng_A"], [0, -2.869493e-14, 0], rtol=1e-6)
    assert rows[YARK_ZERO]["ng_fitted"][1] and rows[YARK_ZERO]["ng_A"][1] == 0
    g = rows[GRAV]
    assert g["ng_model"] == "" and not g["ng_fitted"].any() and np.all(g["ng_A"] == 0)
    np.testing.assert_array_equal(g["cov_full"][:6, :6], g["cov0"])
    assert np.all(g["cov_full"][6:] == 0) and np.all(g["cov_full"][:, 6:] == 0)
    assert out.dtype == ORBIT_DTYPE


def test_load_summary_line(capsys):
    O.load_orbits(_data(), with_filter=False, ephem=FakeSun())
    line = capsys.readouterr().out
    assert "non-grav 2 comets and 2 Yarkovsky (0 unknown models kept gravity-only, " \
           "0 without a usable full covariance)" in line


def _rewrite(tmp_path, edit):
    """The test data, with ``edit(name, dict) -> dict`` applied to each
    row's JSON."""
    t = pq.read_table(_data())
    names = t["unpacked_primary_provisional_designation"].to_pylist()
    js = [json.dumps(edit(n, json.loads(s))) for n, s in zip(names, t["mpc_orb_jsonb"].to_pylist())]
    t = t.set_column(t.schema.get_field_index("mpc_orb_jsonb"), "mpc_orb_jsonb", pa.array(js, pa.string()))
    path = tmp_path / "orbits.parquet"
    pq.write_table(t, path)
    return path


def test_load_unknown_model_kept_gravity_only(tmp_path):
    def edit(name, j):
        if name == COMET:
            j["CAR"]["coefficient_names"][7] = "DT"
        return j
    stats = {}
    rows, _ = _load(FakeSun(), _rewrite(tmp_path, edit), stats=stats)
    assert stats["nongrav"]["unparsed"] == 1 and stats["nongrav"]["comet"] == 1
    r = rows[COMET]
    assert r["ng_model"] == "" and r["has_cov"]
    assert np.all(r["cov_full"][6:] == 0)
    np.testing.assert_array_equal(r["cov_full"][:6, :6], r["cov0"])


def test_load_bad_full_covariance(tmp_path):
    """A fitted block that isn't PSD (the A variance negative) makes has_cov
    False, with NaN cov0 and cov_full; a missing A entry likewise."""
    def edit(name, j):
        c = j["CAR"]["covariance"]
        if name == YARK:
            c["cov66"] = -abs(c["cov66"])
        if name == COMET:
            c.pop("cov67")
        return j
    stats = {}
    rows, _ = _load(FakeSun(), _rewrite(tmp_path, edit), stats=stats)
    assert stats["nongrav"]["cov_not_psd"] == 1 and stats["nongrav"]["cov_missing"] == 1
    for name in (YARK, COMET):
        r = rows[name]
        assert r["ng_model"] and not r["has_cov"]
        assert np.all(np.isnan(r["cov0"])) and np.all(np.isnan(r["cov_full"]))
    assert rows[COMET_BIG]["has_cov"] and rows[GRAV]["has_cov"]


def test_parse_car_nongrav_matches_file():
    t = pq.read_table(_data())
    a = O.parse_car(t["mpc_orb_jsonb"], nthreads=2, chunk=2, nongrav=True)
    b = O.parse_car_file(_data(), nthreads=2, chunk=2, nongrav=True)
    assert sorted(a[3]) == sorted(b[3]) and len(a[3]) == 4
    names = t["unpacked_primary_provisional_designation"].to_pylist()
    assert sorted(names[k] for k in a[3]) == sorted([COMET, COMET_BIG, YARK, YARK_ZERO])
    # without nongrav, the three outputs as before
    assert len(O.parse_car(t["mpc_orb_jsonb"])) == 3


# ---------------------------------------------------------------------------
# coarse
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def ephem():
    return ea.open_ephem()


@pytest.fixture(scope="module")
def rows(ephem):
    return _load(ephem)[0]


def _plain(state0, epoch, t, ephem, ng):
    """(K, 6) states of plain ASSIST integrations with ``ng`` applied, with
    the coarse pass's step control, both directions from epoch."""
    import assist
    import rebound
    out = np.empty((len(t), 6))
    for idx in (np.flatnonzero(t >= epoch), np.flatnonzero(t < epoch)[::-1]):
        if not len(idx):
            continue
        sim = rebound.Simulation()
        sim.t = float(epoch)
        sim.add(x=state0[0], y=state0[1], z=state0[2], vx=state0[3], vy=state0[4], vz=state0[5])
        ax = assist.Extras(sim, ephem)
        nongrav.apply(ax, ng)
        sim.ri_ias15.adaptive_mode = 2
        for k in idx:
            ax.integrate_or_interpolate(float(t[k]))
            p = sim.particles[0]
            out[k] = (p.x, p.y, p.z, p.vx, p.vy, p.vz)
    return out


def _times(r):
    return float(r["epoch"]) + np.array([-365.0, -100.0, -10.0, 10.0, 100.0, 365.0])


def _coarse(r, t, ephem):
    ph = {}
    tr = propagate.coarse(r, t, np.zeros((len(t), 3)), ephem, _phi=ph)
    assert tr.ok.all()
    return tr, ph


@needs_assist
def test_gravity_only_unchanged(rows, ephem):
    """Gravity-only: dA is 0, C(t) is Phi cov0 Phi^T exactly, and the states
    are those of a plain integration without non-gravs."""
    r = rows[GRAV]
    t = _times(r)
    tr, ph = _coarse(r, t, ephem)
    assert np.all(ph["dA"] == 0) and ph["dA"].shape == (len(t), 6, 3)
    Phi = ph["phi"]
    c = Phi @ r["cov0"] @ np.transpose(Phi, (0, 2, 1))
    np.testing.assert_array_equal(tr.cov, 0.5 * (c + np.transpose(c, (0, 2, 1))))
    np.testing.assert_array_equal(ph["state"], _plain(r["state0"], r["epoch"], t, ephem, nongrav.NONE))


@needs_assist
@pytest.mark.parametrize("name", [COMET, COMET_BIG, YARK, YARK_ZERO])
def test_states_with_nongrav(rows, ephem, name):
    """The coarse states are a plain integration's with ssp.nongrav.apply
    (the variational particles don't change the step control), and differ
    from gravity-only ones (but for 2018 CW2, A2 = 0)."""
    r = rows[name]
    t = _times(r)
    _, ph = _coarse(r, t, ephem)
    ng = nongrav.from_orbit(r)
    np.testing.assert_array_equal(ph["state"], _plain(r["state0"], r["epoch"], t, ephem, ng))
    d = np.abs(ph["state"][:, :3] - _plain(r["state0"], r["epoch"], t, ephem, nongrav.NONE)[:, :3]).max()
    if name == YARK_ZERO:
        assert d == 0
    else:
        assert d > 1e-9             # > 150 m


@needs_assist
@pytest.mark.parametrize("name", [COMET, COMET_BIG, YARK, YARK_ZERO])
def test_dA_vs_finite_difference(rows, ephem, name):
    """d state / d A from the variational particles against central
    differences of plain integrations. The step is 1000 sigma(A): the
    trajectory is linear in A to well beyond it, and the differences of
    plain integrations carry IAS15's step-to-step noise (~1e-14 AU), which
    swamps smaller steps (the agreement improves as the step grows, from
    1e-3 at 1 sigma to the 1e-6..1e-8 here)."""
    r = rows[name]
    t = _times(r)
    _, ph = _coarse(r, t, ephem)
    ng = nongrav.from_orbit(r)
    dA = ph["dA"]
    for i in range(3):
        if not ng.fitted[i]:
            continue
        h = 1000 * np.sqrt(r["cov_full"][6 + i, 6 + i])
        Ap, Am = ng.A.copy(), ng.A.copy()
        Ap[i] += h
        Am[i] -= h
        fd = (_plain(r["state0"], r["epoch"], t, ephem, ng._replace(A=Ap))
              - _plain(r["state0"], r["epoch"], t, ephem, ng._replace(A=Am))) / (2 * h)
        for rws in (slice(0, 3), slice(3, 6)):
            a, b = dA[:, rws, i], fd[:, rws]
            # relative to the largest, at +-1 yr (near the epoch dA is ~0)
            err = np.linalg.norm(a - b, axis=1) / np.linalg.norm(b, axis=1).max()
            assert err.max() < 1e-4, (name, i, rws, err)


@needs_assist
def test_phi_includes_nongrav_partials(rows, ephem):
    """ASSIST's variational equations include the non-grav acceleration's
    state partials: Phi(ng) - Phi(gravity-only) matches the same difference
    of central differences, for the comet with the largest A1."""
    r = rows[COMET_BIG]
    t = float(r["epoch"]) + np.array([100.0, 365.0])
    rg = r.copy()
    rg["ng_model"] = ""
    dphi = _coarse(r, t, ephem)[1]["phi"] - _coarse(rg, t, ephem)[1]["phi"]
    ng = nongrav.from_orbit(r)
    h = np.array([1e-6] * 3 + [1e-8] * 3)
    fd = np.empty((2, len(t), 6, 6))
    for n, g in enumerate((ng, nongrav.NONE)):
        for k in range(6):
            dp = np.zeros(6)
            dp[k] = h[k]
            fd[n, :, :, k] = (_plain(r["state0"] + dp, r["epoch"], t, ephem, g)
                              - _plain(r["state0"] - dp, r["epoch"], t, ephem, g)) / (2 * h[k])
    dfd = fd[0] - fd[1]
    assert np.abs(dfd[-1]).max() > 1e-2          # the effect is there to see
    assert np.abs(dphi - dfd).max() < 1e-3 * np.abs(dfd).max()


@needs_assist
@pytest.mark.parametrize("name", [COMET, YARK])
def test_covariance_includes_A(rows, ephem, name):
    """C(t) = J cov_full J^T, J = [Phi | dA] with the unfitted columns 0,
    and it isn't the state-only Phi cov0 Phi^T."""
    r = rows[name]
    t = float(r["epoch"]) + np.array([-1095.0, 365.0, 1095.0])
    tr, ph = _coarse(r, t, ephem)
    ng = nongrav.from_orbit(r)
    J = np.concatenate([ph["phi"], np.where(ng.fitted, ph["dA"], 0.0)], axis=2)
    c = J @ r["cov_full"] @ np.transpose(J, (0, 2, 1))
    np.testing.assert_allclose(tr.cov, 0.5 * (c + np.transpose(c, (0, 2, 1))), rtol=1e-12, atol=0)
    cs = ph["phi"] @ r["cov0"] @ np.transpose(ph["phi"], (0, 2, 1))
    ratio = np.sqrt(np.trace(tr.cov[:, :3, :3], axis1=1, axis2=2) / np.trace(cs[:, :3, :3], axis1=1, axis2=2))
    assert np.abs(ratio - 1).max() > 1e-3, ratio
    assert np.all(np.isfinite(tr.sigma_major))


@needs_assist
def test_covariance_monte_carlo(rows, ephem):
    """A small Monte Carlo over (state0, A) for 145P at +-3 years: the
    sample position covariance's largest sigma matches C(t)'s to within the
    sampling error (200 draws: ~5% 1-sigma), where the state-only sigma is
    off by more."""
    r = rows[COMET]
    t = float(r["epoch"]) + np.array([-1095.0, 1095.0])
    tr, ph = _coarse(r, t, ephem)
    ng = nongrav.from_orbit(r)
    sel = np.concatenate([np.arange(6), 6 + np.flatnonzero(ng.fitted)])
    draws = np.random.default_rng(5).multivariate_normal(
        np.concatenate([r["state0"], ng.A[ng.fitted]]), r["cov_full"][np.ix_(sel, sel)], size=200,
        method="eigh")
    pos = np.empty((len(draws), len(t), 3))
    for k, d in enumerate(draws):
        A = ng.A.copy()
        A[ng.fitted] = d[6:]
        pos[k] = _plain(d[:6], r["epoch"], t, ephem, ng._replace(A=A))[:, :3]
    smc = np.array([np.sqrt(np.linalg.eigvalsh(np.cov(pos[:, j].T))[-1]) for j in range(len(t))])
    spred = np.sqrt(np.linalg.eigvalsh(tr.cov[:, :3, :3])[:, -1])
    cs = ph["phi"] @ r["cov0"] @ np.transpose(ph["phi"], (0, 2, 1))
    sstate = np.sqrt(np.linalg.eigvalsh(cs[:, :3, :3])[:, -1])
    np.testing.assert_allclose(smc / spred, 1.0, atol=0.2)
    assert np.abs(sstate / smc - 1).max() > 0.2, (smc, spred, sstate)


@needs_assist
@pytest.mark.skipif("nongrav" not in inspect.signature(ea._propagate_one).parameters,
                    reason="ephem_assist._propagate_one has no nongrav= yet (WP N1)")
@pytest.mark.parametrize("name", [COMET, YARK])
def test_coarse_vs_precise_pass(rows, ephem, name):
    """The coarse states agree with the precise pass's _propagate_one with
    the same non-gravs (it uses a tighter IAS15 epsilon)."""
    r = rows[name]
    t = _times(r)
    _, ph = _coarse(r, t, ephem)
    s0 = r["state0"]
    X, V = ea._propagate_one(s0[:3], s0[3:], r["epoch"], t, ephem, nongrav=nongrav.from_orbit(r))
    np.testing.assert_allclose(ph["state"][:, :3], X.T, rtol=0, atol=2e-8)
