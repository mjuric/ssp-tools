"""The precise pass with MPC non-gravitational forces (WP N1 of
docs/design/nongrav.md): ssp.ephem_assist._propagate_one and
compute_ephemerides_one take a NonGrav; gravity-only orbits are bitwise
unchanged; SSSource passes each object's NonGrav through.

tests/data/nongrav_orbits.json holds three 2026-10-01 mpc_orbits rows (the
element columns and mpc_orb_jsonb): the comet P/2003 K2 (A1, A2) and the
Yarkovsky asteroids 2004 TG10 and 2001 QE96 (e = 0.03). The ASSIST tests
need SSP_ASSIST_*; nothing needs the network.
"""

import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ssp import nongrav as N
from ssp import sssource

from test_nearbysso_propagate import load_orbit_rows, needs_assist, x05_state

NG_JSON = Path(__file__).parent / "data" / "nongrav_orbits.json"
PERTURBERS_JSON = Path(__file__).parent / "data" / "nearbysso_perturbers.json"
AU_KM = 149597870.7
COMET, YARK, YARK_CIRC = "P/2003 K2", "2004 TG10", "2001 QE96"


def _rows():
    return {r["unpacked_primary_provisional_designation"]: r for r in json.loads(NG_JSON.read_text())}


@pytest.fixture(scope="module")
def ephem():
    from ssp.ephem_assist import open_ephem
    return open_ephem()


def _state0(row, ephem):
    """(X0, V0, t0) of an mpc_orbits row, as compute_ephemerides_one has it."""
    from astropy.time import Time

    from ssp import ephem_assist as ea
    t0 = Time(float(row["epoch_mjd"]), format="mjd", scale="tt").tdb.mjd - ea.MJD_J2000
    sun = ephem.get_particle(ea.ASSIST_SUN, t0)
    X0, V0 = ea.elements_row_to_bary_icrf(row, np.array([sun.x, sun.y, sun.z]),
                                          np.array([sun.vx, sun.vy, sun.vz]))
    return X0, V0, t0


def _propagate(row, ephem, t, ng=N.NONE):
    from ssp import ephem_assist as ea
    X0, V0, t0 = _state0(row, ephem)
    return ea._propagate_one(X0, V0, t0, t0 + np.asarray(t, dtype=np.float64), ephem, nongrav=ng)


def _reference(X0, V0, t0, t, ephem):
    """The gravity-only integration exactly as _propagate_one did it before
    non-gravs (for ordinary orbits)."""
    import assist
    import rebound

    from ssp import ephem_assist as ea
    sim = rebound.Simulation()
    sim.t = float(t0)
    ax = assist.Extras(sim, ephem)
    sim.ri_ias15.adaptive_mode = ea.PRECISE_ADAPTIVE_MODE
    sim.ri_ias15.epsilon = ea.PRECISE_EPSILON
    sim.add(x=float(X0[0]), y=float(X0[1]), z=float(X0[2]),
            vx=float(V0[0]), vy=float(V0[1]), vz=float(V0[2]))
    order = np.argsort(t)
    X = np.empty((3, len(t)))
    V = np.empty((3, len(t)))
    for k in order:
        ax.integrate_or_interpolate(float(t[k]))
        p = sim.particles[0]
        X[:, k] = (p.x, p.y, p.z)
        V[:, k] = (p.vx, p.vy, p.vz)
    return X, V


def _no_apply(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("ssp.nongrav.apply called for a gravity-only orbit")
    monkeypatch.setattr(N, "apply", boom)


# --------------------------------------------------------------------------
# ASSIST itself
# --------------------------------------------------------------------------

@needs_assist
def test_assist_defaults(ephem):
    """NON_GRAVITATIONAL is on by default, with no particle_params and the
    1/r^2 g(r) (what the Yarkovsky model relies on)."""
    import assist
    import rebound
    ax = assist.Extras(rebound.Simulation(), ephem)
    assert "NON_GRAVITATIONAL" in ax.forces
    assert not ax._particle_params
    assert {k: getattr(ax, k) for k in N.G_OF_R["yarkovsky"]} == N.G_OF_R["yarkovsky"]


# --------------------------------------------------------------------------
# Gravity only: bitwise unchanged
# --------------------------------------------------------------------------

@needs_assist
@pytest.mark.parametrize("which", ["ordinary", "comet_elements"])
def test_gravity_only_bitwise(ephem, monkeypatch, which):
    from ssp import ephem_assist as ea
    if which == "ordinary":
        o = load_orbit_rows(ephem)["2007 VY347"]
        X0, V0, t0 = o["state0"][:3], o["state0"][3:], float(o["epoch"])
    else:   # a comet's elements, integrated without its non-gravs
        X0, V0, t0 = _state0(_rows()[COMET], ephem)
    t = t0 + np.array([-400.0, -30.0, 0.5, 20.0, 365.0])
    ref = _reference(X0, V0, t0, t, ephem)
    _no_apply(monkeypatch)
    for kw in ({}, dict(nongrav=N.NONE), dict(nongrav=None),
               dict(nongrav=N.NonGrav(np.zeros(3), "", np.zeros(3, bool), None))):
        X, V = ea._propagate_one(X0, V0, t0, t, ephem, **kw)
        assert X.tobytes() == ref[0].tobytes() and V.tobytes() == ref[1].tobytes(), kw


@needs_assist
def test_compute_ephemerides_one_gravity_only_bitwise(ephem, monkeypatch):
    from astropy.time import Time

    from ssp import ephem_assist as ea
    row = _rows()[YARK]
    t = float(row["epoch_mjd"]) + np.linspace(-300, 100, 7)
    times = Time(t, format="mjd", scale="tai")
    obs_pos, obs_vel = x05_state(times.tdb.mjd - ea.MJD_J2000, ephem)
    kw = dict(row=row, obs_pos=obs_pos.T, obs_vel=obs_vel.T * AU_KM / 86400.0)
    a = ea.compute_ephemerides_one(YARK, times, None, ephem, **kw)
    _no_apply(monkeypatch)
    b = ea.compute_ephemerides_one(YARK, times, None, ephem, nongrav=N.NONE, **kw)
    for f in a._fields:
        assert np.asarray(getattr(a, f)).tobytes() == np.asarray(getattr(b, f)).tobytes(), f
    monkeypatch.undo()
    c = ea.compute_ephemerides_one(YARK, times, None, ephem, nongrav=N.nongrav_params(row["mpc_orb_jsonb"]),
                                   **kw)
    sep = ea.util.sky_separation_arcsec(a.ra_deg, a.dec_deg, c.ra_deg, c.dec_deg)
    assert np.all(sep[np.abs(t - row["epoch_mjd"]) > 50] > 0)


@needs_assist
def test_self_perturber_ignores_nongrav(ephem, monkeypatch):
    """Ceres (one of ASSIST's own perturbers) with a NonGrav: integrated as
    without one, and apply is never called."""
    from ssp import ephem_assist as ea
    o = load_orbit_rows(ephem, PERTURBERS_JSON)["A801 AA"]
    X0, V0, t0 = o["state0"][:3], o["state0"][3:], float(o["epoch"])
    t = t0 + np.array([-100.0, 50.0])
    a = ea._propagate_one(X0, V0, t0, t, ephem)
    _no_apply(monkeypatch)
    ng = N.nongrav_params(_rows()[COMET]["mpc_orb_jsonb"])
    b = ea._propagate_one(X0, V0, t0, t, ephem, nongrav=ng)
    assert a[0].tobytes() == b[0].tobytes() and a[1].tobytes() == b[1].tobytes()


# --------------------------------------------------------------------------
# With non-gravs
# --------------------------------------------------------------------------

@needs_assist
def test_apply_after_extras(ephem, monkeypatch):
    """apply gets the simulation's Extras once, after it is attached (with
    the forces already set), and the integration has the comet g(r)."""
    from ssp import ephem_assist as ea
    calls = []
    real = N.apply

    def rec(ax, ng):
        calls.append((ax, ng, list(ax.forces)))
        real(ax, ng)
    monkeypatch.setattr(N, "apply", rec)
    row = _rows()[COMET]
    ng = N.nongrav_params(row["mpc_orb_jsonb"])
    _propagate(row, ephem, [10.0], ng)
    assert len(calls) == 1
    ax, got, forces = calls[0]
    assert got is ng and "NON_GRAVITATIONAL" in forces
    assert {k: getattr(ax, k) for k in N.G_OF_R["comet"]} == N.G_OF_R["comet"]
    assert ea._propagate_one.__defaults__[-1] is N.NONE


@needs_assist
def test_comet_plausible(ephem):
    """P/2003 K2 (q 0.52 AU, perihelion 200 d before its epoch; A1 2.3e-9,
    A2 6e-11 au/d^2): the non-gravs, strong near the Sun, move it by
    thousands of km through perihelion and by only km in the six months after
    its epoch (r > 1.4 AU, where g(r) is small); linearly in A and with the
    opposite sign for -A."""
    row = _rows()[COMET]
    ng = N.nongrav_params(row["mpc_orb_jsonb"])
    assert ng.model == "comet" and ng.A[0] > 1e-9
    t = np.array([-365.0, -200.0, -30.0, 30.0, 180.0])
    X0, _ = _propagate(row, ephem, t)
    d1 = _propagate(row, ephem, t, ng)[0] - X0
    d2 = _propagate(row, ephem, t, ng._replace(A=2 * ng.A))[0] - X0
    dm = _propagate(row, ephem, t, ng._replace(A=-ng.A))[0] - X0
    km = np.linalg.norm(d1, axis=0) * AU_KM
    assert np.all(km[[0, 1]] > 1e3) and np.all(km < 1e5), km
    assert 1 < km[4] < 100 and km[3] < km[4] and km[2] < km[1], km
    np.testing.assert_allclose(d2, 2 * d1, rtol=0.02, atol=1e-9)
    np.testing.assert_allclose(dm, -d1, rtol=0.02, atol=1e-9)


@needs_assist
def test_yarkovsky_along_track_drift(ephem):
    """2001 QE96 (e 0.03, a 1.31 AU; A2 -1.44e-13 au/d^2 with g = 1/r^2): a
    transverse T shifts a near-circular orbit along the track by -1.5 T t^2
    (a secular drift; the periodic terms, a few T/n^2 ~ 3e-9 AU, are ~2% of
    it at 1,000 days). This checks the units (1e-10 au/d^2), the direction
    (along the motion) and the sign."""
    from ssp import ephem_assist as ea
    row = _rows()[YARK_CIRC]
    ng = N.nongrav_params(row["mpc_orb_jsonb"])
    assert ng.model == "yarkovsky"
    np.testing.assert_allclose(ng.A[1], -1.44468888e-13, rtol=1e-6)
    t = np.array([-1000.0, 1000.0])
    X0, V0 = _propagate(row, ephem, t)
    X1, _ = _propagate(row, ephem, t, ng)
    _, _, t0 = _state0(row, ephem)
    sun = [ephem.get_particle(ea.ASSIST_SUN, t0 + tk) for tk in t]
    r = np.linalg.norm(X0 - np.array([[s.x, s.y, s.z] for s in sun]).T, axis=0)
    a = float(row["q"]) / (1 - float(row["e"]))
    vhat = V0 / np.linalg.norm(V0, axis=0)
    along = np.sum((X1 - X0) * vhat, axis=0)
    expect = -1.5 * ng.A[1] / a**2 * t**2
    assert np.all(expect > 0)                     # (A2 < 0: falls inward, runs ahead)
    np.testing.assert_allclose(along, expect, rtol=0.06)
    assert np.all(np.linalg.norm(X1 - X0, axis=0) < 1.2 * np.abs(along))
    assert np.all(r > 1.2) and np.all(r < 1.4)


#: Marsden, Sekanina & Yeomans (1973) water-ice g(r), written out here
#: independently of ssp.nongrav.G_OF_R.
MARSDEN = dict(alpha=0.1112620426, r0=2.808, nm=2.15, nn=5.093, nk=4.6142)


def _g(r, alpha, r0, nm, nn, nk):
    return alpha * (r / r0) ** -nm * (1 + (r / r0) ** nn) ** -nk


def test_g_of_r_pinned():
    """The comet g(r) constants, and its normalization g(1 au) = 1."""
    assert N.G_OF_R["comet"] == MARSDEN
    assert N.G_OF_R["yarkovsky"] == dict(alpha=1.0, r0=1.0, nm=2.0, nn=5.093, nk=0.0)
    assert abs(_g(1.0, **N.G_OF_R["comet"]) - 1.0) < 1e-8
    np.testing.assert_allclose(_g(2.808, **N.G_OF_R["comet"]), 0.1112620426 * 2 ** -4.6142, rtol=1e-12)
    for r in (0.3, 1.0, 2.5, 7.0):
        assert _g(r, **N.G_OF_R["yarkovsky"]) == pytest.approx(r ** -2, rel=1e-14)


@needs_assist
@pytest.mark.parametrize("model", ["comet", "yarkovsky"])
@pytest.mark.parametrize("axis", [0, 1, 2])
def test_acceleration_rtn(ephem, model, axis):
    """The non-gravitational acceleration at the epoch, from the symmetric
    second difference of short integrations with and without it, (d(+h) +
    d(-h)) / h^2, is A_i g(r) along the radial (A1), transverse (A2, along
    h x r, i.e. with the motion) or normal (A3, along r x v) heliocentric
    direction, with g(r) for the model written out independently. This pins
    the sign and direction of each component and the g(r) constants."""
    from ssp import ephem_assist as ea
    row = _rows()[COMET]          # (P/2003 K2's state; any orbit would do)
    X0, V0, t0 = _state0(row, ephem)
    sun = ephem.get_particle(ea.ASSIST_SUN, t0)
    r = X0 - np.array([sun.x, sun.y, sun.z])
    v = V0 - np.array([sun.vx, sun.vy, sun.vz])
    rhat = r / np.linalg.norm(r)
    hhat = np.cross(r, v) / np.linalg.norm(np.cross(r, v))
    that = np.cross(hhat, rhat)
    A = np.zeros(3)
    A[axis] = 1e-7
    ng = N.NonGrav(A, model, A != 0, None)
    h = 2.0
    t = np.array([-h, h])
    d = _propagate(row, ephem, t, ng)[0] - _propagate(row, ephem, t)[0]
    acc = (d[:, 0] + d[:, 1]) / h**2
    g = _g(np.linalg.norm(r), **(MARSDEN if model == "comet" else dict(alpha=1.0, r0=1.0, nm=2.0,
                                                                         nn=5.093, nk=0.0)))
    expect = A[axis] * g * (rhat, that, hhat)[axis]
    assert np.dot(that, v) > 0
    np.testing.assert_allclose(acc, expect, rtol=0, atol=1e-3 * abs(expect).max())

# --------------------------------------------------------------------------
# SSSource
# --------------------------------------------------------------------------

def _orbits_file(path, rows, row_group_size=2):
    pq.write_table(pa.table({
        "unpacked_primary_provisional_designation": [r[0] for r in rows],
        "mpc_orb_jsonb": pa.array([r[1] for r in rows], type=pa.string()),
    }), path, row_group_size=row_group_size)


def _car(extra=(), values=(), non_gravs=True):
    names = ["x", "y", "z", "vx", "vy", "vz", *extra]
    n = len(names)
    return json.dumps({
        "CAR": {"coefficient_names": names, "coefficient_values": [0.0] * 6 + list(values),
                "covariance": {f"cov{i}{k}": 1e-12 * (i == k) for i in range(n) for k in range(i, n)}},
        "non_grav_booleans": {"non_gravs": non_gravs}})


def test_load_nongravs(tmp_path, capsys):
    real = _rows()
    rows = [
        ("2020 AA", _car(non_gravs=False)),
        (COMET, real[COMET]["mpc_orb_jsonb"]),
        ("2020 BB", _car(["DT"], [1.0])),                    # unknown: gravity only, counted
        (YARK, real[YARK]["mpc_orb_jsonb"]),
        ("2020 CC", _car(non_gravs=True)),                   # flagged, but nothing fitted
        ("2020 DD", _car(["yarkovsky"], [-2.0])),            # not requested
        ("2020 EE", None),
        ("2020 FF", _car(["A1", "A2"], [1e-9, 2e-10])),
    ]
    p = tmp_path / "mpc_orbits.parquet"
    _orbits_file(p, rows)
    assert pq.ParquetFile(p).num_row_groups == 4
    want = ["2020 AA", COMET, "2020 BB", YARK, "2020 CC", "2020 EE", "2020 FF", "2020 ZZ", "", None]
    ng, n_err = sssource.load_nongravs(p, want)
    assert n_err == 1 and "2020 BB" in capsys.readouterr().err
    assert sorted(ng) == sorted([COMET, YARK, "2020 FF"])
    assert ng[COMET].model == "comet" and ng[YARK].model == "yarkovsky"
    for d in (COMET, YARK):
        np.testing.assert_array_equal(ng[d].A, N.nongrav_params(real[d]["mpc_orb_jsonb"]).A)
    np.testing.assert_array_equal(ng["2020 FF"].A, [1e-9, 2e-10, 0])
    assert sssource.load_nongravs(p, []) == ({}, 0)


def test_load_nongravs_no_json_column(tmp_path, capsys):
    p = tmp_path / "mpc_orbits.parquet"
    pq.write_table(pa.table({"unpacked_primary_provisional_designation": ["2020 AA"]}), p)
    assert sssource.load_nongravs(p, ["2020 AA"]) == ({}, 0)
    assert "mpc_orb_jsonb" in capsys.readouterr().err


def _recording_fake(record):
    from test_sssource_parallel import _fake_ephemerides

    def fake(provID, ephTimes, mpcorb, ephem, row=None, obs_pos=None, obs_vel=None, nongrav=None):
        record[provID] = nongrav
        e = _fake_ephemerides(provID, ephTimes, mpcorb, ephem, row=row, obs_pos=obs_pos, obs_vel=obs_vel)
        if nongrav is not None and nongrav.model:      # (visible from forked workers too)
            e.ra_deg = e.ra_deg + 1e3 * nongrav.A[1]
        return e
    return fake


@pytest.mark.parametrize("workers", [1, 3])
def test_compute_ephemerides_passes_nongrav(monkeypatch, workers):
    from test_sssource_parallel import _tables
    rec = {}
    monkeypatch.setattr(sssource, "compute_ephemerides_one", _recording_fake(rec))
    monkeypatch.setattr(sssource, "open_ephem", lambda: None)
    sss, obs_state, dia_eph, mpcorb = _tables()
    base = sss.copy()
    sssource.compute_ephemerides(base, obs_state, dia_eph, mpcorb, workers=1)
    desig = sorted(set(sss["designation"]))
    ngs = {desig[1]: N.NonGrav(np.array([0, 1e-3, 0]), "yarkovsky", np.array([0, 1, 0], bool), None),
           desig[5]: N.NonGrav(np.array([1e-9, 2e-3, 0]), "comet", np.array([1, 1, 0], bool), None)}
    rec.clear()
    sssource.compute_ephemerides(sss, obs_state, dia_eph, mpcorb, workers=workers, chunk_factor=2,
                                 nongravs=ngs)
    if workers == 1:
        assert set(rec) == set(desig)
        for d in desig:
            assert rec[d] is ngs[d] if d in ngs else rec[d] is N.NONE, d
    for d in desig:
        m = sss["designation"] == d
        shift = 1e3 * ngs[d].A[1] if d in ngs else 0.0
        np.testing.assert_allclose(sss["ephRa"][m], (base["ephRa"][m] + shift) % 360, atol=1e-9, err_msg=d)


def test_build_sssource_passes_nongrav(tmp_path, monkeypatch):
    """build_sssource reads each object's NonGrav from mpc_orbits'
    mpc_orb_jsonb and passes it to compute_ephemerides_one."""
    from test_sssource_widened import OBJECTS, _FakeEllipse, _fake_observatory, make_inputs
    rec = {}
    monkeypatch.setattr(sssource, "compute_ephemerides_one", _recording_fake(rec))
    monkeypatch.setattr(sssource, "open_ephem", lambda: None)
    monkeypatch.setattr(sssource.util, "observatory_barycentric_posvel", _fake_observatory)
    monkeypatch.setattr(sssource, "_ellipse", _FakeEllipse())
    make_inputs(tmp_path)
    t = pq.read_table(tmp_path / "mpc_orbits.parquet")
    js = {OBJECTS["A"][0]: _car(["yarkovski"], [-3.0]),
          OBJECTS["B"][0]: _car(["A1", "A2"], [1e-9, -2e-10]),
          OBJECTS["D"][0]: _car(non_gravs=False)}
    col = [js[d] for d in t["unpacked_primary_provisional_designation"].to_pylist()]
    pq.write_table(t.append_column("mpc_orb_jsonb", pa.array(col, type=pa.string())),
                   tmp_path / "mpc_orbits.parquet")
    sssource.build_sssource(tmp_path, tmp_path, workers=1)
    A, B, D = OBJECTS["A"][0], OBJECTS["B"][0], OBJECTS["D"][0]
    assert set(rec) == {A, B, D}
    assert rec[A].model == "yarkovsky"
    np.testing.assert_allclose(rec[A].A, [0, -3e-10, 0])
    assert rec[B].model == "comet"
    np.testing.assert_array_equal(rec[B].A, [1e-9, -2e-10, 0])
    assert rec[D] is N.NONE


def test_load_nongravs_flag_or_coefficients(tmp_path, capsys):
    """A row is parsed when either its non_gravs flag is true or its CAR
    has coefficients beyond vz; a disagreement is warned about."""
    rows = [
        ("2020 AA", _car(["yarkovsky"], [-2.0], non_gravs=False)),   # coefficients, no flag
        ("2020 BB", _car(non_gravs=True)),                           # flag, no coefficients
        ("2020 CC", _car(["A1", "A2"], [1e-9, 2e-10])),              # both
        ("2020 DD", _car(non_gravs=False)),                          # neither
    ]
    p = tmp_path / "mpc_orbits.parquet"
    _orbits_file(p, rows)
    ng, n_err = sssource.load_nongravs(p, [r[0] for r in rows])
    assert n_err == 0 and sorted(ng) == ["2020 AA", "2020 CC"]
    np.testing.assert_allclose(ng["2020 AA"].A, [0, -2e-10, 0])
    err = capsys.readouterr().err
    assert "2020 AA" in err and "2020 BB" in err and "2020 CC" not in err and "2020 DD" not in err


def test_load_nongravs_duplicates(tmp_path, capsys):
    """A designation on several rows: the last parseable fit wins, and a
    later unparseable or gravity-only row does not remove an earlier fit."""
    rows = [
        ("2020 AA", _car(["A1"], [1e-9])),
        ("2020 AA", _car(["A1"], [2e-9])),          # last fit: wins
        ("2020 AA", _car(["DT"], [1.0])),           # unparseable: counted, the fit kept
        ("2020 AA", _car(non_gravs=False)),         # gravity only: the fit kept
        ("2020 BB", _car(["DT"], [1.0])),           # unparseable alone: gravity only
    ]
    p = tmp_path / "mpc_orbits.parquet"
    _orbits_file(p, rows)
    ng, n_err = sssource.load_nongravs(p, ["2020 AA", "2020 BB"])
    assert n_err == 2 and sorted(ng) == ["2020 AA"]
    np.testing.assert_array_equal(ng["2020 AA"].A, [2e-9, 0, 0])
    err = capsys.readouterr().err
    assert "earlier row's fit kept" in err and "2020 BB" in err


def test_compute_ephemerides_batch_passes_nongrav(monkeypatch):
    from ssp import ephem_assist as ea
    rec = {}

    def fake(provID, ephTimes, mpcorb, ephem, observer_code="X05", nongrav=None, **kw):
        rec[provID] = nongrav
        return provID
    monkeypatch.setattr(ea, "compute_ephemerides_one", fake)
    monkeypatch.setattr(ea, "open_ephem", lambda *a: None)
    ng = N.NonGrav(np.array([1e-9, 0, 0]), "comet", np.array([1, 0, 0], bool), None)
    out = ea.compute_ephemerides_batch({"A": None, "B": None}, None, nongravs={"B": ng})
    assert out == {"A": "A", "B": "B"} and rec["A"] is N.NONE and rec["B"] is ng
    ea.compute_ephemerides_batch({"A": None}, None)
    assert rec["A"] is N.NONE
