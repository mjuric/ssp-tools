"""The parallel SSSource ephemerides (workers > 1) are identical to the
serial ones, bit for bit, and EPH_FIELDS lists exactly the fields
compute_sssource_entry writes."""

import os
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from ssp import sssource
from ssp.sssource import EPH_FIELDS, WORK_DTYPE, compute_ephemerides, compute_sssource_entry

# (as build_sssource makes it: the object key and the ephemeris columns)
SSS_DTYPE = WORK_DTYPE
OBS_DTYPE = [("dia_index", np.int64), ("obs_pos", np.float64, 3), ("obs_vel", np.float64, 3)]


def _fake_ephemerides(provID, ephTimes, mpcorb, ephem, row=None, obs_pos=None, obs_vel=None, nongrav=None):
    """A deterministic stand-in for compute_ephemerides_one (no ASSIST)."""
    t = ephTimes.tai.mjd - 60800.0
    k = float(row["q"])
    n = len(t)

    def vec(a):
        return np.array([a + np.sin(t * k), a * 0.5 + np.cos(t), 0.1 * a + t * 1e-3]) + obs_pos

    return SimpleNamespace(
        ra_deg=(k * 37.0 + t * 0.1) % 360 - 10, dec_deg=np.clip(k - 3 + 0.01 * t, -89, 89),
        mu_lon=0.1 * k + 0 * t, mu_lat=-0.05 * t, mu_total=np.hypot(0.1 * k, 0.05 * t),
        helio_pos=vec(k), helio_vel=vec(2 * k) + obs_vel, topo_pos=vec(k - 1), topo_vel=vec(k + 1) - obs_vel,
        phase_angle=np.full(n, 3.0 * k), H=float(row["h"]), G=float(row["g"]),
    )


def _tables(n_obj=40, seed=2, epoch=60800.0):
    """Synthetic sss (grouped by object, not sorted), obs_state, dia_eph
    and mpcorb, with very uneven observation counts."""
    rng = np.random.default_rng(seed)
    counts = rng.choice([1, 2, 3, 5, 8, 13, 40, 120], size=n_obj, p=[.1, .1, .15, .15, .2, .15, .1, .05])
    oids = rng.permutation(n_obj) * 7 + 3        # grouped, in no particular order
    n = int(counts.sum())
    sss = np.zeros(n, dtype=SSS_DTYPE)
    sss["ssObjectId"] = np.repeat(oids, counts)
    sss["designation"] = [f"2025 A{o:04d}" for o in sss["ssObjectId"]]

    dia_index = rng.permutation(n + 50)[:n]      # DiaSources in another order, with extra rows
    dia_eph = np.zeros(n + 50, dtype=[(c, np.float64) for c in ("midpointMjdTai", "ra", "dec")])
    dia_eph["midpointMjdTai"] = epoch + rng.uniform(0, 300, n + 50)
    dia_eph["ra"] = rng.uniform(0, 360, n + 50)
    dia_eph["dec"] = rng.uniform(-30, 30, n + 50)

    obs_state = np.zeros(n, dtype=OBS_DTYPE)
    obs_state["dia_index"] = dia_index
    # (roughly the Earth's orbit)
    ph = 2 * np.pi * (dia_eph["midpointMjdTai"][dia_index] - epoch) / 365.25
    obs_state["obs_pos"] = np.stack([np.cos(ph), 0.917 * np.sin(ph), 0.398 * np.sin(ph)], axis=1)
    obs_state["obs_vel"] = 29.8 * np.stack([-np.sin(ph), 0.917 * np.cos(ph), 0.398 * np.cos(ph)], axis=1)

    desig = [f"2025 A{o:04d}" for o in oids]
    q = rng.uniform(1.8, 3.0, n_obj)
    mpcorb = pd.DataFrame(dict(
        unpacked_primary_provisional_designation=desig, q=q, e=rng.uniform(0.01, 0.3, n_obj),
        i=rng.uniform(0, 25, n_obj), node=rng.uniform(0, 360, n_obj), argperi=rng.uniform(0, 360, n_obj),
        peri_time=epoch - rng.uniform(0, 1500, n_obj), epoch_mjd=epoch, h=rng.uniform(14, 20, n_obj), g=0.15,
    )).set_index("unpacked_primary_provisional_designation", drop=False)
    return sss, obs_state, dia_eph, mpcorb


@pytest.fixture
def fake_ephem(monkeypatch):
    # (inherited by the forked workers)
    monkeypatch.setattr(sssource, "compute_ephemerides_one", _fake_ephemerides)
    monkeypatch.setattr(sssource, "open_ephem", lambda: None)


def _run(workers, chunk_factor=8, covs=None, **kw):
    sss, obs_state, dia_eph, mpcorb = _tables(**kw)
    compute_ephemerides(sss, obs_state, dia_eph, mpcorb, workers=workers, chunk_factor=chunk_factor,
                        covs=covs)
    return sss


def _assert_identical(a, b):
    assert a.dtype == b.dtype
    for f in a.dtype.names:
        if a.dtype[f].kind == "O":
            assert list(a[f]) == list(b[f]), f
        else:
            assert a[f].tobytes() == b[f].tobytes(), f


@pytest.mark.parametrize("chunk_factor", [1, 8, 100])
def test_parallel_identical_to_serial(fake_ephem, chunk_factor):
    serial = _run(workers=1)
    assert np.all(np.isfinite(serial["ephRa"])) and np.all(serial["ephVmag"] != 0)
    _assert_identical(serial, _run(workers=3, chunk_factor=chunk_factor))


def test_parallel_few_objects(fake_ephem):
    # more workers than objects
    _assert_identical(_run(workers=1, n_obj=2, seed=3), _run(workers=4, n_obj=2, seed=3))


def test_ungrouped_input_fails(fake_ephem):
    sss, obs_state, dia_eph, mpcorb = _tables(n_obj=5)
    sss["ssObjectId"][0] = sss["ssObjectId"][-1]
    with pytest.raises(ValueError, match="grouped"):
        compute_ephemerides(sss, obs_state, dia_eph, mpcorb, workers=2)


def test_worker_exception_fails_the_build(monkeypatch):
    def boom(provID, *args, **kwargs):
        raise RuntimeError(f"ephemeris exploded for {provID}")
    monkeypatch.setattr(sssource, "compute_ephemerides_one", boom)
    monkeypatch.setattr(sssource, "open_ephem", lambda: None)
    sss, obs_state, dia_eph, mpcorb = _tables(n_obj=10, seed=5)
    with pytest.raises(RuntimeError, match="ephemeris exploded"):
        compute_ephemerides(sss, obs_state, dia_eph, mpcorb, workers=2)
    assert sssource._PARALLEL == {}


def test_eph_fields_are_what_compute_sssource_entry_writes(fake_ephem):
    sss, obs_state, dia_eph, mpcorb = _tables(n_obj=1, seed=6)
    # fill every field with a sentinel, then see which ones change
    for f in sss.dtype.names:
        if sss.dtype[f].kind == "f":
            sss[f] = -12345.0
        elif sss.dtype[f].kind in "iu":
            sss[f] = sss[f] if f == "ssObjectId" else 12345
    before = sss.copy()
    compute_sssource_entry(sss, obs_state, mpcorb, dia_eph, None)
    changed = [f for f in sss.dtype.names
               if sss.dtype[f].kind != "O" and sss[f].tobytes() != before[f].tobytes()]
    assert sorted(changed) == sorted(EPH_FIELDS)
    assert len(set(EPH_FIELDS)) == len(EPH_FIELDS)


class _FakeEllipse:
    """A stand-in for ssp.sssource_ellipse: records the inputs it gets
    (serial runs only: forked workers record in their own copy), counts one
    step-cap stop per call, and returns values made from its inputs."""

    def __init__(self):
        self.calls = []

    def load_orbit_covariances(self, path, designations, ephem, *, verbose=True):
        raise AssertionError("not used here")

    def ephemeris_ellipse(self, orbit, t_assist, obs_pos, topo_pos, ephem):
        self.calls.append(dict(orbit=orbit, t_assist=t_assist, obs_pos=obs_pos, topo_pos=topo_pos))
        sssource._propagate.STEP_CAP_STOPS += 1
        rng = np.linalg.norm(topo_pos, axis=1)
        return orbit["k"] * rng, 2 * orbit["k"] * rng, -orbit["k"] * t_assist * 1e-6


@pytest.fixture
def fake_ellipse(monkeypatch):
    fake = _FakeEllipse()
    monkeypatch.setattr(sssource, "_ellipse", fake)
    return fake


@pytest.fixture
def recorded_ephem(monkeypatch):
    """fake_ephem, also recording, per object, what compute_ephemerides_one
    got (obs_pos) and returned (topo_pos)."""
    rec = {}

    def fake(provID, ephTimes, mpcorb, ephem, row=None, obs_pos=None, obs_vel=None, nongrav=None):
        e = _fake_ephemerides(provID, ephTimes, mpcorb, ephem, row=row, obs_pos=obs_pos, obs_vel=obs_vel)
        rec[provID] = dict(obs_pos=np.array(obs_pos), topo_pos=np.array(e.topo_pos))
        return e
    monkeypatch.setattr(sssource, "compute_ephemerides_one", fake)
    monkeypatch.setattr(sssource, "open_ephem", lambda: None)
    return rec


def _covs(n_obj=40, seed=2, skip=3):
    """Fake orbit covariances for every object of _tables but every
    ``skip``-th, keyed by designation."""
    sss = _tables(n_obj=n_obj, seed=seed)[0]
    desig = sorted(set(sss["designation"]))
    return {d: {"k": 1e-5 * (j + 1), "designation": d} for j, d in enumerate(desig) if j % skip}


def test_ellipse_columns(fake_ephem, fake_ellipse):
    covs = _covs()
    sss = _run(workers=1, covs=covs)
    has = np.isin(sss["designation"], list(covs))
    assert has.any() and (~has).any()
    for c in sssource.ELLIPSE_COLUMNS:
        assert np.all(np.isfinite(sss[c][has])) and np.all(np.isnan(sss[c][~has])), c
    # one call per object with a covariance, with its orbit
    assert len(fake_ellipse.calls) == len(set(sss["designation"][has]))
    assert {c["orbit"]["designation"] for c in fake_ellipse.calls} == set(sss["designation"][has])


def test_ellipse_inputs(recorded_ephem, fake_ellipse):
    """The ellipse gets the observation times as ASSIST times (TDB days
    since J2000), the observer positions the precise pass used, and the
    precise pass's float64 line of sight."""
    from astropy.time import Time
    from ssp.ephem_assist import MJD_J2000

    sss, obs_state, dia_eph, mpcorb = _tables()
    covs = _covs()
    compute_ephemerides(sss, obs_state, dia_eph, mpcorb, workers=1, covs=covs)
    assert fake_ellipse.calls
    for call in fake_ellipse.calls:
        d = call["orbit"]["designation"]
        rows = sss["designation"] == d
        t = dia_eph["midpointMjdTai"][obs_state["dia_index"][rows]]
        expect_t = Time(t, format="mjd", scale="tai").tdb.mjd - MJD_J2000
        assert call["t_assist"].dtype == np.float64 and np.array_equal(call["t_assist"], expect_t), d
        # (the observer positions, (K, 3), not the velocities; the same
        # ones compute_ephemerides_one got)
        assert np.array_equal(call["obs_pos"], obs_state["obs_pos"][rows]), d
        assert np.array_equal(call["obs_pos"], recorded_ephem[d]["obs_pos"].T), d
        # (EphResult.topo_pos.T in float64, not the float32 topo_x/y/z)
        tp = call["topo_pos"]
        assert tp.dtype == np.float64 and tp.shape == (rows.sum(), 3), d
        assert np.array_equal(tp, recorded_ephem[d]["topo_pos"].T), d
        f32 = np.stack([sss[c][rows] for c in ("topo_x", "topo_y", "topo_z")], axis=1).astype(np.float64)
        assert not np.array_equal(tp, f32), d


@pytest.mark.parametrize("workers", [1, 3])
def test_step_cap_stops_reset_and_summed(fake_ephem, fake_ellipse, workers):
    covs = _covs()
    sss, obs_state, dia_eph, mpcorb = _tables()
    n = len(set(sss["designation"]) & set(covs))        # (one stop per call)
    sssource._propagate.STEP_CAP_STOPS = 1000           # (stale: must be reset)
    try:
        stops = compute_ephemerides(sss, obs_state, dia_eph, mpcorb, workers=workers, chunk_factor=4,
                                    covs=covs)
    finally:
        sssource._propagate.STEP_CAP_STOPS = 0
    assert stops == n


def test_ellipse_parallel_identical_to_serial(fake_ephem, fake_ellipse):
    covs = _covs()
    _assert_identical(_run(workers=1, covs=covs), _run(workers=3, chunk_factor=4, covs=covs))


def test_no_covariances_no_ellipse_call(fake_ephem, fake_ellipse):
    sss = _run(workers=1, covs=None)
    assert fake_ellipse.calls == []
    for c in sssource.ELLIPSE_COLUMNS:
        assert np.all(np.isnan(sss[c])), c


@pytest.mark.skipif(
    not (os.environ.get("SSP_ASSIST_PLANETS") and os.environ.get("SSP_ASSIST_ASTEROIDS")),
    reason="SSP_ASSIST_PLANETS / SSP_ASSIST_ASTEROIDS not set",
)
def test_parallel_identical_to_serial_assist():
    pytest.importorskip("assist")
    kw = dict(n_obj=8, seed=7)
    serial = _run(workers=1, **kw)
    assert np.all(np.isfinite(serial["ephRa"]))
    _assert_identical(serial, _run(workers=3, chunk_factor=2, **kw))
