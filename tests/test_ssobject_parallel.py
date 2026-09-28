"""The parallel SSObject build (workers > 1) gives output identical to the
serial one, bit for bit, in every column."""

import numpy as np
import pandas as pd
import pytest

from ssp import photfit, ssobject
from ssp.ssobject import _balanced_chunks, compute_ssobject


def _tables(n_obj=40, seed=2):
    rng = np.random.default_rng(seed)
    sss, dia, orbits = [], [], []
    src = 1000
    for k in range(n_obj):
        oid = 10 + 3 * k
        desig = f"2025 A{k:03d}"
        # very uneven observation counts: mostly small, a few large
        n = int(rng.choice([1, 2, 3, 5, 8, 13, 40, 120], p=[.1, .1, .15, .15, .2, .15, .1, .05]))
        phase = np.sort(rng.uniform(1, 30, n))
        band = rng.choice(list("grizy"), n)
        mag = 18 + 0.03 * phase + rng.normal(0, 0.05, n)
        # object 5 has no orbit (NaN ephemerides); it gets no SSObject
        ephRa = np.nan if k == 5 else 10.0
        obsid = [f"o{src + j}" for j in range(n)]
        sss.append(pd.DataFrame(dict(
            ssObjectId=oid, diaSourceId=src + np.arange(n), designation=desig,
            processing="DP2-DS", obsid=obsid, primary=True, phaseAngle=phase,
            topoRange=rng.uniform(0.8, 2.5, n), helioRange=rng.uniform(1.5, 3.5, n), ephRa=ephRa,
        )))
        dia.append(pd.DataFrame(dict(
            diaSourceId=src + np.arange(n), midpointMjdTai=60800 + np.sort(rng.uniform(0, 300, n)),
            ra=10.0, dec=1.0, extendedness=np.where(rng.random(n) < 0.3, np.nan, rng.random(n)),
            band=band, psfFlux=10 ** ((31.4 - mag) / 2.5), psfFluxErr=rng.uniform(20, 60, n),
            obsid=obsid,
        )))
        # object 7 is missing from mpc_orbits (no Tisserand J, no MOID)
        if k != 7:
            orbits.append(dict(
                unpacked_primary_provisional_designation=desig, q=rng.uniform(0.9, 3.0),
                e=rng.uniform(0.0, 0.4), i=rng.uniform(0, 30), node=rng.uniform(0, 360),
                argperi=rng.uniform(0, 360), epoch_mjd=61000.0,
            ))
        src += n
    sss = pd.concat(sss, ignore_index=True)
    dia = pd.concat(dia, ignore_index=True)

    # non-primary rows (repeated submissions of an existing source, with
    # their own obsid) for a few objects, kept grouped by ssObjectId
    extra_sss, extra_dia = [], []
    for k in (1, n_obj // 2, n_obj - 1):
        oid = 10 + 3 * k
        row = sss[sss["ssObjectId"] == oid].iloc[[0]]
        new_obsid = row["obsid"].iloc[0] + "-again"
        extra_sss.append(row.assign(obsid=new_obsid, primary=False))
        extra_dia.append(dia[dia["obsid"] == row["obsid"].iloc[0]].assign(obsid=new_obsid))
    # and undesignated detections (ssObjectId 0)
    und = sss.iloc[[0, 1]].assign(ssObjectId=0, designation="", obsid=["u0", "u1"],
                                  diaSourceId=[1, 2], ephRa=np.nan)
    extra_dia.append(dia.iloc[[0, 1]].assign(obsid=["u0", "u1"], diaSourceId=[1, 2]))
    sss = pd.concat([und, sss] + extra_sss, ignore_index=True)
    sss = sss.sort_values("ssObjectId", kind="stable", ignore_index=True)
    dia = pd.concat([dia] + extra_dia, ignore_index=True)
    return sss, dia, pd.DataFrame(orbits)


def _assert_identical(a, b):
    assert a.dtype == b.dtype and len(a) == len(b)
    for col in a.dtype.names:
        x, y = a[col], b[col]
        if x.dtype.kind == "f":
            assert np.array_equal(x.view(f"u{x.itemsize}"), y.view(f"u{y.itemsize}")), col
        else:
            assert np.array_equal(x, y), col
    assert a.tobytes() == b.tobytes()


def test_parallel_matches_serial(capsys):
    sss, dia, orb = _tables()
    serial = compute_ssobject(sss, dia, orb, workers=1)
    capsys.readouterr()
    par = compute_ssobject(sss, dia, orb, workers=3)
    out = capsys.readouterr().out
    assert "[objects] 39/39 objects" in out and "[MOID] 38/38 objects" in out   # ran in the pool

    # sanity: the case exercises what it's meant to
    assert len(serial) == 39                              # object 5 has no orbit
    assert np.all(np.diff(serial["ssObjectId"]) > 0)      # ascending ssObjectId
    no_row = serial["designation"].astype("U") == "2025 A007"  # object 7 has no orbit row
    assert np.all(serial["tisserand_J"][no_row] == 0) and np.all(serial["MOIDEarth"][no_row] == 0)
    assert np.all(serial["tisserand_J"][~no_row] != 0) and np.all(serial["MOIDEarth"][~no_row] > 0)
    assert np.isfinite(serial["g_H"]).sum() > 5
    assert serial["nObs"].min() <= 2 and serial["nObs"].max() >= 40

    _assert_identical(serial, par)


def test_parallel_matches_serial_few_chunks():
    # fewer chunks than workers, and more workers than objects per chunk
    sss, dia, orb = _tables(n_obj=6, seed=3)
    _assert_identical(compute_ssobject(sss, dia, orb, workers=1),
                      compute_ssobject(sss, dia, orb, workers=4, chunk_factor=1))


def test_parallel_without_orbits():
    sss, dia, _ = _tables(n_obj=10, seed=4)
    _assert_identical(compute_ssobject(sss, dia, None, workers=1),
                      compute_ssobject(sss, dia, None, workers=2))


def test_worker_exception_fails_the_build(monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("fit exploded")
    monkeypatch.setattr(photfit, "fitHG12", boom)   # inherited by the forked workers
    sss, dia, orb = _tables(n_obj=10, seed=5)
    with pytest.raises(RuntimeError, match="fit exploded"):
        compute_ssobject(sss, dia, orb, workers=2)
    assert ssobject._PARALLEL == {}


@pytest.mark.parametrize("n_chunks", [1, 3, 8, 50])
def test_balanced_chunks(n_chunks):
    w = np.array([1, 1, 100, 1, 1, 1, 50, 1, 1, 1, 1, 3])
    chunks = _balanced_chunks(w, n_chunks)
    assert chunks[0][0] == 0 and chunks[-1][1] == len(w)
    assert all(e > s for s, e in chunks)
    assert all(a[1] == b[0] for a, b in zip(chunks[:-1], chunks[1:]))
    assert len(chunks) <= min(n_chunks, len(w))
    assert _balanced_chunks(np.array([]), 4) == []
