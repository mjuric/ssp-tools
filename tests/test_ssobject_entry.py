"""compute_ssobject on pyarrow-backed inputs (as the CLI reads them), with
nulls: the columns the per-object function reads are converted to numpy
once, and nulls must behave as the per-object pandas reductions did."""

import numpy as np
import pandas as pd

from ssp.ssobject import compute_ssobject

from test_ssobject_parallel import widen


def _read_back(df, tmp_path, name):
    """Round-trip through Parquet, as ssp-build-ssobject reads its inputs."""
    path = tmp_path / f"{name}.parquet"
    df.to_parquet(path)
    return pd.read_parquet(path, engine="pyarrow", dtype_backend="pyarrow")


def _tables(tmp_path, n_obj=5, n=9):
    rng = np.random.default_rng(3)
    ids = np.repeat(np.arange(1, n_obj + 1), n)
    k = np.tile(np.arange(n), n_obj)
    phase = 2 + 2.5 * k
    obsid = [f"o{i}" for i in range(len(ids))]
    sss = pd.DataFrame(dict(
        ssObjectId=ids, diaSourceId=np.arange(len(ids)) + 100,
        designation=[f"2025 A{i}" for i in ids], processing="DP2-DS", obsid=obsid, primary=True,
        phaseAngle=phase, topoRange=1.5, helioRange=2.3, ephRa=10.0,
    ))
    mag = 18 + 0.03 * phase + rng.normal(0, 0.02, len(ids))
    flux = 10 ** ((31.4 - mag) / 2.5)
    flux[3] = -5.0                                   # non-positive flux: no magnitude
    ext = rng.uniform(0, 1, len(ids))
    ext[[0, 5, 9]] = np.nan                          # some nulls
    ext[2 * n:3 * n] = np.nan                        # an object with no extendedness at all
    dia = pd.DataFrame(dict(
        diaSourceId=np.arange(len(ids)) + 100, midpointMjdTai=60800 + 0.5 * k + ids,
        ra=10.0, dec=1.0, extendedness=pd.array(ext, dtype="Float64"),  # NaN -> null
        band=np.where(k % 2 == 0, "r", "g"), psfFlux=flux, psfFluxErr=30.0, obsid=obsid,
    ))
    return _read_back(widen(sss, dia), tmp_path, "sss"), ext, dia


def test_nulls_and_statistics(tmp_path):
    sss, ext, dia_np = _tables(tmp_path)
    assert sss["extendedness"].isna().sum() == 3 + 9        # the nulls survived the round trip
    obj = compute_ssobject(sss, None)
    assert len(obj) == 5

    for j, oid in enumerate(range(1, 6)):
        m = (dia_np["obsid"].str[1:].astype(int) // 9) == j
        t = dia_np["midpointMjdTai"][m].to_numpy()
        e = ext[m.to_numpy()].astype(np.float32).astype(float)   # (SSObservation is float32)
        e = e[~np.isnan(e)]
        assert obj["ssObjectId"][j] == oid
        assert obj["nObs"][j] == 9
        # (compared at the precision of the SSObject columns)
        f = lambda c, v: obj[c].dtype.type(v)    # noqa: E731
        assert obj["firstObservationMjdTai"][j] == f("firstObservationMjdTai", t.min())
        assert obj["arc"][j] == f("arc", np.ptp(t))
        if len(e):
            assert obj["extendednessMin"][j] == f("extendednessMin", e.min())
            assert obj["extendednessMax"][j] == f("extendednessMax", e.max())
            assert obj["extendednessMedian"][j] == f("extendednessMedian", np.median(e))
        else:
            assert np.isnan(obj["extendednessMin"][j]) and np.isnan(obj["extendednessMedian"][j])

    # the non-positive flux is dropped from object 1's g fit, not the count
    assert obj["g_nObs"][0] == 4 and obj["g_nObsUsed"][0] == 3


def test_serial_equals_parallel_on_pyarrow_inputs(tmp_path):
    sss, _, _ = _tables(tmp_path)
    a = compute_ssobject(sss, None, workers=1)
    b = compute_ssobject(sss, None, workers=3)
    assert a.tobytes() == b.tobytes()
