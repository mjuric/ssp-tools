"""SSObject counts each detection once: non-primary SSSource rows (a trail's
-B endpoint, repeated submissions) and undesignated rows are ignored."""

import numpy as np
import pandas as pd

from ssp.ssobject import compute_ssobject


def _tables(n=8):
    rng = np.random.default_rng(1)
    phase = np.linspace(2, 25, n)
    sss = pd.DataFrame(dict(
        ssObjectId=np.full(n, 7), diaSourceId=np.arange(n) + 100, designation="2025 AA1",
        collection="DP2-DS", obsid=[f"o{i}" for i in range(n)], primary=True,
        phaseAngle=phase, topoRange=1.5, helioRange=2.3, ephRa=10.0,
    ))
    mag = 18 + 0.03 * phase + rng.normal(0, 0.02, n)
    dia = pd.DataFrame(dict(
        diaSourceId=np.arange(n) + 100, midpointMjdTai=60800 + np.arange(n), ra=10.0, dec=1.0,
        extendedness=np.nan, band="r", psfFlux=10 ** ((31.4 - mag) / 2.5), psfFluxErr=30.0,
        obsid=[f"o{i}" for i in range(n)],
    ))
    return sss, dia


def test_nonprimary_and_undesignated_rows_do_not_count():
    sss, dia = _tables()
    ref = compute_ssobject(sss, dia, None)

    # a second submission of source 103 (same diaSourceId, new obsid) and an
    # undesignated detection
    dup = sss.iloc[[3]].assign(obsid="o3-again", primary=False)
    und = sss.iloc[[0]].assign(ssObjectId=0, designation="", obsid="u0", diaSourceId=999, ephRa=np.nan)
    sss2 = pd.concat([und, sss.iloc[:4], dup, sss.iloc[4:]], ignore_index=True)
    dia2 = pd.concat([dia, dia.iloc[[3]].assign(obsid="o3-again"),
                      dia.iloc[[0]].assign(obsid="u0", diaSourceId=999)], ignore_index=True)
    obj = compute_ssobject(sss2, dia2, None)

    assert len(obj) == len(ref) == 1
    for c in ("nObs", "r_nObs", "arc", "firstObservationMjdTai", "r_H", "r_G12", "r_nObsUsed"):
        assert obj[c][0] == ref[c][0], c
    assert ref["nObs"][0] == 8 and np.isfinite(ref["r_H"][0])
    assert np.isnan(obj["extendednessMedian"][0])
