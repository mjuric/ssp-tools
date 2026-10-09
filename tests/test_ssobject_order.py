"""SSObject doesn't depend on the order of its input rows: random
permutations of each object's SSObservation rows (and of the objects) give a
bitwise identical SSObject, on synthetic data and on real objects."""

import os

import numpy as np
import pandas as pd
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from ssp import ssobject
from ssp.ssobject import compute_ssobject

from test_ssobject_parallel import _assert_identical, _tables


def _shuffled(sss, seed):
    """``sss`` with each object's rows in a random order, and the objects
    in a random order (still grouped)."""
    rng = np.random.default_rng(seed)
    oid = sss["ssObjectId"].to_numpy()
    _, group = np.unique(oid, return_inverse=True)
    group_rank = rng.permutation(group.max() + 1)[group]
    order = np.lexsort((rng.random(len(sss)), group_rank))
    out = sss.iloc[order].reset_index(drop=True)
    assert not np.array_equal(order, np.arange(len(sss)))
    return out


def test_permutations_synthetic():
    sss, orbits = _tables(n_obj=40, seed=11)
    ref = compute_ssobject(sss, orbits)
    assert np.isfinite(ref["r_H"]).sum() > 5
    for seed in range(4):
        _assert_identical(ref, compute_ssobject(_shuffled(sss, seed), orbits))
    _assert_identical(ref, compute_ssobject(_shuffled(sss, 9), orbits, workers=3))


# A sample of real objects whose fits differed between the dia_sources and
# SSObservation row orders before the fix: 2-point fits, fits at a single phase
# angle, and well-posed fits (G12 within the search tolerance), in each band.
REAL_SSOBSERVATION = ("/sdf/data/rubin/user/mjuric/sssource-widened/integration/2026-09-30/out/"
                      "sssource.parquet")
REAL_SAMPLE = [
    4698171701206207264, 4699592356212525856, 4769649895566101280, 4771647738258475808,
    4986665960266615328, 5202275865340168992, 5203705243458620192, 5636310193646160672,
    5779021386843441952, 5780767346967792416, 5852762337478462240, 5922307530384558880,
    5922329507698723616, 6141001441054051104, 6211944203282369312, 6284585552112012064,
    6356622349968624416, 6428660144207383328,
    # the regression cases of test_photfit_order.py
    5635187669466106656, 5492494137040128800, 4914929483702684448,
]


@pytest.mark.skipif(not os.path.exists(REAL_SSOBSERVATION), reason="needs the 2026-09-30 fixture (USDF)")
def test_permutations_real_sample():
    t = pq.read_table(REAL_SSOBSERVATION, columns=ssobject.SSS_COLUMNS,
                      filters=pc.field("ssObjectId").isin(REAL_SAMPLE))
    sss = t.to_pandas(types_mapper=pd.ArrowDtype)
    ref = compute_ssobject(sss, None)
    assert len(ref) == len(REAL_SAMPLE)
    for seed in range(6):
        _assert_identical(ref, compute_ssobject(_shuffled(sss, seed), None))
