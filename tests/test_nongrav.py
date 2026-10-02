"""The MPC non-gravitational parameters: parsing, units, ASSIST settings."""
import json

import numpy as np
import pytest

from ssp import nongrav as N
from ssp.nearbysso import _contract as C


def _car(names, values, n_cov=None):
    n = len(names) if n_cov is None else n_cov
    cov = {f"cov{i}{k}": (1.0 + i + k) * 1e-12 for i in range(10) for k in range(i, 10) if k < n}
    return json.dumps({"CAR": {"coefficient_names": names, "coefficient_values": values, "covariance": cov}})


STATE = ["x", "y", "z", "vx", "vy", "vz"]


def test_gravity_only():
    assert N.nongrav_params(None) is N.NONE
    assert N.nongrav_params(_car(STATE, [0] * 6)).model == ""


def test_comet():
    ng = N.nongrav_params(_car(STATE + ["A1", "A2"], [0] * 6 + [1.8e-9, -1.8e-10]))
    assert ng.model == "comet"
    np.testing.assert_array_equal(ng.A, [1.8e-9, -1.8e-10, 0])
    np.testing.assert_array_equal(ng.fitted, [True, True, False])
    assert ng.cov.shape == (8, 8)


@pytest.mark.parametrize("name", ["yarkovski", "yarkovsky"])
def test_yarkovsky_units(name):
    # Apophis: MPC -2.869e-4 (1e-10 au/d^2) vs JPL A2 -2.902e-14 au/d^2
    ng = N.nongrav_params(_car(STATE + [name], [0] * 6 + [-2.869e-4]))
    assert ng.model == "yarkovsky"
    np.testing.assert_allclose(ng.A, [0, -2.869e-14, 0])
    assert ng.cov[6, 6] == pytest.approx(13e-12 * 1e-20)
    assert ng.cov[0, 6] == pytest.approx(7e-12 * 1e-10)


def test_unknown_coefficient():
    with pytest.raises(ValueError):
        N.nongrav_params(_car(STATE + ["DT"], [0] * 7))


def test_missing_covariance_entry():
    assert N.nongrav_params(_car(STATE + ["A1"], [0] * 6 + [1e-9], n_cov=6)).cov is None


def test_g_of_r_models():
    assert set(N.G_OF_R) == {"comet", "yarkovsky"}
    assert N.G_OF_R["comet"]["r0"] == 2.808


def test_orbit_dtype_has_nongrav():
    for f in ("ng_model", "ng_A", "ng_fitted", "cov_full"):
        assert f in C.ORBIT_DTYPE.names
    assert C.ORBIT_DTYPE["cov_full"].shape == (9, 9)
