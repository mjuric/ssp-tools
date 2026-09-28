"""The NearbySSO contract imports and its dtypes are self-consistent."""
from ssp.nearbysso import _contract as C


def test_contract():
    assert C.MATCH_RADIUS_ARCSEC == 5.0 and C.SIGMA_MAX_ARCSEC == 10.0
    assert C.NEAR_DELTA_AU == 0.02
    assert C.ORBIT_DTYPE["cov0"].shape == (6, 6)
    assert set(C.DIA_COLUMNS) <= {"diaSourceId", "visit", "midpointMjdTai", "ra", "dec"}
    assert C.NEARBYSSO_DTYPE.names[0] == "diaSourceId"
    assert len(C.CoarseTrack._fields) == 12
    assert C.CoarseTrack._field_defaults == {"cov": None}
