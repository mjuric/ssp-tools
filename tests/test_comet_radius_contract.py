"""The comet match radius in the NearbySSO contract
(docs/design/comet-radius.md)."""
import numpy as np

from ssp.nearbysso._contract import (MATCH_RADIUS_ARCSEC, MATCH_RADIUS_COMET_ARCSEC,
                                     match_radius)


def test_match_radius():
    assert (MATCH_RADIUS_ARCSEC, MATCH_RADIUS_COMET_ARCSEC) == (5.0, 15.0)
    comets = ["C/2025 N1", "P/2002 T6", "D/1993 F2", "I/2017 U1"]
    others = ["A/2017 U1", "S/2004 S 46", "2025 MH352", "1930 BM", ""]
    assert all(match_radius(d) == 15.0 for d in comets)
    assert all(match_radius(d) == 5.0 for d in others)
    r = match_radius(np.array(comets + others))
    assert r.dtype == np.float64 and r.tolist() == [15.0] * 4 + [5.0] * 5
