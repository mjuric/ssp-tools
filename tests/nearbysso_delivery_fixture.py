"""A small, valid NearbySSO delivery (NearbySSO.parquet and its sidecar,
NearbySSO_internal.parquet), written from the schema and
ssp.nearbysso._contract, for the delivery check's tests.

Seven rows on night 20251101 (visits ...10 and ...11):

  row  diaSourceId  visit  designation  placement   flag   note
  0    100          ...10  2020 AB      on detector False  matched
  1    101          ...10  2020 AC      on detector False  matched
  2    105          ...11  2020 AB      NULL        True   matched, no WCS
  3    NULL         ...10  2020 AD      -29.9"      True   unmatched, far off
  4    NULL         ...10  2020 AE      +12"        False  unmatched
  5    NULL         ...11  2020 AC      -3"         False  unmatched
  6    NULL         ...11  2020 AD      +200"       False  unmatched
"""
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ssp.delivery_check import FELIS_ARROW, day_obs_mjd
from ssp.delivery_contract import delivery_schema
from ssp.nearbysso._contract import NEARBYSSO_SIDECAR_DTYPE, NEARBYSSO_SIDECAR_FILE

DAY_OBS = 20251101
V10, V11 = DAY_OBS * 100000 + 10, DAY_OBS * 100000 + 11
#: the visits' (header) times: 03:00 and 03:01 UTC the next day
T10 = float(day_obs_mjd([DAY_OBS])[0]) + 1 + 3 / 24
T11 = T10 + 60 / 86400
S = 1 / 86400

ROWS = dict(
    diaSourceId=[100, 101, 105, None, None, None, None],
    visit=[V10, V10, V11, V10, V10, V11, V11],
    detector=[10, 11, 12, 10, 20, 30, 40],
    designation=["2020 AB", "2020 AC", "2020 AB", "2020 AD", "2020 AE", "2020 AC", "2020 AD"],
    ephOffset=[0.5, 1.0, 0.2, None, None, None, None],
    diaDistanceRank=[1, 1, 2, None, None, None, None],
    ephRateRa=[0.1, 0.2, 0.3, 0.25, 0.1, 0.2, 0.3],
    ephRateDec=[0.0, 0.1, -0.1, 0.0, 0.0, 0.1, 0.0],
)
SIDE = dict(
    eph_x=[100.5, 2000.0, None, -150.0, 300.0, -15.0, 2000.0],
    eph_y=[200.5, 1000.0, None, 100.0, 300.0, 50.0, 2000.0],
    ephDetectorEdgeDist=[20.0, 200.0, None, -29.9, 12.0, -3.0, 200.0],
    midpointMjdTai=[T10 + 0.1 * S, T10 - 0.2 * S, T11, T10, T10 + 0.05 * S, T11 + 0.2 * S, T11 - 0.1 * S],
    midpointMjdTai_flag=[False, False, True, True, False, False, False],
)


def nearbysso_table(**over):
    """NearbySSO (the schema's columns and types), ROWS with ``over``
    replacing columns (lists)."""
    rows = {**ROWS, **over}
    n = len(rows["visit"])
    out = {}
    for c in delivery_schema()["NearbySSO"]:
        name, dt = c["name"], c["datatype"]
        typ = pa.string() if dt in ("char", "string", "text") else FELIS_ARROW[dt]
        vals = rows.get(name)
        if vals is None:
            vals = [float(k) if pa.types.is_floating(typ) else k for k in range(n)]
        out[name] = pa.array(vals, typ)
    return pa.table(out)


def sidecar_table(nss=None, **over):
    """The sidecar of ``nss`` (default: nearbysso_table()): its key, then
    SIDE with ``over`` replacing columns."""
    nss = nearbysso_table() if nss is None else nss
    side = {**SIDE, **over}
    types = {"f8": pa.float64(), "f4": pa.float32(), "b1": pa.bool_(), "i8": pa.int64()}
    out = {}
    for name in NEARBYSSO_SIDECAR_DTYPE.names:
        if name in ("diaSourceId", "designation", "visit"):
            out[name] = nss[name]
        else:
            out[name] = pa.array(side[name], types[NEARBYSSO_SIDECAR_DTYPE[name].str.lstrip("<>|=")])
    return pa.table(out)


def write_nearbysso(d, nss=None, side=None):
    """Write NearbySSO.parquet and its sidecar into ``d``."""
    nss = nearbysso_table() if nss is None else nss
    side = sidecar_table(nss) if side is None else side
    pq.write_table(nss, d / "NearbySSO.parquet")
    if side is not False:
        pq.write_table(side, d / NEARBYSSO_SIDECAR_FILE)
    return d


def take(t, order):
    return t.take(pa.array(np.asarray(order, np.int64)))
