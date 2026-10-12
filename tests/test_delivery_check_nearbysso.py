"""NearbySSO's own delivery checks (ssp.delivery_check.check_nearbysso):
the sidecar, the NULL pattern, the row order, the margin, the times and the
visits, on the small delivery of tests/nearbysso_delivery_fixture.py, one
passing case and the failures real bugs would make. Network-free."""
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nearbysso_delivery_fixture import (
    ROWS,
    SIDE,
    T10,
    V10,
    nearbysso_table,
    sidecar_table,
    take,
    write_nearbysso,
)
from ssp import delivery_check as D
from ssp.nearbysso._contract import NEARBYSSO_SIDECAR_FILE

S = 1 / 86400
CHECKS = ["sidecar", "sidecar columns", "sidecar nulls", "sidecar key", "null pattern", "row order",
          "margin", "times", "visits"]


def run(d):
    return {r.name: r for r in D.check_nearbysso(d)}


def failed(rs):
    return sorted(n for n, r in rs.items() if not r.ok)


def with_col(t, name, vals):
    return t.set_column(t.schema.get_field_index(name), name, pa.array(vals, t.schema.field(name).type))


def edit(lst, i, v):
    lst = list(lst)
    lst[i] = v
    return lst


def side_with(**edits):
    """The fixture's sidecar with ``col=(row, value)`` edits."""
    return sidecar_table(**{c: edit(SIDE[c], i, v) for c, (i, v) in edits.items()})


def test_pass(tmp_path):
    rs = run(write_nearbysso(tmp_path))
    assert list(rs) == CHECKS
    assert failed(rs) == [], rs
    assert "3 matched, 4 unmatched rows" in rs["null pattern"].detail
    assert "2 flagged rows (1 without a placement)" in rs["times"].detail


def test_check_delivery_runs_it(tmp_path):
    write_nearbysso(tmp_path)
    rs = D.check_delivery(tmp_path, tables=["NearbySSO"])["NearbySSO"]
    names = [r.name for r in rs]
    assert names[:6] == ["file", "columns", "types", "nulls", "char lengths", "primary key"]
    assert names[6:] == CHECKS and all(r.ok for r in rs)


def test_check_delivery_reports_a_crash_as_fail(tmp_path, monkeypatch):
    write_nearbysso(tmp_path)

    def boom(d):
        raise RuntimeError("bug")
    monkeypatch.setattr(D, "check_nearbysso", boom)
    rs = D.check_delivery(tmp_path, tables=["NearbySSO"])["NearbySSO"]
    assert rs[-1] == D.CheckResult("nearbysso", False, "cannot check (RuntimeError: bug)")


def test_no_wcs_delivery_passes(tmp_path):
    """A build without placement: matched rows only, NULL placement, every
    row flagged at the visit's time."""
    nss = take(nearbysso_table(), [0, 1, 2])
    side = sidecar_table(nss, eph_x=[None] * 3, eph_y=[None] * 3, ephDetectorEdgeDist=[None] * 3,
                         midpointMjdTai=[T10, T10, T10 + 60 * S], midpointMjdTai_flag=[True] * 3)
    assert failed(run(write_nearbysso(tmp_path, nss, side))) == []


# ---- the sidecar ------------------------------------------------------------

def test_sidecar_missing(tmp_path):
    rs = run(write_nearbysso(tmp_path, side=False))
    assert list(rs) == ["sidecar"] and "missing" in rs["sidecar"].detail


def test_sidecar_row_count(tmp_path):
    nss = nearbysso_table()
    rs = run(write_nearbysso(tmp_path, nss, take(sidecar_table(nss), range(6))))
    assert failed(rs) == ["sidecar"] and "6 rows, NearbySSO 7" in rs["sidecar"].detail


def test_sidecar_misaligned(tmp_path):
    """The sidecar in another order (sorted by the key, say): the key
    check catches it."""
    nss = nearbysso_table()
    side = take(sidecar_table(nss), [1, 0, 2, 3, 4, 5, 6])
    rs = run(write_nearbysso(tmp_path, nss, side))
    assert "sidecar key" in failed(rs)
    assert "diaSourceId: 2 rows differ, first at row 0 (101 vs NearbySSO 100)" in rs["sidecar key"].detail


def test_sidecar_key_null_vs_value(tmp_path):
    nss = nearbysso_table()
    side = with_col(sidecar_table(nss), "diaSourceId", edit(ROWS["diaSourceId"], 4, 7))
    rs = run(write_nearbysso(tmp_path, nss, side))
    assert failed(rs) == ["sidecar key"]
    assert "row 4 (7 vs NearbySSO None)" in rs["sidecar key"].detail


def test_sidecar_wrong_designation(tmp_path):
    nss = nearbysso_table()
    side = with_col(sidecar_table(nss), "designation", edit(ROWS["designation"], 6, "2020 ZZ"))
    assert failed(run(write_nearbysso(tmp_path, nss, side))) == ["sidecar key"]


@pytest.mark.parametrize("col,typ", [("ephDetectorEdgeDist", pa.float64()), ("eph_x", pa.float32()),
                                     ("midpointMjdTai_flag", pa.int8()), ("visit", pa.float64())])
def test_sidecar_wrong_type(tmp_path, col, typ):
    nss = nearbysso_table()
    side = sidecar_table(nss)
    vals = side[col].to_pylist()
    arr = pa.array([int(v) for v in vals] if col == "midpointMjdTai_flag" else vals, typ)
    side = side.set_column(side.schema.get_field_index(col), col, arr)
    rs = run(write_nearbysso(tmp_path, nss, side))
    assert failed(rs) == ["sidecar columns"] and col in rs["sidecar columns"].detail


def test_sidecar_missing_and_extra_columns(tmp_path):
    nss = nearbysso_table()
    side = sidecar_table(nss).drop_columns(["eph_y"]).append_column("x", pa.array(range(7)))
    rs = run(write_nearbysso(tmp_path, nss, side))
    assert list(rs)[-1] == "sidecar columns" and failed(rs) == ["sidecar columns"]


@pytest.mark.parametrize("col", ["midpointMjdTai", "midpointMjdTai_flag"])
def test_sidecar_null_in_non_null(tmp_path, col):
    rs = run(write_nearbysso(tmp_path, side=side_with(**{col: (0, None)})))
    assert "sidecar nulls" in failed(rs) and f"{col}: 1 NULLs" in rs["sidecar nulls"].detail


def test_sidecar_placement_partly_null(tmp_path):
    rs = run(write_nearbysso(tmp_path, side=side_with(ephDetectorEdgeDist=(0, None))))
    # (and, unplaced, it should have been flagged)
    assert failed(rs) == ["sidecar nulls", "times"]
    assert "not NULL together in 1 rows" in rs["sidecar nulls"].detail


def test_sidecar_nan_time(tmp_path):
    rs = run(write_nearbysso(tmp_path, side=side_with(midpointMjdTai=(1, float("nan")))))
    assert "sidecar nulls" in failed(rs) and "midpointMjdTai: 1 non-finite" in rs["sidecar nulls"].detail


# ---- the NULL pattern -------------------------------------------------------

@pytest.mark.parametrize("col,row,value", [("ephOffset", 0, None), ("diaDistanceRank", 1, None),
                                           ("ephOffset", 4, 1.0), ("diaDistanceRank", 5, 1)])
def test_null_pattern_broken(tmp_path, col, row, value):
    nss = nearbysso_table(**{col: edit(ROWS[col], row, value)})
    rs = run(write_nearbysso(tmp_path, nss))
    assert failed(rs) == ["null pattern"] and col in rs["null pattern"].detail


def test_rank_zero(tmp_path):
    rs = run(write_nearbysso(tmp_path, nearbysso_table(diaDistanceRank=edit(ROWS["diaDistanceRank"], 0, 0))))
    assert failed(rs) == ["null pattern"] and "diaDistanceRank < 1" in rs["null pattern"].detail


# ---- row order --------------------------------------------------------------

@pytest.mark.parametrize("order,what", [
    ([0, 1, 3, 2, 4, 5, 6], "a matched row after an unmatched one"),
    ([1, 0, 2, 3, 4, 5, 6], "matched rows not by diaSourceId"),
    ([0, 1, 2, 5, 3, 4, 6], "unmatched rows not strictly by (visit, designation)"),     # by visit
    ([0, 1, 2, 4, 3, 5, 6], "unmatched rows not strictly by (visit, designation)"),     # by designation
])
def test_row_order(tmp_path, order, what):
    """NearbySSO and its sidecar reordered together (so the sidecar still
    matches): the order check alone fails."""
    nss = take(nearbysso_table(), order)
    side = take(sidecar_table(), order)
    rs = run(write_nearbysso(tmp_path, nss, side))
    assert failed(rs) == ["row order"] and what in rs["row order"].detail


def test_same_dia_source_id_twice_is_in_order(tmp_path):
    """Repeated diaSourceIds (two objects, or a repeated input id) tie: no
    order is imposed among them."""
    nss = nearbysso_table(diaSourceId=edit(ROWS["diaSourceId"], 1, 100))
    assert "row order" not in failed(run(write_nearbysso(tmp_path, nss)))


# ---- the margin ------------------------------------------------------------

def test_unmatched_beyond_margin(tmp_path):
    rs = run(write_nearbysso(tmp_path, side=side_with(ephDetectorEdgeDist=(3, -31.0))))
    assert failed(rs) == ["margin"] and "1 unmatched rows beyond the margin" in rs["margin"].detail


def test_unmatched_beyond_margin_by_its_move(tmp_path):
    """Up to |rate| * MARGIN_MOVE_DT_S beyond the margin passes: the cut is
    made at the visit's time. Row 3 moves at 0.25 deg/day = 0.0104"/s."""
    ok = -30.0 - 0.0104 * D.MARGIN_MOVE_DT_S + 0.001
    rs = run(write_nearbysso(tmp_path, side=side_with(ephDetectorEdgeDist=(3, ok))))
    assert failed(rs) == [] and "1 beyond it only by their move" in rs["margin"].detail
    bad = -30.0 - 0.0105 * D.MARGIN_MOVE_DT_S - 0.001
    rs = run(write_nearbysso(tmp_path, side=side_with(ephDetectorEdgeDist=(3, bad))))
    assert failed(rs) == ["margin"]


def test_matched_beyond_margin_is_allowed(tmp_path):
    rs = run(write_nearbysso(tmp_path, side=side_with(ephDetectorEdgeDist=(0, -500.0))))
    assert failed(rs) == []


def test_unmatched_without_placement(tmp_path):
    side = sidecar_table(eph_x=edit(SIDE["eph_x"], 4, None), eph_y=edit(SIDE["eph_y"], 4, None),
                         ephDetectorEdgeDist=edit(SIDE["ephDetectorEdgeDist"], 4, None),
                         midpointMjdTai=edit(SIDE["midpointMjdTai"], 4, T10),
                         midpointMjdTai_flag=edit(SIDE["midpointMjdTai_flag"], 4, True))
    rs = run(write_nearbysso(tmp_path, side=side))
    assert failed(rs) == ["margin"] and "1 unmatched rows without a placement" in rs["margin"].detail


# ---- times ---------------------------------------------------------------

def test_no_placement_not_flagged(tmp_path):
    side = sidecar_table(midpointMjdTai_flag=edit(SIDE["midpointMjdTai_flag"], 2, False))
    rs = run(write_nearbysso(tmp_path, side=side))
    assert failed(rs) == ["times"] and "without a placement not flagged" in rs["times"].detail


def test_time_on_another_night(tmp_path):
    side = sidecar_table(midpointMjdTai=edit(SIDE["midpointMjdTai"], 0, SIDE["midpointMjdTai"][0] + 1))
    rs = run(write_nearbysso(tmp_path, side=side))
    assert "times" in failed(rs) and "not in their visit's night" in rs["times"].detail


def test_time_in_days_not_mjd(tmp_path):
    """A JD (or any other wrong scale) is not the visit's night."""
    side = sidecar_table(midpointMjdTai=[t + 2400000.5 for t in SIDE["midpointMjdTai"]])
    assert "times" in failed(run(write_nearbysso(tmp_path, side=side)))


def test_flagged_rows_of_a_visit_with_own_times(tmp_path):
    """Flagged rows of a visit at their DiaSources' own (differing) times,
    e.g. without a correction table: within the gradient, they pass; a
    flagged row 2 s off (a wrong time) does not."""
    side = sidecar_table(midpointMjdTai=edit(SIDE["midpointMjdTai"], 4, T10 + 0.01 * S),
                         midpointMjdTai_flag=edit(SIDE["midpointMjdTai_flag"], 4, True))
    rs = run(write_nearbysso(tmp_path, side=side))
    assert failed(rs) == [] and "flagged <= 0.010000 s" in rs["times"].detail
    side = sidecar_table(midpointMjdTai=edit(SIDE["midpointMjdTai"], 4, T10 + 2 * S),
                         midpointMjdTai_flag=edit(SIDE["midpointMjdTai_flag"], 4, True))
    rs = run(write_nearbysso(tmp_path, side=side))
    assert failed(rs) == ["times"] and "flagged rows' times span more than 0.6 s" in rs["times"].detail


def test_corrected_times_spread_too_wide(tmp_path):
    side = sidecar_table(midpointMjdTai=edit(SIDE["midpointMjdTai"], 4, T10 + 0.5 * S))   # 0.7 s from row 1
    rs = run(write_nearbysso(tmp_path, side=side))
    assert failed(rs) == ["times"] and "span more than 0.6 s" in rs["times"].detail


# ---- visits --------------------------------------------------------------

@pytest.mark.parametrize("visit", [20251399 * 100000 + 1, 1, 20191231 * 100000 + 5])
def test_implausible_day_obs(tmp_path, visit):
    nss = nearbysso_table(visit=edit(ROWS["visit"], 6, visit))
    rs = run(write_nearbysso(tmp_path, nss))
    assert "visits" in failed(rs) and "implausible day_obs" in rs["visits"].detail


def test_detector_out_of_range(tmp_path):
    rs = run(write_nearbysso(tmp_path, nearbysso_table(detector=edit(ROWS["detector"], 4, 189))))
    assert failed(rs) == ["visits"] and "detector outside 0..188" in rs["visits"].detail


def test_prediction_both_matched_and_unmatched(tmp_path):
    """(V10, 2020 AE) unmatched, and also matched to DiaSource 101."""
    nss = nearbysso_table(designation=edit(ROWS["designation"], 1, "2020 AE"))
    rs = run(write_nearbysso(tmp_path, nss))
    assert failed(rs) == ["visits"]
    assert f"({V10}, '2020 AE')" in rs["visits"].detail


def test_unreadable_sidecar(tmp_path):
    write_nearbysso(tmp_path)
    (tmp_path / NEARBYSSO_SIDECAR_FILE).write_text("not parquet")
    rs = run(tmp_path)
    assert list(rs) == ["sidecar"] and "cannot read" in rs["sidecar"].detail


def test_nearbysso_lacks_columns(tmp_path):
    nss = nearbysso_table().drop_columns(["ephRateRa"])
    pq.write_table(nss, tmp_path / "NearbySSO.parquet")
    rs = run(tmp_path)
    assert list(rs) == ["nearbysso"] and not rs["nearbysso"].ok
