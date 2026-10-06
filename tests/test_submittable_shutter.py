"""The shutter-motion correction in extract-submitted-sources (WP S1 of
docs/design/shutter-timing.md), against the read-only fixture correction
table (no network: the view is a synthetic table behind a fake fetch).
Skipped without the fixture or the shutter_timing package."""

import json
import os
import shutil
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from astropy.time import Time

from ssp.delivery_contract import SHUTTER_INPUT_COLUMNS, SHUTTER_MANIFEST_FIELD
from ssp.export import submittable as S
from ssp.sssource_contract import MAX_NOT_BUILT_VISITS

pytest.importorskip("shutter_timing.corrections")

FIXTURE = Path(os.environ.get("SSP_SHUTTER_FIXTURE",
                              "/sdf/data/rubin/user/mjuric/shutter-timing/fixtures/correction-table-v1"))
TABLE = FIXTURE / "table"
pytestmark = pytest.mark.skipif(not (TABLE / "manifest.json").exists(),
                                reason=f"no fixture correction table at {FIXTURE}")

MAS = 1 / 3.6e6
MS = 1e-3 / 86400            # one ms in days

# the pipeline's visit times (MJD TAI) of the fixture's corrected visits:
# their exposure logs' header midpoints (header_mid_mjd_tai), as the
# visit-time guard requires; any value for the others
VISIT_T = {2026010500249: 61046.25468065427, 2026010500250: 61046.25486987154,
           2026071200100: 61233.98278583563, 2026010500251: 61046.26, 2026010500999: 61046.27,
           2026010600001: 61046.9, 2025081000030: 60897.1}
# a status-0 source 0.216 s after its visit midpoint (the shift exceeds DT_MS)
V0, D0, X0, Y0 = 2026010500249, 178, 2000.0, 2000.0


def examples():
    return json.loads((FIXTURE / "examples.json").read_text())


def reference(visit, detector, x, y, table_dir=TABLE):
    from shutter_timing.corrections import corrected_midpoints
    return corrected_midpoints(np.atleast_1d(visit), np.atleast_1d(detector), np.atleast_1d(x),
                               np.atleast_1d(y), table_dir=table_dir, require_uniform=True)


def utc(tai_mjd):
    """MJD TAI -> a UTC datetime (to the microsecond), as obs_sbn has it."""
    return Time(tai_mjd, format="mjd", scale="tai").utc.to_datetime()


def obs_table(rows):
    cols = {c: [r.get(c) for r in rows] for c in S.OBS_COLUMNS}
    cols["stn"] = ["X05"] * len(rows)
    types = dict(obstime=pa.timestamp("us"), ra=pa.float64(), dec=pa.float64(), mag=pa.float64())
    return pa.table({c: pa.array(v, types.get(c, pa.string())) for c, v in cols.items()})


def view_table(rows, visit_type=pa.int64()):
    """SubmittableSources-shaped, with the columns the correction reads."""
    import astropy.units as u
    from astropy.coordinates import Latitude, Longitude
    from cdshealpix.nested import lonlat_to_healpix

    names = ["processing", "id", "visit", "detector", "x", "y", "band", "midpointMjdTai", "ra", "dec",
             "psfFlux", "trailRa", "trailDec", "hpix29"]
    types = dict(processing=pa.string(), id=pa.int64(), visit=visit_type, detector=pa.int32(),
                 band=pa.string(), hpix29=pa.int64())
    cols = {c: [r.get(c) for r in rows] for c in names}
    cols["processing"] = [r.get("processing", "AP-DS") for r in rows]
    cols["band"] = [r.get("band", "r") for r in rows]
    cols["psfFlux"] = [1000.0] * len(rows)
    cols["dec"] = [r.get("dec", 1.0) for r in rows]
    ra, dec = np.array(cols["ra"], float), np.array(cols["dec"], float)
    cols["hpix29"] = lonlat_to_healpix(Longitude(ra, unit=u.deg), Latitude(dec, unit=u.deg), 29).tolist()
    return pa.table({c: pa.array(v, types.get(c, pa.float64())) for c, v in cols.items()})


def fake_fetch(view, log=None):
    """Id tasks: filter on (processing, id). Position tasks: apply the
    query's time predicates (the minute buckets and the BETWEEN range), as
    ClickHouse would; the cells are left to the client-side pairing."""
    import re

    def fetch(tasks):
        out = []
        for key, sql, params, sets in tasks:
            t = view
            if "q" in sets:
                mask = pc.is_in(t["id"], pa.array(sets["q"], pa.int64()))
                if params:
                    mask = pc.and_(mask, pc.equal(t["processing"], params["label"]))
            else:
                lo, hi = map(float, re.search(r"BETWEEN (\S+) AND (\S+)\n", sql).groups())
                tm = t["midpointMjdTai"].to_numpy()
                bucket = np.floor(tm * 1440).astype(np.int64)
                mask = pa.array((tm >= lo) & (tm <= hi) & np.isin(bucket, sets["b"]))
                if log is not None:
                    log.append((lo, hi, sets["b"]))
            out.append((key, t.filter(mask)))
        return out
    return fetch


def run(tmp_path, obs_rows, view_rows, table=TABLE, **kw):
    pq.write_table(obs_table(obs_rows), tmp_path / "obs.parquet")
    report = {}
    rc = S.extract(tmp_path / "obs.parquet", tmp_path / "dia.parquet", fake_fetch(view_table(view_rows)),
                   correction_table=table, report=report, **kw)
    assert rc == 0
    out = {r["obsid"]: r for r in pq.read_table(tmp_path / "dia.parquet").to_pylist()}
    unres = {r["obsid"]: r for r in pq.read_table(tmp_path / "dia.unresolved.parquet").to_pylist()}
    return out, unres, report[SHUTTER_MANIFEST_FIELD[0]]


#
# The lookup
#

def test_lookup_matches_examples():
    ex = examples()["rows"]
    view = view_table([dict(id=i, visit=e["visit"], detector=e["detector"], x=e["x"], y=e["y"], ra=10.0,
                            midpointMjdTai=VISIT_T[e["visit"]]) for i, e in enumerate(ex)])
    corr = S.Corrections(TABLE)
    corr.lookup(view, np.arange(len(ex)))
    assert list(corr.status) == [e["status"] for e in ex]
    for e, t in zip(ex, corr.t):
        if e["t_mid_mjd_tai"] is None:
            assert np.isnan(t)
        else:
            assert t == e["t_mid_mjd_tai"]
    p = corr.provenance()
    # fixture-specific provenance
    x = examples()
    assert (p["calibration_id"], p["table_format"], p["package_version"]) == (
        x["calibration_id"], x["table_format"], x["package_version"])
    assert corr.calls == 1          # one call for the batch


def test_lookup_batches_by_night(monkeypatch):
    ex = examples()["rows"]
    view = view_table([dict(id=i, visit=e["visit"], detector=e["detector"], x=e["x"], y=e["y"], ra=10.0,
                            midpointMjdTai=VISIT_T[e["visit"]]) for i, e in enumerate(ex)])
    monkeypatch.setattr(S, "LOOKUP_BATCH_ROWS", 1)
    corr = S.Corrections(TABLE)
    corr.lookup(view, np.arange(len(ex))[::-1])
    assert list(corr.status) == [e["status"] for e in ex]
    assert corr.calls == len({e["visit"] // 100000 for e in ex})      # whole nights per call
    corr.lookup(view, np.arange(len(ex)))                             # already looked up: no call
    assert corr.calls == len({e["visit"] // 100000 for e in ex})


#
# End to end
#

def test_four_statuses(tmp_path, capsys):
    """Each example's measurement, submitted at its visit time: the
    corrected ones get the corrected time, the others the visit's, flagged."""
    ex = examples()["rows"]
    view, obs = [], []
    for i, e in enumerate(ex):
        view.append(dict(id=100 + i, visit=e["visit"], detector=e["detector"], x=e["x"], y=e["y"],
                         ra=10.0 + i, midpointMjdTai=VISIT_T[e["visit"]]))
        obs.append(dict(obsid=f"o{i}", obssubid=f"LSST-AP-DS-{100 + i}", ra=10.0 + i, dec=1.0,
                        obstime=utc(VISIT_T[e["visit"]]), band="Lr", mag=20.0))
    out, unres, rep = run(tmp_path, obs, view)
    assert not unres and len(out) == len(ex)
    for i, e in enumerate(ex):
        r = out[f"o{i}"]
        assert r["midpointMjdTaiVisit"] == VISIT_T[e["visit"]]
        corrected = e["status"] in (0, 1)
        assert r["midpointMjdTai"] == (e["t_mid_mjd_tai"] if corrected else VISIT_T[e["visit"]])
        assert r["midpointMjdTai_flag"] is (e["status"] in (2, 3))
        assert r["midpointMjdTai_flag_degraded"] is (e["status"] == 1)
        assert r["obstime_basis"] in ("visit", "both")
        assert (r["obstime_basis"] == "both") is (corrected and abs(r["dt_corrected_ms"]) <= S.DT_MS)
    n = {s: sum(e["status"] == s for e in ex) for s in range(4)}
    assert rep["status"] == dict(ok=n[0], degraded=n[1], omitted=n[2], not_built=n[3], outside_coverage=0,
                                 time_mismatch=0, shift_too_large=0, already_corrected=0)
    assert rep["first_night"] == 20250810 and rep["time_mismatch_visits"] == []   # the fixture's
    nb = sorted(e["visit"] for e in ex if e["status"] == 3)
    assert rep["not_built_visits"] == nb
    assert sum(rep["obstime_basis"].values()) == len(ex)
    assert rep["table_dir"] == str(TABLE)
    assert rep["calibration_id"] == examples()["calibration_id"]
    # the NOT_BUILT visits are named in a warning
    err = capsys.readouterr().err
    assert "not built" in err and all(str(v) in err for v in nb)

    schema = pq.read_schema(tmp_path / "dia.parquet")
    assert schema.names[-len(S.SHUTTER_COLUMNS):] == S.SHUTTER_COLUMNS
    assert set(SHUTTER_INPUT_COLUMNS) <= set(schema.names)
    assert schema.field("midpointMjdTai").type == pa.float64()
    t = pq.read_table(tmp_path / "dia.parquet")
    for c in ("midpointMjdTai", "midpointMjdTaiVisit", "midpointMjdTai_flag", "midpointMjdTai_flag_degraded",
              "obstime_basis"):
        assert t[c].null_count == 0, c


def test_two_basis_match(tmp_path):
    """Matches on the visit time, the corrected time, or both; 50 ms off
    both does not match."""
    tc = reference(V0, D0, X0, Y0).t_mid_mjd_tai[0]
    tv = VISIT_T[V0]
    assert (tc - tv) / MS > 100           # the correction exceeds DT_MS
    t94 = reference(V0, 94, 2000.0, 2000.0).t_mid_mjd_tai[0]
    assert 1 < abs(t94 - tv) / MS < S.DT_MS
    src = dict(visit=V0, detector=D0, x=X0, y=Y0)
    view = [
        dict(id=1, ra=10.0, midpointMjdTai=tv, **src),
        dict(id=2, ra=11.0, midpointMjdTai=tv, **src),
        dict(id=3, ra=12.0, midpointMjdTai=tv, **src),
        # a source whose correction is within DT_MS of the visit time
        dict(id=4, ra=13.0, midpointMjdTai=tv, visit=V0, detector=94, x=2000.0, y=2000.0),
        dict(id=5, ra=14.0, midpointMjdTai=tv, **src),
    ]
    obs = [
        dict(obsid="o1", obssubid="1", ra=10.0, dec=1.0, obstime=utc(tc), band="Lr", mag=20.0),
        dict(obsid="o2", obssubid="2", ra=11.0, dec=1.0, obstime=utc(tv), band="Lr", mag=20.0),
        dict(obsid="o4", obssubid="4", ra=13.0, dec=1.0, obstime=utc(t94), band="Lr", mag=20.0),
        # 50 ms before the visit time (and so 266 ms before the corrected time)
        dict(obsid="o5", obssubid="5", ra=14.0, dec=1.0, obstime=utc(tv - 50 * MS), band="Lr", mag=20.0),
        # no id: the position pass, on the corrected time
        dict(obsid="o3", obssubid="hand", ra=12.0, dec=1.0, obstime=utc(tc), band="Lr", mag=20.0),
    ]
    out, unres, rep = run(tmp_path, obs, view)
    assert sorted(out) == ["o1", "o2", "o3", "o4"] and sorted(unres) == ["o5"]
    assert unres["o5"]["reason"] == "no_pass"
    assert {k: r["obstime_basis"] for k, r in out.items()} == dict(o1="corrected", o2="visit", o3="corrected",
                                                                  o4="both")
    assert (out["o3"]["match"], out["o3"]["diaSourceId"]) == ("position", 3)
    for k in ("o1", "o2", "o3"):
        assert out[k]["midpointMjdTai"] == tc and out[k]["midpointMjdTaiVisit"] == tv
        assert not out[k]["midpointMjdTai_flag"] and not out[k]["midpointMjdTai_flag_degraded"]
    assert abs(out["o1"]["dt_corrected_ms"]) < 0.01 and out["o1"]["dt_ms"] == pytest.approx(-(tc - tv) / MS)
    assert rep["obstime_basis"] == dict(visit=1, corrected=2, both=1)
    assert rep["status"] == dict(ok=4, degraded=0, omitted=0, not_built=0, outside_coverage=0,
                                 time_mismatch=0, shift_too_large=0, already_corrected=0)
    assert rep["not_built_visits"] == []


def test_position_window_covers_both_times():
    """The position query's window and minute buckets reach MAX_SHUTTER_SHIFT_S
    beyond DT_MS with a table, and stay as before without one."""
    tai = np.array([61046.0 + 1 / 1440 - 0.1 / 86400])        # 0.1 s before a minute boundary
    obs = dict(tai=tai, ra=np.array([10.0]), dec=np.array([1.0]))
    (_, sql0, _, sets0), = S.position_queries(obs, np.array([0]), "ssp")
    (_, sql1, _, sets1), = S.position_queries(obs, np.array([0]), "ssp", shift_s=S.MAX_SHUTTER_SHIFT_S)
    m = int(np.floor(tai[0] * 1440))
    assert list(sets0["b"]) == [m] and list(sets1["b"]) == [m, m + 1]
    tai = np.array([61046.0 + 30 / 86400])                       # mid-minute
    obs["tai"] = tai
    (_, sql0, _, sets0), = S.position_queries(obs, np.array([0]), "ssp")
    (_, sql1, _, sets1), = S.position_queries(obs, np.array([0]), "ssp", shift_s=S.MAX_SHUTTER_SHIFT_S)
    assert f"{float(tai[0] - S.DT_MS / 1e3 / 86400)!r}" in sql0
    assert f"{float(tai[0] - (S.DT_MS / 1e3 + S.MAX_SHUTTER_SHIFT_S) / 86400)!r}" in sql1
    assert f"{float(tai[0] + (S.DT_MS / 1e3 + S.MAX_SHUTTER_SHIFT_S) / 86400)!r}" in sql1


def test_position_pass_fetches_corrected_match_across_a_minute(tmp_path):
    """The view's (visit) time is in the minute before the obs time; only
    the corrected time matches: the widened buckets must fetch it."""
    tc = reference(V0, D0, X0, Y0).t_mid_mjd_tai[0]
    tv = VISIT_T[V0]
    # shift both so that a minute boundary falls between tv and tc
    b = np.ceil(tv * 1440) / 1440
    off = b - (tv + tc) / 2
    view = [dict(id=1, ra=10.0, midpointMjdTai=tv + off, visit=V0, detector=D0, x=X0, y=Y0)]
    obs = [dict(obsid="o", obssubid="hand", ra=10.0, dec=1.0, obstime=utc(tc + off), band="Lr", mag=20.0)]
    assert np.floor((tv + off) * 1440) != np.floor((tc + off) * 1440)
    o, _, _ = S.load_obs(obs_table(obs))
    tasks = S.position_queries(o, np.array([0]), "ssp", shift_s=S.MAX_SHUTTER_SHIFT_S)
    (_, got), = fake_fetch(view_table(view))(tasks)
    assert len(got) == 1
    (_, got), = fake_fetch(view_table(view))(S.position_queries(o, np.array([0]), "ssp"))
    assert len(got) == 0


def test_not_built_limit(tmp_path, capsys):
    """NOT_BUILT visits get the visit time, flagged, with a warning; more
    than max_not_built_visits of them fail the extract, writing nothing."""
    nb = [2026010600001, 2026010600002, 2026010500999]       # no night; a visit missing from its log
    view = [dict(id=i, ra=10.0 + i, midpointMjdTai=61046.27 + i / 86400, visit=v, detector=94, x=2000.0,
                 y=2000.0) for i, v in enumerate(nb)]
    obs = [dict(obsid=f"o{i}", obssubid=str(i), ra=10.0 + i, dec=1.0, obstime=utc(61046.27 + i / 86400),
                band="Lr", mag=20.0) for i in range(len(nb))]
    with pytest.raises(S.CorrectionError, match="stage 0"):
        run(tmp_path, obs, view, max_not_built_visits=2)
    assert not (tmp_path / "dia.parquet").exists()
    out, _, rep = run(tmp_path, obs, view, max_not_built_visits=3)
    assert rep["not_built_visits"] == sorted(nb) and rep["status"]["not_built"] == 3
    assert all(r["midpointMjdTai_flag"] and r["midpointMjdTai"] == r["midpointMjdTaiVisit"]
               for r in out.values())
    assert "warning" in capsys.readouterr().err
    # the default is the contract's
    assert S.Corrections(TABLE).max_not_built_visits == MAX_NOT_BUILT_VISITS
    import argparse
    p = argparse.ArgumentParser()
    S.add_correction_args(p)
    a = p.parse_args([])
    assert a.max_not_built_visits == MAX_NOT_BUILT_VISITS
    assert S.correction_table(a) == S.DEFAULT_CORRECTION_TABLE
    assert S.correction_table(p.parse_args(["--correction-table", "none"])) is None


def test_not_built_counts_written_rows_only(tmp_path):
    """A NOT_BUILT candidate that loses (here: too far) does not count."""
    view = [dict(id=1, ra=10.0, midpointMjdTai=VISIT_T[V0], visit=V0, detector=D0, x=X0, y=Y0),
            dict(id=1, processing="NV-S", ra=10.0, dec=1.0 + 500 * MAS, midpointMjdTai=VISIT_T[V0],
                 visit=2026010600001, detector=94, x=2000.0, y=2000.0)]
    obs = [dict(obsid="o", obssubid="1", ra=10.0, dec=1.0, obstime=utc(VISIT_T[V0]), band="Lr", mag=20.0)]
    out, _, rep = run(tmp_path, obs, view, max_not_built_visits=0)
    assert out["o"]["processing"] == "AP-DS" and rep["not_built_visits"] == []


def test_outside_coverage(tmp_path):
    """Visits on nights before the table's first night (read from the
    table) get the visit time, flagged, are never looked up, and do not
    count toward the NOT_BUILT limit."""
    first = S.Corrections(TABLE).first_night
    assert first == min(int(p.name[12:20]) for p in TABLE.glob("corrections_*.parquet"))
    early = [(first // 100 - 1) * 100 + 1 + k for k in range(5)]     # five nights in the month before
    visits = [d * 100000 + 7 for d in early]
    t0 = 60800.0
    view = [dict(id=i, ra=10.0 + i, midpointMjdTai=t0 + i, visit=v, detector=4, x=2000.0, y=2000.0)
            for i, v in enumerate(visits)]
    view.append(dict(id=99, ra=30.0, midpointMjdTai=VISIT_T[V0], visit=V0, detector=D0, x=X0, y=Y0))
    common = dict(dec=1.0, band="Lr", mag=20.0)
    obs = [dict(obsid=f"o{i}", obssubid=str(i), ra=10.0 + i, obstime=utc(t0 + i), **common)
           for i in range(len(visits))]
    obs.append(dict(obsid="c", obssubid="99", ra=30.0, obstime=utc(VISIT_T[V0]), **common))
    out, _, rep = run(tmp_path, obs, view, max_not_built_visits=0)
    for i in range(len(visits)):
        r = out[f"o{i}"]
        assert r["midpointMjdTai_flag"] and not r["midpointMjdTai_flag_degraded"]
        assert r["midpointMjdTai"] == r["midpointMjdTaiVisit"] == t0 + i
        assert r["obstime_basis"] == "visit"
    assert not out["c"]["midpointMjdTai_flag"]
    assert rep["status"]["outside_coverage"] == len(visits) and rep["status"]["not_built"] == 0
    assert rep["first_night"] == first and rep["not_built_visits"] == []


def test_outside_coverage_never_looked_up(monkeypatch):
    import shutter_timing.corrections as C
    calls = []
    real = C.corrected_midpoints
    def spy(v, *a, **k):
        calls.append(np.asarray(v))
        return real(v, *a, **k)
    monkeypatch.setattr(C, "corrected_midpoints", spy)
    corr = S.Corrections(TABLE)
    early = (corr.first_night - 1) * 100000 + 1
    view = view_table([dict(id=1, ra=1.0, midpointMjdTai=0.0, visit=early, detector=1, x=1.0, y=1.0),
                       dict(id=2, ra=1.0, midpointMjdTai=VISIT_T[V0], visit=V0, detector=D0, x=X0, y=Y0)])
    corr.lookup(view, np.array([0, 1]))
    assert list(corr.status) == [S.OUTSIDE_COVERAGE, S.OK]
    assert len(calls) == 1 and list(calls[0]) == [V0]
    corr = S.Corrections(TABLE)
    corr.lookup(view, np.array([0]))        # only outside: no call at all
    assert len(calls) == 1 and corr.calls == 0


def test_empty_table_has_no_coverage(tmp_path):
    """A table directory without nights: no first night; everything is
    NOT_BUILT (and the limit applies)."""
    corr = S.Corrections(tmp_path)
    assert corr.first_night is None
    view = view_table([dict(id=1, ra=1.0, midpointMjdTai=0.0, visit=2024112800001, detector=1, x=1.0, y=1.0)])
    corr.lookup(view, np.array([0]))
    assert list(corr.status) == [S.NOT_BUILT]


def test_warnings_list_at_most_20_visits(capsys):
    corr = S.Corrections(TABLE, max_not_built_visits=1000)
    visits = np.arange(25) + 2026010600001
    nan = np.full(25, np.nan)
    nb, tm = corr.check(visits, np.full(25, S.NOT_BUILT), nan, nan)
    assert nb == visits.tolist() and tm == []
    err = capsys.readouterr().err
    assert str(visits[19]) in err and str(visits[20]) not in err and "... and 5 more" in err
    nb, tm = corr.check(visits, np.full(25, S.TIME_MISMATCH), np.full(25, 0.07), nan)
    assert nb == [] and tm == visits.tolist()
    err = capsys.readouterr().err
    assert "+70.0 ms" in err and "... and 5 more" in err and str(visits[20]) not in err
    nb, tm = corr.check(visits, np.full(25, S.SHIFT_TOO_LARGE), nan, np.full(25, -3.5))
    assert nb == [] and tm == []
    err = capsys.readouterr().err
    assert "3.500 s" in err and "... and 5 more" in err and str(visits[20]) not in err


def test_header_guard_unit():
    """Header within MAX_HEADER_MISMATCH_S: applied; beyond: TIME_MISMATCH;
    a visit missing from the log (NaN): the table's status stands. The
    pipeline time equal to the corrected one: ALREADY_CORRECTED. A shift
    beyond MAX_CORRECTION_S: SHIFT_TOO_LARGE, header match or not."""
    assert S.VISIT_TIME_GUARD is S.header_guard
    h0 = 61046.25
    cases = [  # (pipeline - header [s], header present, corrected - header [s], verdict)
        (0.0009, True, 0.2, S.GUARD_APPLY),
        (-0.0009, True, 0.2, S.GUARD_APPLY),
        (0.0011, True, 0.2, S.TIME_MISMATCH),
        (0.0, False, 0.2, S.GUARD_APPLY),
        (0.2, True, 0.2, S.ALREADY_CORRECTED),          # pipeline = corrected
        (0.2009, True, 0.2, S.ALREADY_CORRECTED),
        (0.2011, True, 0.2, S.TIME_MISMATCH),           # neither
        (0.0, True, 3.5, S.SHIFT_TOO_LARGE),
        (0.0, True, -3.5, S.SHIFT_TOO_LARGE),
        (0.0, True, 2.5, S.GUARD_APPLY),
        (0.5, True, 3.6, S.SHIFT_TOO_LARGE),            # neither, and too large
        # a correction under 1 ms on a header-matching (or unlogged) visit
        # is an ordinary correction, not an already-corrected input
        (0.0, True, 0.0005, S.GUARD_APPLY),
        (0.0, False, 0.0005, S.GUARD_APPLY),
    ]
    h = np.array([h0 if c[1] else np.nan for c in cases])
    tp = np.array([h0 + c[0] / 86400 for c in cases])
    tcorr = np.array([h0 + c[2] / 86400 for c in cases])
    verdict, dh, shift = S.header_guard({"header_mid_mjd_tai": h}, tp, tcorr)
    assert list(verdict) == [c[3] for c in cases]
    assert shift == pytest.approx([c[2] - c[0] for c in cases], abs=1e-5)
    assert dh[0] == pytest.approx(0.0009, abs=1e-6) and np.isnan(dh[3])


def test_correction_cap_boundary(monkeypatch):
    """|shift| == MAX_CORRECTION_S is applied; just beyond it is not."""
    h0 = 61046.25
    tcorr = np.array([h0 + 3.0 / 86400, h0 - 3.0 / 86400])
    tp = np.array([h0, h0])
    shift = (tcorr - tp) * 86400
    monkeypatch.setattr(S, "MAX_CORRECTION_S", float(np.abs(shift).min()))
    verdict, _, _ = S.header_guard({"header_mid_mjd_tai": tp}, tp, tcorr)
    i = int(np.argmin(np.abs(shift)))
    assert verdict[i] == S.GUARD_APPLY and verdict[1 - i] in (S.GUARD_APPLY, S.SHIFT_TOO_LARGE)
    monkeypatch.setattr(S, "MAX_CORRECTION_S", float(np.abs(shift).min()) * (1 - 1e-12))
    verdict, _, _ = S.header_guard({"header_mid_mjd_tai": tp}, tp, tcorr)
    assert list(verdict) == [S.SHIFT_TOO_LARGE] * 2


def test_shift_window_covers_the_cap():
    assert S.MAX_SHUTTER_SHIFT_S >= S.MAX_CORRECTION_S


def _late(tmp_path, shift_s):
    """A fixture copy where V0 is a late-readout visit: header midpoint (=
    pipeline time) ``shift_s`` after the corrected time of (D0, X0, Y0).
    Returns (table, corrected time, pipeline time)."""
    table = _copy_table(tmp_path)
    tc = reference(V0, D0, X0, Y0, table_dir=table).t_mid_mjd_tai[0]
    tp = tc + shift_s / 86400
    _set_header(table, V0, tp)
    return table, tc, tp


@pytest.mark.parametrize("shift, expect", [(3.5, "shift_too_large"), (2.5, "ok")])
def test_cap_end_to_end(tmp_path, capsys, shift, expect):
    table, tc, tp = _late(tmp_path, shift)
    view = [dict(id=1, ra=10.0, midpointMjdTai=tp, visit=V0, detector=D0, x=X0, y=Y0)]
    obs = [dict(obsid="o", obssubid="1", ra=10.0, dec=1.0, obstime=utc(tp), band="Lr", mag=20.0)]
    out, _, rep = run(tmp_path, obs, view, table=table)
    r = out["o"]
    assert rep["status"][expect] == 1 and sum(rep["status"].values()) == 1
    if expect == "ok":
        assert r["midpointMjdTai"] == tc and not r["midpointMjdTai_flag"]
    else:
        assert r["midpointMjdTai"] == tp and r["midpointMjdTai_flag"]
        assert not r["midpointMjdTai_flag_degraded"]
        err = capsys.readouterr().err
        assert f"{V0} ({shift:.3f} s)" in err
    assert r["midpointMjdTaiVisit"] == tp


@pytest.mark.parametrize("degraded", [False, True])
def test_already_corrected(tmp_path, degraded):
    """The pipeline time is the corrected time: it stands, unflagged, with
    the degraded flag following the correction's status."""
    v, d, x, y = (V1, D1, X1, Y1) if degraded else (V0, D0, X0, Y0)
    tc = reference(v, d, x, y).t_mid_mjd_tai[0]
    tp = tc + 0.0004 / 86400                     # within 1 ms of the corrected time
    view = [dict(id=1, ra=10.0, midpointMjdTai=tp, visit=v, detector=d, x=x, y=y)]
    # submitted from the already-corrected time
    obs = [dict(obsid="o", obssubid="1", ra=10.0, dec=1.0, obstime=utc(tp), band="Lr", mag=20.0)]
    out, unres, rep = run(tmp_path, obs, view)
    r = out["o"]
    assert r["midpointMjdTai"] == r["midpointMjdTaiVisit"] == tp
    assert not r["midpointMjdTai_flag"] and r["midpointMjdTai_flag_degraded"] is degraded
    assert rep["status"]["already_corrected"] == 1 and sum(rep["status"].values()) == 1
    corr = S.Corrections(TABLE)
    corr.lookup(view_table(view), np.array([0]))
    assert list(corr.status) == [S.ALREADY_CORRECTED_DEGRADED if degraded else S.ALREADY_CORRECTED]
    assert corr.t[0] == tp


def test_matching_neither_is_time_mismatch(tmp_path):
    tc = reference(V0, D0, X0, Y0).t_mid_mjd_tai[0]
    tp = VISIT_T[V0] + 0.05 / 86400               # 50 ms off the header, ~166 ms off the corrected time
    assert abs(tc - tp) * 86400 > 0.1
    view = [dict(id=1, ra=10.0, midpointMjdTai=tp, visit=V0, detector=D0, x=X0, y=Y0)]
    obs = [dict(obsid="o", obssubid="1", ra=10.0, dec=1.0, obstime=utc(tp), band="Lr", mag=20.0)]
    out, _, rep = run(tmp_path, obs, view)
    assert out["o"]["midpointMjdTai"] == tp and out["o"]["midpointMjdTai_flag"]
    assert rep["status"]["time_mismatch"] == 1 and rep["time_mismatch_visits"] == [V0]


def test_exposure_log():
    corr = S.Corrections(TABLE)
    log = corr.exposure_log(np.array([V0, 2026010500251, 2026010500999, 2026071200100]))
    assert set(log) == set(S.GUARD_LOG_COLUMNS)
    h = log["header_mid_mjd_tai"]
    assert h[0] == VISIT_T[V0] and h[3] == VISIT_T[2026071200100]
    assert np.isnan(h[1]) and np.isnan(h[2])        # failed (NaN in the log); not in the log


def _set_header(table, visit, value):
    """Set ``visit``'s header_mid_mjd_tai in the exposure log of a table copy,
    keeping the file's metadata."""
    p = table / f"exposures_{visit // 100000}.parquet"
    t = pq.read_table(p)
    md = t.schema.metadata
    h = t["header_mid_mjd_tai"].to_numpy().copy()
    h[t["visit"].to_numpy() == visit] = value
    i = t.column_names.index("header_mid_mjd_tai")
    t = t.set_column(i, "header_mid_mjd_tai", pa.array(h))
    pq.write_table(t.replace_schema_metadata(md), p)


def test_time_mismatch(tmp_path, capsys):
    """A pipeline time that disagrees with the (perturbed) header midpoint:
    the visit time, flagged, warned about, counted and listed."""
    table = _copy_table(tmp_path)
    _set_header(table, V0, VISIT_T[V0] + 0.1 / 86400)
    tv = VISIT_T[V0]
    src = dict(visit=V0, detector=D0, x=X0, y=Y0)
    view = [dict(id=1, ra=10.0, midpointMjdTai=tv, **src),
            dict(id=2, ra=11.0, midpointMjdTai=VISIT_T[2026071200100], visit=2026071200100, detector=94,
                 x=2000.0, y=2000.0)]
    common = dict(dec=1.0, band="Lr", mag=20.0)
    obs = [dict(obsid="m", obssubid="1", ra=10.0, obstime=utc(tv), **common),
           dict(obsid="k", obssubid="2", ra=11.0, obstime=utc(VISIT_T[2026071200100]), **common)]
    out, _, rep = run(tmp_path, obs, view, table=table)
    m = out["m"]
    assert m["midpointMjdTai_flag"] and not m["midpointMjdTai_flag_degraded"]
    assert m["midpointMjdTai"] == m["midpointMjdTaiVisit"] == tv and m["obstime_basis"] == "visit"
    assert not out["k"]["midpointMjdTai_flag"]
    assert rep["status"]["time_mismatch"] == 1 and rep["time_mismatch_visits"] == [V0]
    err = capsys.readouterr().err
    assert f"{V0} (-100.0 ms)" in err and "header midpoint" in err


def test_large_consistent_offset_is_corrected(tmp_path):
    """A late-readout visit: the pipeline time equals the header midpoint,
    both 1.5 s after the table's visit midpoint. The correction is applied
    however large, and the position pass finds a row submitted at the
    corrected time."""
    table = _copy_table(tmp_path)
    late = VISIT_T[V0] + 1.5 / 86400
    _set_header(table, V0, late)
    tc = reference(V0, D0, X0, Y0, table_dir=table).t_mid_mjd_tai[0]
    assert (late - tc) * 86400 > 1.2
    src = dict(visit=V0, detector=D0, x=X0, y=Y0)
    view = [dict(id=1, ra=10.0, midpointMjdTai=late, **src), dict(id=2, ra=11.0, midpointMjdTai=late, **src)]
    common = dict(dec=1.0, band="Lr", mag=20.0)
    obs = [dict(obsid="v", obssubid="1", ra=10.0, obstime=utc(late), **common),
           dict(obsid="p", obssubid="hand", ra=11.0, obstime=utc(tc), **common)]
    out, unres, rep = run(tmp_path, obs, view, table=table)
    assert not unres
    for k in ("v", "p"):
        assert out[k]["midpointMjdTai"] == tc and out[k]["midpointMjdTaiVisit"] == late
        assert not out[k]["midpointMjdTai_flag"]
    assert out["v"]["obstime_basis"] == "visit"
    assert (out["p"]["obstime_basis"], out["p"]["match"]) == ("corrected", "position")
    assert rep["status"]["time_mismatch"] == 0 and rep["status"]["ok"] == 2


def test_no_usable_id(tmp_path):
    """No obs_sbn row has a usable id: no crash; the position pass runs."""
    tv = VISIT_T[V0]
    view = [dict(id=1, ra=10.0, midpointMjdTai=tv, visit=V0, detector=D0, x=X0, y=Y0)]
    obs = [dict(obsid="o", obssubid="hand", ra=10.0, dec=1.0, obstime=utc(tv), band="Lr", mag=20.0),
           dict(obsid="u", obssubid=None, ra=50.0, dec=1.0, obstime=utc(tv), band="Lr", mag=20.0)]
    tasks = S.id_queries(S.load_obs(obs_table(obs))[0], "ssp", 10)
    assert len(tasks) == 1 and len(tasks[0][3]["q"]) == 0
    out, unres, _ = run(tmp_path, obs, view)
    assert (out["o"]["match"], out["o"]["diaSourceId"]) == ("position", 1)
    assert unres["u"]["reason"] == "no_id"
    pq.write_table(obs_table(obs), tmp_path / "obs.parquet")       # and without a table
    assert S.extract(tmp_path / "obs.parquet", tmp_path / "d2.parquet", fake_fetch(view_table(view))) == 0
    assert pq.read_table(tmp_path / "d2.parquet")["obsid"].to_pylist() == ["o"]


#
# Errors
#

def _copy_table(tmp_path):
    dst = tmp_path / "table"
    shutil.copytree(TABLE, dst)
    dst.chmod(0o755)
    for p in dst.iterdir():
        p.chmod(0o644)
    return dst


def _set_metadata(path, **kv):
    t = pq.read_table(path)
    md = dict(t.schema.metadata)
    md.update({k.encode(): v.encode() for k, v in kv.items()})
    pq.write_table(t.replace_schema_metadata(md), path)


def _one(tmp_path, table, visit=V0, visit_type=pa.int64()):
    pq.write_table(obs_table([dict(obsid="o", obssubid="1", ra=10.0, dec=1.0, obstime=utc(VISIT_T[V0]),
                                   band="Lr", mag=20.0)]), tmp_path / "obs.parquet")
    view = view_table([dict(id=1, ra=10.0, midpointMjdTai=VISIT_T[V0], visit=visit, detector=D0, x=X0, y=Y0)],
                      visit_type=visit_type)
    return S.extract(tmp_path / "obs.parquet", tmp_path / "dia.parquet", fake_fetch(view),
                     correction_table=table)


@pytest.mark.parametrize("damage, error", [
    ("calibration", "CalibrationMismatchError"),
    ("format", "TableFormatError"),
    ("integrity", "TableIntegrityError"),
])
def test_table_errors(tmp_path, damage, error):
    table = _copy_table(tmp_path)
    if damage == "calibration":     # another night on another calibration: a mixed table
        for f in ("corrections_20250810.parquet", "exposures_20250810.parquet"):
            _set_metadata(table / f, calibration_id="0123456789ab")
    elif damage == "format":
        for f in ("corrections_20260105.parquet", "exposures_20260105.parquet"):
            _set_metadata(table / f, table_format="99")
    else:                           # the night's log without its rows
        (table / "corrections_20260105.parquet").unlink()
    with pytest.raises(S.CorrectionError, match=error):
        _one(tmp_path, table)
    assert not (tmp_path / "dia.parquet").exists()


def test_value_errors(tmp_path):
    with pytest.raises(S.CorrectionError, match="ValueError"):
        _one(tmp_path, TABLE, visit=V0 + 0.5, visit_type=pa.float64())     # a non-integral visit
    with pytest.raises(S.CorrectionError, match="null visit"):
        _one(tmp_path, TABLE, visit=None)
    with pytest.raises(S.CorrectionError, match="does not exist"):
        _one(tmp_path, tmp_path / "nowhere")
    assert not (tmp_path / "dia.parquet").exists()


def test_cli_reports_correction_errors(tmp_path, monkeypatch, capsys):
    pq.write_table(obs_table([]), tmp_path / "obs.parquet")
    monkeypatch.setattr("sys.argv", ["extract-submitted-sources", str(tmp_path / "obs.parquet"),
                                     str(tmp_path / "dia.parquet"),
                                     "--correction-table", str(tmp_path / "no")])
    with pytest.raises(SystemExit) as e:
        S.main()
    assert e.value.code == 1 and "does not exist" in capsys.readouterr().err


#
# Without a table: unchanged
#

def test_without_table_unchanged(tmp_path):
    """No correction table: the uncorrected extract, bitwise (same columns,
    same values), and the corrected-only match is not made."""
    tc = reference(V0, D0, X0, Y0).t_mid_mjd_tai[0]
    tv = VISIT_T[V0]
    src = dict(visit=V0, detector=D0, x=X0, y=Y0)
    view = view_table([dict(id=i, ra=9.0 + i, midpointMjdTai=tv, **src) for i in (1, 2, 3)])
    common = dict(dec=1.0, band="Lr", mag=20.0)
    obs = obs_table([dict(obsid="o1", obssubid="1", ra=10.0, obstime=utc(tv), **common),
                     dict(obsid="o2", obssubid="2", ra=11.0, obstime=utc(tc), **common),
                     dict(obsid="o3", obssubid="hand", ra=12.0, obstime=utc(tv), **common)])
    pq.write_table(obs, tmp_path / "obs.parquet")
    outs = {}
    for name, table in (("none", None), ("table", TABLE)):
        d = tmp_path / name
        d.mkdir()
        rc = S.extract(tmp_path / "obs.parquet", d / "dia.parquet", fake_fetch(view), correction_table=table)
        assert rc == 0
        outs[name] = pq.read_table(d / "dia.parquet")
    plain, corr = outs["none"], outs["table"]
    assert not set(S.SHUTTER_COLUMNS) & set(plain.column_names)
    assert plain.column_names[-len(S.EXTRA_COLUMNS):] == S.EXTRA_COLUMNS
    assert sorted(plain["obsid"].to_pylist()) == ["o1", "o3"]     # o2 matches only on the corrected time
    assert sorted(corr["obsid"].to_pylist()) == ["o1", "o2", "o3"]
    # the rows both match are the same, except for the corrected time
    c = corr.filter(pc.is_in(corr["obsid"], plain["obsid"])).sort_by("obsid")
    p = plain.sort_by("obsid")
    assert c["midpointMjdTaiVisit"].equals(p["midpointMjdTai"])
    for name in p.column_names:
        if name != "midpointMjdTai":
            assert c[name].equals(p[name]), name


#
# Review round: boundaries and edge cases
#

V1, D1, X1, Y1 = 2026010500250, 0, 100.0, 100.0         # the fixture's DEGRADED example


def test_time_mismatch_on_degraded(tmp_path):
    """The guard applies to DEGRADED corrections too, not only OK ones."""
    view = view_table([dict(id=1, ra=1.0, midpointMjdTai=VISIT_T[V1], visit=V1, detector=D1, x=X1, y=Y1)])
    corr = S.Corrections(TABLE)
    corr.lookup(view, np.array([0]))
    assert list(corr.status) == [S.DEGRADED]
    table = _copy_table(tmp_path)
    _set_header(table, V1, VISIT_T[V1] + 0.1 / 86400)
    corr = S.Corrections(table)
    corr.lookup(view, np.array([0]))
    assert list(corr.status) == [S.TIME_MISMATCH] and np.isnan(corr.t[0])
    assert corr.dvis[0] == pytest.approx(-0.1, abs=1e-5)


def test_first_night_is_in_coverage():
    """The table's first night itself is covered: its visits are looked up
    (OMITTED / NOT_BUILT), not OUTSIDE_COVERAGE."""
    corr = S.Corrections(TABLE)
    first = corr.first_night
    assert first == 20250810            # fixture-specific
    v_logged, v_absent = first * 100000 + 30, first * 100000 + 999
    view = view_table([dict(id=i, ra=1.0, midpointMjdTai=60897.1, visit=v, detector=94, x=2000.0, y=2000.0)
                       for i, v in enumerate((v_logged, v_absent, (first - 1) * 100000 + 30))])
    corr.lookup(view, np.arange(3))
    assert list(corr.status) == [S.OMITTED, S.NOT_BUILT, S.OUTSIDE_COVERAGE]


def test_not_built_loser_within_3_mas_does_not_count(tmp_path):
    """NOT_BUILT candidates that pass the position test (so are looked up)
    but lose -- on time, or on rank -- do not count toward the limit."""
    tv = VISIT_T[V0]
    nb = dict(visit=2026010600001, detector=94, x=2000.0, y=2000.0)
    view = [dict(id=1, ra=10.0, midpointMjdTai=tv, visit=V0, detector=D0, x=X0, y=Y0),
            # same position, 1 s off: fails the time test
            dict(id=1, processing="NV-S", ra=10.0, midpointMjdTai=tv + 1 / 86400, **nb),
            # same position and time, wrong band: passes but loses the rank
            dict(id=1, processing="DP2-DS", ra=10.0, band="z", midpointMjdTai=tv, **nb)]
    obs = [dict(obsid="o", obssubid="1", ra=10.0, dec=1.0, obstime=utc(tv), band="Lr", mag=20.0)]
    out, _, rep = run(tmp_path, obs, view, max_not_built_visits=0)
    assert out["o"]["processing"] == "AP-DS" and out["o"]["n_pass"] == 2
    assert rep["not_built_visits"] == [] and rep["status"]["not_built"] == 0
    # and both losers were indeed looked up (NOT_BUILT)
    v = view_table(view)
    c = S.Corrections(TABLE)
    o, _, _ = S.load_obs(obs_table(obs))
    S.correct(c, o, np.zeros(3, int), v, np.arange(3))
    assert list(c.status) == [S.OK, S.NOT_BUILT, S.NOT_BUILT]


def test_corrected_time_boundary(monkeypatch):
    """The time test on the corrected time: |dt| <= DT_MS passes (the
    boundary included), 10.001 ms and 20 ms fail; the visit time is far."""
    tai = 61046.25
    obs = dict(ra=np.full(3, 10.0), dec=np.full(3, 1.0), tai=np.full(3, tai),
               band_stripped=np.array(["r"] * 3, dtype=object), mag=np.full(3, 20.0))
    cand = view_table([dict(id=1, ra=10.0, midpointMjdTai=tai - 0.2 / 86400)])
    k = np.zeros(1, int)
    for off_ms, passes in ((10.0, True), (10.001, False), (20.0, False), (-10.001, False)):
        t_corr = np.array([tai + off_ms / 86400e3])
        dtc = (t_corr[0] - tai) * 86400e3
        if off_ms == 10.0:      # make the boundary exact: DT_MS == |dt|
            monkeypatch.setattr(S, "DT_MS", abs(dtc))
        else:
            monkeypatch.setattr(S, "DT_MS", 10.0)
        sc = S.score({k_: v[:1] for k_, v in obs.items()}, k, cand, k, t_corr)
        assert bool(sc["passed"][0]) is passes, off_ms
        assert sc["obstime_basis"][0] == ("corrected" if passes else None)


def test_corrected_boundary_end_to_end(tmp_path):
    tc = reference(V0, D0, X0, Y0).t_mid_mjd_tai[0]
    tv = VISIT_T[V0]
    src = dict(visit=V0, detector=D0, x=X0, y=Y0)
    view = [dict(id=i, ra=10.0 + i, midpointMjdTai=tv, **src) for i in (1, 2, 3)]
    common = dict(dec=1.0, band="Lr", mag=20.0)
    obs = [dict(obsid="a", obssubid="1", ra=11.0, obstime=utc(tc + 9.99 * MS), **common),
           dict(obsid="b", obssubid="2", ra=12.0, obstime=utc(tc + 10.01 * MS), **common),
           dict(obsid="c", obssubid="3", ra=13.0, obstime=utc(tc - 20 * MS), **common)]
    out, unres, _ = run(tmp_path, obs, view)
    assert sorted(out) == ["a"] and sorted(unres) == ["b", "c"]


@pytest.mark.parametrize("with_table", [False, True])
def test_position_window_by_mode(tmp_path, monkeypatch, with_table):
    """Without a table the position query's window is the uncorrected one
    (shift_s 0); with one, MAX_SHUTTER_SHIFT_S."""
    seen = []
    real = S.position_queries

    def spy(*a, **kw):
        seen.append(kw.get("shift_s", 0.0) if len(a) < 4 else a[3])
        return real(*a, **kw)
    monkeypatch.setattr(S, "position_queries", spy)
    tv = VISIT_T[V0]
    view = view_table([dict(id=1, ra=10.0, midpointMjdTai=tv, visit=V0, detector=D0, x=X0, y=Y0)])
    obs = [dict(obsid="o", obssubid="hand", ra=10.0, dec=1.0, obstime=utc(tv), band="Lr", mag=20.0)]
    pq.write_table(obs_table(obs), tmp_path / "obs.parquet")
    assert S.extract(tmp_path / "obs.parquet", tmp_path / "d.parquet", fake_fetch(view),
                     correction_table=TABLE if with_table else None) == 0
    assert seen == [S.MAX_SHUTTER_SHIFT_S if with_table else 0.0]


@pytest.mark.parametrize("with_table", [False, True])
def test_nothing_resolves(tmp_path, with_table):
    """No obs_sbn row resolves: empty outputs, no crash (predates S1)."""
    tv = VISIT_T[V0]
    view = [dict(id=1, ra=50.0, midpointMjdTai=tv, visit=V0, detector=D0, x=X0, y=Y0)]
    obs = [dict(obsid="o", obssubid="1", ra=10.0, dec=1.0, obstime=utc(tv), band="Lr", mag=20.0),
           dict(obsid="h", obssubid="hand", ra=20.0, dec=1.0, obstime=utc(tv), band="Lr", mag=20.0)]
    pq.write_table(obs_table(obs), tmp_path / "obs.parquet")
    report = {}
    assert S.extract(tmp_path / "obs.parquet", tmp_path / "d.parquet", fake_fetch(view_table(view)),
                     correction_table=TABLE if with_table else None, report=report) == 0
    out = pq.read_table(tmp_path / "d.parquet")
    assert out.num_rows == 0 and "primary" in out.column_names
    assert sorted(pq.read_table(tmp_path / "d.unresolved.parquet")["obsid"].to_pylist()) == ["h", "o"]
    if with_table:
        rep = report[SHUTTER_MANIFEST_FIELD[0]]
        assert set(S.SHUTTER_COLUMNS) <= set(out.column_names)
        assert sum(rep["status"].values()) == 0 and rep["not_built_visits"] == []
