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

# visit midpoints (MJD TAI) of the fixture's corrected visits, as their
# exposure logs have them; any value for the others
VISIT_T = {2026010500249: 61046.25468055982, 2026010500250: 61046.25486978367,
           2026071200100: 61233.98278576869, 2026010500251: 61046.26, 2026010500999: 61046.27,
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
                            midpointMjdTai=0.0) for i, e in enumerate(ex)])
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
    assert rep["status"] == dict(ok=n[0], degraded=n[1], omitted=n[2], not_built=n[3])
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
    src = dict(visit=V0, detector=D0, x=X0, y=Y0)
    view = [
        dict(id=1, ra=10.0, midpointMjdTai=tv, **src),
        dict(id=2, ra=11.0, midpointMjdTai=tv, **src),
        dict(id=3, ra=12.0, midpointMjdTai=tv, **src),
        dict(id=4, ra=13.0, midpointMjdTai=tc - 3 * MS, **src),       # visit time ~ corrected time
        dict(id=5, ra=14.0, midpointMjdTai=tc - 100 * MS, **src),     # see o5
    ]
    obs = [
        dict(obsid="o1", obssubid="1", ra=10.0, dec=1.0, obstime=utc(tc), band="Lr", mag=20.0),
        dict(obsid="o2", obssubid="2", ra=11.0, dec=1.0, obstime=utc(tv), band="Lr", mag=20.0),
        dict(obsid="o4", obssubid="4", ra=13.0, dec=1.0, obstime=utc(tc), band="Lr", mag=20.0),
        # 50 ms off both the visit time and the corrected time
        dict(obsid="o5", obssubid="5", ra=14.0, dec=1.0, obstime=utc(tc - 50 * MS), band="Lr", mag=20.0),
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
    assert rep["status"] == dict(ok=4, degraded=0, omitted=0, not_built=0)
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
