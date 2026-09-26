"""Tests for ssp.export.submittable (no network: queries are answered by a
fake ``fetch`` from a synthetic view table)."""

import datetime

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from ssp.export import submittable as S

T0 = datetime.datetime(2025, 6, 9, 3, 0, 0)
MAS = 1 / 3.6e6  # one mas in degrees


def obs_table(rows):
    """obs_sbn-shaped table from dicts of obs_sbn columns."""
    cols = {c: [r.get(c) for r in rows] for c in S.OBS_COLUMNS}
    cols["stn"] = [r.get("stn", "X05") for r in rows]
    types = dict(obstime=pa.timestamp("us"), ra=pa.float64(), dec=pa.float64(), mag=pa.float64())
    return pa.table({c: pa.array(v, types.get(c, pa.string())) for c, v in cols.items()})


def tai(dt):
    return S.utc_to_tai_mjd(pa.array([dt], pa.timestamp("us")))[0]


def view_table(rows):
    """SubmittableSources-shaped table, plus a made-up extra column."""
    names = ["collection", "id", "band", "mjd_tai", "ra", "dec", "psfFlux", "psfFluxErr",
             "trailRa", "trailDec", "hpix29", "madeUpColumn"]
    types = dict(collection=pa.string(), id=pa.int64(), band=pa.string(), hpix29=pa.int64(),
                 madeUpColumn=pa.string())
    cols = {c: [r.get(c) for r in rows] for c in names}
    # hpix29 as the ingest computes it: the depth-29 cell of (ra, dec)
    ra = np.array([r["ra"] for r in rows], dtype=float)
    dec = np.array([r["dec"] for r in rows], dtype=float)
    if len(rows):
        from astropy.coordinates import Latitude, Longitude
        from cdshealpix.nested import lonlat_to_healpix
        import astropy.units as u
        cols["hpix29"] = lonlat_to_healpix(Longitude(ra, unit=u.deg), Latitude(dec, unit=u.deg), 29).tolist()
    cols["psfFlux"] = [r.get("psfFlux", 1000.0) for r in rows]
    cols["madeUpColumn"] = [f"x{i}" for i in range(len(rows))]
    return pa.table({c: pa.array(v, types.get(c, pa.float64())) for c, v in cols.items()})


def fake_fetch(view):
    """Answer run_queries-style tasks from ``view``: id tasks by filtering on
    (collection, id); position tasks by returning everything (the client-side
    cell pairing and verification must do the rest)."""
    def fetch(tasks):
        out = []
        for key, sql, params, sets in tasks:
            t = view
            if "q" in sets:
                mask = pc.is_in(t["id"], pa.array(sets["q"], pa.int64()))
                if params:
                    mask = pc.and_(mask, pc.equal(t["collection"], params["label"]))
                t = t.filter(mask)
            out.append((key, t))
        return out
    return fetch


#
# Parsing
#

def test_parse_obssubid():
    s = ["LSST-DP2-DS-123", " LSST-AP-DS-170666254671544360-A ", "LSST-AP-DS-5-B", "456",
         "LSST-NEW-THING-X-7", "garbage", None, "", "LSST-DP2-DS-", "99999999999999999999",
         "12-A", "LSST-DP2-DS-12-C"]
    label, ids, part = S.parse_obssubid(s)
    assert list(label) == ["DP2-DS", "AP-DS", "AP-DS", None, "NEW-THING-X", None, None, None, None, None,
                           None, None]
    assert list(ids) == [123, 170666254671544360, 5, 456, 7, -1, -1, -1, -1, -1, -1, -1]
    assert list(part) == ["", "A", "B", "", "", "", "", "", "", "", "", ""]


def test_strip_band():
    assert list(S.strip_band(["Lr", "r", "Ly", None, "L"])) == ["r", "r", "y", None, "L"]


def test_utc_to_tai():
    t = pa.array([datetime.datetime(2025, 1, 1), datetime.datetime(2026, 9, 1, 12, 0, 0, 123456), None],
                 pa.timestamp("us"))
    out = S.utc_to_tai_mjd(t)
    assert out[0] == pytest.approx(60676 + 37 / 86400, abs=1e-11)
    assert (out[1] - (61284.5 + 0.123456 / 86400)) * 86400 == pytest.approx(37, abs=1e-5)
    assert np.isnan(out[2])


def test_midpoint_ra_wrap():
    ra, dec, t = S.midpoint(np.array([359.9999, 10.0]), np.array([1.0, -5.0]), np.array([1.0, 2.0]),
                            np.array([0.0003, 10.0002]), np.array([1.0002, -5.0002]), np.array([3.0, 2.0]))
    assert ra == pytest.approx([0.0001, 10.0001])
    assert dec == pytest.approx([1.0001, -5.0001])
    assert t == pytest.approx([2.0, 2.0])


def test_load_obs_pairs_and_reasons():
    rows = [
        dict(obsid="a", obssubid="LSST-AP-DS-5-A", ra=359.9999, dec=1.0, obstime=T0, band="Lr", mag=20.0),
        dict(obsid="b", obssubid="LSST-AP-DS-5-B", ra=0.0001, dec=1.0002,
             obstime=T0 + datetime.timedelta(seconds=30), band="Lr", mag=20.1),
        dict(obsid="c", obssubid="LSST-AP-DS-6-A", ra=10.0, dec=1.0, obstime=T0, band="Lg", mag=20.0),
        dict(obsid="d", obssubid="hand", ra=10.0, dec=1.0, obstime=T0, band="g", mag=20.0),
        dict(obsid="e", obssubid="42", ra=10.0, dec=1.0, obstime=T0, band="g", mag=20.0),
        dict(obsid="f", obssubid="42", ra=10.0, dec=1.0, obstime=T0, band="g", mag=20.0, stn="I41"),
    ]
    obs, tbl, n_pairs = S.load_obs(obs_table(rows))
    assert n_pairs == 1 and len(tbl) == 5
    assert list(obs["obsid"]) == ["a", "c", "d", "e"]
    assert list(obs["obsid_b"]) == ["b", None, None, None]
    assert list(obs["reason"]) == ["", "unpaired_trail", "no_id", ""]
    assert list(obs["id"]) == [5, -1, -1, 42]
    assert obs["ra"][0] == pytest.approx(0.0) and obs["dec"][0] == pytest.approx(1.0001)
    assert (obs["tai"][0] - tai(T0)) * 86400 == pytest.approx(15, abs=1e-5)
    assert obs["band_stripped"][0] == "r" and obs["mag"][0] == 20.0


#
# Verification + ranking
#

def _obs(ra=10.0, dec=1.0, band="r", mag=20.0, n=1):
    return dict(ra=np.full(n, ra), dec=np.full(n, dec), tai=np.full(n, tai(T0)),
                band_stripped=np.array([band] * n, dtype=object), mag=np.full(n, mag))


def _cand(**kw):
    base = dict(collection="DP2-DS", id=1, band="r", mjd_tai=tai(T0), ra=10.0, dec=1.0)
    return view_table([{**base, **k} for k in kw.get("rows", [{}])])


def test_score_psf_trail_and_edges():
    t = tai(T0)
    cand = _cand(rows=[
        dict(dec=1.0 + 2.9 * MAS),                                              # psf, just inside
        dict(dec=1.0 + 3.1 * MAS),                                              # psf, just outside
        dict(dec=1.0 + 500 * MAS, trailRa=10.0, trailDec=1.0 + 1 * MAS),        # trail match
        dict(mjd_tai=t + 9.9e-3 / 86400),                                       # time, inside
        dict(mjd_tai=t - 10.1e-3 / 86400),                                      # time, outside
        dict(band="z", psfFlux=-5.0),                                           # band mismatch passes
    ])
    obs = _obs(n=1)
    k = np.arange(len(cand))
    sc = S.score(obs, np.zeros(len(k), int), cand, k)
    assert list(sc["passed"]) == [True, False, True, True, False, True]
    assert sc["sep_mas"][2] == pytest.approx(1.0, abs=1e-3)
    assert list(sc["band_ok"]) == [True] * 5 + [False]
    assert np.isnan(sc["dmag"][5])
    assert sc["dmag"][0] == pytest.approx(20.0 - (31.4 - 2.5 * 3))


def _rank(rows):
    c = np.array([r[0] for r in rows], dtype=object)
    i = np.array([r[1] for r in rows])
    sep = np.array([r[2] for r in rows], dtype=float)
    b = np.array([r[3] for r in rows], dtype=bool)
    return S.rank(np.zeros(len(rows), int), c, i, sep, b)


def test_rank_preferences():
    # band_ok beats separation
    win, n, amb = _rank([("DP2-DS", 1, 0.1, False), ("DP2-DS", 2, 2.0, True)])
    assert list(win) == [1] and list(n) == [2] and not amb[0]
    # 002-DS never wins over a live label, even when closer
    win, _, amb = _rank([("002-DS", 1, 0.0, True), ("AP-DS", 1, 1.0, True)])
    assert list(win) == [1] and not amb[0]
    # a never-seen label ranks normally; ties break on (collection, id)
    win, _, amb = _rank([("pDP2-DS", 7, 0.5, True), ("DP2-DS", 7, 0.5, True), ("ZZ-NEW", 7, 0.1, True)])
    assert list(win) == [2] and not amb[0]
    win, _, amb = _rank([("pDP2-DS", 7, 0.5, True), ("DP2-DS", 7, 0.5, True)])
    assert list(win) == [1] and amb[0]
    # within AMBIGUOUS_MAS: flagged, but the closer one still wins
    win, _, amb = _rank([("DP2-DS", 9, 0.5, True), ("DP2-DS", 8, 0.505, True)])
    assert list(win) == [0] and amb[0]
    # not ambiguous when the runner-up differs in band_ok
    win, _, amb = _rank([("DP2-DS", 1, 0.5, True), ("DP2-DS", 2, 0.5, False)])
    assert list(win) == [0] and not amb[0]


def test_rank_groups():
    oi = np.array([3, 1, 3, 1, 1])
    c = np.array(["A"] * 5, dtype=object)
    win, n, amb = S.rank(oi, c, np.arange(5), np.array([1.0, 2.0, 0.5, 0.1, 3.0]), np.ones(5, bool))
    assert list(win) == [3, 2] and list(n) == [3, 2]


def test_join_ids():
    ai, bi = S.join_ids(np.array([5, 1, 5, 9]), np.array([5, 5, 1, 7]))
    pairs = sorted(zip(ai.tolist(), bi.tolist()))
    assert pairs == [(0, 0), (0, 1), (1, 2), (2, 0), (2, 1)]


#
# End to end with a fake fetch
#

def _scenario():
    t = T0
    rows = [
        # labelled id; the view also has it in pDP2-DS (must not be used)
        dict(obsid="o1", obssubid="LSST-DP2-DS-100", ra=10.0, dec=1.0, obstime=t, band="Lr", mag=20.0),
        # bare id, identical in DP2-DS and pDP2-DS -> ambiguous
        dict(obsid="o2", obssubid="200", ra=11.0, dec=1.0, obstime=t, band="Lg", mag=20.0),
        # submitted band y, view says z: resolves, band_ok false
        dict(obsid="o3", obssubid="300", ra=12.0, dec=1.0, obstime=t, band="Ly", mag=20.0),
        # A/B trail pair; the view's trail centroid is the midpoint
        dict(obsid="o4a", obssubid="LSST-AP-DS-400-A", ra=13.0, dec=1.0, obstime=t, band="Li", mag=19.0,
             submission_id="s4a", trksub="t4a", trkid="k4a"),
        dict(obsid="o4b", obssubid="LSST-AP-DS-400-B", ra=13.0, dec=1.0002,
             obstime=t + datetime.timedelta(seconds=30), band="Li", mag=19.0,
             submission_id="s4b", trksub="t4b", trkid="k4b"),
        # hand submission: no id, found by position
        dict(obsid="o5", obssubid="hand", ra=14.0, dec=1.0, obstime=t, band="Lr", mag=20.0),
        # unknown label -> no candidate -> found by position (in DP2-DS)
        dict(obsid="o6", obssubid="LSST-NOPE-DS-600", ra=15.0, dec=1.0, obstime=t, band="Lr", mag=20.0),
        # id exists but 1" away, nothing at the position -> no_pass
        dict(obsid="o7", obssubid="LSST-DP2-DS-700", ra=16.0, dec=1.0, obstime=t, band="Lr", mag=20.0),
        # nothing anywhere -> unresolved no_id
        dict(obsid="o8", obssubid=None, ra=17.0, dec=1.0, obstime=t, band="Lr", mag=20.0),
    ]
    tt = tai(t)
    view = [
        dict(collection="DP2-DS", id=100, band="r", mjd_tai=tt, ra=10.0, dec=1.0 + 0.5 * MAS),
        dict(collection="pDP2-DS", id=100, band="r", mjd_tai=tt, ra=10.0, dec=1.0),
        dict(collection="DP2-DS", id=200, band="g", mjd_tai=tt, ra=11.0, dec=1.0),
        dict(collection="pDP2-DS", id=200, band="g", mjd_tai=tt, ra=11.0, dec=1.0),
        dict(collection="NV-S", id=200, band="g", mjd_tai=tt, ra=50.0, dec=1.0),
        dict(collection="DP2-S", id=300, band="z", mjd_tai=tt + 1.4e-3 / 86400, ra=12.0, dec=1.0),
        dict(collection="AP-DS", id=400, band="i", mjd_tai=tt + 15 / 86400, ra=13.0, dec=1.0 + 0.3 / 3600,
             trailRa=13.0, trailDec=1.0001),
        dict(collection="DP2-DS", id=500, band="r", mjd_tai=tt, ra=14.0, dec=1.0 + 1 * MAS),
        dict(collection="DP2-DS", id=601, band="r", mjd_tai=tt, ra=15.0, dec=1.0),
        dict(collection="DP2-DS", id=700, band="r", mjd_tai=tt, ra=16.0, dec=1.0 + 1000 * MAS),
    ]
    return obs_table(rows), view_table(view)


def test_extract_end_to_end(tmp_path):
    obs, view = _scenario()
    pq.write_table(obs, tmp_path / "obs.parquet")
    rc = S.extract(tmp_path / "obs.parquet", tmp_path / "dia.parquet", fake_fetch(view))
    assert rc == 0
    out = pq.read_table(tmp_path / "dia.parquet").to_pylist()
    by = {r["obsid"]: r for r in out}
    assert sorted(by) == ["o1", "o2", "o3", "o4a", "o5", "o6"]

    assert (by["o1"]["collection"], by["o1"]["diaSourceId"], by["o1"]["match"]) == ("DP2-DS", 100, "id")
    assert (by["o2"]["collection"], by["o2"]["ambiguous"], by["o2"]["n_pass"]) == ("DP2-DS", True, 2)
    assert by["o3"]["band_ok"] is False and by["o3"]["dt_ms"] == pytest.approx(1.4, abs=1e-3)
    assert by["o4a"]["obsid_b"] == "o4b" and by["o4a"]["diaSourceId"] == 400
    assert by["o4a"]["sep_mas"] < 0.01 and abs(by["o4a"]["dt_ms"]) < 1e-3
    assert (by["o4a"]["submission_id"], by["o4a"]["trksub"], by["o4a"]["trkid"]) == ("s4a", "t4a", "k4a")
    assert (by["o5"]["diaSourceId"], by["o5"]["match"]) == (500, "position")
    assert (by["o6"]["diaSourceId"], by["o6"]["match"]) == (601, "position")

    # all view columns pass through, renamed; missing required ones null-filled
    r = by["o1"]
    assert r["madeUpColumn"] == "x0" and "id" not in r and "mjd_tai" not in r
    assert r["midpointMjdTai"] == pytest.approx(tai(T0))
    assert r["extendedness"] is None and r["psfFluxErr"] is None
    schema = pq.read_schema(tmp_path / "dia.parquet")
    for name, typ in S.REQUIRED_COLUMNS.items():
        assert schema.field(name).type == typ

    unres = {r["obsid"]: r for r in pq.read_table(tmp_path / "dia.unresolved.parquet").to_pylist()}
    assert unres["o7"]["reason"] == "no_pass" and unres["o7"]["best_sep_mas"] == pytest.approx(1000, abs=0.01)
    assert unres["o8"]["reason"] == "no_id" and unres["o8"]["best_sep_mas"] is None
    assert sorted(unres) == ["o7", "o8"]


def test_extract_double_submission(tmp_path):
    # Like the real case: one detection submitted twice (two obsids, same
    # bare obssubid, obstime 4 ms apart), in two submissions. The earliest
    # submission's row is kept, the other goes to duplicates.parquet.
    ms4 = datetime.timedelta(milliseconds=4)
    rows = [
        dict(obsid="Ltt1late", obssubid="100", submission_id="2026-04-25T01:36:42.617_0000BuRx",
             trksub="late",
             ra=10.0, dec=1.0, obstime=T0 + ms4, band="Li", mag=20.0),
        dict(obsid="Lsa1early", obssubid="100", submission_id="2026-02-06T01:14:28.408_0000Bl6Z",
             trksub="early",
             ra=10.0, dec=1.0, obstime=T0, band="Li", mag=20.0),
        dict(obsid="other", obssubid="101", submission_id="2026-04-25T01:36:42.617_0000BuRx",
             ra=11.0, dec=1.0, obstime=T0, band="Li", mag=20.0),
    ]
    view = view_table([
        dict(collection="NV-S", id=100, band="i", mjd_tai=tai(T0) + 2e-3 / 86400, ra=10.0, dec=1.0),
        dict(collection="NV-S", id=101, band="i", mjd_tai=tai(T0), ra=11.0, dec=1.0),
    ])
    pq.write_table(obs_table(rows), tmp_path / "obs.parquet")
    assert S.extract(tmp_path / "obs.parquet", tmp_path / "dia.parquet", fake_fetch(view)) == 0
    out = pq.read_table(tmp_path / "dia.parquet")
    assert sorted(out["obsid"].to_pylist()) == ["Lsa1early", "other"]
    assert out.filter(pc.equal(out["obsid"], "Lsa1early"))["trksub"].to_pylist() == ["early"]
    dups = pq.read_table(tmp_path / "dia.duplicates.parquet").to_pylist()
    assert len(dups) == 1
    d = dups[0]
    assert (d["obsid"], d["kept_obsid"]) == ("Ltt1late", "Lsa1early")
    assert (d["collection"], d["diaSourceId"]) == ("NV-S", 100)
    assert d["dt_ms"] == pytest.approx(-2.0, abs=1e-3)
    assert pq.read_table(tmp_path / "dia.unresolved.parquet").num_rows == 0


def test_dedupe_tiebreak():
    t = pa.table(dict(collection=["A", "B", "A", "A"], diaSourceId=pa.array([1, 1, 2, 1], pa.int64()),
                      obsid=["z", "y", "x", "w"]))
    keep, drop, kept = S.dedupe(t, np.array(["2026-01", "2026-01", "2026-01", "2026-01"], dtype=object))
    assert list(keep) == [1, 2, 3] and list(drop) == [0] and list(kept) == [3]
    keep, drop, kept = S.dedupe(t, np.array(["2025-12", None, "2026-01", "2026-01"], dtype=object))
    assert list(keep) == [0, 1, 2] and list(drop) == [3] and list(kept) == [0]


def test_build_output_name_clash():
    obs, view = _scenario()
    o, _, _ = S.load_obs(obs)
    info = dict(sep_mas=[0.0], dt_ms=[0.0], dmag=[0.0], band_ok=[True], n_pass=[1], ambiguous=[False])
    ok = S.build_output(o, view, np.array([0]), np.array([0]), ["id"], info)
    assert len(set(ok.column_names)) == len(ok.column_names)
    for bad in (view.append_column("sep_mas", view["ra"]), view.append_column("diaSourceId", view["id"]),
                view.append_column("trkid", view["band"])):
        with pytest.raises(ValueError, match="clash"):
            S.build_output(o, bad, np.array([0]), np.array([0]), ["id"], info)


def test_read_chpass(tmp_path):
    p = tmp_path / "chpass"
    p.write_text("# comment\nother:8123:ssp:u0:p0\n*:8123:ssp:u1:p\\:1\n*:*:*:u2:p2\n")
    p.chmod(0o600)
    assert S.read_chpass(p, "h", 8123, "ssp") == ("u1", "p:1")
    assert S.read_chpass(p, "h", 8123, "ssp", user="u2") == ("u2", "p2")
    p.chmod(0o640)
    with pytest.raises(SystemExit):
        S.read_chpass(p, "h", 8123, "ssp")
