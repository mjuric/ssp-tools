"""WP3: read_dia, build_visits, DiaIndex.match and VisitIndex.candidates,
against brute force. No network: the observer's position is stubbed."""
import warnings

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from astropy.time import Time

from ssp import util
from ssp.nearbysso import _contract as C
from ssp.nearbysso import visits as V

R = C.MATCH_RADIUS_ARCSEC


@pytest.fixture(autouse=True)
def _no_network_observer(monkeypatch):
    """A fake X05 state (build_visits otherwise fetches the MPC obscodes)."""
    import astropy.units as u

    def fake(obscode, t):
        assert obscode == "X05"
        x = np.atleast_1d(t.tai.mjd)
        return (np.stack([x, 2 * x, 3 * x]) * u.au, np.stack([x, -x, 0 * x]) * u.au / u.day)
    monkeypatch.setattr(util, "observatory_barycentric_posvel", fake)


def unit(ra, dec):
    return V._unit_vectors(ra, dec)


def radec(q):
    """RA, Dec [deg] of unit vectors."""
    ra = np.degrees(np.arctan2(q[..., 1], q[..., 0])) % 360.0
    return ra, np.degrees(np.arcsin(np.clip(q[..., 2], -1, 1)))


def offset(ra, dec, r_arcsec, phi):
    """Points r_arcsec from (ra, dec) [deg] in direction phi."""
    p = unit(ra, dec)
    a = np.radians(ra)
    e1 = np.stack([-np.sin(a), np.cos(a), 0 * a], -1)
    e2 = np.cross(p, e1)
    r = np.radians(np.asarray(r_arcsec) / 3600.0)
    q = p * np.cos(r)[..., None] + np.sin(r)[..., None] * (np.cos(phi)[..., None] * e1
                                                           + np.sin(phi)[..., None] * e2)
    return radec(q)


def make_dia(visit_ids, centers, n_per, spread_deg, rng, t0=60800.0):
    """A visit-sorted synthetic catalog, n_per sources per visit uniformly in
    a disk of spread_deg about each centre."""
    vis, ra, dec, t = [], [], [], []
    for i, (v, (cra, cdec)) in enumerate(zip(visit_ids, centers)):
        r = spread_deg * 3600 * np.sqrt(rng.uniform(0, 1, n_per))
        a, d = offset(np.full(n_per, cra), np.full(n_per, cdec), r, rng.uniform(0, 2 * np.pi, n_per))
        vis.append(np.full(n_per, v))
        ra.append(a)
        dec.append(d)
        t.append(np.full(n_per, t0 + 0.01 * i))
    dia = {"visit": np.concatenate(vis).astype(np.int64), "ra": np.concatenate(ra),
           "dec": np.concatenate(dec), "midpointMjdTai": np.concatenate(t)}
    dia["diaSourceId"] = np.arange(dia["ra"].size, dtype=np.int64) * 7 + 3
    return dia


# --------------------------------------------------------------------------
# read_dia
# --------------------------------------------------------------------------

def test_read_dia_projection_filter_sort(tmp_path):
    rng = np.random.default_rng(0)
    n = 5000
    tbl = pa.table({
        "band": pa.array(rng.choice(["g", "r"], n)),
        "diaSourceId": pa.array(rng.permutation(n).astype(np.int64) + 100),
        "visit": pa.array(rng.integers(0, 20, n).astype(np.int64) + 2025050100000),
        # with rows exactly on both bounds: the slice is [t_lo, t_hi)
        "midpointMjdTai": pa.array(np.r_[rng.uniform(60000, 60010, n - 20), [60002.0] * 10, [60005.0] * 10]),
        "ra": pa.array(rng.uniform(0, 360, n)),
        "dec": pa.array(rng.uniform(-90, 90, n)),
        "extra": pa.array(np.zeros(n)),
    })
    path = tmp_path / "dia.parquet"
    pq.write_table(tbl, path, row_group_size=500)

    dia = V.read_dia(path, 60002.0, 60005.0)
    assert set(dia) == set(C.DIA_COLUMNS)
    t = tbl.column("midpointMjdTai").to_numpy()
    sel = (t >= 60002.0) & (t < 60005.0)
    assert dia["visit"].size == sel.sum()
    assert (dia["midpointMjdTai"] == 60002.0).sum() == 10 and not (dia["midpointMjdTai"] == 60005.0).any()
    assert (dia["midpointMjdTai"] >= 60002.0).all() and (dia["midpointMjdTai"] < 60005.0).all()
    order = np.lexsort((dia["diaSourceId"], dia["visit"]))
    assert (order == np.arange(order.size)).all()
    # the rows are intact
    ref = {c: tbl.column(c).to_numpy()[sel] for c in C.DIA_COLUMNS}
    ro = np.lexsort((ref["diaSourceId"], ref["visit"]))
    for c in C.DIA_COLUMNS:
        np.testing.assert_array_equal(dia[c], ref[c][ro])
    assert dia["diaSourceId"].dtype == np.int64 and dia["ra"].dtype == np.float64

    # open-ended and unfiltered
    assert V.read_dia(path)["ra"].size == n
    assert V.read_dia(path, t_lo_mjd=60005.0)["ra"].size == (t >= 60005.0).sum()
    assert V.read_dia(path, t_hi_mjd=60005.0)["ra"].size == (t < 60005.0).sum()
    assert V.read_dia(path, 60005.0, 60005.0 + 1e-9)["ra"].size == 10
    assert V.read_dia(path, 60002.0 - 1e-9, 60002.0)["ra"].size == 0
    empty = V.read_dia(path, 70000.0, 70001.0)
    assert set(empty) == set(C.DIA_COLUMNS) and empty["ra"].size == 0
    assert V.build_visits(empty).size == 0


def test_read_dia_nulls(tmp_path):
    tbl = pa.table({"diaSourceId": pa.array([1, 2, 3], pa.int64()),
                    "visit": pa.array([5, None, 5], pa.int64()),
                    "midpointMjdTai": [1.0, 1.0, 1.0], "ra": [1.0, 2.0, 3.0], "dec": [0.0, 0.0, 0.0]})
    pq.write_table(tbl, tmp_path / "n.parquet")
    with pytest.warns(UserWarning, match="dropped 1"):
        dia = V.read_dia(tmp_path / "n.parquet")
    assert list(dia["diaSourceId"]) == [1, 3]


def test_read_dia_nonfinite(tmp_path):
    """Rows with a non-finite ra, dec or time, or |dec| > 90, are dropped
    (a NaN ra would otherwise make the visit's centre NaN), counted apart
    from the nulls."""
    tbl = pa.table({"diaSourceId": pa.array(np.arange(8), pa.int64()),
                    "visit": pa.array([1, 1, 1, 1, 2, 2, 2, 2], pa.int64()),
                    "midpointMjdTai": [0.0, 0.0, np.nan, 0.0, 1.0, 1.0, 1.0, None],
                    "ra": [1.0, np.nan, 3.0, 4.0, 5.0, np.inf, 7.0, 8.0],
                    "dec": [0.0, 0.0, 0.0, 95.0, 0.0, 0.0, -np.inf, 0.0]})
    pq.write_table(tbl, tmp_path / "f.parquet", row_group_size=3)
    with pytest.warns(UserWarning, match="dropped 1 DiaSources with nulls .* and 5 with a non-finite"):
        dia = V.read_dia(tmp_path / "f.parquet")
    assert list(dia["diaSourceId"]) == [0, 4]
    vis = V.build_visits(dia)
    assert np.isfinite(vis["center"]).all() and np.isfinite(vis["radius"]).all()
    V.VisitIndex(vis)


# --------------------------------------------------------------------------
# build_visits
# --------------------------------------------------------------------------

def test_build_visits():
    rng = np.random.default_rng(1)
    vids = [2025050100010, 2025050100011, 2025050200003]
    centers = [(10.0, -20.0), (359.9, 0.5), (45.0, 89.5)]
    dia = make_dia(vids, centers, 300, 1.75, rng)
    vis = V.build_visits(dia)
    assert vis.dtype == C.VISIT_DTYPE
    assert list(vis["visit"]) == vids
    assert list(vis["night"]) == [20250501, 20250501, 20250502]
    assert list(vis["dia_start"]) == [0, 300, 600] and list(vis["dia_end"]) == [300, 600, 900]
    for i in range(3):
        s = slice(vis["dia_start"][i], vis["dia_end"][i])
        uv = unit(dia["ra"][s], dia["dec"][s])
        c = uv.mean(0)
        c /= np.linalg.norm(c)
        np.testing.assert_allclose(vis["center"][i], c, atol=1e-14)
        ang = V._angle(c, uv)
        assert vis["radius"][i] >= ang.max() and vis["radius"][i] - ang.max() < 1e-8
        assert 1.7 < np.degrees(vis["radius"][i]) < 1.85  # the mean centre is off the disk centre
        t = dia["midpointMjdTai"][s][0]
        assert vis["t_tai_mjd"][i] == t
        tt = Time(t, format="mjd", scale="tai").tdb
        assert abs(vis["t"][i] - (tt.jd - 2451545.0)) < 1e-9
        np.testing.assert_allclose(vis["obs_pos"][i], [t, 2 * t, 3 * t])
        np.testing.assert_allclose(vis["obs_vel"][i], np.array([t, -t, 0]) * 1731.45683681, rtol=1e-8)


def test_blocks_and_threads(monkeypatch):
    """Many small blocks, in threads, give the same visits and index."""
    rng = np.random.default_rng(9)
    dia = make_dia(np.arange(40) + 2025050100000, [(i * 9.0, i - 20.0) for i in range(40)], 97, 0.5, rng)
    vis1 = V.build_visits(dia, threads=1)
    idx1 = V.DiaIndex(dia, vis1, threads=1)
    monkeypatch.setattr(V, "_CHUNK", 250)
    assert len(list(V._visit_blocks(vis1["dia_start"], vis1["dia_end"]))) > 10
    vis2 = V.build_visits(dia, threads=4)
    idx2 = V.DiaIndex(dia, vis2, threads=4)
    np.testing.assert_array_equal(vis1, vis2)
    np.testing.assert_array_equal(idx1.key, idx2.key)
    np.testing.assert_array_equal(idx1.row, idx2.row)
    assert (np.diff(idx2.key) >= 0).all()


def test_build_visits_time_spread():
    """Sources with their own times (shutter-corrected): the visit's time is
    their median, without a warning (docs/design/shutter-timing.md); with a
    shared time, exactly that time."""
    rng = np.random.default_rng(2)
    dia = make_dia([7, 8, 9], [(0.0, 0.0), (5.0, 0.0), (10.0, 0.0)], 5, 0.1, rng)
    t8 = 60002.123456789
    dia["midpointMjdTai"] = np.r_[np.array([1.0, 1.0, 1.0 + 1e-3, 1.0 + 2e-3, 1.0 + 2e-3]) + 60000,
                                  np.full(5, t8),
                                  60003.5 + np.array([-0.24, 0.1, 0.24, 0.0, -0.05]) / 86400.0]
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        vis = V.build_visits(dia)
    assert vis["t_tai_mjd"][0] == 60001.001
    assert vis["t_tai_mjd"][1] == t8
    assert vis["t_tai_mjd"][2] == np.median(dia["midpointMjdTai"][10:15])
    assert vis["t_tai_mjd"][2] == 60003.5
    # (even sizes: the mean of the middle two)
    d2 = {k: v[:4] for k, v in dia.items()}
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert V.build_visits(d2)["t_tai_mjd"][0] == np.median(d2["midpointMjdTai"])


def test_build_visits_unsorted():
    dia = {"visit": np.array([2, 1]), "ra": np.zeros(2), "dec": np.zeros(2),
           "midpointMjdTai": np.zeros(2), "diaSourceId": np.arange(2)}
    with pytest.raises(ValueError):
        V.build_visits(dia)


# --------------------------------------------------------------------------
# DiaIndex.match
# --------------------------------------------------------------------------

def brute_match(dia, vis, vidx, ra, dec, r):
    out = []
    for k in range(ra.size):
        v = vidx[k]
        if not (0 <= v < len(vis)):
            continue
        s, e = vis["dia_start"][v], vis["dia_end"][v]
        sep = util.sky_separation_arcsec(ra[k], dec[k], dia["ra"][s:e], dia["dec"][s:e])
        for j in np.flatnonzero(sep <= r):
            out.append((k, s + j, sep[j]))
    return out


def dense_catalog(rng, centers, n_per, spread_deg):
    """Sources in small, dense patches (so a 5" disk holds several), placed
    at the given centres: generic, cell corners, poles, RA wrap."""
    return make_dia(np.arange(len(centers)) + 2025050100000, centers, n_per, spread_deg, rng)


def check_match(dia, vis, idx, vidx, ra, dec, r=R):
    pred, row, sep = idx.match(vidx, ra, dec, r)
    ref = brute_match(dia, vis, vidx, ra, dec, r)
    got = list(zip(pred.tolist(), row.tolist()))
    assert got == [(a, b) for a, b, _ in ref]
    np.testing.assert_array_equal(sep, [c for _, _, c in ref])
    return len(ref)


@pytest.mark.parametrize("where", ["random", "corners", "poles", "wrap"])
def test_match_completeness(where):
    from cdshealpix import nested
    import astropy.units as u
    rng = np.random.default_rng(3)
    spread = 30.0 / 3600  # 30" patches of 2000 sources: ~0.7 per 5"-radius disk
    if where == "random":
        centers = [(rng.uniform(0, 360), np.degrees(np.arcsin(rng.uniform(-1, 1)))) for _ in range(6)]
    elif where == "corners":
        # vertices of order-14 and order-15 cells, including base-cell corners
        # (where only 7 neighbours exist) and equatorial ones
        base = [(0.0, np.degrees(np.arcsin(2 / 3))), (45.0, 0.0), (90.0, -np.degrees(np.arcsin(2 / 3))),
                (123.4, 41.81)]
        centers = []
        for a, d in base:
            c = nested.lonlat_to_healpix(a * u.deg, d * u.deg, 14)
            lon, lat = nested.vertices(np.atleast_1d(c), 14)
            centers.append((lon.deg[0, 0], lat.deg[0, 0]))
        centers += base
    elif where == "poles":
        centers = [(0.0, 90.0), (123.0, 89.999), (0.0, -90.0), (300.0, -89.998)]
    else:
        centers = [(0.0, 0.0), (359.999, 12.0), (0.0005, -30.0), (360.0 - 1e-9, 60.0)]
    dia = dense_catalog(rng, centers, 2000, spread)
    vis = V.build_visits(dia)
    idx = V.DiaIndex(dia, vis)
    assert idx.query_depth(R) == 14
    # predictions: in each patch, including exactly on the sources and
    # (for the corners) on the cell vertices themselves
    m = 3000
    vidx = rng.integers(0, len(vis), m)
    cra = np.array([c[0] for c in centers])[vidx]
    cdec = np.array([c[1] for c in centers])[vidx]
    ra, dec = offset(cra, cdec, spread * 3600 * np.sqrt(rng.uniform(0, 1.2, m)), rng.uniform(0, 2 * np.pi, m))
    nv = len(vis)
    ra[:nv], dec[:nv], vidx[:nv] = [c[0] for c in centers], [c[1] for c in centers], np.arange(nv)
    # some predictions exactly on sources, some just short of 5" from one
    j = rng.integers(0, dia["ra"].size, 200)
    vj = dia["visit"][j] - dia["visit"][0]
    ra[-400:-200], dec[-400:-200], vidx[-400:-200] = dia["ra"][j], dia["dec"][j], vj
    ra2, dec2 = offset(dia["ra"][j], dia["dec"][j], np.full(200, R * 0.99999), rng.uniform(0, 2 * np.pi, 200))
    ra[-200:], dec[-200:], vidx[-200:] = ra2, dec2, vj
    ra = np.where(rng.uniform(size=m) < 0.1, ra - 360.0, ra)  # RA given out of [0, 360)
    n = check_match(dia, vis, idx, vidx, ra, dec)
    assert n > m * 0.5


def test_match_exhaustive_corner_ring():
    """Predictions on 5"-radius rings around the sources themselves: each must
    find its source, wherever the ring crosses cell edges."""
    rng = np.random.default_rng(4)
    centers = [(45.0, 0.0), (0.0, np.degrees(np.arcsin(2 / 3))), (10.0, 89.9995), (359.9995, -5.0)]
    dia = dense_catalog(rng, centers, 300, 20.0 / 3600)
    vis = V.build_visits(dia)
    idx = V.DiaIndex(dia, vis)
    nsrc = dia["ra"].size
    phi = np.linspace(0, 2 * np.pi, 24, endpoint=False)
    row = np.repeat(np.arange(nsrc), phi.size)
    ra, dec = offset(dia["ra"][row], dia["dec"][row], np.full(row.size, R * (1 - 1e-4)), np.tile(phi, nsrc))
    vidx = np.searchsorted(vis["dia_end"], row, side="right")
    pred, drow, sep = idx.match(vidx, ra, dec, R)
    found = set(zip(pred.tolist(), drow.tolist()))
    assert all((k, row[k]) in found for k in range(row.size))
    assert (sep <= R).all()


def test_match_edge_cases():
    rng = np.random.default_rng(5)
    dia = dense_catalog(rng, [(10.0, 10.0), (10.0, 10.0)], 50, 5.0 / 3600)
    vis = V.build_visits(dia)
    idx = V.DiaIndex(dia, vis)
    # same position, other visit: matches only that visit's sources
    p, r, _ = idx.match([1], [10.0], [10.0], 60.0)
    assert r.size == 50 and (r >= 50).all()
    # out-of-range visits and NaNs match nothing; empty input
    p, r, s = idx.match([-1, 2, 0], [10.0, 10.0, np.nan], [10.0, 10.0, 10.0], R)
    assert p.size == 0
    p, r, s = idx.match([], [], [], R)
    assert p.size == 0 and p.dtype == np.int64
    # larger radii use coarser orders
    assert idx.query_depth(3.9) == 15 and idx.query_depth(8.0) == 14 and idx.query_depth(8.1) == 13
    k = 500
    vidx = rng.integers(0, 2, k)
    ra, dec = offset(np.full(k, 10.0), np.full(k, 10.0), rng.uniform(0, 40, k), rng.uniform(0, 2 * np.pi, k))
    for rr in (1.0, 3.0, 12.0, 30.0):
        check_match(dia, vis, idx, vidx, ra, dec, rr)


# --------------------------------------------------------------------------
# VisitIndex.candidates
# --------------------------------------------------------------------------

def synthetic_visits(rng, nights=3, per_night=150, radius_deg=1.75):
    """Visits on a band of sky, radius ~1.75 deg, nights at MJD 60800 + i
    (visits spread over 9 h of each night), by building VISIT_DTYPE rows
    directly."""
    rows = []
    for n in range(nights):
        tv = np.sort(rng.uniform(0.0, 0.375, per_night)) + 60800.1 + n
        for i, t in enumerate(tv):
            rows.append((20250501 + n, i, t))
    vis = np.zeros(len(rows), dtype=C.VISIT_DTYPE)
    vis["night"] = [r[0] for r in rows]
    vis["visit"] = vis["night"] * 100000 + [r[1] for r in rows]
    vis["t_tai_mjd"] = [r[2] for r in rows]
    vis["t"] = vis["t_tai_mjd"] - 51544.5
    ra = rng.uniform(0, 360, len(rows))
    ra[:40] = rng.uniform(-3, 3, 40) % 360  # around the RA wrap
    dec = rng.uniform(-30, 30, len(rows))
    dec[40:60] = rng.uniform(85, 90, 20)    # and the pole
    vis["center"] = unit(ra, dec)
    vis["radius"] = np.radians(radius_deg) * rng.uniform(0.95, 1.0, len(rows))
    return vis


def truth_in_visit(vis, pos_at, r_arcsec):
    """Visits whose field (radius + r) contains the true position at t_v."""
    ra, dec = pos_at(vis["t"])
    return set(np.flatnonzero(V._angle(vis["center"], unit(ra, dec))
                              <= vis["radius"] + np.radians(r_arcsec / 3600)).tolist())


def make_track(pos_at, t, sigma=1.0, ok=True, h=1e-4, delta=2.0):
    ra, dec = pos_at(t)
    ra1, dec1 = pos_at(t + h)
    ra0, dec0 = pos_at(t - h)
    dra = ((ra1 - ra0 + 180) % 360 - 180) * np.cos(np.radians(dec)) / (2 * h)
    ddec = (dec1 - dec0) / (2 * h)
    K = t.size
    return C.CoarseTrack(t=t, ra=ra, dec=dec, rate_ra=dra, rate_dec=ddec, ra_err=np.full(K, 1e-4),
                         dec_err=np.full(K, 1e-4), ra_dec_cov=np.zeros(K), sigma_major=np.full(K, sigma),
                         ok=np.full(K, ok), delta=np.broadcast_to(np.asarray(delta, float), (K,)).copy())


def great_circle_path(ra0, dec0, pa_deg, rate, t0, accel=0.0):
    """A position function moving along a great circle, rate [deg/day] and
    an along-track acceleration [deg/day^2]."""
    p0 = unit(ra0, dec0)
    a = np.radians(ra0)
    e1 = np.array([-np.sin(a), np.cos(a), 0.0])
    e2 = np.cross(p0, e1)
    dirn = np.cos(np.radians(pa_deg)) * e2 + np.sin(np.radians(pa_deg)) * e1

    def pos_at(t):
        s = np.radians(rate * (t - t0) + 0.5 * accel * (t - t0) ** 2)
        q = p0 * np.cos(s)[..., None] + dirn * np.sin(s)[..., None]
        return radec(q)
    return pos_at


@pytest.mark.parametrize("sample", ["night_t", "start"])
def test_candidates_complete(sample):
    rng = np.random.default_rng(6)
    vis = synthetic_visits(rng)
    vi = V.VisitIndex(vis)
    assert list(vi.nights) == [20250501, 20250502, 20250503]
    tsamp = vi.night_t if sample == "night_t" else vi.night_tmin - 0.1
    ntot = 0
    for trial in range(300):
        rate = [0.0, 0.25, 1.0, 3.0, 15.0, 60.0][trial % 6]
        v0 = rng.integers(len(vis))
        c = vis["center"][v0]
        ra0, dec0 = np.degrees(np.arctan2(c[1], c[0])) % 360, np.degrees(np.arcsin(c[2]))
        # start near the edge of a visit's field, so it's borderline
        ra0, dec0 = offset(np.array(ra0), np.array(dec0), np.degrees(vis["radius"][v0]) * 3600 * 0.999,
                           rng.uniform(0, 2 * np.pi))
        pos_at = great_circle_path(ra0, dec0, rng.uniform(0, 360), rate, vis["t"][v0],
                                   accel=rng.normal(0, 0.3 * rate))
        track = make_track(pos_at, tsamp)
        got = set(vi.candidates(track, V.DEFAULT_CANDIDATE_MARGIN_ARCSEC).tolist())
        truth = truth_in_visit(vis, pos_at, R)
        assert truth <= got, (trial, rate, truth - got)
        ntot += len(truth)
        # for uniform great-circle motion, the match radius alone suffices
        lin = great_circle_path(ra0, dec0, rng.uniform(0, 360), rate, vis["t"][v0])
        got = set(vi.candidates(make_track(lin, tsamp), R * 1.001).tolist())
        assert truth_in_visit(vis, lin, R) <= got
    assert ntot > 300


def test_candidates_brute_force_formula():
    """candidates equals the contract formula evaluated for every visit."""
    rng = np.random.default_rng(7)
    vis = synthetic_visits(rng)
    vi = V.VisitIndex(vis)
    for trial in range(200):
        rate = [0.1, 1.0, 10.0, 100.0][trial % 4]
        pos_at = great_circle_path(rng.uniform(0, 360), rng.uniform(-40, 40), rng.uniform(0, 360), rate,
                                   26356.0)
        track = make_track(pos_at, vi.night_t + rng.normal(0, 0.1, 3))
        track = track._replace(delta=rng.uniform(0.03, 3.0) + rng.uniform(-0.01, 0.01) * (track.t - 26357))
        margin = 30.0
        got = vi.candidates(track, margin)
        # brute force, the terms computed independently
        vel = [rate_vector(track, i) for i in range(3)]
        order = np.argsort(track.t)
        exp = []
        for v in range(len(vis)):
            n = np.searchsorted(vi.nights, vis["night"][v])
            k = np.argmin(np.abs(track.t - vi.night_t[n]))
            w = np.radians(np.hypot(track.rate_ra[k], track.rate_dec[k]))
            dt = vis["t"][v] - track.t[k]
            pos = list(order).index(k)
            nb = [order[j] for j in (pos - 1, pos + 1) if 0 <= j < 3]
            acc = max(np.linalg.norm(vel[j] - vel[k]) / abs(track.t[j] - track.t[k]) for j in nb)
            ddel = max(abs(track.delta[j] - track.delta[k]) / abs(track.t[j] - track.t[k]) for j in nb)
            A = np.arcsin(V.R_EARTH_AU / max(track.delta[k] - ddel * abs(dt), V.R_EARTH_AU))
            x = V.OMEGA_EARTH * abs(dt)
            coef = max(2.0, abs(np.exp(1j * x) - 1 - 1j * x))
            # the great-circle extrapolation is the path itself
            ra, dec = pos_at(np.array(vis["t"][v]))
            sep = V._angle(vis["center"][v], unit(ra, dec))
            tol = (vis["radius"][v] + w * abs(dt) + coef * A * (1 + A) + 0.5 * acc * dt**2
                   + np.radians(margin / 3600))
            if sep <= tol:
                exp.append(v)
        np.testing.assert_array_equal(got, exp)


def rate_vector(track, k):
    """The 3D on-sky rate vector [rad/day] of sample k."""
    a, d = np.radians(track.ra[k]), np.radians(track.dec[k])
    e_ra = np.array([-np.sin(a), np.cos(a), 0.0])
    e_dec = np.array([-np.sin(d) * np.cos(a), -np.sin(d) * np.sin(a), np.cos(d)])
    return np.radians(track.rate_ra[k]) * e_ra + np.radians(track.rate_dec[k]) * e_dec


def test_diurnal_coefficient():
    """f(W dt) = |exp(i W dt) - 1 - i W dt| exceeds 2 beyond 0.338 d."""
    assert V.diurnal_coefficient(0.0) == 2.0 and V.diurnal_coefficient(0.3) == 2.0
    assert abs(V.diurnal_coefficient(0.5) - 3.7387) < 1e-3
    assert 1.99 < V.diurnal_coefficient(0.3378) <= 2.0 < V.diurnal_coefficient(0.3395)
    # it bounds the departure of a rotating unit vector from its linear
    # extrapolation, for any phase and any projection (ellipse) of it
    th = np.linspace(0, 2 * np.pi, 721)
    for dt in (0.1, 0.3, 0.45, 0.6):
        x = V.OMEGA_EARTH * dt
        for a, c in ((1, 1), (1, 0.3), (0.2, 1)):
            e1 = a * (np.sin(th + x) - np.sin(th) - x * np.cos(th))
            e2 = c * (np.cos(th + x) - np.cos(th) + x * np.sin(th))
            assert np.hypot(e1, e2).max() <= V.diurnal_coefficient(dt) * (1 + 1e-12)


def test_default_margin():
    """The default margin covers the match radius, the light-time shift of
    the geometric coarse track (v_perp / c: <= 55" for v_perp <= 80 km/s)
    and 30" of safety; the explicit terms cover the rest."""
    c_kms = 299792.458
    assert np.degrees(80.0 / c_kms) * 3600 < 55.1
    assert V.DEFAULT_CANDIDATE_MARGIN_ARCSEC >= C.MATCH_RADIUS_ARCSEC + 55 + 30


def test_tolerance_formula():
    """_tolerance against the contract's formula, written out, at a few
    (delta, d delta/dt, dt): radius + w|dt| + D + 0.5 a dt^2 + margin,
    D = A max(2, |exp(iWdt) - 1 - iWdt|) (1 + A), A = arcsin(R_E / (delta -
    |d delta/dt| |dt|))."""
    radius, w, acc, margin = np.radians(1.75), np.radians(0.3), np.radians(2.0), np.radians(90 / 3600)
    for delta, ddel, dt in ((2.0, 0.0, 0.2), (0.05, 0.01, 0.3), (0.03, 0.02, 0.1), (0.021, 0.005, 0.45),
                            (1.0, 0.5, 0.0), (0.02, 0.05, 0.25)):
        x = V.OMEGA_EARTH * dt
        A = np.arcsin(V.R_EARTH_AU / (delta - ddel * dt))
        D = A * max(2.0, abs(np.exp(1j * x) - 1 - 1j * x)) * (1 + A)
        exp = radius + w * dt + D + 0.5 * acc * dt**2 + margin
        got = V._tolerance(radius, w, delta, ddel, acc, dt, margin)
        assert abs(got - exp) <= 1e-15 * exp, (delta, ddel, dt)
    # delta_eff at or below R_earth: A = 90 deg
    assert V._tolerance(0.0, 0.0, 0.001, 1.0, 0.0, 0.3, 0.0) >= np.pi / 2 * 2


def test_sample_derivatives():
    """The rate and distance changes per day to the adjacent usable samples,
    the larger side; a lone usable sample gets 0; unordered input."""
    t = np.array([2.0, 0.0, 1.0, 3.0, 5.0])
    vel = np.zeros((5, 3))
    vel[:, 0] = [0.3, 0.0, 0.1, 0.35, 9.0]      # rad/day
    delta = np.array([1.2, 1.0, 1.05, 1.25, 9.0])
    usable = np.array([True, True, True, True, False])
    acc, ddel = V._sample_derivatives(t, vel, delta, usable)
    # in time order: samples 1, 2, 0, 3; changes 0.1, 0.2, 0.05 per day
    np.testing.assert_allclose(acc[[1, 2, 0, 3]], [0.1, 0.2, 0.2, 0.05])
    np.testing.assert_allclose(ddel[[1, 2, 0, 3]], [0.05, 0.15, 0.15, 0.05])
    assert acc[4] == 0 and ddel[4] == 0
    a1, d1 = V._sample_derivatives(t[:1], vel[:1], delta[:1], usable[:1])
    assert a1[0] == 0 and d1[0] == 0
    # a NaN delta next to a sample makes its change infinite
    _, dn = V._sample_derivatives(t, vel, np.where(np.arange(5) == 2, np.nan, delta), usable)
    assert np.isinf(dn[[1, 2, 0]]).all() and np.isclose(dn[3], 0.05)


def test_candidates_grid_pad_borderline():
    """Fast movers with w dt_max ~ 1-1.9 deg, and visits whose centres are
    anywhere within the tolerance of the extrapolated position: all are
    candidates. This needs the lookup to allow for radius + 2 w dt_max
    around the sample (the extrapolation, then the w |dt| slack), or it
    must fall back to the whole night."""
    rng = np.random.default_rng(11)
    for trial in range(60):
        w_deg = rng.uniform(5.0, 9.5)                    # deg/day; dt_max ~ 0.2 d
        pos_at = great_circle_path(rng.uniform(0, 360), rng.uniform(-50, 50), rng.uniform(0, 360), w_deg,
                                   26357.2)
        tv = 26356.0 + np.arange(3)[:, None] + np.linspace(0.0, 0.4, 40)[None, :]
        tv = tv.ravel()
        ra, dec = pos_at(tv)
        wr = np.radians(w_deg)
        slack = wr * np.abs(tv - np.round(tv - 0.2 - 26356.0) - 26356.2) + np.radians(R / 3600)
        off = (np.radians(1.75) + rng.uniform(0, 0.98, tv.size) * slack) * 206264.806
        cra, cdec = offset(ra, dec, off, rng.uniform(0, 2 * np.pi, tv.size))
        vis = np.zeros(tv.size, dtype=C.VISIT_DTYPE)
        vis["night"] = 20250501 + np.repeat(np.arange(3), 40)
        vis["visit"] = vis["night"] * 100000 + np.tile(np.arange(40), 3)
        vis["t"] = tv
        vis["center"] = unit(cra, cdec)
        vis["radius"] = np.radians(1.75)
        vi = V.VisitIndex(vis)
        np.testing.assert_allclose(vi.night_t, 26356.2 + np.arange(3))
        track = make_track(pos_at, vi.night_t, delta=1e9)
        got = vi.candidates(track, R)
        np.testing.assert_array_equal(got, np.arange(tv.size))


def test_candidates_eligibility():
    rng = np.random.default_rng(8)
    vis = synthetic_visits(rng)
    vi = V.VisitIndex(vis)
    c = vis["center"][5]
    ra0, dec0 = np.degrees(np.arctan2(c[1], c[0])) % 360, np.degrees(np.arcsin(c[2]))
    pos_at = great_circle_path(ra0, dec0, 30.0, 0.2, vis["t"][5])
    tr = make_track(pos_at, vi.night_t)
    all_c = vi.candidates(tr, 60.0)
    assert 5 in all_c
    n0 = vis["night"][5]
    assert set(vis["night"][all_c]) >= {n0}
    # night 0 ineligible by sigma, then by ok
    for field, val in (("sigma_major", np.array([C.SIGMA_MAX_ARCSEC * 1.01, 1, 1])),
                       ("ok", np.array([False, True, True])), ("sigma_major", np.array([np.nan, 1, 1]))):
        c2 = vi.candidates(tr._replace(**{field: val}), 60.0)
        assert vi.last_sigma_gated == 1
        assert not (vis["night"][c2] == vi.nights[0]).any()
        np.testing.assert_array_equal(c2, all_c[vis["night"][all_c] != vi.nights[0]])
    # exactly at the limit is eligible
    c3 = vi.candidates(tr._replace(sigma_major=np.full(3, C.SIGMA_MAX_ARCSEC)), 60.0)
    np.testing.assert_array_equal(c3, all_c)
    assert vi.last_skipped == 0 and vi.last_sigma_gated == 0
    # samples far from every night: nothing, and the nights are counted
    assert vi.candidates(tr._replace(t=tr.t + 5.0), 60.0).size == 0
    assert vi.last_skipped == 3 and vi.last_sigma_gated == 0
    vi.candidates(tr._replace(t=tr.t + 1.3), 60.0)   # the first night is 1.3 d from any
    assert vi.last_skipped == 1
    # empty track
    e = C.CoarseTrack(*[np.zeros(0)] * 11)
    assert vi.candidates(e, 60.0).size == 0


def test_candidates_field_edge_and_fast():
    """A stationary object just inside a visit's edge, and a very fast one
    that crosses the field during the night (the fallback path)."""
    vis = np.zeros(3, dtype=C.VISIT_DTYPE)
    vis["night"] = 20250501
    vis["visit"] = vis["night"] * 100000 + np.arange(3)
    vis["t"] = 26356.0 + np.array([0.0, 0.1, 0.4])
    vis["center"] = unit(np.array([100.0, 150.0, 200.0]), np.array([0.0, 0.0, 0.0]))
    vis["radius"] = np.radians(1.75)
    vi = V.VisitIndex(vis)
    # stationary at 1.75 deg - 1" from visit 0's centre, sampled 0.2 d earlier
    edge = great_circle_path(np.array(100.0 + 1.75 - 1 / 3600), np.array(0.0), 90.0, 0.0, 26356.0)
    tr = make_track(edge, np.array([26356.2]))
    assert list(vi.candidates(tr, 0.0)) == [0]
    # the same but just outside: needs the margin
    edge2 = great_circle_path(np.array(100.0 + 1.75 + 4 / 3600), np.array(0.0), 90.0, 0.0, 26356.0)
    tr2 = make_track(edge2, np.array([26356.2]), delta=1e6)
    assert list(vi.candidates(tr2, 0.0)) == []
    assert list(vi.candidates(tr2, 5.0)) == [0]
    # 500 deg/day eastward: at 100 deg at t=0, 150 at 0.1, 300 at 0.4
    fast = great_circle_path(np.array(100.0), np.array(0.0), 90.0, 500.0, 26356.0)
    trf = make_track(fast, np.array([26356.2]))
    got = vi.candidates(trf, 0.0)
    assert {0, 1} <= set(got.tolist())


# --------------------------------------------------------------------------
# close approaches: diurnal parallax and a changing rate
# --------------------------------------------------------------------------

SITE_LAT = np.radians(-30.24)


def flyby(rng, delta0, t0, v_rad_kms, v_perp_kms):
    """A geocentric object at delta0 [AU] at t0, moving with the given
    radial and transverse speeds and a random acceleration, seen from a
    site rotating with the Earth: returns topo(t) -> (ra, dec, delta)."""
    xhat = unit(rng.uniform(0, 360), np.degrees(np.arcsin(rng.uniform(-0.9, 0.9))))
    perp = np.cross(xhat, rng.normal(size=3))
    perp /= np.linalg.norm(perp)
    G = rng.normal(size=3) * 1e-6                      # AU/day^2
    X0, V0 = xhat * delta0, (v_rad_kms * xhat + v_perp_kms * perp) / 1731.45683681
    th0 = rng.uniform(0, 2 * np.pi)

    def topo(t):
        t = np.asarray(t, dtype=np.float64)
        dt = (t - t0)[..., None]
        X = X0 + V0 * dt + 0.5 * G * dt**2
        th = th0 + V.OMEGA_EARTH * (t - t0)
        rho = V.R_EARTH_AU * np.stack([np.cos(SITE_LAT) * np.cos(th), np.cos(SITE_LAT) * np.sin(th),
                                       np.full(th.shape, np.sin(SITE_LAT))], -1)
        Y = X - rho
        d = np.linalg.norm(Y, axis=-1)
        ra, dec = radec(Y / d[..., None])
        return ra, dec, d
    return topo


def flyby_track(topo, t, h=1e-6):
    ra, dec, d = topo(t)
    p0, p1 = unit(*topo(t - h)[:2]), unit(*topo(t + h)[:2])
    v = (p1 - p0) / (2 * h)                            # rad/day, tangent to the sky
    a = np.radians(ra)
    dd = np.radians(dec)
    e_ra = np.stack([-np.sin(a), np.cos(a), 0 * a], -1)
    e_dec = np.stack([-np.sin(dd) * np.cos(a), -np.sin(dd) * np.sin(a), np.cos(dd)], -1)
    K = t.size
    return C.CoarseTrack(t=t, ra=ra, dec=dec, rate_ra=np.degrees((v * e_ra).sum(-1)),
                         rate_dec=np.degrees((v * e_dec).sum(-1)), ra_err=np.full(K, 1e-4),
                         dec_err=np.full(K, 1e-4), ra_dec_cov=np.zeros(K), sigma_major=np.ones(K),
                         ok=np.ones(K, bool), delta=d)


def flyby_visits(rng, topo, nights, per_night=60):
    """Visits (radius 1.75 deg) with times over 9 h of each night: half
    centred near where the object is at some other time of that night, half
    on the edge of the field (0.9-1.0 radius) around it at the visit time."""
    rows = []
    for n in range(nights):
        tn = 26356.1 + n + np.sort(rng.uniform(0.0, 0.375, per_night))
        tc = np.where(rng.uniform(size=per_night) < 0.5, 26356.1 + n + rng.uniform(0.0, 0.375, per_night), tn)
        ra, dec, _ = topo(tc)
        off = np.where(tc == tn, np.radians(1.75) * rng.uniform(0.9, 1.0, per_night),
                       rng.uniform(0, 1, per_night) ** 0.3 * np.radians(2.5)) * 206264.806
        cra, cdec = offset(ra, dec, off, rng.uniform(0, 2 * np.pi, per_night))
        for i in range(per_night):
            rows.append((20250501 + n, i, tn[i], cra[i], cdec[i]))
    vis = np.zeros(len(rows), dtype=C.VISIT_DTYPE)
    vis["night"] = [r[0] for r in rows]
    vis["visit"] = vis["night"] * 100000 + [r[1] for r in rows]
    vis["t"] = [r[2] for r in rows]
    vis["t_tai_mjd"] = vis["t"] + 51544.5
    vis["center"] = unit(np.array([r[3] for r in rows]), np.array([r[4] for r in rows]))
    vis["radius"] = np.radians(1.75)
    return vis


@pytest.mark.parametrize("drange", [(2e-4, 0.003), (0.003, 0.03), (0.03, 0.1)])
def test_candidates_close_approach(drange):
    """Close approaches against brute force: distances log-uniform in
    ``drange`` at a time anywhere from 1.5 nights before the first sampled
    night to 1.5 after the last, radial speeds up to 40 km/s, transverse ones
    up to 40 km/s or nearly none (so the on-sky rate is small and the diurnal
    parallax, up to ~1.2 deg at 0.002 AU, dominates), 1-, 2- and 4-night
    tracks (a 1-night track has no rate-change estimate). Closest approaches
    stay beyond 2e-4 AU. No visit the object is in is missed, with just the
    match radius as the margin (the geometric truth has no light time), nor
    with the default. Without the delta-dependent parts (delta -> infinity)
    there are misses."""
    rng = np.random.default_rng(int(drange[0] * 1e5))
    n_true = n_miss_far = n_cand = n_trial = 0
    while n_trial < 150:
        nights = int(rng.choice([1, 2, 4]))
        d0 = np.exp(rng.uniform(np.log(drange[0]), np.log(drange[1])))
        v_rad = rng.uniform(-40, 40)
        v_perp = rng.choice([rng.uniform(0, 0.5), rng.uniform(0, 40)])
        t0 = 26356.1 + rng.uniform(-1.5, nights + 1.5)
        topo = flyby(rng, d0, t0, v_rad, v_perp)
        if topo(np.linspace(26354.0, 26358.0 + nights, 40000))[2].min() < 2e-4:
            continue
        n_trial += 1
        vis = flyby_visits(rng, topo, nights)
        vi = V.VisitIndex(vis)
        track = flyby_track(topo, vi.night_t)
        ra, dec, _ = topo(vis["t"])
        truth = set(np.flatnonzero(V._angle(vis["center"], unit(ra, dec))
                                   <= vis["radius"] + np.radians(R / 3600)).tolist())
        got = set(vi.candidates(track, R * 1.001).tolist())
        assert truth <= got, (n_trial, d0, v_rad, v_perp, t0, sorted(truth - got))
        assert truth <= set(vi.candidates(track, V.DEFAULT_CANDIDATE_MARGIN_ARCSEC).tolist())
        far = set(vi.candidates(track._replace(delta=np.full(track.t.size, 1e9)), R * 1.001).tolist())
        n_true += len(truth)
        n_cand += len(got)
        n_miss_far += len(truth - far)
    assert n_true > 1000
    if drange[0] < 0.01:
        assert n_miss_far > 0
    print(f"delta {drange}: {n_true} true, {n_cand} candidates, {n_miss_far} missed with delta -> infinity")


def test_sky_grid_disk_complete():
    """Every point of a disk lies in one of the cells the grid registers the
    disk in: random disks, near the poles and across RA 0/360."""
    rng = np.random.default_rng(10)
    g = V._SkyGrid(2.0)
    c = g.cell(np.array([0.0, 360.0, -1e-9]), np.array([0.0, 0.0, 0.0]))
    assert c[0] == c[1] == g.cell(0.0, 0.0) and c[2] == c[0] + g.nra[45] - 1
    for trial in range(400):
        ra = rng.choice([rng.uniform(0, 360), rng.uniform(-2, 2) % 360])
        dec = rng.choice([np.degrees(np.arcsin(rng.uniform(-1, 1))),
                          rng.uniform(80, 90) * rng.choice([-1, 1])])
        r = rng.uniform(0.01, 5.0)
        cells = set(g.disk(ra, dec, r).tolist())
        m = 3000
        pr, pd = offset(np.full(m, ra), np.full(m, dec), r * 3600 * np.sqrt(rng.uniform(0, 1, m)),
                        rng.uniform(0, 2 * np.pi, m))
        pr2, pd2 = offset(np.full(m, ra), np.full(m, dec), np.full(m, r * 3600 * 0.999999),
                          rng.uniform(0, 2 * np.pi, m))
        got = set(g.cell(np.r_[pr, pr2], np.r_[pd, pd2]).tolist())
        assert got <= cells, (ra, dec, r)


def test_healpix_matches_cdshealpix():
    """The NumPy HEALPix hash and neighbourhood reproduce cdshealpix: random
    points, near the poles, near the base-cell corners and edges, RA 0/360."""
    import astropy.units as u
    from astropy.coordinates import Latitude, Longitude
    from cdshealpix import nested
    rng = np.random.default_rng(12)
    n = 400_000
    ra = rng.uniform(0, 360, n)
    dec = np.degrees(np.arcsin(rng.uniform(-1, 1, n)))
    q = n // 4
    dec[:q] = rng.choice([-1, 1], q) * (90 - np.abs(rng.normal(0, 0.01, q)))
    dec[q:2 * q] = rng.choice([-1, 1], q) * np.degrees(np.arcsin(2 / 3)) + rng.normal(0, 1e-4, q)
    ra[2 * q:3 * q] = rng.integers(0, 8, q) * 45 + rng.normal(0, 1e-4, q)
    ra[:5], dec[:5] = [0.0, 360.0 - 1e-12, 90.0, 180.0, 45.0], [90.0, -90.0, 0.0, 41.8103149, -41.8103149]
    ra %= 360.0
    for depth in (0, 5, 13, 14, 15):
        cells = V._healpix(ra, dec, depth)
        def hp(a, d):
            return nested.lonlat_to_healpix(Longitude(a * u.deg), Latitude(d * u.deg), depth).astype(np.int64)
        ref = hp(ra, dec)
        # points exactly on a cell boundary (the vertices given) may go to
        # either cell; everywhere else the cells are identical
        bad = np.flatnonzero(cells != ref)
        assert set(bad.tolist()) <= set(range(5))
        for i in bad:
            eps = 1e-9 * np.array([[1, 0], [-1, 0], [0, 1], [0, -1], [1, 1], [-1, -1], [1, -1], [-1, 1]])
            assert cells[i] in hp((ra[i] + eps[:, 0]) % 360, np.clip(dec[i] + eps[:, 1], -90, 90))
        nb = V._neighbourhood(*V._hash_fxy(ra[:20000], dec[:20000], depth), depth)
        nref = nested.neighbours(cells[:20000].astype(np.uint64), depth)
        np.testing.assert_array_equal(np.sort(nb, axis=1), np.sort(nref, axis=1))
