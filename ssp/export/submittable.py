"""Build ``dia_sources.parquet`` from ClickHouse ``ssp.SubmittableSources``.

For every X05 row of an MPC ``obs_sbn`` Parquet dump, find the source
measurement it was submitted from and write one row per resolved
observation, carrying all of the view's columns plus the linkage
(``obsid``) and match diagnostics. This is an alternative to building
``dia_sources.parquet`` from the Butler with ``extract-catalog``.

Matching:

1. **By id.** ``obsSubID`` is ``LSST-<collection>-<id>`` (or, for trailed
   sources submitted as two endpoints, ``...-<id>-A`` / ``-B``), or a bare
   ``<id>`` from before labels existed. Labelled ids are looked up in their
   collection, bare ids in all collections.
2. **Verified.** A candidate passes if its PSF or trail centroid is within
   ``SEP_MAS`` of the submitted position and its time within ``DT_MS``.
   Band and magnitude are recorded but never reject.
3. **Ranked.** One winner per observation; see ``rank()``.
4. **By position+time**, for the rows the id pass could not resolve,
   following ``ssp-submit/ops/psv_crossmatch.py``.

Credentials: ``SSP_CH_USER``/``SSP_CH_PASSWORD`` if set, else ``~/.chpass``
(pgpass format, mode 0600). There is deliberately no password flag.
"""

from __future__ import annotations

import argparse
import io
import os
import stat
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from astropy.time import Time

DEFAULT_HOST = "sdfiana035.sdf.slac.stanford.edu"
DEFAULT_PORT = 8123
DEFAULT_DATABASE = "ssp"
VIEW = "SubmittableSources"

# The server is shared: never run more than this many queries at once.
MAX_WORKERS = 8

# Acceptance: 11% of rows were submitted with 6-decimal degrees (~1.8 mas
# rounding per coordinate), hence 3 mas; observed |dt| is <= 6 ms.
SEP_MAS = 3.0
DT_MS = 10.0

# Two passing candidates closer than this in separation are a tie.
AMBIGUOUS_MAS = 0.01

#: Labels whose rows are SUPERSEDED BY CONSTRUCTION -- another collection
#: serves a better row for the same id. These must never outrank a live label
#: on a tie.
#:
#: Without this, `collection` was doing two jobs in one sort key: the
#: deterministic tie-break (below) AND, when no --order was given and every
#: _pri was 0, the preference. `'002-DS' < 'AP-DS'` lexicographically, so the
#: superseded copy was presented as rank 1 -- "the one to substitute" per this
#: module's docstring -- by alphabetical accident.
#:
#: `001-DS` is deliberately NOT here: its 70 rows are a genuine recovered
#: processing, the only surviving copy of those measurements, not a row
#: superseded by a sibling.
#: (Copied from ssp-submit/ops/psv_crossmatch.py.)
DEPRIORITIZED_LABELS = ("002-DS",)

# Position fallback blocking (see psv_crossmatch.CELL_ORDER): an order-16
# cell is ~3.2", and hpix29 >> CELL_SHIFT is the order-16 cell index.
CELL_ORDER = 16
CELL_SHIFT = 2 * (29 - CELL_ORDER)

# View columns renamed to their DiaSource names on output.
RENAMES = {"id": "diaSourceId", "mjd_tai": "midpointMjdTai"}

# DiaSource columns that SSSource/SSObject read. Any missing from the view
# is added as an all-null column of this type (today: extendedness).
REQUIRED_COLUMNS = {
    "diaSourceId": pa.int64(),
    "midpointMjdTai": pa.float64(),
    "ra": pa.float64(),
    "dec": pa.float64(),
    "band": pa.string(),
    "psfFlux": pa.float64(),
    "psfFluxErr": pa.float64(),
    "extendedness": pa.float64(),
}

# Columns build_output appends to the view's.
EXTRA_COLUMNS = ["obsid", "obsid_b", "obssubid", "submission_id", "trksub", "trkid", "match", "sep_mas",
                 "dt_ms", "dmag", "band_ok", "n_pass", "ambiguous"]

# obs_sbn columns we read; the ones after "band" are only passed through
# to the unresolved report.
OBS_COLUMNS = [
    "obsid", "obssubid", "stn", "obstime", "ra", "dec", "mag", "band",
    "trksub", "trkid", "provid", "permid", "submission_id",
]

# LSST-<label>-<id>[-A|-B], or a bare <id>. The label is any string.
OBSSUBID_RE = r"^(?:LSST-(?P<label>.+)-(?P<id>\d+)(?:-(?P<part>[AB]))?|(?P<bare>\d+))$"


#
# Credentials
#


def read_chpass(path, host, port, database, user=None):
    """Resolve (user, password) from a ``~/.pgpass``-format file.

    Lines are ``host:port:database:user:password``, ``#`` starts a comment,
    ``*`` matches anything in the first four fields, ``\\:`` is a literal
    colon, and the first matching line wins. Refuses group/world readable
    files, as libpq does for ``~/.pgpass``.
    """
    path = Path(path)
    mode = path.stat().st_mode
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise SystemExit(f"{path}: permissions {stat.filemode(mode)} are too open; chmod 600 {path}")

    def split(line):
        out, cur, esc = [], [], False
        for ch in line:
            if esc:
                cur.append(ch)
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == ":":
                out.append("".join(cur))
                cur = []
            else:
                cur.append(ch)
        out.append("".join(cur))
        return out

    for lineno, raw in enumerate(path.read_text().splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        fields = split(line)
        if len(fields) != 5:
            raise SystemExit(f"{path}:{lineno}: expected 5 colon-separated fields, got {len(fields)}")
        f_host, f_port, f_db, f_user, f_pass = fields
        if f_host in ("*", host) and f_port in ("*", str(port)) and f_db in ("*", database) \
                and (user is None or f_user in ("*", user)):
            return (user or f_user), f_pass
    raise SystemExit(f"{path}: no line matches {user or '<any user>'}@{host}:{port}/{database}")


def credentials(host, port, database, user=None):
    """``SSP_CH_USER``/``SSP_CH_PASSWORD`` if set, else ``~/.chpass``."""
    env_user, env_pass = os.environ.get("SSP_CH_USER"), os.environ.get("SSP_CH_PASSWORD")
    if env_user and env_pass is not None and (user is None or user == env_user):
        return env_user, env_pass
    chpass = Path.home() / ".chpass"
    if chpass.exists():
        return read_chpass(chpass, host, port, database, user)
    raise SystemExit(
        "no credentials: set SSP_CH_USER/SSP_CH_PASSWORD, or create ~/.chpass "
        "(mode 0600) with a line host:port:database:user:password"
    )


#
# obs_sbn
#


def _arr(x, typ=None):
    """Any array-like -> a contiguous pyarrow Array (``pa.array`` on a
    ChunkedArray goes through Python objects, which is slow)."""
    if isinstance(x, pa.ChunkedArray):
        x = x.combine_chunks()
    return x if isinstance(x, pa.Array) and (typ is None or x.type == typ) else pa.array(x, typ)


def parse_obssubid(obssubid):
    """Parse obsSubIDs into ``(label, id, part)`` numpy arrays.

    ``label`` is None for bare ids, ``id`` is -1 where there is no usable
    id, ``part`` is "A"/"B" for trail endpoints and "" otherwise.
    Whitespace is stripped first.
    """
    s = pc.utf8_trim_whitespace(_arr(obssubid, pa.string()))
    m = pc.extract_regex(s, OBSSUBID_RE)
    label, digits, part, bare = (m.field(k) for k in ("label", "id", "part", "bare"))
    ok = pc.fill_null(pc.is_valid(m), False).to_numpy(zero_copy_only=False)

    # Unmatched optional groups come back as "", matched rows as strings.
    digits = pc.if_else(pc.equal(digits, ""), bare, digits)
    # 20 digits cannot be an int64; the uint64 cast below then cannot fail.
    ok &= pc.fill_null(pc.less_equal(pc.utf8_length(digits), 19), False).to_numpy(zero_copy_only=False)
    ids = np.full(len(s), -1, dtype=np.int64)
    u = pc.cast(pc.if_else(pa.array(ok), digits, "0"), pa.uint64()).to_numpy(zero_copy_only=False)
    ok &= u <= np.iinfo(np.int64).max
    ids[ok] = u[ok].astype(np.int64)

    label = pc.if_else(pc.equal(label, ""), pa.scalar(None, pa.string()), label)
    label = label.to_numpy(zero_copy_only=False)
    label[~ok] = None
    part = pc.fill_null(part, "").to_numpy(zero_copy_only=False)
    part[~ok] = ""
    return label, ids, part


def utc_to_tai_mjd(obstime):
    """Timestamp (UTC, any unit) array -> MJD TAI float64 (NaN for nulls)."""
    us = pc.cast(_arr(obstime).cast(pa.timestamp("us")), pa.int64())
    valid = pc.is_valid(us).to_numpy(zero_copy_only=False)
    us = pc.fill_null(us, 0).to_numpy()[valid]
    out = np.full(len(valid), np.nan)
    if valid.any():
        # Two-part MJD keeps the full microsecond precision through the
        # UTC -> TAI conversion.
        days, rem = np.divmod(us, 86_400_000_000)
        t = Time(days + 40587.0, rem / 86_400e6, format="mjd", scale="utc").tai
        out[valid] = t.jd1 - 2400000.5 + t.jd2
    return out


def strip_band(band):
    """``Lr`` -> ``r``; bands without the prefix are returned unchanged."""
    b = _arr(band, pa.string())
    has_l = pc.and_(pc.starts_with(b, "L"), pc.equal(pc.utf8_length(b), 2))
    return pc.if_else(has_l, pc.utf8_slice_codeunits(b, 1), b).to_numpy(zero_copy_only=False)


def midpoint(ra1, dec1, t1, ra2, dec2, t2):
    """Mean of two trail endpoints, handling RA wrap at 0/360."""
    dra = (np.asarray(ra2) - ra1 + 180.0) % 360.0 - 180.0
    return (ra1 + dra / 2) % 360.0, (np.asarray(dec1) + dec2) / 2, (np.asarray(t1) + t2) / 2


def load_obs(tbl):
    """Turn an obs_sbn table into the logical observation rows to resolve.

    Returns ``(obs, tbl, n_pairs)``: ``obs`` is a dict of equal-length
    numpy arrays, one entry per logical row, ``tbl`` the X05 rows of the
    input (``obs["row"]`` indexes it). Each -A/-B trail endpoint pair is
    merged into its -A row (midpoint position and time, band/mag from -A,
    the -B obsid in ``obsid_b``). ``reason`` is "" for rows that go to the
    id pass, else why they go straight to the fallback.
    """
    tbl = tbl.filter(pc.equal(tbl["stn"], "X05"))
    label, ids, part = parse_obssubid(tbl["obssubid"])
    obs = dict(row=np.arange(len(tbl)), obsid=tbl["obsid"].to_numpy(),
               submission_id=tbl["submission_id"].to_numpy(), trksub=tbl["trksub"].to_numpy(),
               trkid=tbl["trkid"].to_numpy(),
               obssubid=pc.utf8_trim_whitespace(_arr(tbl["obssubid"])).to_numpy(zero_copy_only=False),
               label=label, id=ids, tai=utc_to_tai_mjd(tbl["obstime"]), band_stripped=strip_band(tbl["band"]))
    obs["ra"], obs["dec"], obs["mag"] = (_f64(tbl, c) for c in ("ra", "dec", "mag"))
    obs["obsid_b"] = np.full(len(ids), None, dtype=object)
    obs["reason"] = np.where(ids < 0, "no_id", "").astype(object)

    # Pair trail endpoints: a key with exactly one -A and exactly one -B.
    key = np.full(len(ids), None, dtype=object)
    trail = np.flatnonzero(part != "")
    key[trail] = [f"{label[i]}\0{ids[i]}" for i in trail]
    ia, ib = np.flatnonzero(part == "A"), np.flatnonzero(part == "B")
    ua, ca = np.unique(key[ia], return_counts=True)
    ub, cb = np.unique(key[ib], return_counts=True)
    good = np.intersect1d(ua[ca == 1], ub[cb == 1])
    ia, ib = ia[np.isin(key[ia], good)], ib[np.isin(key[ib], good)]
    ia, ib = ia[np.argsort(key[ia])], ib[np.argsort(key[ib])]
    assert np.all(key[ia] == key[ib])

    unpaired = (part != "") & ~np.isin(np.arange(len(ids)), np.concatenate([ia, ib]))
    obs["id"][unpaired] = -1
    obs["reason"][unpaired] = "unpaired_trail"

    obs["ra"][ia], obs["dec"][ia], obs["tai"][ia] = midpoint(
        obs["ra"][ia], obs["dec"][ia], obs["tai"][ia], obs["ra"][ib], obs["dec"][ib], obs["tai"][ib]
    )
    obs["obsid_b"][ia] = obs["obsid"][ib]
    keep = np.ones(len(ids), dtype=bool)
    keep[ib] = False
    obs = {k: v[keep] for k, v in obs.items()}
    return obs, tbl, len(ia)


#
# Matching (pure; no network)
#


def sep_mas(ra1, dec1, ra2, dec2):
    """Haversine separation in mas. (Unit-vector dot products bottom out at
    ~4 mas in float64, more than the acceptance radius.)"""
    ra1, dec1, ra2, dec2 = (np.radians(x) for x in (ra1, dec1, ra2, dec2))
    h = np.sin((dec2 - dec1) / 2) ** 2 + np.cos(dec1) * np.cos(dec2) * np.sin((ra2 - ra1) / 2) ** 2
    return np.degrees(2 * np.arcsin(np.sqrt(np.clip(h, 0, 1)))) * 3.6e6


def join_ids(a, b):
    """Many-to-many inner join: all ``(i, j)`` with ``a[i] == b[j]``."""
    a, b = np.asarray(a), np.asarray(b)
    order = np.argsort(b, kind="stable")
    bs = b[order]
    lo, hi = np.searchsorted(bs, a, "left"), np.searchsorted(bs, a, "right")
    n = hi - lo
    ai = np.repeat(np.arange(len(a)), n)
    within = np.arange(n.sum()) - np.repeat(np.cumsum(n) - n, n)
    return ai, order[np.repeat(lo, n) + within]


def _f64(tbl, name):
    return pc.fill_null(tbl[name].cast(pa.float64()), np.nan).to_numpy().copy()


def score(obs, oi, cand, ci):
    """Verify obs-row/candidate pairs ``(obs row oi[k], cand row ci[k])``.

    Returns a dict of per-pair arrays: ``sep_mas`` (the closer of the PSF
    and trail centroids), ``dt_ms`` (view - obs), ``band_ok``, ``dmag``
    (recorded only) and ``passed``.
    """
    ra, dec = obs["ra"][oi], obs["dec"][oi]
    with np.errstate(invalid="ignore"):
        s_psf = sep_mas(ra, dec, _f64(cand, "ra")[ci], _f64(cand, "dec")[ci])
        s_trail = sep_mas(ra, dec, _f64(cand, "trailRa")[ci], _f64(cand, "trailDec")[ci])
    sep = np.fmin(s_psf, s_trail)
    dt = (_f64(cand, "mjd_tai")[ci] - obs["tai"][oi]) * 86400e3

    band = cand["band"].to_numpy(zero_copy_only=False)[ci]
    band_ok = np.asarray(band == obs["band_stripped"][oi], dtype=bool) & (band != None)  # noqa: E711

    flux = _f64(cand, "psfFlux")[ci]
    with np.errstate(invalid="ignore", divide="ignore"):
        dmag = np.where(flux > 0, obs["mag"][oi] - (31.4 - 2.5 * np.log10(flux)), np.nan)

    with np.errstate(invalid="ignore"):
        passed = (sep <= SEP_MAS) & (np.abs(dt) <= DT_MS)
    return dict(sep_mas=sep, dt_ms=dt, band_ok=band_ok, dmag=dmag, passed=passed)


def rank(oi, collection, ids, sep, band_ok):
    """Pick one winner per obs row among (already passing) pairs.

    Sort key, ascending: (deprioritized label, not band_ok, sep_mas,
    collection, id) -- the last two only make the choice deterministic.
    Returns ``(pair index of each winner, n_pass, ambiguous)``, one entry
    per distinct obs row, in ascending ``oi`` order. ``ambiguous`` means
    the runner-up ties the winner on (deprioritized, band_ok) and is
    within AMBIGUOUS_MAS in separation.
    """
    oi = np.asarray(oi)
    if len(oi) == 0:
        return np.zeros(0, int), np.zeros(0, int), np.zeros(0, bool)
    deprio = np.isin(collection, DEPRIORITIZED_LABELS)
    _, ccode = np.unique(collection.astype(str), return_inverse=True)
    order = np.lexsort((ids, ccode, sep, ~band_ok, deprio, oi))
    so = oi[order]
    start = np.flatnonzero(np.r_[True, so[1:] != so[:-1]])
    n_pass = np.diff(np.r_[start, len(so)])
    win = order[start]

    ambiguous = np.zeros(len(win), dtype=bool)
    two = n_pass > 1
    w, r = win[two], order[start[two] + 1]
    ambiguous[two] = ((deprio[w] == deprio[r]) & (band_ok[w] == band_ok[r])
                      & (np.abs(sep[r] - sep[w]) < AMBIGUOUS_MAS))
    return win, n_pass, ambiguous


def resolve(obs, oi, cand, ci):
    """Score and rank pairs; returns (obs rows, winning cand rows, per-row
    info dict) for resolved rows, and (rows, best sep, best dt) for rows
    that had candidates but none passing."""
    sc = score(obs, oi, cand, ci)
    p = sc["passed"]
    collection = cand["collection"].to_numpy(zero_copy_only=False)[ci]
    ids = cand["id"].to_numpy()[ci]
    win, n_pass, ambiguous = rank(oi[p], collection[p], ids[p], sc["sep_mas"][p], sc["band_ok"][p])
    win = np.flatnonzero(p)[win]
    info = dict(sep_mas=sc["sep_mas"][win], dt_ms=sc["dt_ms"][win], dmag=sc["dmag"][win],
                band_ok=sc["band_ok"][win], n_pass=n_pass, ambiguous=ambiguous)

    # Best (closest) failing candidate, for the unresolved report.
    fail = np.setdiff1d(np.unique(oi), oi[win])
    f = np.flatnonzero(np.isin(oi, fail))
    f = f[np.lexsort((np.nan_to_num(sc["sep_mas"][f], nan=np.inf), oi[f]))]
    f = f[np.r_[True, oi[f][1:] != oi[f][:-1]]] if len(f) else f
    return (oi[win], ci[win], info), (oi[f], sc["sep_mas"][f], sc["dt_ms"][f])


#
# Queries
#


def ext_data(**sets):
    """clickhouse-connect external data: one Int64 column per named set
    (the file and its column share the name)."""
    from clickhouse_connect.driver.external import ExternalData

    ext = ExternalData()
    for name, values in sets.items():
        data = "\n".join(map(str, np.asarray(values, dtype=np.int64))).encode()
        ext.add_file(file_name=name, data=data, structure=[f"{name} Int64"], fmt="TSV")
    return ext


def id_queries(obs, database, chunk_size):
    """Id-pass queries: ``[(label, sql, params, ext sets)]``. Labelled ids
    are filtered on their collection (15x faster than not); bare ids
    (label None) search all collections."""
    tasks = []
    labels = obs["label"]
    usable = obs["id"] >= 0
    for label in sorted(set(labels[usable & (labels != None)])) + [None]:  # noqa: E711
        sel = usable & ((labels == label) if label is not None else (labels == None))  # noqa: E711
        ids = np.unique(obs["id"][sel])
        where = "collection = {label:String} AND " if label is not None else ""
        sql = f"SELECT * FROM {database}.{VIEW} WHERE {where}id IN (SELECT q FROM q)"
        for k in range(0, len(ids), chunk_size):
            tasks.append((label, sql, {"label": label} if label is not None else None,
                          dict(q=ids[k:k + chunk_size])))
    return tasks


def cell_block(ra, dec):
    """Order-CELL_ORDER cell + 8 neighbours per position, ``(n, 9)`` int64,
    -1 where a neighbour does not exist."""
    import astropy.units as u
    from astropy.coordinates import Latitude, Longitude
    from cdshealpix.nested import lonlat_to_healpix, neighbours

    ipix = lonlat_to_healpix(Longitude(ra, unit=u.deg), Latitude(dec, unit=u.deg), CELL_ORDER)
    return neighbours(ipix, CELL_ORDER).astype(np.int64)


def time_buckets(tai, tol_s):
    """Distinct minute buckets covering ``[t - tol, t + tol]`` for all t."""
    lo = np.floor((tai - tol_s / 86400) * 1440).astype(np.int64)
    hi = np.floor((tai + tol_s / 86400) * 1440).astype(np.int64)
    steps = range(int((hi - lo).max()) + 1)
    b = np.concatenate([lo + k for k in steps])
    return np.unique(b[b <= np.tile(hi, len(steps))])


def position_queries(obs, rows, database):
    """Position+time queries for ``rows``, one per night:
    ``[(rows of that night, sql, None, ext sets)]``. The predicates follow
    psv_crossmatch.build_query; the bucket expression must be written
    exactly so for ClickHouse's skip indexes to apply."""
    tol_d = DT_MS / 1e3 / 86400
    night = np.floor(obs["tai"][rows])
    tasks = []
    for n in np.unique(night):
        r = rows[night == n]
        tai = obs["tai"][r]
        cells = cell_block(obs["ra"][r], obs["dec"][r])
        sql = (
            f"SELECT * FROM {database}.{VIEW}\n"
            f"WHERE mjd_tai BETWEEN {float(tai.min() - tol_d)!r} AND {float(tai.max() + tol_d)!r}\n"
            f"  AND toInt64(floor(mjd_tai * 1440)) IN (SELECT b FROM b)\n"
            f"  AND hpix29 IS NOT NULL\n"
            f"  AND bitShiftRight(hpix29, {CELL_SHIFT}) IN (SELECT c FROM c)"
        )
        tasks.append((r, sql, None, dict(b=time_buckets(tai, DT_MS / 1e3), c=np.unique(cells[cells >= 0]))))
    return tasks


def run_queries(tasks, host, port, database, user, workers):
    """Run ``(key, sql, params, ext sets)`` tasks on a pool of ``workers``
    threads, one client each; returns ``[(key, pyarrow.Table)]``."""
    import clickhouse_connect

    user, password = credentials(host, port, database, user)
    local = threading.local()

    def run(task):
        key, sql, params, sets = task
        if not hasattr(local, "client"):
            local.client = clickhouse_connect.get_client(
                host=host, port=port, database=database, username=user, password=password,
                # A killed client must not leave its queries running.
                settings={"max_execution_time": 3600, "cancel_http_readonly_queries_on_client_close": 1},
            )
        # Parquet, not Arrow: Arrow arrives as thousands of tiny batches.
        raw = local.client.raw_query(sql + "\nFORMAT Parquet", parameters=params,
                                     external_data=ext_data(**sets))
        return key, pq.read_table(io.BytesIO(raw))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(run, tasks))


#
# Output
#


def build_output(obs, cand, rows, ci, match, info):
    """The dia_sources table: the winning view rows (all columns, renamed),
    required columns null-filled, plus linkage and match diagnostics."""
    names = [RENAMES.get(c, c) for c in cand.column_names]
    clash = sorted({c for c in names if names.count(c) > 1} | (set(names) & set(EXTRA_COLUMNS)))
    if clash:
        raise ValueError(f"view columns {clash} clash (after renaming {RENAMES}) with each other "
                         f"or with the columns this tool adds; refusing to write duplicate names")
    out = cand.take(pa.array(ci, pa.int64())).rename_columns(names)
    for name, typ in REQUIRED_COLUMNS.items():
        if name not in out.column_names:
            out = out.append_column(name, pa.nulls(len(out), typ))
        elif out.schema.field(name).type != typ:
            out = out.set_column(out.column_names.index(name), name, out[name].cast(typ))
    extra = dict(
        obsid=pa.array(obs["obsid"][rows], pa.string()),
        obsid_b=pa.array(obs["obsid_b"][rows], pa.string()),
        obssubid=pa.array(obs["obssubid"][rows], pa.string()),
        # the submitted tracklet: (submission_id, trksub), and MPC's trkid
        **{c: pa.array(obs[c][rows], pa.string()) for c in ("submission_id", "trksub", "trkid")},
        match=pa.array(match, pa.string()),
        sep_mas=info["sep_mas"], dt_ms=info["dt_ms"],
        dmag=pa.array(info["dmag"], pa.float64(), from_pandas=True),
        band_ok=info["band_ok"], n_pass=pa.array(info["n_pass"], pa.int32()), ambiguous=info["ambiguous"],
    )
    assert list(extra) == EXTRA_COLUMNS
    for name, col in extra.items():
        out = out.append_column(name, pa.array(col))
    return out


def _codes(values):
    """Sortable integer codes for a string array (None sorts last)."""
    v = np.array(["\uffff" if x is None else x for x in values], dtype=object)
    return np.unique(v.astype(str), return_inverse=True)[1]


def dedupe(out, submission_id):
    """One output row per (collection, diaSourceId).

    The same detection can be submitted to the MPC more than once (e.g.
    two submissions of one tracklet, both published). Keep the row whose
    obs_sbn row came from the earliest submission (submission_id starts
    with its ISO timestamp), tie-broken on obsid. Returns
    ``(kept row indices, dropped row indices, index of the kept row for
    each dropped one)``.
    """
    code = pc.dictionary_encode(out["collection"]).combine_chunks().indices.to_numpy()
    ids = out["diaSourceId"].to_numpy()
    order = np.lexsort((_codes(out["obsid"].to_pylist()), _codes(submission_id), ids, code))
    first = np.r_[True, (code[order][1:] != code[order][:-1]) | (ids[order][1:] != ids[order][:-1])]
    group_first = order[np.flatnonzero(first)[np.cumsum(first) - 1]]
    return np.sort(order[first]), order[~first], group_first[~first]


def unresolved_table(obs, tbl, rows, best):
    """The obs_sbn rows (of ``tbl``; for a trail pair its -A row) that did
    not resolve, with the reason and the closest failing candidate's
    sep/dt, if there was one."""
    bsep = np.full(len(obs["id"]), np.nan)
    bdt = np.full(len(obs["id"]), np.nan)
    br, bs, bd = best
    bsep[br], bdt[br] = bs, bd
    out = tbl.take(pa.array(obs["row"][rows], pa.int64())).drop_columns(["stn"])
    out = out.append_column("obsid_b", pa.array(obs["obsid_b"][rows], pa.string()))
    out = out.append_column("reason", pa.array(obs["reason"][rows], pa.string()))
    out = out.append_column("best_sep_mas", pa.array(bsep[rows], from_pandas=True))
    return out.append_column("best_dt_ms", pa.array(bdt[rows], from_pandas=True))


def _counts(values):
    u, c = np.unique(np.asarray(values, dtype=str), return_counts=True)
    return ", ".join(f"{k}: {n:,}" for k, n in sorted(zip(u, c), key=lambda x: -x[1])) or "-"


def extract(obs_path, out_path, fetch, database=DEFAULT_DATABASE, chunk_size=250_000):
    """Resolve the X05 rows of ``obs_path`` against the view and write
    ``out_path``, ``<stem>.unresolved.parquet`` and
    ``<stem>.duplicates.parquet``. ``fetch`` runs a list of query tasks
    (see ``run_queries``). Returns the exit code."""
    timings = {}
    t0 = time.time()

    tbl = pq.read_table(obs_path, columns=OBS_COLUMNS)
    obs, tbl, n_pairs = load_obs(tbl)
    n_x05 = len(tbl)
    s = obs["obssubid"]
    su, sc = np.unique(s[s != None].astype(str), return_counts=True)  # noqa: E711
    n_reused = int(np.sum((sc > 1) & (su != "")))
    n = len(obs["id"])
    timings["read obs_sbn"] = time.time() - t0

    # Id pass
    t0 = time.time()
    results = fetch(id_queries(obs, database, chunk_size))
    parts, oi, ci, offset = [], [], [], 0
    by_label = {}
    for label, t in results:
        by_label.setdefault(label, []).append(t)
    for label, ts in by_label.items():
        t = pa.concat_tables(ts)
        sel = np.flatnonzero((obs["id"] >= 0) & (obs["label"] == label if label is not None
                                                 else obs["label"] == None))  # noqa: E711
        a, b = join_ids(obs["id"][sel], t["id"].to_numpy())
        oi.append(sel[a])
        ci.append(b + offset)
        parts.append(t)
        offset += len(t)
    cand = pa.concat_tables(parts)
    oi, ci = np.concatenate(oi), np.concatenate(ci)
    (r_id, c_id, i_id), best = resolve(obs, oi, cand, ci)
    reason = obs["reason"]
    todo = np.ones(n, dtype=bool)
    todo[r_id] = False
    reason[todo & (reason == "")] = "no_candidate"
    reason[best[0]] = "no_pass"
    timings["id pass"] = time.time() - t0
    print(f"id pass: {len(cand):,} candidates for {len(np.unique(oi)):,} rows, {len(r_id):,} resolved")

    # Position+time fallback for everything else
    t0 = time.time()
    rows = np.flatnonzero(todo & np.isfinite(obs["ra"]) & np.isfinite(obs["dec"]) & np.isfinite(obs["tai"]))
    r_pos, c_pos, i_pos = np.zeros(0, int), np.zeros(0, int), None
    if len(rows):
        results = fetch(position_queries(obs, rows, database))
        oi, ci, poff = [], [], len(cand)
        for r, t in results:
            cells = cell_block(obs["ra"][r], obs["dec"][r])
            rr = np.repeat(r, cells.shape[1])
            a, b = join_ids(cells.ravel(), t["hpix29"].to_numpy() >> CELL_SHIFT)
            keep = cells.ravel()[a] >= 0
            oi.append(rr[a[keep]])
            ci.append(b[keep] + poff)
            poff += len(t)
        cand = pa.concat_tables([cand] + [t for _, t in results])
        oi, ci = np.concatenate(oi), np.concatenate(ci)
        (r_pos, c_pos, i_pos), best_pos = resolve(obs, oi, cand, ci)
        # A row's best failing candidate from the id pass is the one to report.
        keep = ~np.isin(best_pos[0], best[0])
        best = tuple(np.concatenate([x, y[keep]]) for x, y in zip(best, best_pos))
    timings["position pass"] = time.time() - t0

    # Assemble
    t0 = time.time()
    rows_all = np.concatenate([r_id, r_pos])
    info = i_id if i_pos is None else {k: np.concatenate([i_id[k], i_pos[k]]) for k in i_id}
    match = np.array(["id"] * len(r_id) + ["position"] * len(r_pos), dtype=object)
    order = np.argsort(rows_all, kind="stable")
    out = build_output(obs, cand, rows_all[order], np.concatenate([c_id, c_pos])[order], match[order],
                       {k: v[order] for k, v in info.items()})

    # Sources claimed by more than one obs_sbn row: keep the earliest
    # submission's, report the rest (they get no SSSource row).
    rows_all = rows_all[order]
    keep, drop, kept = dedupe(out, obs["submission_id"][rows_all])
    dups = tbl.take(pa.array(obs["row"][rows_all[drop]], pa.int64())).drop_columns(["stn"])
    dups = dups.append_column("kept_obsid", out["obsid"].take(pa.array(kept, pa.int64())))
    for c in ("collection", "diaSourceId", "sep_mas", "dt_ms"):
        dups = dups.append_column(c, out[c].take(pa.array(drop, pa.int64())))
    n_dup_sources = len(np.unique(kept))
    out = out.take(pa.array(keep, pa.int64()))
    assert len(dedupe(out, obs["submission_id"][rows_all[keep]])[1]) == 0

    resolved = np.zeros(n, dtype=bool)
    resolved[rows_all] = True
    unres = unresolved_table(obs, tbl, np.flatnonzero(~resolved), best)
    stem = str(out_path)[:-8] if str(out_path).endswith(".parquet") else str(out_path)
    pq.write_table(out, out_path, compression="zstd")
    pq.write_table(unres, f"{stem}.unresolved.parquet", compression="zstd")
    pq.write_table(dups, f"{stem}.duplicates.parquet", compression="zstd")
    timings["assemble + write"] = time.time() - t0

    print(f"\nobs_sbn X05 rows read:            {n_x05:,}")
    print(f"A/B trail pairs merged:           {n_pairs:,}  -> {n:,} logical rows")
    print(f"obsSubIDs used more than once:    {n_reused:,}")
    print(f"resolved by id:                   {len(r_id):,}")
    print(f"resolved by position:             {len(r_pos):,}  (id-pass reason: {_counts(reason[r_pos])})")
    print(f"unresolved:                       {len(unres):,}  ({_counts(unres['reason'].to_numpy(False))})")
    print(f"sources claimed by more than one obs_sbn row: {n_dup_sources:,} sources, "
          f"{len(drop):,} rows dropped (see {stem}.duplicates.parquet)")
    print(f"ambiguous:                        {pc.sum(out['ambiguous']).as_py() or 0:,}")
    print(f"band_ok = false:                  {pc.sum(pc.invert(out['band_ok'])).as_py() or 0:,}")
    print(f"per collection:                   {_counts(out['collection'].to_numpy(False))}")
    print(f"wrote {len(out):,} rows to {out_path}, {len(unres):,} to {stem}.unresolved.parquet")
    for k, v in timings.items():
        print(f"  {k:20s} {v:8.1f} s")
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Build dia_sources.parquet for the X05 rows of an MPC obs_sbn dump "
                    "from ssp.SubmittableSources",
        epilog="Credentials: SSP_CH_USER/SSP_CH_PASSWORD if set, else ~/.chpass "
               "(pgpass format, mode 0600).",
    )
    parser.add_argument("obs_sbn", help="MPC obs_sbn Parquet file")
    parser.add_argument("output", help="Output dia_sources Parquet file")
    parser.add_argument("--host", default=DEFAULT_HOST, help="ClickHouse host (default: %(default)s)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help="ClickHouse HTTP port (default: %(default)s)")
    parser.add_argument("--database", default=DEFAULT_DATABASE,
                        help="ClickHouse database (default: %(default)s)")
    parser.add_argument("--user", default=None, help="ClickHouse user (default: from the credentials)")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS,
                        help=f"Concurrent queries, at most {MAX_WORKERS} (default: %(default)s)")
    parser.add_argument("--chunk-size", type=int, default=250_000,
                        help="Ids per query (default: %(default)s)")
    args = parser.parse_args()
    if not 1 <= args.workers <= MAX_WORKERS:
        parser.error(f"--workers must be between 1 and {MAX_WORKERS} (the server is shared)")

    t0 = time.time()
    def fetch(tasks):
        return run_queries(tasks, args.host, args.port, args.database, args.user, args.workers)

    rc = extract(args.obs_sbn, args.output, fetch, database=args.database, chunk_size=args.chunk_size)
    print(f"total wall time: {time.time() - t0:.1f} s")
    sys.exit(rc)


if __name__ == "__main__":
    main()
