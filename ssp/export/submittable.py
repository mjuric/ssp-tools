"""Build ``dia_sources.parquet`` from ClickHouse ``ssp.SubmittableSources``.

For every X05 row of an MPC ``obs_sbn`` Parquet dump, find the source
measurement it was submitted from and write one row per resolved
observation, carrying all of the view's columns plus the linkage
(``obsid``) and match diagnostics. This is an alternative to building
``dia_sources.parquet`` from the Butler with ``extract-catalog``.

Matching:

1. **By id.** ``obsSubID`` is ``LSST-<processing>-<id>`` (or, for trailed
   sources submitted as two endpoints, ``...-<id>-A`` / ``-B``), or a bare
   ``<id>`` from before labels existed. Labelled ids are looked up in their
   processing, bare ids in all processings.
2. **Verified.** A candidate passes if its PSF or trail centroid is within
   ``SEP_MAS`` of the submitted position and its time within ``DT_MS``.
   Band and magnitude are recorded but never reject.
3. **Ranked.** One winner per observation; see ``rank()``.
4. **By position+time**, for the rows the id pass could not resolve,
   following ``ssp-submit/ops/psv_crossmatch.py``.

Shutter-motion correction (docs/design/shutter-timing.md): with a
correction table (``--correction-table``, on by default), every candidate
that passes the position test gets its source's corrected exposure midpoint
(``shutter_timing.corrections.corrected_midpoints``) *before* matching, and
the time test passes if the obs_sbn time is within ``DT_MS`` of the visit
time **or** of the corrected time (``obstime_basis``: ``visit``,
``corrected``, ``both``); the position pass's time window is widened to
cover both. The output's ``midpointMjdTai`` is then the corrected time
where there is one, ``midpointMjdTaiVisit`` the visit's, and
``midpointMjdTai_flag``/``_flag_degraded`` say which (see
``ssp.ssobservation_contract``, "Shutter-motion-corrected times"). Without a
table, the extract is exactly the uncorrected one.

Credentials: ``SSP_CH_USER``/``SSP_CH_PASSWORD`` if set, else ``~/.chpass``
(pgpass format, mode 0600). There is deliberately no password flag.
"""

from __future__ import annotations

import argparse
import io
import os
import re
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

from ssp.delivery_contract import SHUTTER_INPUT_COLUMNS, SHUTTER_MANIFEST_FIELD
from ssp.ssobservation_contract import (MATCH_METHODS, MAX_CORRECTION_S, MAX_HEADER_MISMATCH_S,
                                        MAX_NOT_BUILT_VISITS)

# The ClickHouse ("River") server has moved more than once (sdfiana035 ->
# 172.24.10.116 on 2026-10-04 -> sdfiana032 on 2026-10-05). Its operators keep
# the current host in HOST_FILE, one line "river:<host>"; current_host() reads
# it, and FALLBACK_HOST is used only when the file is missing or has no such
# line. An explicit --host always wins. HTTP only; keep it out of SDF's proxy
# (bypass_proxy).
HOST_FILE = Path.home() / ".clickhouse.host"
HOST_KEY = "river"
FALLBACK_HOST = "sdfiana032.sdf.slac.stanford.edu"
DEFAULT_PORT = 8123
DEFAULT_DATABASE = "ssp"
VIEW = "SubmittableSources"

# The server is shared: never run more than this many queries at once.
MAX_WORKERS = 8

# Acceptance: 11% of rows were submitted with 6-decimal degrees (~1.8 mas
# rounding per coordinate), hence 3 mas; observed |dt| is <= 6 ms.
SEP_MAS = 3.0
DT_MS = 10.0

# The shutter-motion correction table, kept up to date by ssp-daily
# (shutter-timing-table, hourly). "none" on the command line disables the
# correction.
DEFAULT_CORRECTION_TABLE = "/sdf/data/rubin/user/mjuric/shutter-timing/corrections"

# The position pass's time window must also fetch candidates whose
# *corrected* time is within DT_MS of the obs_sbn time, so it is widened by a
# bound on |corrected - pipeline visit time|, per night
# (Corrections.window_shift): the night's largest |table visit midpoint -
# header midpoint| (the visit-level part of the shift; the pipeline time is
# the header midpoint wherever a correction is applied) plus
# SHUTTER_GRADIENT_MAX_S (the per-source part), and never less than
# MIN_WINDOW_SHIFT_S. position_queries adds DT_MS itself. Normal nights get
# +-(3 s + DT_MS); a night with a hung readout, as much as it needs.
MIN_WINDOW_SHIFT_S = MAX_CORRECTION_S
#: |corrected - the table's visit midpoint| across the focal plane: 0.244 s
#: at most on the 2026-10-04 inputs (8.07M sources); with a margin.
SHUTTER_GRADIENT_MAX_S = 0.3

# How an obs_sbn row's time matched its measurement's (within DT_MS).
OBSTIME_BASES = ("visit", "corrected", "both")

# Rows per corrected_midpoints call (whole nights per call; bounds the
# memory a call needs for its nights' tables).
LOOKUP_BATCH_ROWS = 4_000_000

# Columns build_output appends with a correction table: the contract's
# SHUTTER_INPUT_COLUMNS, plus (corrected time - obstime) as a diagnostic.
SHUTTER_COLUMNS = ["midpointMjdTaiVisit", "midpointMjdTai_flag", "midpointMjdTai_flag_degraded",
                   "obstime_basis", "dt_corrected_ms"]
assert set(SHUTTER_INPUT_COLUMNS) <= set(SHUTTER_COLUMNS)

# corrected_midpoints status codes (shutter_timing.CorrectionStatus); ours,
# set by the visit-time guard (header_guard) on corrected sources, or for
# nights the table does not cover:
#   OUTSIDE_COVERAGE   before the table's first night, never looked up
#   TIME_MISMATCH      the pipeline's visit time matches neither the
#                      exposure log's header midpoint nor the corrected
#                      time: not applied
#   LARGE_SHIFT        |corrected - pipeline| > MAX_CORRECTION_S: applied,
#                      but marked degraded
#   ALREADY_CORRECTED  the pipeline's time is the corrected time: it stands
#                      (_DEGRADED: of a DEGRADED correction)
#   NOT_LOOKED_UP      candidates that never needed a lookup
OK, DEGRADED, OMITTED, NOT_BUILT = 0, 1, 2, 3
OUTSIDE_COVERAGE, TIME_MISMATCH, LARGE_SHIFT = 4, 5, 6
ALREADY_CORRECTED, ALREADY_CORRECTED_DEGRADED = 7, 8
NOT_LOOKED_UP = 255
#: The manifest's status names -> the codes they count.
STATUS_NAMES = {"ok": (OK,), "degraded": (DEGRADED,), "omitted": (OMITTED,), "not_built": (NOT_BUILT,),
                "outside_coverage": (OUTSIDE_COVERAGE,), "time_mismatch": (TIME_MISMATCH,),
                "large_shift": (LARGE_SHIFT,),
                "already_corrected": (ALREADY_CORRECTED, ALREADY_CORRECTED_DEGRADED)}
#: Statuses whose corrected time is applied (or, already corrected, stands);
#: every other one keeps the visit time, flagged.
APPLIED = (OK, DEGRADED)
TIME_STANDS = (OK, DEGRADED, LARGE_SHIFT, ALREADY_CORRECTED, ALREADY_CORRECTED_DEGRADED)
#: ... and of those, the degraded ones.
DEGRADED_ALL = (DEGRADED, LARGE_SHIFT, ALREADY_CORRECTED_DEGRADED)
# header_guard's verdict for a source whose correction is applied.
GUARD_APPLY = 0

# Visits listed in a warning at most (the manifest has them all).
WARN_VISITS = 20


class CorrectionError(RuntimeError):
    """The shutter-motion correction cannot be applied (the table is
    missing, mixed, malformed or too stale); the extract writes nothing."""


# Two passing candidates closer than this in separation are a tie.
AMBIGUOUS_MAS = 0.01

#: Labels whose rows are SUPERSEDED BY CONSTRUCTION -- another processing
#: serves a better row for the same id. These must never outrank a live label
#: on a tie.
#:
#: Without this, `processing` was doing two jobs in one sort key: the
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

#: DP2 was submitted from twice: the April 2026 submissions (combsub-20260422,
#: preredo-20260422) from a DP2 prerelease run (pDP2-DS), the 2026-06-04 ones
#: (post-dp2-*) and later from the final run (DP2-DS). The PSV headers don't
#: say so; the linker inputs do -- their positions and magnitudes are copied
#: bit-for-bit from dia_source_dp2_v30_0_0 (April) and dia_source_dp2 (June),
#: and no June observation matches pDP2-DS alone, nor any April one DP2-DS
#: alone. The two runs share most ids and are mostly -- not always --
#: identical, so a bare obsSubID that both accept is resolved by submission
#: date, not by separation: pDP2-DS if submitted before the cutoff, else
#: DP2-DS. (Later submissions should carry the LSST-DP2-DS- prefix anyway.)
#: The cutoff is compared with submission_id, which starts with its ISO UTC
#: timestamp.
DP2_PRERELEASE, DP2_FINAL, DP2_CUTOFF = "pDP2-DS", "DP2-DS", "2026-06-04"

# Position fallback blocking (see psv_crossmatch.CELL_ORDER): an order-16
# cell is ~3.2", and hpix29 >> CELL_SHIFT is the order-16 cell index.
CELL_ORDER = 16
CELL_SHIFT = 2 * (29 - CELL_ORDER)

# View columns renamed to their DiaSource names on output.
RENAMES = {"id": "diaSourceId"}

# DiaSource columns that SSObservation/SSObject read. Any missing from the view
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
EXTRA_COLUMNS = ["obsid", "obssubid", "submission_id", "trksub", "trkid", "primary", "match", "matchMethod",
                 "sep_mas", "dt_ms", "dmag", "band_ok", "n_pass", "ambiguous"]

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


def current_host(path=None):
    """The ClickHouse host named by ``path`` (default HOST_FILE), a line
    "river:<host>"; FALLBACK_HOST if the file is missing, unreadable or has
    no such line."""
    path = Path(path) if path is not None else HOST_FILE
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return FALLBACK_HOST
    for line in lines:
        key, sep, host = line.strip().partition(":")
        if sep and key.strip() == HOST_KEY and host.strip():
            return host.strip()
    return FALLBACK_HOST


def bypass_proxy(host):
    """Exclude ``host`` from the http(s)_proxy that clickhouse_connect would
    otherwise take from the environment (SDF sets one, and its squid proxy
    refuses the ClickHouse server: HTTP 403). Adds ``host`` to ``no_proxy``
    and ``NO_PROXY``; a no-op without a proxy, or if it's already there."""
    if not any(os.environ.get(v) for v in ("http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY")):
        return
    for var in ("no_proxy", "NO_PROXY"):
        names = [n.strip() for n in os.environ.get(var, "").split(",") if n.strip()]
        if host not in names and "*" not in names:
            os.environ[var] = ",".join([host] + names)


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
    matched as one logical row, its -A row (midpoint position and time,
    band/mag from -A; ``row_b`` is the -B row of ``tbl``, else -1).
    ``reason`` is "" for rows that go to the id pass, else why they go
    straight to the fallback.
    """
    tbl = tbl.filter(pc.equal(tbl["stn"], "X05"))
    label, ids, part = parse_obssubid(tbl["obssubid"])
    obs = dict(row=np.arange(len(tbl)), obsid=tbl["obsid"].to_numpy(),
               obssubid=pc.utf8_trim_whitespace(_arr(tbl["obssubid"])).to_numpy(zero_copy_only=False),
               label=label, id=ids, tai=utc_to_tai_mjd(tbl["obstime"]), band_stripped=strip_band(tbl["band"]),
               submission_id=pc.fill_null(tbl["submission_id"], "").to_numpy(zero_copy_only=False))
    obs["ra"], obs["dec"], obs["mag"] = (_f64(tbl, c) for c in ("ra", "dec", "mag"))
    obs["row_b"] = np.full(len(ids), -1)
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
    obs["row_b"][ia] = obs["row"][ib]
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


def pair_sep(obs, oi, cand, ci):
    """Per pair: the separation (mas) of the closer of the PSF and trail
    centroids from the submitted position."""
    ra, dec = obs["ra"][oi], obs["dec"][oi]
    with np.errstate(invalid="ignore"):
        s_psf = sep_mas(ra, dec, _f64(cand, "ra")[ci], _f64(cand, "dec")[ci])
        s_trail = sep_mas(ra, dec, _f64(cand, "trailRa")[ci], _f64(cand, "trailDec")[ci])
    return np.fmin(s_psf, s_trail)


def score(obs, oi, cand, ci, t_corr=None):
    """Verify obs-row/candidate pairs ``(obs row oi[k], cand row ci[k])``.

    Returns a dict of per-pair arrays: ``sep_mas`` (the closer of the PSF
    and trail centroids), ``dt_ms`` (view - obs), ``band_ok``, ``dmag``
    (recorded only) and ``passed``. With ``t_corr`` (per cand row, the
    corrected time, NaN where there is none) the time test also passes on
    the corrected time, and the dict has ``dt_corrected_ms`` (corrected -
    obs) and ``obstime_basis`` (per pair: "visit", "corrected", "both", or
    None where the time test fails).
    """
    sep = pair_sep(obs, oi, cand, ci)
    dt = (_f64(cand, "midpointMjdTai")[ci] - obs["tai"][oi]) * 86400e3

    band = cand["band"].to_numpy(zero_copy_only=False)[ci]
    band_ok = np.asarray(band == obs["band_stripped"][oi], dtype=bool) & (band != None)  # noqa: E711

    flux = _f64(cand, "psfFlux")[ci]
    with np.errstate(invalid="ignore", divide="ignore"):
        dmag = np.where(flux > 0, obs["mag"][oi] - (31.4 - 2.5 * np.log10(flux)), np.nan)

    with np.errstate(invalid="ignore"):
        on_visit = np.abs(dt) <= DT_MS
        if t_corr is None:
            passed = (sep <= SEP_MAS) & on_visit
            return dict(sep_mas=sep, dt_ms=dt, band_ok=band_ok, dmag=dmag, passed=passed)
        dtc = (np.asarray(t_corr)[ci] - obs["tai"][oi]) * 86400e3
        on_corr = np.abs(dtc) <= DT_MS
        passed = (sep <= SEP_MAS) & (on_visit | on_corr)
    basis = np.full(len(dt), None, dtype=object)
    basis[on_visit] = "visit"
    basis[on_corr] = "corrected"
    basis[on_visit & on_corr] = "both"
    return dict(sep_mas=sep, dt_ms=dt, band_ok=band_ok, dmag=dmag, passed=passed,
                dt_corrected_ms=dtc, obstime_basis=basis)


def dp2_demoted(oi, processing, early):
    """Per pair: True for the DP2 processing that loses under the DP2 rule
    (see DP2_CUTOFF) -- DP2-DS for a row submitted before the cutoff,
    pDP2-DS after it -- on rows where both DP2-DS and pDP2-DS pass.
    ``early`` is per pair. Other processings are never demoted."""
    oi = np.asarray(oi)
    both = np.intersect1d(oi[processing == DP2_PRERELEASE], oi[processing == DP2_FINAL])
    loser = np.where(early, DP2_FINAL, DP2_PRERELEASE)
    return np.isin(oi, both) & (processing == loser)


def rank(oi, processing, ids, sep, band_ok, demoted=None):
    """Pick one winner per obs row among (already passing) pairs.

    Sort key, ascending: (deprioritized label, demoted, not band_ok,
    sep_mas, processing, id) -- the last two only make the choice
    deterministic. ``demoted`` (per pair, default none) is the DP2 rule's
    loser, see dp2_demoted. Returns ``(pair index of each winner, n_pass,
    ambiguous)``, one entry per distinct obs row, in ascending ``oi``
    order. ``ambiguous`` means the runner-up ties the winner on
    (deprioritized, demoted, band_ok) and is within AMBIGUOUS_MAS in
    separation.
    """
    oi = np.asarray(oi)
    if len(oi) == 0:
        return np.zeros(0, int), np.zeros(0, int), np.zeros(0, bool)
    deprio = np.isin(processing, DEPRIORITIZED_LABELS)
    demoted = np.zeros(len(oi), bool) if demoted is None else np.asarray(demoted)
    _, ccode = np.unique(processing.astype(str), return_inverse=True)
    order = np.lexsort((ids, ccode, sep, ~band_ok, demoted, deprio, oi))
    so = oi[order]
    start = np.flatnonzero(np.r_[True, so[1:] != so[:-1]])
    n_pass = np.diff(np.r_[start, len(so)])
    win = order[start]

    ambiguous = np.zeros(len(win), dtype=bool)
    two = n_pass > 1
    w, r = win[two], order[start[two] + 1]
    ambiguous[two] = ((deprio[w] == deprio[r]) & (demoted[w] == demoted[r]) & (band_ok[w] == band_ok[r])
                      & (np.abs(sep[r] - sep[w]) < AMBIGUOUS_MAS))
    return win, n_pass, ambiguous


def resolve(obs, oi, cand, ci, t_corr=None):
    """Score and rank pairs; returns (obs rows, winning cand rows, per-row
    info dict) for resolved rows, and (rows, best sep, best dt) for rows
    that had candidates but none passing. ``t_corr``: see ``score``; with
    it, the info dict also has ``dt_corrected_ms`` and ``obstime_basis``."""
    sc = score(obs, oi, cand, ci, t_corr)
    p = sc["passed"]
    processing = cand["processing"].to_numpy(zero_copy_only=False)[ci]
    ids = cand["id"].to_numpy()[ci]
    early = obs["submission_id"][oi[p]] < DP2_CUTOFF
    demoted = dp2_demoted(oi[p], processing[p], early)
    win, n_pass, ambiguous = rank(oi[p], processing[p], ids[p], sc["sep_mas"][p], sc["band_ok"][p], demoted)
    # rows the DP2 rule decided: both DP2 processings passed
    dp2_rule = np.isin(oi[p][win], oi[p][demoted])
    win = np.flatnonzero(p)[win]
    info = dict(sep_mas=sc["sep_mas"][win], dt_ms=sc["dt_ms"][win], dmag=sc["dmag"][win],
                band_ok=sc["band_ok"][win], n_pass=n_pass, ambiguous=ambiguous, dp2_rule=dp2_rule)
    if t_corr is not None:
        info.update(dt_corrected_ms=sc["dt_corrected_ms"][win], obstime_basis=sc["obstime_basis"][win])

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
    are filtered on their processing (15x faster than not); bare ids
    (label None) search all processings."""
    tasks = []
    labels = obs["label"]
    usable = obs["id"] >= 0
    for label in sorted(set(labels[usable & (labels != None)])) + [None]:  # noqa: E711
        sel = usable & ((labels == label) if label is not None else (labels == None))  # noqa: E711
        ids = np.unique(obs["id"][sel])
        where = "processing = {label:String} AND " if label is not None else ""
        sql = f"SELECT * FROM {database}.{VIEW} WHERE {where}id IN (SELECT q FROM q)"
        for k in range(0, len(ids), chunk_size):
            tasks.append((label, sql, {"label": label} if label is not None else None,
                          dict(q=ids[k:k + chunk_size])))
    if not tasks:
        # no usable id at all: one empty query, for the view's schema
        tasks.append((None, f"SELECT * FROM {database}.{VIEW} WHERE id IN (SELECT q FROM q)", None,
                      dict(q=np.zeros(0, np.int64))))
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


def position_queries(obs, rows, database, shift_s=0.0):
    """Position+time queries for ``rows``, one per night:
    ``[(rows of that night, sql, None, ext sets)]``. The predicates follow
    psv_crossmatch.build_query; the bucket expression must be written
    exactly so for ClickHouse's skip indexes to apply. The view's (visit)
    time must be within DT_MS + ``shift_s`` seconds of the obs_sbn time
    (``shift_s`` > 0 also fetches the candidates whose corrected time,
    within ``shift_s`` of the visit time, may match). ``shift_s``: seconds,
    or a function of the night (the integer MJD) giving them."""
    night = np.floor(obs["tai"][rows])
    tasks = []
    for n in np.unique(night):
        tol_s = DT_MS / 1e3 + (shift_s(int(n)) if callable(shift_s) else shift_s)
        tol_d = tol_s / 86400
        r = rows[night == n]
        tai = obs["tai"][r]
        cells = cell_block(obs["ra"][r], obs["dec"][r])
        sql = (
            f"SELECT * FROM {database}.{VIEW}\n"
            f"WHERE midpointMjdTai BETWEEN {float(tai.min() - tol_d)!r} AND {float(tai.max() + tol_d)!r}\n"
            f"  AND toInt64(floor(midpointMjdTai * 1440)) IN (SELECT b FROM b)\n"
            f"  AND hpix29 IS NOT NULL\n"
            f"  AND bitShiftRight(hpix29, {CELL_SHIFT}) IN (SELECT c FROM c)"
        )
        tasks.append((r, sql, None, dict(b=time_buckets(tai, tol_s), c=np.unique(cells[cells >= 0]))))
    return tasks


def run_queries(tasks, host, port, database, user, workers):
    """Run ``(key, sql, params, ext sets)`` tasks on a pool of ``workers``
    threads, one client each; returns ``[(key, pyarrow.Table)]``."""
    import clickhouse_connect

    user, password = credentials(host, port, database, user)
    bypass_proxy(host)
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
# Shutter-motion correction
#


#: Exposure-log columns (exposures_<day_obs>.parquet) read per visit: the
#: guard's header midpoint, and the table's visit midpoint (for the
#: position window). shutter_timing's public API does not return them: read
#: from the table's files.
GUARD_LOG_COLUMNS = ("header_mid_mjd_tai", "t_mid_visit_mjd_tai")


def header_guard(log, t_pipe, t_corr):
    """The visit-time guard (owner decisions 2026-10-06), for corrected
    sources (``t_corr`` finite). Per source:

    - GUARD_APPLY: the pipeline's time equals the exposure log's header
      midpoint ``header_mid_mjd_tai`` ((MJD-BEG + MJD-END)/2) to within
      MAX_HEADER_MISMATCH_S, or the visit is not in the log (NaN: the
      table's own status stands): the correction is applied;
    - LARGE_SHIFT: as GUARD_APPLY (applied), but |corrected - pipeline| >
      MAX_CORRECTION_S: marked degraded, with a warning;
    - ALREADY_CORRECTED: otherwise, if the pipeline's time equals the
      corrected time to within MAX_HEADER_MISMATCH_S (an already-corrected
      input): it stands. The header match is tried first: a correction
      smaller than MAX_HEADER_MISMATCH_S would otherwise make ~0.6% of
      ordinary sources look "already corrected" (52,037 rows of the
      2026-10-04 inputs). (An already-corrected time is within 1 ms of the
      corrected one, so never a large shift.)
    - TIME_MISMATCH: neither matches: not applied, however large the shift.

    ``log``: GUARD_LOG_COLUMNS arrays aligned with ``t_pipe`` (MJD TAI).
    Returns ``(verdict, d_header_s, shift_s)``: pipeline - header, and
    corrected - pipeline."""
    d_header = (t_pipe - log["header_mid_mjd_tai"]) * 86400
    shift = (t_corr - t_pipe) * 86400
    verdict = np.full(len(shift), GUARD_APPLY, np.uint8)
    with np.errstate(invalid="ignore"):
        off_header = np.abs(d_header) > MAX_HEADER_MISMATCH_S
        verdict[~off_header & (np.abs(shift) > MAX_CORRECTION_S)] = LARGE_SHIFT
        verdict[off_header] = TIME_MISMATCH
        verdict[off_header & (np.abs(shift) <= MAX_HEADER_MISMATCH_S)] = ALREADY_CORRECTED
    return verdict, d_header, shift


#: The visit-time guard in force (a function like header_guard, or None).
VISIT_TIME_GUARD = header_guard


class Corrections:
    """The shutter-motion correction table, looked up per candidate.

    ``lookup(cand, rows)`` fills ``t`` (the corrected MJD TAI, NaN where it
    is not applied), ``status`` (CorrectionStatus, or OUTSIDE_COVERAGE,
    TIME_MISMATCH, NOT_LOOKED_UP) and ``dvis`` (the guard's time difference,
    s; NaN where not compared) for the given rows of ``cand``; ``extend(n)``
    grows them with the candidate table. Lookup errors (a mixed, malformed
    or inconsistent table, a non-integral visit or detector) raise
    CorrectionError.

    Nights before the table's first night (``first_night``, its earliest
    ``corrections_<day_obs>.parquet``) are outside its coverage (e.g.
    ComCam): OUTSIDE_COVERAGE, never looked up. ``guard`` (default
    VISIT_TIME_GUARD) is applied to the corrected sources (see
    header_guard): ALREADY_CORRECTED (``t`` is the pipeline's time),
    TIME_MISMATCH (not applied), LARGE_SHIFT (applied, degraded). It sees
    the visit's row of the night's exposure log (shutter_timing's API does
    not return it). ``dshift``: corrected - pipeline time [s], where
    compared.
    """

    def __init__(self, table_dir, max_not_built_visits=MAX_NOT_BUILT_VISITS, guard=VISIT_TIME_GUARD):
        self.table_dir = Path(table_dir)
        if not self.table_dir.is_dir():
            raise CorrectionError(f"the correction table {self.table_dir} does not exist (ssp-daily's "
                                  f"shutter-timing-table --out {self.table_dir} builds it)")
        self.max_not_built_visits = max_not_built_visits
        self.guard = guard
        nights = [int(m.group(1)) for p in self.table_dir.iterdir()
                  if (m := re.fullmatch(r"corrections_(\d{8})\.parquet", p.name))]
        self.first_night = min(nights) if nights else None
        self.t = np.zeros(0)
        self.dvis = np.zeros(0)
        self.dshift = np.zeros(0)
        self.status = np.zeros(0, np.uint8)
        self.formats, self.calibrations, self.versions = set(), set(), set()
        self.seconds, self.calls = 0.0, 0
        self._logs = {}      # day_obs -> (sorted visits, {column: values})

    def extend(self, n):
        k = n - len(self.t)
        self.t = np.concatenate([self.t, np.full(k, np.nan)])
        self.dvis = np.concatenate([self.dvis, np.full(k, np.nan)])
        self.dshift = np.concatenate([self.dshift, np.full(k, np.nan)])
        self.status = np.concatenate([self.status, np.full(k, NOT_LOOKED_UP, np.uint8)])

    def night_log(self, day_obs, required=False):
        """The night's exposure log: ``(sorted visits, {GUARD_LOG_COLUMNS:
        values})``, cached; empty if the night is not built (an error if
        ``required``)."""
        if day_obs not in self._logs:
            path = self.table_dir / f"exposures_{day_obs}.parquet"
            if not path.exists() and not required:
                return np.zeros(0, np.int64), {c: np.zeros(0) for c in GUARD_LOG_COLUMNS}
            try:
                t = pq.read_table(path, columns=["visit", *GUARD_LOG_COLUMNS])
            except (OSError, KeyError, pa.ArrowInvalid) as e:
                raise CorrectionError(f"cannot read the exposure log {path}: {e}") from e
            v = t["visit"].to_numpy()
            o = np.argsort(v)
            self._logs[day_obs] = (v[o], {c: _f64(t, c)[o] for c in GUARD_LOG_COLUMNS})
        return self._logs[day_obs]

    def window_shift(self, mjd_night):
        """The position pass's window widening [s] for obs_sbn times in
        [mjd_night, mjd_night + 1) (MJD): the largest |table visit midpoint
        - header midpoint| of the day_obs nights those times can fall in,
        plus SHUTTER_GRADIENT_MAX_S, and at least MIN_WINDOW_SHIFT_S."""
        from astropy.time import Time

        # day_obs is the date at UTC-12h: an MJD night spans two of them
        days = {int(Time(mjd_night + k, format="mjd").strftime("%Y%m%d")) for k in (-1, 0)}
        worst = 0.0
        for d in days:
            _, cols = self.night_log(d)
            dv = np.abs(cols["t_mid_visit_mjd_tai"] - cols["header_mid_mjd_tai"]) * 86400
            dv = dv[np.isfinite(dv)]
            if len(dv):
                worst = max(worst, float(dv.max()))
        return max(MIN_WINDOW_SHIFT_S, worst + SHUTTER_GRADIENT_MAX_S)

    def exposure_log(self, visits):
        """GUARD_LOG_COLUMNS of ``visits`` (in built nights) from their
        nights' exposure logs, as arrays aligned with ``visits`` (NaN where
        the visit is not logged)."""
        out = {c: np.full(len(visits), np.nan) for c in GUARD_LOG_COLUMNS}
        day = visits // 100000
        for d in np.unique(day).tolist():
            v, cols = self.night_log(d, required=True)
            if not len(v):
                continue
            sel = np.flatnonzero(day == d)
            i = np.minimum(np.searchsorted(v, visits[sel]), len(v) - 1)
            hit = v[i] == visits[sel]
            for c in GUARD_LOG_COLUMNS:
                out[c][sel[hit]] = cols[c][i[hit]]
        return out

    def lookup(self, cand, rows):
        """Look up ``rows`` of ``cand`` (not already looked up), in batches
        of whole nights."""
        from shutter_timing import CalibrationMismatchError, TableFormatError, TableIntegrityError
        from shutter_timing.corrections import corrected_midpoints

        self.extend(len(cand))
        rows = np.unique(rows)
        rows = rows[self.status[rows] == NOT_LOOKED_UP]
        if not len(rows):
            return
        missing = [c for c in ("visit", "detector", "x", "y") if c not in cand.column_names]
        if missing:
            raise CorrectionError(f"the view lacks the columns {missing} the shutter-motion correction needs")
        idx = pa.array(rows, pa.int64())
        visit, detector = (cand[c].take(idx) for c in ("visit", "detector"))
        if visit.null_count or detector.null_count:
            raise CorrectionError(f"{visit.null_count + detector.null_count} candidate(s) have a null visit "
                                  "or detector; cannot look up their corrected times")
        visit = visit.to_numpy()
        detector = detector.to_numpy()
        x, y = (_f64(cand, c)[rows] for c in ("x", "y"))     # null -> NaN -> OMITTED
        t_pipe = _f64(cand, "midpointMjdTai")[rows]

        # outside the table's coverage: never looked up. (A visit with a
        # non-integral number fails in corrected_midpoints, below.)
        day = np.floor_divide(visit, 100000)
        outside = day < self.first_night if self.first_night is not None else np.zeros(len(rows), bool)
        self.status[rows[outside]] = OUTSIDE_COVERAGE

        # whole nights per call, at most LOOKUP_BATCH_ROWS rows (unless one
        # night has more)
        inside = np.flatnonzero(~outside)
        order = inside[np.argsort(day[inside], kind="stable")]
        dsort = day[order]
        bounds, lo = [], 0
        for s in np.flatnonzero(dsort[1:] != dsort[:-1]) + 1:
            if s - lo >= LOOKUP_BATCH_ROWS:
                bounds.append((lo, s))
                lo = s
        if len(order):
            bounds.append((lo, len(order)))

        t0 = time.time()
        for lo, hi in bounds:
            sel = order[lo:hi]
            try:
                r = corrected_midpoints(visit[sel], detector[sel], x[sel], y[sel], table_dir=self.table_dir,
                                        require_uniform=True)
            except (CalibrationMismatchError, TableFormatError, TableIntegrityError, ValueError) as e:
                raise CorrectionError(f"the correction table {self.table_dir}: "
                                      f"{type(e).__name__}: {e}") from e
            self.calls += 1
            t, status = r.t_mid_mjd_tai.copy(), r.status.astype(np.uint8)
            dvis, dshift = np.full(len(sel), np.nan), np.full(len(sel), np.nan)
            if self.guard is not None:
                a = np.flatnonzero(np.isin(status, APPLIED))
                tp = t_pipe[sel][a]
                verdict, dvis[a], dshift[a] = self.guard(self.exposure_log(visit[sel][a]), tp, t[a])
                done = verdict == ALREADY_CORRECTED
                t[a[done]] = tp[done]                       # the time stands
                status[a[done]] = np.where(status[a[done]] == DEGRADED, ALREADY_CORRECTED_DEGRADED,
                                           ALREADY_CORRECTED)
                status[a[verdict == LARGE_SHIFT]] = LARGE_SHIFT      # applied, degraded
                status[a[verdict == TIME_MISMATCH]] = TIME_MISMATCH  # not applied
                t[a[verdict == TIME_MISMATCH]] = np.nan
            self.t[rows[sel]] = t
            self.dvis[rows[sel]] = dvis
            self.dshift[rows[sel]] = dshift
            self.status[rows[sel]] = status
            self.formats.add(int(r.table_format))
            if r.days:      # nights were read: their calibration and version
                self.calibrations.add(r.calibration_id)
                self.versions.add(r.package_version)
        self.seconds += time.time() - t0

    def provenance(self):
        """table_dir, table_format, calibration_id, package_version of the
        nights read (comma-joined if several; "unknown" if none), and the
        table's first night."""
        def join(s):
            return ",".join(sorted(s)) if s else "unknown"
        return dict(table_dir=str(self.table_dir), table_format=max(self.formats) if self.formats else None,
                    calibration_id=join(self.calibrations), package_version=join(self.versions),
                    first_night=self.first_night)

    def check(self, visits, status, dvis, dshift):
        """Warn about the NOT_BUILT, TIME_MISMATCH and LARGE_SHIFT visits of
        the output rows (at most WARN_VISITS named); fail if more than
        ``max_not_built_visits`` are NOT_BUILT. Returns the NOT_BUILT and
        TIME_MISMATCH visits, sorted."""
        visits, status, dvis, dshift = (np.asarray(a) for a in (visits, status, dvis, dshift))
        big = status == LARGE_SHIFT
        tl = np.unique(visits[big]).tolist()
        if tl:
            ds = {v: float(np.max(np.abs(dshift[big & (visits == v)]))) for v in tl}
            print(f"warning: {len(tl)} visit(s) whose correction moves the time by more than "
                  f"{MAX_CORRECTION_S:g} s (largest |shift| per visit); applied, marked degraded: "
                  f"{_some([f'{v} ({ds[v]:.3f} s)' for v in tl])}", file=sys.stderr, flush=True)
        nb = np.unique(visits[status == NOT_BUILT]).tolist()
        if len(nb) > self.max_not_built_visits:
            raise CorrectionError(
                f"{len(nb)} visits are not built in the correction table {self.table_dir} (more than "
                f"--max-not-built-visits {self.max_not_built_visits}): is the table build (ssp-daily's "
                f"shutter-timing-table --out {self.table_dir}) stale or failing? Nights affected: "
                f"{_some(sorted({v // 100000 for v in nb}))}")
        if nb:
            print(f"warning: {len(nb)} visit(s) not built in the correction table (their night has no "
                  f"table, or the visit is not in its exposure log, e.g. a raw that arrived late); "
                  f"they keep the visit time, flagged: {_some(nb)}", file=sys.stderr, flush=True)
        mm = status == TIME_MISMATCH
        tm = np.unique(visits[mm]).tolist()
        if tm:
            dv = {v: float(np.median(dvis[mm & (visits == v)])) for v in tm}
            print(f"warning: {len(tm)} visit(s) whose visit time differs from both the exposure log's "
                  f"header midpoint and the corrected time by more than {MAX_HEADER_MISMATCH_S * 1e3:g} ms "
                  f"(pipeline - header); not corrected, they keep the visit time, flagged: "
                  f"{_some([f'{v} ({dv[v] * 1e3:+.1f} ms)' for v in tm])}", file=sys.stderr, flush=True)
        return nb, tm


def _some(items, n=WARN_VISITS):
    """At most ``n`` items, then '... and N more'."""
    items = list(items)
    head = ", ".join(map(str, items[:n]))
    return head + (f", ... and {len(items) - n} more" if len(items) > n else "")


def shutter_columns(corr, cand, ci, info):
    """The corrected time and the SHUTTER_COLUMNS for winning candidates
    ``ci``: ``(midpointMjdTai, {name: array})``. ``midpointMjdTai`` is the
    corrected time where it is applied (OK or DEGRADED) or already in place
    (ALREADY_CORRECTED: the time as is), else the visit's, flagged. The
    degraded flag follows the correction's status."""
    t_visit = _f64(cand, "midpointMjdTai")[ci]
    t, status = corr.t[ci], corr.status[ci]
    assert not np.any(status == NOT_LOOKED_UP), "a winning candidate was never looked up"
    corrected = np.isin(status, TIME_STANDS)
    assert np.all(np.isfinite(t[corrected])) and np.all(np.isnan(t[~corrected]))
    cols = dict(
        midpointMjdTaiVisit=t_visit,
        midpointMjdTai_flag=~corrected,
        midpointMjdTai_flag_degraded=np.isin(status, DEGRADED_ALL),
        obstime_basis=pa.array(info["obstime_basis"], pa.string()),
        dt_corrected_ms=pa.array(info["dt_corrected_ms"], pa.float64(), from_pandas=True),
    )
    assert list(cols) == SHUTTER_COLUMNS
    return np.where(corrected, t, t_visit), cols


def shutter_report(corr, out, status, not_built, time_mismatch):
    """The manifest's SHUTTER_MANIFEST_FIELD entry, from the written rows."""
    basis = out["obstime_basis"].to_numpy(zero_copy_only=False)
    return {**corr.provenance(),
            "obstime_basis": {b: int(np.sum(basis == b)) for b in OBSTIME_BASES},
            "status": {name: int(np.isin(status, codes).sum()) for name, codes in STATUS_NAMES.items()},
            "not_built_visits": not_built,
            "time_mismatch_visits": time_mismatch}


def correct(corr, obs, oi, cand, ci):
    """Look up the corrected times of the candidates of pairs ``(oi, ci)``
    that pass the position test; returns ``corr.t`` (per cand row), or None
    without a correction table."""
    if corr is None:
        return None
    with np.errstate(invalid="ignore"):
        near = pair_sep(obs, oi, cand, ci) <= SEP_MAS
    corr.lookup(cand, ci[near])
    return corr.t


#
# Output
#


def build_output(obs, tbl, cand, rows, ci, match, info, corr=None):
    """The dia_sources table, one row per resolved obs_sbn row (of
    ``tbl``): the winning view rows (all columns, renamed), required
    columns null-filled, plus linkage and match diagnostics. ``match`` is
    "id"/"position" per row of ``rows``; ``matchMethod`` refines "id" to
    "obssubid_trail" for a merged -A/-B pair, else "obssubid" (see
    MATCH_METHODS). The -B row of a trail pair repeats its -A row's match.
    With ``corr`` (a looked-up Corrections), ``midpointMjdTai`` is the
    corrected time and SHUTTER_COLUMNS follow EXTRA_COLUMNS (see
    shutter_columns). Returns ``(table, is_b, src)``, ``src`` the cand row
    of each output row."""
    names = [RENAMES.get(c, c) for c in cand.column_names]
    added = set(EXTRA_COLUMNS) | (set(SHUTTER_COLUMNS) if corr is not None else set())
    clash = sorted({c for c in names if names.count(c) > 1} | (set(names) & added))
    if clash:
        raise ValueError(f"view columns {clash} clash (after renaming {RENAMES}) with each other "
                         f"or with the columns this tool adds; refusing to write duplicate names")

    rb = obs["row_b"][rows]
    match = np.asarray(match, dtype=object)
    method = np.where(match == "position", "position", np.where(rb >= 0, "obssubid_trail", "obssubid"))
    assert set(method) <= set(MATCH_METHODS)
    k = np.concatenate([np.arange(len(rows)), np.flatnonzero(rb >= 0)])
    trow = np.concatenate([obs["row"][rows], rb[rb >= 0]])
    is_b = np.arange(len(k)) >= len(rows)
    order = np.argsort(trow, kind="stable")
    k, trow, is_b = k[order], trow[order], is_b[order]

    out = cand.take(pa.array(ci[k], pa.int64())).rename_columns(names)
    for name, typ in REQUIRED_COLUMNS.items():
        if name not in out.column_names:
            out = out.append_column(name, pa.nulls(len(out), typ))
        elif out.schema.field(name).type != typ:
            out = out.set_column(out.column_names.index(name), name, out[name].cast(typ))
    ident = tbl.take(pa.array(trow, pa.int64()))
    extra = dict(
        obsid=ident["obsid"],
        obssubid=pc.utf8_trim_whitespace(ident["obssubid"]),
        # the submitted tracklet: (submission_id, trksub), and MPC's trkid
        **{c: ident[c] for c in ("submission_id", "trksub", "trkid")},
        primary=primary_flags(out["processing"], out["diaSourceId"], is_b, ident["submission_id"],
                              ident["obsid"]),
        match=pa.array(match[k], pa.string()),
        matchMethod=pa.array(method[k], pa.string()),
        sep_mas=info["sep_mas"][k], dt_ms=info["dt_ms"][k],
        dmag=pa.array(info["dmag"][k], pa.float64(), from_pandas=True),
        band_ok=info["band_ok"][k], n_pass=pa.array(np.asarray(info["n_pass"])[k], pa.int32()),
        ambiguous=info["ambiguous"][k],
    )
    assert list(extra) == EXTRA_COLUMNS
    if corr is not None:
        t, cols = shutter_columns(corr, cand, ci[k], {c: info[c][k] for c in ("obstime_basis",
                                                                                  "dt_corrected_ms")})
        out = out.set_column(out.column_names.index("midpointMjdTai"), "midpointMjdTai",
                             pa.array(t, pa.float64()))
        extra.update(cols)
    for name, col in extra.items():
        out = out.append_column(name, col if isinstance(col, (pa.Array, pa.ChunkedArray)) else pa.array(col))
    return out, is_b, ci[k]


def _codes(values):
    """Sortable integer codes for a string array (nulls last)."""
    return pc.rank(pc.fill_null(_arr(values, pa.string()), "\uffff"), tiebreaker="dense").to_numpy()


def primary_flags(processing, ids, is_b, submission_id, obsid):
    """True on exactly one row per (processing, diaSourceId).

    The same source is claimed by both rows of a trail pair, and can be
    claimed by several submissions of the same detection. The primary row
    is the -A row of a pair, else the row from the earliest submission
    (submission_id starts with its ISO timestamp), tie-broken on obsid.
    """
    code, ids = _codes(processing), np.asarray(ids)
    if len(ids) == 0:
        return np.zeros(0, dtype=bool)
    order = np.lexsort((_codes(obsid), _codes(submission_id), is_b, ids, code))
    first = np.r_[True, (code[order][1:] != code[order][:-1]) | (ids[order][1:] != ids[order][:-1])]
    primary = np.zeros(len(ids), dtype=bool)
    primary[order[first]] = True
    return primary


def unresolved_table(obs, tbl, rows, best):
    """The obs_sbn rows (of ``tbl``; both rows of a trail pair) that did
    not resolve, with the reason and the closest failing candidate's
    sep/dt, if there was one."""
    bsep = np.full(len(obs["id"]), np.nan)
    bdt = np.full(len(obs["id"]), np.nan)
    br, bs, bd = best
    bsep[br], bdt[br] = bs, bd
    rb = obs["row_b"][rows]
    rows = np.concatenate([rows, rows[rb >= 0]])
    trow = np.concatenate([obs["row"][rows[:len(rb)]], rb[rb >= 0]])
    out = tbl.take(pa.array(trow, pa.int64())).drop_columns(["stn"])
    out = out.append_column("reason", pa.array(obs["reason"][rows], pa.string()))
    out = out.append_column("best_sep_mas", pa.array(bsep[rows], from_pandas=True))
    return out.append_column("best_dt_ms", pa.array(bdt[rows], from_pandas=True))


def _counts(values):
    u, c = np.unique(np.asarray(values, dtype=str), return_counts=True)
    return ", ".join(f"{k}: {n:,}" for k, n in sorted(zip(u, c), key=lambda x: -x[1])) or "-"


def extract(obs_path, out_path, fetch, database=DEFAULT_DATABASE, chunk_size=250_000, correction_table=None,
            max_not_built_visits=MAX_NOT_BUILT_VISITS, report=None):
    """Resolve the X05 rows of ``obs_path`` against the view and write
    ``out_path`` and ``<stem>.unresolved.parquet``. ``fetch`` runs a list
    of query tasks (see ``run_queries``). Returns the exit code.

    ``correction_table``: the shutter-motion correction table directory
    (None: no correction, the uncorrected extract). With it, the extract
    raises CorrectionError (writing nothing) if the table cannot be used or
    more than ``max_not_built_visits`` of the written rows' visits are not
    built in it, and fills ``report`` (a dict, if given) with the
    manifest's SHUTTER_MANIFEST_FIELD entry."""
    timings = {}
    corr = Corrections(correction_table, max_not_built_visits) if correction_table is not None else None
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
    t_corr = correct(corr, obs, oi, cand, ci)
    (r_id, c_id, i_id), best = resolve(obs, oi, cand, ci, t_corr)
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
        results = fetch(position_queries(obs, rows, database,
                                         shift_s=corr.window_shift if corr is not None else 0.0))
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
        t_corr = correct(corr, obs, oi, cand, ci)
        (r_pos, c_pos, i_pos), best_pos = resolve(obs, oi, cand, ci, t_corr)
        # A row's best failing candidate from the id pass is the one to report.
        keep = ~np.isin(best_pos[0], best[0])
        best = tuple(np.concatenate([x, y[keep]]) for x, y in zip(best, best_pos))
    timings["position pass"] = time.time() - t0

    # Assemble
    t0 = time.time()
    rows_all = np.concatenate([r_id, r_pos])
    info = i_id if i_pos is None else {k: np.concatenate([i_id[k], i_pos[k]]) for k in i_id}
    match = np.array(["id"] * len(r_id) + ["position"] * len(r_pos), dtype=object)
    out, is_b, src = build_output(obs, tbl, cand, rows_all, np.concatenate([c_id, c_pos]), match, info, corr)
    if corr is not None:
        status = corr.status[src]
        not_built, time_mismatch = corr.check(cand["visit"].to_numpy()[src], status, corr.dvis[src],
                                              corr.dshift[src])
        shutter = shutter_report(corr, out, status, not_built, time_mismatch)
        if report is not None:
            report[SHUTTER_MANIFEST_FIELD[0]] = shutter
    # rows the DP2 rule decided, by the processing it picked
    won = cand["processing"].to_numpy(zero_copy_only=False)[np.concatenate([c_id, c_pos])]
    dp2_won = won[info["dp2_rule"]]

    # obsid is the key; each source has exactly one primary row
    key = ["processing", "diaSourceId"]
    assert pc.count_distinct(out["obsid"]).as_py() == len(out)
    g = out.select(key + ["primary"]).group_by(key).aggregate([("primary", "sum")])
    assert len(g) == 0 or pc.all(pc.equal(g["primary_sum"], 1)).as_py()
    n_sources = len(g)
    # sources claimed by more than one submission (not counting -B rows)
    nb = out.select(key + ["primary", "obsid", "submission_id", "trksub"]).filter(pa.array(~is_b))
    g = nb.group_by(key).aggregate([("obsid", "count")])
    claimed = nb.join(g.filter(pc.greater(g["obsid_count"], 1)).select(key), key, join_type="inner")

    resolved = np.zeros(n, dtype=bool)
    resolved[rows_all] = True
    unres = unresolved_table(obs, tbl, np.flatnonzero(~resolved), best)
    stem = str(out_path)[:-8] if str(out_path).endswith(".parquet") else str(out_path)
    pq.write_table(out, out_path, compression="zstd")
    pq.write_table(unres, f"{stem}.unresolved.parquet", compression="zstd")
    timings["assemble + write"] = time.time() - t0

    print(f"\nobs_sbn X05 rows read:            {n_x05:,}")
    print(f"A/B trail pairs merged:           {n_pairs:,}  -> {n:,} logical rows")
    print(f"obsSubIDs used more than once:    {n_reused:,}")
    print(f"resolved by id:                   {len(r_id):,}")
    print(f"resolved by position:             {len(r_pos):,}  (id-pass reason: {_counts(reason[r_pos])})")
    print(f"per matchMethod:                  {_counts(out['matchMethod'].to_numpy(False))}")
    print(f"unresolved:                       {len(unres):,}  ({_counts(unres['reason'].to_numpy(False))})")
    print(f"rows written:                     {len(out):,}, for {n_sources:,} distinct sources")
    n_b = int(is_b.sum())
    print(f"non-primary rows:                 {len(out) - n_sources:,}  (trail -B endpoints: {n_b:,}, "
          f"further submissions of a detection: {len(out) - n_sources - n_b:,})")
    claimed = claimed.sort_by([("processing", "ascending"), ("diaSourceId", "ascending"),
                               ("primary", "descending")])
    for r in claimed.to_pylist():
        print("   claimed by several submissions:", r)
    print(f"ambiguous:                        {pc.sum(out['ambiguous']).as_py() or 0:,}")
    print(f"DP2-DS vs pDP2-DS, by date:       {len(dp2_won):,}  ({_counts(dp2_won)}; "
          f"pDP2-DS if submitted before {DP2_CUTOFF})")
    print(f"band_ok = false:                  {pc.sum(pc.invert(out['band_ok'])).as_py() or 0:,}")
    print(f"per processing:                   {_counts(out['processing'].to_numpy(False))}")
    if corr is not None:
        print(f"shutter correction:               {shutter['table_dir']} (format {shutter['table_format']}, "
              f"calibration {shutter['calibration_id']}, built by {shutter['package_version']})")
        print(f"  correction status:              {shutter['status']}")
        print(f"  obstime basis:                  {shutter['obstime_basis']}")
        print(f"  not built visits:               {len(not_built):,}")
        timings["correction lookup"] = corr.seconds
    print(f"wrote {out_path} and {stem}.unresolved.parquet")
    for k, v in timings.items():
        print(f"  {k:20s} {v:8.1f} s")
    return 0


def add_correction_args(parser):
    """--correction-table and --max-not-built-visits (shared with
    ssp-extract-sso-inputs)."""
    parser.add_argument("--correction-table", default=DEFAULT_CORRECTION_TABLE, metavar="DIR",
                        help="The shutter-motion correction table (shutter-timing-table's --out); "
                             "'none' for no correction (default: %(default)s)")
    parser.add_argument("--max-not-built-visits", type=int, default=MAX_NOT_BUILT_VISITS, metavar="N",
                        help="Fail if more than N visits of the extracted rows are not built in the "
                             "correction table (default: %(default)s)")


def correction_table(args):
    """The --correction-table directory, or None for 'none'."""
    return None if args.correction_table.strip().lower() in ("", "none") else args.correction_table


def main():
    parser = argparse.ArgumentParser(
        description="Build dia_sources.parquet for the X05 rows of an MPC obs_sbn dump "
                    "from ssp.SubmittableSources",
        epilog="Credentials: SSP_CH_USER/SSP_CH_PASSWORD if set, else ~/.chpass "
               "(pgpass format, mode 0600).",
    )
    parser.add_argument("obs_sbn", help="MPC obs_sbn Parquet file")
    parser.add_argument("output", help="Output dia_sources Parquet file")
    parser.add_argument("--host", default=None,
                        help=f"ClickHouse host (default: from {HOST_FILE}, else {FALLBACK_HOST})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help="ClickHouse HTTP port (default: %(default)s)")
    parser.add_argument("--database", default=DEFAULT_DATABASE,
                        help="ClickHouse database (default: %(default)s)")
    parser.add_argument("--user", default=None, help="ClickHouse user (default: from the credentials)")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS,
                        help=f"Concurrent queries, at most {MAX_WORKERS} (default: %(default)s)")
    parser.add_argument("--chunk-size", type=int, default=250_000,
                        help="Ids per query (default: %(default)s)")
    add_correction_args(parser)
    args = parser.parse_args()
    if not 1 <= args.workers <= MAX_WORKERS:
        parser.error(f"--workers must be between 1 and {MAX_WORKERS} (the server is shared)")

    args.host = args.host or current_host()
    t0 = time.time()
    def fetch(tasks):
        return run_queries(tasks, args.host, args.port, args.database, args.user, args.workers)

    try:
        rc = extract(args.obs_sbn, args.output, fetch, database=args.database, chunk_size=args.chunk_size,
                     correction_table=correction_table(args), max_not_built_visits=args.max_not_built_visits)
    except CorrectionError as e:
        print(f"extract-submitted-sources: error: shutter-motion correction: {e}; nothing written",
              file=sys.stderr)
        sys.exit(1)
    print(f"total wall time: {time.time() - t0:.1f} s")
    sys.exit(rc)


if __name__ == "__main__":
    main()
