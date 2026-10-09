"""Validation harness for the NearbySSO builder (WP5 of
docs/design/nearbysso.md, "Validation").

Black-box checks of the builder's *outputs* (``nearbysso.parquet``, of
``_contract.NEARBYSSO_DTYPE``) against its inputs (the DiaSource Parquet,
``mpc_orbits``) and independent references (SSObservation, JPL Horizons, the
JPL SBDB). It is written from the design and the contract only: the orbit
filter, the visits, the brute-force prefilter and the matching are
re-implemented here, and the builder's code is called only through the
contract (``propagate.coarse`` for sigma), so a shared bug is unlikely.

Match radius: per object, the contract's ``match_radius(designation)``
(5", and 15" for comets and ISOs, designations C/ P/ D/ I/;
docs/design/comet-radius.md). Every "within the radius" decision below
(the brute-force matcher, the explanation of misses, the rank check's
pool, the mock) uses the radius of the object concerned; the competition
between objects for a DiaSource compares separations in arcsec.

Subcommands (each writes ``<out>/<name>.txt`` and ``<out>/<name>.parquet``)::

  same-orbits          NearbySSO vs SSObservation built from the SAME
                       mpc_orbits
  dp2-intersection     vs DP2 SSSource (other orbits/cuts); reported only
  horizons-adjudicate  which side of each discrepancy matches Horizons
  horizons-positions   stratified Horizons spot check, < 1 mas RMS gate
  horizons-sigma       our ellipse (SBDB orbit+covariance) vs Horizons 3-sigma
  brute-force          coarse-pass safety: all orbits, exact ephemerides
  rank                 diaDistanceRank recomputed by brute force from the
                       DiaSources of each sampled row's visit
  mock-nearbysso       (development) a NearbySSO file faked from SSObservation,
                       with injected faults, to exercise the checks
  mock-rank            (development) a NearbySSO file with diaDistanceRank
                       added by brute force, optionally with faults

Exit status: 0 pass (or report-only), 1 a gate failed, 3 incomplete (e.g.
sigma unknown because propagate.coarse isn't implemented yet).

HORIZONS / SBDB ETIQUETTE: every request goes through one `Polite`
instance: strictly serial, >= --min-interval s (default 1.5) apart, <= 60
epochs per request, and at most --max-queries requests per run. Never run
these subcommands in parallel or from the test suite.

Example::

  python -m bench.nearbysso_validate same-orbits \\
      --nearbysso nearbysso.parquet --ssobservation ssobservation.parquet \\
      --dia dia.parquet --orbits mpc_orbits.parquet --out report/
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import warnings

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from astropy.time import Time
import astropy.units as u

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from bench.ephem_bench import horizons_observer, horizons_request  # noqa: E402
from ssp import util  # noqa: E402
from ssp import ssobservation_parts as SP  # noqa: E402
from ssp.ephem_assist import (  # noqa: E402
    ASSIST_SUN,
    C_AU_PER_DAY,
    GM_SUN,
    MJD_J2000,
    cometary_to_helio_ecliptic,
    compute_ephemerides_one,
    ecliptic_to_equatorial,
    elements_row_to_bary_icrf,
    open_ephem,
)
from ssp.nearbysso import _contract as C  # noqa: E402
from bench import time_shift as TS  # noqa: E402

#: The default (non-comet) match radius; per object, see `radius_of`.
RADIUS = C.MATCH_RADIUS_ARCSEC
#: A NearbySSO row whose ephOffset exceeds its object's match radius by more
#: than this [arcsec] (the float32 ephOffset's resolution) is wrong.
OFFSET_EPS = 1e-4
SIGMA_MAX = C.SIGMA_MAX_ARCSEC
NSS_COLUMNS = list(C.NEARBYSSO_DTYPE.names)
EPH_COMPARED = ["ephRa", "ephDec", "ephRateRa", "ephRateDec", "ephVmag"]
AU_DAY_TO_KM_S = (1.0 * u.au / u.day).to_value(u.km / u.s)

#: mpc_orbits columns read (renamed: designation, packed)
ORBIT_COLUMNS = [
    "unpacked_primary_provisional_designation", "packed_primary_provisional_designation",
    "q", "e", "i", "node", "argperi", "peri_time", "epoch_mjd", "h", "g", "normalized_rms",
]
ELEMENTS = ["q", "e", "i", "node", "argperi", "peri_time"]

#: Default tolerances for "identical" eph* values (same-orbits). Identical
#: code and inputs should agree bitwise; these allow for integrator step
#: choices that depend on the set of requested times (numerical noise).
TOL = dict(pos_mas=0.1, rate_deg_day=1e-7, vmag=1e-4)
#: SSObservation columns read, where present, for the time-shift allowance
#: (bench/time_shift.py): SSObservation predicts at its (shutter-corrected)
#: midpointMjdTai, read as ``sss_midpointMjdTai``, NearbySSO at the
#: DiaSource's. Where the two times are equal the tolerances are TOL's.
SHIFT_COLUMNS = ["topoRange", "topoRangeRate", "helioRange", "helioRangeRate"]

#: Objects whose NearbySSO ephemerides are, by design, not from their own
#: mpc_orbits elements, so a Horizons check "with our own elements" can't
#: gate them: reported separately (docs/design/nearbysso.md, "ASSIST's own
#: perturbers", owner decision 2026-09-28).
KNOWN_EXCEPTIONS = {
    "1930 BM": "Pluto: DE440 positions (ASSIST body 10), not its MPC orbit",
}

#: Statuses that fail the same-orbits gate.
FAIL_STATUSES = ("value_mismatch", "wrong_nearest", "unexplained")


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------

class Report:
    """Collects the text report; writes <out>/<name>.txt and tables."""

    def __init__(self, name, out_dir):
        self.name, self.out_dir, self.lines = name, out_dir, []
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)

    def __call__(self, *args):
        line = " ".join(str(a) for a in args)
        print(line, flush=True)
        self.lines.append(line)

    def write(self, table=None, **extra_tables):
        if not self.out_dir:
            return
        with open(os.path.join(self.out_dir, f"{self.name}.txt"), "w") as fh:
            fh.write("\n".join(self.lines) + "\n")
        tables = dict(extra_tables)
        if table is not None:
            tables[""] = table
        for suffix, df in tables.items():
            if df is None:
                continue
            fn = f"{self.name}{'.' + suffix if suffix else ''}.parquet"
            _to_parquet(df, os.path.join(self.out_dir, fn))
        print(f"wrote {self.out_dir}/{self.name}.txt (+ .parquet)")


def _to_parquet(df, path):
    df = df.copy()
    for c in df.columns:     # vector columns (e.g. unit vectors) as lists
        if df[c].dtype == object and len(df) and isinstance(df[c].iloc[0], np.ndarray):
            df[c] = df[c].map(list)
    df.to_parquet(path, index=False)


def _stats(x, unit=""):
    x = np.asarray(x, dtype=np.float64)
    x = x[np.isfinite(x)]
    if len(x) == 0:
        return "n=0"
    return (f"n={len(x)} median={np.median(x):.3g} rms={np.sqrt(np.mean(x ** 2)):.3g} "
            f"max|.|={np.max(np.abs(x)):.3g}{(' ' + unit) if unit else ''}")


# ---------------------------------------------------------------------------
# Horizons / SBDB etiquette
# ---------------------------------------------------------------------------

class QueryBudgetExceededError(RuntimeError):
    pass


class Polite:
    """The single gate for every JPL request: serial, paced, budgeted.

    ``call(fn, *args)`` sleeps until ``min_interval`` seconds after the end
    of the previous request, refuses once ``max_queries`` have been made,
    and appends a line per request to ``log_path`` (if given).
    """

    def __init__(self, min_interval=1.5, max_queries=20, log_path=None, sleep=time.sleep,
                 clock=time.monotonic):
        assert min_interval >= 1.0, "JPL etiquette: at least 1 s between requests"
        self.min_interval, self.max_queries, self.log_path = min_interval, max_queries, log_path
        self.n, self._last, self._sleep, self._clock = 0, None, sleep, clock

    def call(self, what, fn, *args, **kwargs):
        if self.n >= self.max_queries:
            raise QueryBudgetExceededError(f"query budget of {self.max_queries} exhausted")
        if self._last is not None:
            wait = self.min_interval - (self._clock() - self._last)
            if wait > 0:
                self._sleep(wait)
        self.n += 1
        ok = "ok"
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            ok = f"error: {exc}"[:200]
            raise
        finally:
            self._last = self._clock()
            if self.log_path:
                with open(self.log_path, "a") as fh:
                    fh.write(f"{time.strftime('%Y-%m-%dT%H:%M:%S')}\t{what}\t{ok}\n")


def chunked_unique_times(mjd, max_per=60):
    """Unique sorted times and the chunks (<= max_per) to request them in.
    Returns (unique, inverse, list of index slices into unique)."""
    uniq, inv = np.unique(np.asarray(mjd, dtype=np.float64), return_inverse=True)
    chunks = [slice(s, min(s + max_per, len(uniq))) for s in range(0, len(uniq), max_per)]
    return uniq, inv, chunks


def horizons_by_times(fetch, mjd_tai, polite, what, max_per=60):
    """Call ``fetch(Time) -> columns`` on <= max_per unique sorted epochs at
    a time (through ``polite``) and return the columns aligned to mjd_tai."""
    uniq, inv, chunks = chunked_unique_times(mjd_tai, max_per)
    parts = []
    for sl in chunks:
        t = Time(uniq[sl], format="mjd", scale="tai")
        parts.append(polite.call(what, fetch, t))
    keys = set.intersection(*[set(p) for p in parts]) if parts else set()
    return {k: np.concatenate([p[k] for p in parts])[inv] for k in keys}


def _col(cols, *prefixes):
    for p in prefixes:
        for k in cols:
            if k.startswith(p):
                return cols[k]
    raise KeyError(f"none of {prefixes} in Horizons columns {sorted(cols)}")


def horizons_own_elements(row, mjd_tai, polite, quantities="1", with_hg=False):
    """Horizons observer quantities for ``row``'s own elements (X05), via
    bench.ephem_bench.horizons_observer, aligned to mjd_tai (TAI MJD)."""
    extra = {"H": f"{float(row['h']):.3f}", "G": f"{float(row['g']):.3f}"} if with_hg else None

    def fetch(t):
        cols, _ = horizons_observer(row, t, quantities, C.OBSCODE, extra=extra)
        return cols
    return horizons_by_times(fetch, mjd_tai, polite, f"horizons own-elements {row['designation']}")


def horizons_jpl_orbit(command, mjd_tai, polite, quantities="1,36,37"):
    """Horizons observer quantities for JPL's own orbit of ``command``."""
    def fetch(t):
        params = {
            "format": "text", "COMMAND": command, "OBJ_DATA": "NO", "MAKE_EPHEM": "YES",
            "EPHEM_TYPE": "OBSERVER", "CENTER": f"'{C.OBSCODE}'",
            "TLIST": ",".join(f"{x:.10f}" for x in t.utc.jd), "TIME_TYPE": "UT",
            "QUANTITIES": f"'{quantities}'", "ANG_FORMAT": "DEG", "extra_prec": "YES",
            "CSV_FORMAT": "YES", "REF_SYSTEM": "ICRF",
        }
        cols, _ = horizons_request(params, len(t))
        return cols
    return horizons_by_times(fetch, mjd_tai, polite, f"horizons jpl-orbit {command}")


SBDB_URL = "https://ssd-api.jpl.nasa.gov/sbdb.api"


def sbdb_query(sstr, polite):
    """One SBDB API request: elements, covariance (cov=mat), full precision."""
    def fetch():
        url = SBDB_URL + "?" + urllib.parse.urlencode(
            {"sstr": sstr, "cov": "mat", "full-prec": "1", "phys-par": "1"})
        req = urllib.request.Request(url, headers={"User-Agent": "ssp-tools-bench/0.1"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8"))
    return polite.call(f"sbdb {sstr}", fetch)


# ---------------------------------------------------------------------------
# Orbits: reading, the filter (re-implemented from the design), classes
# ---------------------------------------------------------------------------

#: The JSON orbit_fit_statistics.arc_length_total texts NearbySSO
#: excludes (lsst-gen-ephemcache's get-mpcorb.py: NOT IN these).
SHORT_ARC_TEXTS = ("0 days", "1 days", "2 days")

# "arc_length_total": followed by a JSON string or a bare value (a number,
# or null). The key occurs at most once per mpc_orb_jsonb (only in
# orbit_fit_statistics; checked on the 2026-09-26 snapshot).
_ARC_RE = r'"arc_length_total"\s*:\s*(?:"(?P<s>[^"]*)"|(?P<o>[^,}\s]+))'


def arc_text_from_json(json_col):
    """orbit_fit_statistics.arc_length_total as text, as SQL's ->> gives
    it (so a JSON number 0 is '0'), None where it is null or absent, for an
    array of mpc_orb_jsonb strings. Vectorized with a regex (json.loads
    of 1.5M documents is slow); the tests check it against json.loads."""
    arr = json_col
    if isinstance(arr, pa.ChunkedArray):   # chunk by chunk (a combined array can overflow)
        parts = [arc_text_from_json(c) for c in arr.chunks]
        return np.concatenate(parts) if parts else np.zeros(0, dtype=object)
    if not isinstance(arr, pa.Array):
        arr = pa.array([x if isinstance(x, str) else None for x in arr], type=pa.string())
    m = pc.extract_regex(arr, _ARC_RE)
    quoted = pc.struct_field(m, "s").to_numpy(zero_copy_only=False).astype(object)
    other = pc.struct_field(m, "o").to_numpy(zero_copy_only=False).astype(object)
    out = np.where(pd.isna(other) | (other == ""), quoted, other).astype(object)
    out[pd.isna(out) | (out == "null")] = None
    out[~pc.is_valid(m).to_numpy(zero_copy_only=False)] = None
    return out


def arc_days(text):
    """The day count of arc texts of the form 'N days' (float), inf for
    anything else (a year range such as '2014-2024' is a long arc), NaN
    where unknown (None)."""
    out = np.full(len(text), np.inf)
    for k, t in enumerate(text):
        if t is None or (isinstance(t, float) and np.isnan(t)):
            out[k] = np.nan
            continue
        parts = str(t).split()
        if len(parts) == 2 and parts[1] in ("day", "days"):
            try:
                out[k] = float(parts[0])
            except ValueError:
                pass
    return out


def read_orbits(path, designations=None, with_json=False):
    """mpc_orbits columns as a DataFrame with ``designation``, ``packed``
    and ``arc_text`` (see `arc_text_from_json`), optionally only the given
    designations, optionally keeping mpc_orb_jsonb. Reads row group by row
    group, so the JSON of the whole catalog is never in memory at once."""
    present = set(pq.read_schema(path).names)
    cols = [c for c in ORBIT_COLUMNS + ["mpc_orb_jsonb"] if c in present]
    if designations is not None:
        want = sorted(set(designations))
        if not want:
            return _empty_orbits(with_json)
        tables = [pq.read_table(path, columns=cols,
                                filters=[("unpacked_primary_provisional_designation", "in", want)])]
    else:
        pf = pq.ParquetFile(path)
        tables = (pf.read_row_group(k, columns=cols) for k in range(pf.num_row_groups))
    parts = []
    for t in tables:
        if "mpc_orb_jsonb" in t.column_names:
            arc = arc_text_from_json(t.column("mpc_orb_jsonb"))
            if not with_json:
                t = t.drop_columns(["mpc_orb_jsonb"])
        else:
            arc = np.full(t.num_rows, None, dtype=object)
        df = t.to_pandas()
        df["arc_text"] = arc
        parts.append(df)
    df = pd.concat(parts, ignore_index=True)
    df = df.rename(columns={"unpacked_primary_provisional_designation": "designation",
                            "packed_primary_provisional_designation": "packed"})
    return df.sort_values("designation", kind="stable").reset_index(drop=True)


def _empty_orbits(with_json):
    cols = ["designation", "packed"] + ORBIT_COLUMNS[2:] + ["arc_text"] + (
        ["mpc_orb_jsonb"] if with_json else [])
    return pd.DataFrame({c: [] for c in cols})


def filter_reason(orbits):
    """Why each orbit is excluded by NearbySSO's rules ('' if kept): the
    first of 'satellite' (a natural satellite: the designation starts with
    'S/'; comets, C/ P/ D/ I/ and A/, are kept since 2026-10-01,
    docs/design/nongrav.md), 'missing_elements' (any of q, e, i, node,
    argperi, peri_time NaN) and 'short_arc' (the JSON arc text is one of
    SHORT_ARC_TEXTS) and 'null_arc' (the arc is null or absent:
    get-mpcorb.py's SQL, ``WHERE NOT ...->>'arc_length_total' IN (...)``,
    is NULL there, so the row is dropped). ``orbits`` needs ``arc_text``
    (from `read_orbits`) or ``mpc_orb_jsonb``."""
    des = orbits["designation"].astype(str).to_numpy()
    satellite = np.char.startswith(des.astype(str), "S/")
    missing = np.zeros(len(orbits), bool)
    for c in ELEMENTS:
        missing |= ~np.isfinite(orbits[c].to_numpy(dtype=np.float64))
    if "arc_text" in orbits:
        arc = orbits["arc_text"].to_numpy(dtype=object)
    else:
        arc = arc_text_from_json(orbits["mpc_orb_jsonb"])
    null = np.array([not isinstance(a, str) for a in arc], dtype=bool)
    short = np.array([isinstance(a, str) and a in SHORT_ARC_TEXTS for a in arc], dtype=bool)
    return np.select([satellite, missing, short, null],
                     ["satellite", "missing_elements", "short_arc", "null_arc"], "")


def radius_of(designations, radius=None):
    """The match radius [arcsec] of each designation, as a float64 array:
    the contract's ``match_radius`` per object, or, if ``radius`` is given
    (a number), that radius for all (an override for ad-hoc studies)."""
    des = np.asarray(designations, dtype=object)
    if radius is not None:
        return np.full(des.shape, float(radius))
    if des.size == 0:
        return np.zeros(des.shape)
    return np.asarray(C.match_radius(np.array([str(d) for d in des.ravel()])),
                      dtype=np.float64).reshape(des.shape)


def reason_lookup(orbits):
    """designation -> filter reason (a dict); unknown objects are
    reported as 'not_in_orbits' by `reasons_for`."""
    return dict(zip(orbits["designation"].to_numpy(), filter_reason(orbits)))


def reasons_for(designations, lookup):
    return np.array([lookup.get(d, "not_in_orbits") for d in designations], dtype=object)


def dynamical_class(q, e, i=None):
    """Coarse dynamical class from (q, e): 'neo' (q < 1.3), 'main_belt'
    (2.0 < a < 3.3), 'trojan' (5.05 < a < 5.35), 'tno' (a > 30), 'other'."""
    q, e = np.asarray(q, float), np.asarray(e, float)
    with np.errstate(divide="ignore", invalid="ignore"):
        a = np.where(e < 1, q / (1 - e), np.inf)
    return np.select(
        [q < 1.3, (a > 2.0) & (a < 3.3), (a > 5.05) & (a < 5.35), a > 30.0],
        ["neo", "main_belt", "trojan", "tno"], "other")


# ---------------------------------------------------------------------------
# Time, observer
# ---------------------------------------------------------------------------

def tai_to_assist(mjd_tai):
    return Time(np.asarray(mjd_tai, dtype=np.float64), format="mjd", scale="tai").tdb.mjd - MJD_J2000


def tt_to_assist(mjd_tt):
    return Time(np.asarray(mjd_tt, dtype=np.float64), format="mjd", scale="tt").tdb.mjd - MJD_J2000


def observer_states(mjd_tai):
    """X05 barycentric ICRF (pos [AU], vel [km/s]), each (N, 3)."""
    t = Time(np.atleast_1d(np.asarray(mjd_tai, dtype=np.float64)), format="mjd", scale="tai")
    r, v = util.observatory_barycentric_posvel(C.OBSCODE, t)
    return r.to_value(u.au).T.copy(), v.to_value(u.km / u.s).T.copy()


def radec_to_vec(ra, dec):
    ra, dec = np.deg2rad(np.asarray(ra, float)), np.deg2rad(np.asarray(dec, float))
    return np.stack([np.cos(dec) * np.cos(ra), np.cos(dec) * np.sin(ra), np.sin(dec)], axis=-1)


def vec_to_radec(v):
    v = np.asarray(v, float)
    ra = np.degrees(np.arctan2(v[..., 1], v[..., 0])) % 360.0
    dec = np.degrees(np.arcsin(np.clip(v[..., 2] / np.linalg.norm(v, axis=-1), -1, 1)))
    return ra, dec


# ---------------------------------------------------------------------------
# Covariances: MPC CAR block, cometary -> Cartesian Jacobian, ORBIT_DTYPE
# ---------------------------------------------------------------------------

#: Ecliptic (J2000, IAU76 obliquity, as ssp.ephem_assist) -> equatorial.
R_ECL2EQ = np.stack([ecliptic_to_equatorial(np.eye(3)[:, k]) for k in range(3)], axis=1)
R6_ECL2EQ = np.kron(np.eye(2), R_ECL2EQ)


def car_cov6(orb_json):
    """The 6x6 state block of the ``CAR`` covariance in an mpc_orb_jsonb
    string (heliocentric ecliptic, AU, AU/day), or None if absent."""
    try:
        cov = (json.loads(orb_json) if isinstance(orb_json, str) else orb_json)["CAR"]["covariance"]
    except (TypeError, KeyError, ValueError):
        return None
    out = np.empty((6, 6))
    for i in range(6):
        for j in range(i, 6):
            v = cov.get(f"cov{i}{j}")
            if v is None:
                return None
            out[i, j] = out[j, i] = float(v)
    return out


def is_pd(cov):
    try:
        np.linalg.cholesky(cov)
        return True
    except np.linalg.LinAlgError:
        return False


def orbit_record(elems, cov_eq, ephem):
    """One ORBIT_DTYPE row from mpc_orbits-style elements (a mapping with
    designation, packed, q, e, i, node, argperi, peri_time, epoch_mjd [TT],
    h, g, and optionally normalized_rms) and a 6x6 equatorial covariance of
    the heliocentric state at epoch (or None)."""
    rec = np.zeros((), dtype=C.ORBIT_DTYPE)
    for f in ("designation", "packed"):
        rec[f] = str(elems.get(f, "") or "")
    for f in ("q", "e", "i", "node", "argperi", "peri_time", "epoch_mjd", "h", "g"):
        rec[f] = float(elems[f]) if elems.get(f) is not None else np.nan
    rec["normalized_rms"] = float(elems.get("normalized_rms", np.nan) or np.nan)
    rec["epoch"] = float(tt_to_assist(rec["epoch_mjd"]))
    sun = ephem.get_particle(ASSIST_SUN, float(rec["epoch"]))
    X, V = elements_row_to_bary_icrf(elems, np.array([sun.x, sun.y, sun.z]),
                                     np.array([sun.vx, sun.vy, sun.vz]))
    rec["state0"] = np.concatenate([X, V])
    ok = cov_eq is not None and np.all(np.isfinite(cov_eq)) and is_pd(cov_eq)
    rec["cov0"] = cov_eq if ok else np.full((6, 6), np.nan)
    rec["has_cov"] = ok
    return rec


def orbit_records_from_mpc(orbits, ephem):
    """ORBIT_DTYPE rows for mpc_orbits rows read with_json=True, the
    covariance from the CAR block rotated ecliptic -> equatorial (the
    helio -> bary translation leaves it unchanged)."""
    out = np.zeros(len(orbits), dtype=C.ORBIT_DTYPE)
    for k, row in enumerate(orbits.to_dict("records")):
        cov = car_cov6(row.get("mpc_orb_jsonb"))
        cov_eq = None if cov is None else R6_ECL2EQ @ cov @ R6_ECL2EQ.T
        out[k] = orbit_record(row, cov_eq, ephem)
    return out


#: The JPL SBDB covariance's element order (cometary).
COMETARY_LABELS = ("e", "q", "tp", "node", "peri", "i")
#: Central-difference steps: e, q [relative], tp [day], node/peri/i [deg].
_FD_STEPS = np.array([1e-7, 1e-7, 1e-4, 1e-6, 1e-6, 1e-6])


def cometary_to_state_eq(el, epoch_tt_mjd):
    """Heliocentric equatorial (ICRF-aligned) state (6,) [AU, AU/day] from
    cometary elements el = (e, q [AU], tp [TT MJD], node, peri, i [deg]),
    via ssp.ephem_assist.cometary_to_helio_ecliptic (the conversion our
    pipeline uses)."""
    e, q, tp, node, peri, inc = (float(x) for x in el)
    X, V = cometary_to_helio_ecliptic(q, e, np.deg2rad(inc), np.deg2rad(node), np.deg2rad(peri),
                                      float(epoch_tt_mjd) - tp)
    return np.concatenate([ecliptic_to_equatorial(X), ecliptic_to_equatorial(V)])


def cometary_jacobian(el, epoch_tt_mjd, steps=None):
    """d state_eq / d (e, q, tp, node, peri, i), (6, 6), by central
    differences (angles per degree, tp per day)."""
    el = np.asarray(el, dtype=np.float64)
    h = np.array(_FD_STEPS if steps is None else steps, dtype=np.float64).copy()
    h[1] *= el[1]
    J = np.empty((6, 6))
    for k in range(6):
        d = np.zeros(6)
        d[k] = h[k]
        J[:, k] = (cometary_to_state_eq(el + d, epoch_tt_mjd)
                   - cometary_to_state_eq(el - d, epoch_tt_mjd)) / (2 * h[k])
    return J


def cometary_cov_to_state_cov(el, cov_el, epoch_tt_mjd):
    """The equatorial state covariance J C J^T for a covariance of the
    cometary elements (in COMETARY_LABELS order and units)."""
    J = cometary_jacobian(el, epoch_tt_mjd)
    out = J @ np.asarray(cov_el, float) @ J.T
    return 0.5 * (out + out.T)


def _jd_tdb_to_mjd_tt(jd_tdb):
    return Time(float(jd_tdb), format="jd", scale="tdb").tt.mjd


def sbdb_orbit(js):
    """Parse an SBDB response into (elements dict, 6x6 cometary covariance
    or None, info dict). The elements are the covariance's own (its epoch
    may differ from the orbit's), in mpc_orbits conventions (TT MJD)."""
    obj, orb = js.get("object", {}), js.get("orbit", {})
    info = {"des": obj.get("des"), "fullname": obj.get("fullname"), "spkid": obj.get("spkid"),
            "orbit_id": orb.get("orbit_id"), "kind": obj.get("kind"), "note": ""}
    cov = orb.get("covariance")
    phys = {p["name"]: p.get("value") for p in js.get("phys_par", []) or []}
    h = float(phys["H"]) if phys.get("H") not in (None, "") else np.nan
    g = float(phys["G"]) if phys.get("G") not in (None, "") else 0.15
    if cov:
        labels = [str(x) for x in cov["labels"]]
        els = {e["name"]: float(e["value"]) for e in cov["elements"]}
        epoch_jd = float(cov["epoch"])
        mat = np.array(cov["data"], dtype=np.float64)
        if tuple(labels[:6]) != COMETARY_LABELS:
            raise ValueError(f"unexpected covariance labels {labels}")
        if len(labels) > 6:
            info["note"] = f"non-grav covariance {labels[6:]}; using the 6x6 block"
        mat = mat[:6, :6]
    else:
        els = {e["name"]: float(e["value"]) for e in orb["elements"]}
        epoch_jd, mat = float(orb["epoch"]), None
        info["note"] = "no covariance"
    elems = {
        "designation": info["des"], "packed": "", "e": els["e"], "q": els["q"], "i": els["i"],
        "node": els["om"], "argperi": els["w"], "peri_time": _jd_tdb_to_mjd_tt(els["tp"]),
        "epoch_mjd": _jd_tdb_to_mjd_tt(epoch_jd), "h": h, "g": g,
    }
    return elems, mat, info


def sbdb_orbit_record(js, ephem):
    """ORBIT_DTYPE row (with the state covariance from the SBDB cometary
    covariance through `cometary_cov_to_state_cov`) and info."""
    elems, mat, info = sbdb_orbit(js)
    cov_eq = None
    if mat is not None:
        el = [elems["e"], elems["q"], elems["peri_time"], elems["node"], elems["argperi"], elems["i"]]
        cov_eq = cometary_cov_to_state_cov(el, mat, elems["epoch_mjd"])
    return orbit_record(elems, cov_eq, ephem), info


# ---------------------------------------------------------------------------
# Sigma, black-box through propagate.coarse
# ---------------------------------------------------------------------------

def ellipse_axes(ra_err, dec_err, cov):
    """(semi-major, semi-minor, position angle of the major axis [deg, east
    of north, in [0, 180)]) of the ellipse with the given on-sky errors and
    covariance (any consistent units)."""
    a, b, c = np.asarray(ra_err, float) ** 2, np.asarray(dec_err, float) ** 2, np.asarray(cov, float)
    tr, det = a + b, a * b - c * c
    disc = np.sqrt(np.maximum(0.25 * (a - b) ** 2 + c * c, 0))
    l1, l2 = 0.5 * tr + disc, np.maximum(0.5 * tr - disc, 0)
    del det
    # major-axis direction (east, north) components: angle from north
    theta = 0.5 * np.degrees(np.arctan2(2 * c, b - a))
    return np.sqrt(l1), np.sqrt(l2), np.mod(theta, 180.0)


def sigma_major_arcsec(ra_err_deg, dec_err_deg, cov_deg2):
    return ellipse_axes(ra_err_deg, dec_err_deg, cov_deg2)[0] * 3600.0


def coarse_at(orbit, mjd_tai, ephem):
    """propagate.coarse for one ORBIT_DTYPE row at the given TAI MJDs
    (any order): returns a dict of arrays aligned to mjd_tai, or None when
    coarse isn't implemented yet."""
    from ssp.nearbysso import propagate
    uniq, inv = np.unique(np.asarray(mjd_tai, float), return_inverse=True)
    obs_pos, _ = observer_states(uniq)
    try:
        tr = propagate.coarse(orbit, tai_to_assist(uniq), obs_pos, ephem)
    except NotImplementedError:
        return None
    return {k: np.asarray(getattr(tr, k))[inv] for k in C.CoarseTrack._fields
            if getattr(tr, k) is not None}


class SigmaOracle:
    """sigma_major [arcsec] of (designation, TAI MJD) pairs: our own
    ORBIT_DTYPE rows from mpc_orbits (CAR covariance), propagated by
    ``propagate.coarse`` at exactly those times. NaN where unknown (coarse
    not implemented, no ASSIST, object not in the orbit file)."""

    def __init__(self, orbits_path, ephem=None):
        self.orbits_path, self._ephem, self.note = orbits_path, ephem, ""

    def ephem(self):
        if self._ephem is None:
            self._ephem = open_ephem()
        return self._ephem

    def __call__(self, designations, mjd_tai):
        designations = np.asarray(designations, dtype=object)
        mjd_tai = np.asarray(mjd_tai, float)
        out = np.full(len(designations), np.nan)
        if len(designations) == 0:
            return out
        try:
            ephem = self.ephem()
        except Exception as exc:     # no ASSIST files configured
            self.note = f"sigma unknown: cannot open ASSIST ({exc})"
            return out
        orbits = read_orbits(self.orbits_path, designations=set(designations), with_json=True)
        recs = orbit_records_from_mpc(orbits, ephem)
        by_des = {r["designation"]: r for r in recs}
        for d in np.unique(designations):
            sel = np.flatnonzero(designations == d)
            if d not in by_des:
                continue
            res = coarse_at(by_des[d], mjd_tai[sel], ephem)
            if res is None:
                self.note = "sigma unknown: propagate.coarse not implemented"
                return out
            out[sel] = np.where(res["ok"], res["sigma_major"], np.nan)
        return out


# ---------------------------------------------------------------------------
# DiaSources
# ---------------------------------------------------------------------------

def read_dia_subset(path, ids=None, visits=None, columns=("diaSourceId", "visit", "midpointMjdTai",
                                                          "ra", "dec")):
    """DiaSource rows with the given diaSourceIds (or visits), row group by
    row group so a 60M-row file doesn't need to fit in memory."""
    pf = pq.ParquetFile(path)
    key, want = ("diaSourceId", ids) if ids is not None else ("visit", visits)
    want = np.unique(np.asarray(want, dtype=np.int64))
    parts = []
    for rg in range(pf.num_row_groups):
        k = pf.read_row_group(rg, columns=[key]).column(0).to_numpy()
        m = np.isin(k, want)
        if m.any():
            t = pf.read_row_group(rg, columns=list(columns))
            parts.append(t.filter(pa.array(m)).to_pandas())
    if not parts:
        return pd.DataFrame({c: [] for c in columns})
    return pd.concat(parts, ignore_index=True)


def derive_visits(dia):
    """Visits from DiaSources (independently of WP3): per visit, the time,
    the normalized mean unit vector and the angle enclosing its sources."""
    dia = dia.sort_values(["visit", "diaSourceId"], kind="stable")
    v = radec_to_vec(dia["ra"].to_numpy(), dia["dec"].to_numpy())
    vis, start = np.unique(dia["visit"].to_numpy(), return_index=True)
    end = np.append(start[1:], len(dia))
    rows = []
    for vi, s, e in zip(vis, start, end):
        c = v[s:e].sum(axis=0)
        c /= np.linalg.norm(c)
        r = np.arccos(np.clip(v[s:e] @ c, -1, 1)).max()
        rows.append((vi, dia["midpointMjdTai"].iloc[s], c, r, s, e))
    return pd.DataFrame(rows, columns=["visit", "t_tai_mjd", "center", "radius", "dia_start",
                                       "dia_end"]), dia.reset_index(drop=True)


# ---------------------------------------------------------------------------
# 1/2. Comparison against an SSObservation
# ---------------------------------------------------------------------------

def compare_to_ssobservation(nss, sss, dia, reason_of, sigma_fn=None, tol=TOL,
                        radius=None, sigma_max=SIGMA_MAX):
    """Compare NearbySSO rows (``nss``) with SSObservation rows (``sss``).

    ``dia``: diaSourceId, midpointMjdTai, ra, dec for the SSObservation rows'
    DiaSources. ``reason_of``: designation -> filter reason ('' kept).
    ``sigma_fn(designations, mjd_tai) -> sigma_major [arcsec]`` (NaN:
    unknown), called only for rows no cheaper reason explains. ``radius``:
    None (the default) for each SSObservation row's object's own match radius
    (`radius_of`), or one radius [arcsec] for all.

    Time shift (docs/design/shutter-timing.md): where ``sss`` has
    ``sss_midpointMjdTai`` (SSObservation's own time) and it differs from the
    DiaSource's ``midpointMjdTai`` by dt, the two predictions differ by
    about rate x dt: the eph* tolerances, the separation-from-radius and
    the nearer-object decisions get bench/time_shift.py's allowances on
    top; at dt = 0 they are unchanged. |dt| > TS.DT_MAX_S is a failure.

    Returns (rows, summary): one row per SSObservation row with its status:

    - ``match``: same designation, eph* within ``tol`` (+ the time shift);
    - ``value_mismatch``: same designation, an eph* value outside ``tol``;
    - ``filtered:<reason>``: the object fails NearbySSO's orbit filter;
    - ``no_diasource`` / ``sss_no_ephemeris``: incomparable inputs;
    - ``separation``: SSObservation's prediction is beyond its object's match
      radius from the DiaSource;
    - ``nearer_object``: NearbySSO gave the DiaSource a nearer object
      (ties by designation);
    - ``sigma``: sigma_major > sigma_max at that time;
    - ``sigma_unknown``: sigma couldn't be computed;
    - ``wrong_nearest``: NearbySSO chose a *farther* object, and ours is
      eligible (a bug);
    - ``unexplained``: missing from NearbySSO for no known reason (a bug).
    """
    nss = pd.DataFrame(nss)
    n_dup = int(nss["diaSourceId"].duplicated().sum())
    nsub = nss.drop_duplicates("diaSourceId")[
        ["diaSourceId", "designation"] + [c for c in EPH_COMPARED + ["ephOffset"] if c in nss]]
    nsub = nsub.rename(columns={c: "nss_" + c for c in nsub.columns if c != "diaSourceId"})
    sss = pd.DataFrame(sss)
    d = sss[["diaSourceId", "designation"] + EPH_COMPARED
            + [c for c in ["sss_midpointMjdTai"] + SHIFT_COLUMNS if c in sss]].copy()
    d = d.merge(pd.DataFrame(dia)[["diaSourceId", "midpointMjdTai", "ra", "dec"]].rename(
        columns={"ra": "dia_ra", "dec": "dia_dec"}), on="diaSourceId", how="left")
    d = d.merge(nsub, on="diaSourceId", how="left")
    n = len(d)

    # a NULL designation (SSObservation rows of no known object) is "" (not in
    # mpc_orbits); pandas 3 keeps NaN through astype(str)
    des = np.array(["" if pd.isna(x) else str(x) for x in d["designation"].to_numpy(dtype=object)],
                   dtype=object)
    rad =radius_of(des, radius)
    nss_des = d["nss_designation"].to_numpy(dtype=object)
    has_nss = pd.notna(d["nss_designation"]).to_numpy()
    same = has_nss & (nss_des == des)
    have_dia = np.isfinite(d["dia_ra"].to_numpy(dtype=float))
    sss_ok = np.isfinite(d["ephRa"].to_numpy(dtype=float)) & np.isfinite(d["ephDec"].to_numpy(dtype=float))
    sep = np.full(n, np.nan)
    m = have_dia & sss_ok
    sep[m] = util.sky_separation_arcsec(d["ephRa"].to_numpy()[m], d["ephDec"].to_numpy()[m],
                                        d["dia_ra"].to_numpy()[m], d["dia_dec"].to_numpy()[m])
    d["sep"] = sep

    # the time shift: SSObservation at its own time, NearbySSO at the
    # DiaSource's
    def opt(c):
        return d[c].to_numpy(dtype=float) if c in d else np.full(n, np.nan)
    dt = TS.dt_days(opt("sss_midpointMjdTai"), d["midpointMjdTai"].to_numpy(dtype=float))
    rate = TS.rate_deg_day(opt("ephRateRa"), opt("ephRateDec"), opt("nss_ephRateRa"), opt("nss_ephRateDec"))
    pos_allow = TS.position_mas(rate, dt)
    rate_allow = TS.rate_allowance(dt, rate, opt("topoRange"), opt("topoRangeRate"), opt("helioRange"),
                                   opt("ephDec"))
    vmag_allow = TS.vmag_allowance(dt, rate, opt("topoRange"), opt("topoRangeRate"), opt("helioRange"),
                                   opt("helioRangeRate"))
    d["dt_s"] = dt * TS.SECONDS_PER_DAY
    d["shift_allow_mas"] = pos_allow
    dt_big = TS.too_large(dt)

    # value differences (where the designation matches)
    dpos = np.full(n, np.nan)
    if same.any():
        dpos[same] = util.sky_separation_arcsec(
            d["ephRa"].to_numpy()[same], d["ephDec"].to_numpy()[same],
            d["nss_ephRa"].to_numpy(dtype=float)[same], d["nss_ephDec"].to_numpy(dtype=float)[same]) * 1e3
    d["d_pos_mas"] = dpos.copy()
    # where dt != 0: the residual once the motion over dt is taken out
    shifted = same & (dt != 0)
    if shifted.any():
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            mean_ra = np.nanmean([opt("ephRateRa"), opt("nss_ephRateRa")], axis=0)
            mean_dec = np.nanmean([opt("ephRateDec"), opt("nss_ephRateDec")], axis=0)
        dpos[shifted] = TS.motion_residual_mas(
            opt("ephRa")[shifted], opt("ephDec")[shifted],
            opt("nss_ephRa")[shifted], opt("nss_ephDec")[shifted],
            mean_ra[shifted], mean_dec[shifted], dt[shifted])
    d["d_pos_resid_mas"] = dpos
    pos_margin = TS.position_margin_mas(rate, dt, TS.angular_acceleration(
        rate, opt("topoRange"), opt("topoRangeRate"), opt("helioRange"), opt("ephDec")))
    for c in ("ephRateRa", "ephRateDec", "ephVmag"):
        d["d_" + c] = np.where(same, d["nss_" + c].to_numpy(dtype=float) - d[c].to_numpy(dtype=float),
                               np.nan)
    bad = (np.nan_to_num(dpos) > tol["pos_mas"] + pos_margin)
    bad |= np.abs(np.nan_to_num(d["d_ephRateRa"])) > tol["rate_deg_day"] + rate_allow
    bad |= np.abs(np.nan_to_num(d["d_ephRateDec"])) > tol["rate_deg_day"] + rate_allow
    bad |= np.abs(np.nan_to_num(d["d_ephVmag"])) > tol["vmag"] + vmag_allow
    # a NaN on one side only is a mismatch too
    for c in EPH_COMPARED:
        a, b = d[c].to_numpy(dtype=float), d["nss_" + c].to_numpy(dtype=float)
        bad |= same & (np.isnan(a) != np.isnan(b))
    identical = same.copy()
    for c in EPH_COMPARED:
        a, b = d[c].to_numpy(dtype=float), d["nss_" + c].to_numpy(dtype=float)
        identical &= (a == b) | (np.isnan(a) & np.isnan(b))

    reason = reasons_for(des, reason_of)
    status = np.full(n, "", dtype=object)
    status[same] = np.where(bad[same], "value_mismatch", "match")
    todo = ~same
    for cond, label in (
        (reason != "", None),
        (~have_dia, "no_diasource"),
        (~sss_ok, "sss_no_ephemeris"),
        # NearbySSO's prediction (at the DiaSource's time) may be beyond
        # the radius where SSObservation's (at its own) is just within it
        (np.nan_to_num(sep, nan=np.inf) > rad - pos_allow / 1e3, "separation"),
    ):
        m = todo & cond
        status[m] = ["filtered:" + r for r in reason[m]] if label is None else label
        todo &= ~m
    # a nearer object took the DiaSource?
    other_off = d["nss_ephOffset"].to_numpy(dtype=float) if "nss_ephOffset" in d else np.full(n, np.nan)
    eps = 1e-4 + pos_allow / 1e3   # arcsec: the float32 ephOffset's resolution (+ the time shift)
    nearer = has_nss & ((other_off < sep - eps)
                        | ((np.abs(other_off - sep) <= eps) & (nss_des.astype(str) < des)))
    m = todo & nearer
    status[m] = "nearer_object"
    todo &= ~m
    sigma = np.full(n, np.nan)
    if todo.any() and sigma_fn is not None:
        sigma[todo] = sigma_fn(des[todo], d["midpointMjdTai"].to_numpy()[todo])
    m_sig = todo & (sigma > sigma_max)
    status[m_sig] = "sigma"
    m_unk = todo & ~np.isfinite(sigma)
    status[m_unk] = "sigma_unknown"
    rest = todo & ~m_sig & ~m_unk
    status[rest & has_nss] = "wrong_nearest"
    status[rest & ~has_nss] = "unexplained"
    d["sigma"] = sigma
    d["status"] = status
    d["match_radius"] = rad
    d["borderline"] = (np.abs(np.nan_to_num(sep, nan=1e9) - rad) < 1e-3) | (
        np.abs(sigma - sigma_max) < 0.01 * sigma_max)
    d["identical"] = identical

    # NearbySSO rows for DiaSources not in this SSObservation at all
    in_sss = np.isin(nss["diaSourceId"].to_numpy(), d["diaSourceId"].to_numpy())
    # NearbySSO rows beyond their own object's match radius (wrong,
    # whatever SSObservation says)
    beyond = beyond_radius(nss, radius)
    counts = pd.Series(status).value_counts().to_dict()
    ms = d[same]
    summary = {
        "n_ssobservation": n, "n_nearbysso": len(nss), "n_nearbysso_duplicate_ids": n_dup,
        "n_nearbysso_not_in_ssobservation": int((~in_sss).sum()),
        "n_nearbysso_beyond_radius": int(beyond.sum()),
        "nearbysso_beyond_radius": nss.loc[beyond, "diaSourceId"].head(10).tolist(),
        "status_counts": {k: int(v) for k, v in sorted(counts.items())},
        "n_identical": int(identical.sum()),
        "max_d_pos_mas": float(np.nanmax(ms["d_pos_mas"])) if len(ms) else np.nan,
        "max_d_pos_resid_mas": float(np.nanmax(ms["d_pos_resid_mas"])) if len(ms) else np.nan,
        "max_d_rateRa_deg_day": float(np.nanmax(np.abs(ms["d_ephRateRa"]))) if len(ms) else np.nan,
        "max_d_rateDec_deg_day": float(np.nanmax(np.abs(ms["d_ephRateDec"]))) if len(ms) else np.nan,
        "max_d_vmag": float(np.nanmax(np.abs(ms["d_ephVmag"]))) if len(ms) else np.nan,
        "n_time_shifted": int((dt != 0).sum()),
        "max_abs_dt_s": float(np.max(np.abs(dt))) * TS.SECONDS_PER_DAY if n else 0.0,
        "n_dt_too_large": int(dt_big.sum()),
        "n_fail": (int(sum(counts.get(s, 0) for s in FAIL_STATUSES)) + n_dup + int(beyond.sum())
                   + int(dt_big.sum())),
        "n_unknown": int(counts.get("sigma_unknown", 0)),
        "n_borderline_fail": int((d["borderline"] & np.isin(status, FAIL_STATUSES)).sum()),
    }
    return d, summary


def beyond_radius(nss, radius=None):
    """Mask of NearbySSO rows whose ephOffset exceeds their object's match
    radius (`radius_of`) by more than OFFSET_EPS: rows that shouldn't
    exist (all False if there's no ephOffset)."""
    nss = pd.DataFrame(nss)
    if "ephOffset" not in nss or not len(nss):
        return np.zeros(len(nss), bool)
    off = nss["ephOffset"].to_numpy(np.float64)
    return np.nan_to_num(off, nan=0.0) > radius_of(nss["designation"].to_numpy(), radius) + OFFSET_EPS


def report_comparison(rep, summary, tol=None):
    rep(f"SSObservation rows:          {summary['n_ssobservation']:,}")
    rep(f"NearbySSO rows:         {summary['n_nearbysso']:,} "
        f"({summary['n_nearbysso_not_in_ssobservation']:,} for DiaSources not in SSObservation; "
        f"{summary['n_nearbysso_duplicate_ids']} duplicate diaSourceIds)")
    rep(f"NearbySSO rows with ephOffset beyond their object's match radius: "
        f"{summary['n_nearbysso_beyond_radius']:,}"
        + (f" (e.g. {summary['nearbysso_beyond_radius']})" if summary['n_nearbysso_beyond_radius'] else ""))
    rep("status of each SSObservation row:")
    for k, v in summary["status_counts"].items():
        rep(f"  {k:32s} {v:>10,}")
    rep(f"bitwise-identical eph* rows: {summary['n_identical']:,}")
    if "n_time_shifted" in summary:
        rep(f"rows where SSObservation's midpointMjdTai differs from the DiaSource's: "
            f"{summary['n_time_shifted']:,} (max |dt| {summary['max_abs_dt_s']:.3f} s; "
            f"{summary['n_dt_too_large']} beyond {TS.DT_MAX_S} s, a failure); their tolerances add "
            f"bench/time_shift.py's allowances, the others are strict")
    rep(f"max |d position|  = {summary['max_d_pos_mas']:.3g} mas")
    if summary.get("n_time_shifted"):
        rep(f"max |d position - rate x dt| = {summary['max_d_pos_resid_mas']:.3g} mas (the plain "
            f"difference where dt = 0)")
    rep(f"max |d rateRa|    = {summary['max_d_rateRa_deg_day']:.3g} deg/day, "
        f"|d rateDec| = {summary['max_d_rateDec_deg_day']:.3g} deg/day")
    rep(f"max |d Vmag|      = {summary['max_d_vmag']:.3g} mag")
    if tol:
        rep(f"tolerances: {tol}")
    if summary["n_borderline_fail"]:
        rep(f"({summary['n_borderline_fail']} failing rows are within 1e-3\" of their match radius or "
            f"1% of the sigma gate)")


def discrepancies(rows):
    """Rows to adjudicate with Horizons (the schema horizons-adjudicate
    reads)."""
    m = rows["status"].isin(FAIL_STATUSES)
    cols = ["designation", "diaSourceId", "midpointMjdTai", "dia_ra", "dia_dec", "status",
            "ephRa", "ephDec", "nss_designation", "nss_ephRa", "nss_ephDec", "sep", "sigma"]
    out = rows.loc[m, cols].rename(columns={"ephRa": "sss_ephRa", "ephDec": "sss_ephDec"})
    return out.reset_index(drop=True)


def _read_nss(path, ids=None):
    cols = [c for c in NSS_COLUMNS if c in pq.read_schema(path).names]
    return pd.read_parquet(path, columns=cols)


#: The DiaSource processing NearbySSO's DiaSources are (PPDB DiaSource).
SSS_PROCESSING = "AP-DS"


def _read_sss(path, extra=(), processing=SSS_PROCESSING):
    """The SSObservation columns compared, one row per DiaSource, keyed by
    diaSourceId.

    A SSObservation (with ``measuredOn``) mixes DiaSources and Sources
    from several processings, with a NULL diaSourceId on Source rows and a
    DiaSource repeated on non-primary rows: keep its ``primary``,
    ``measuredOn == 'difference'`` rows of ``processing`` (None or 'all':
    every processing), and check that (processing, diaSourceId) and, as the
    comparisons join on it, diaSourceId are unique. Read through Arrow, so
    the 64-bit ids stay exact (default pandas would make a NULL-containing
    integer column float64); integer columns with NULLs become pandas Int64.
    An earlier-layout SSObservation is read as is. ``path``: the
    partitioned SSObservation's directory or manifest, or a single Parquet
    file.
    """
    present = set(SP.read_schema(path).names)
    cols = [c for c in ["diaSourceId", "designation", "ssObjectId"] + EPH_COMPARED + ["ephOffset"]
            + list(extra) + ["midpointMjdTai"] + SHIFT_COLUMNS if c in present]
    widened = "measuredOn" in present
    key = ["processing", "measuredOn", "primary"] if widened else []
    t = SP.read_table(path, columns=list(dict.fromkeys(cols + key)))
    if widened:
        keep = pc.and_(pc.equal(t["measuredOn"].cast(pa.string()), "difference"), t["primary"])
        if processing not in (None, "all"):
            keep = pc.and_(keep, pc.equal(t["processing"].cast(pa.string()), processing))
        t = t.filter(pc.fill_null(keep, False))
        k = pa.table({"p": t["processing"].cast(pa.string()), "i": t["diaSourceId"]})
        if k.group_by(["p", "i"]).aggregate([]).num_rows != t.num_rows:
            raise ValueError(f"{path}: (processing, diaSourceId) is not unique on the primary rows")
        if len(pc.unique(t["diaSourceId"])) != t.num_rows:
            raise ValueError(f"{path}: diaSourceId repeats across processings; pick one (--sss-processing)")
        t = t.select(cols)
    out = {}
    for c in t.column_names:
        a = t[c]
        if pa.types.is_dictionary(a.type):
            a = a.cast(a.type.value_type)
        if pa.types.is_integer(a.type) and a.null_count:
            out[c] = pd.array(a.to_pylist(), dtype="Int64")
        else:
            out[c] = a.to_pandas()
    # SSObservation's own time, kept apart from the DiaSource's midpointMjdTai
    return pd.DataFrame(out, columns=t.column_names).rename(columns={"midpointMjdTai": "sss_midpointMjdTai"})


def cmd_same_orbits(args):
    rep = Report("same-orbits", args.out)
    rep("# NearbySSO vs SSObservation built from the same mpc_orbits")
    rep(f"nearbysso={args.nearbysso}\nssobservation={args.ssobservation}\ndia={args.dia}\norbits={args.orbits}")
    nss, sss = _read_nss(args.nearbysso), _read_sss(args.ssobservation, processing=args.sss_processing)
    dia = read_dia_subset(args.dia, ids=np.union1d(sss["diaSourceId"], nss["diaSourceId"]))
    orbits = read_orbits(args.orbits, designations=set(sss["designation"].dropna()))
    sigma_fn = None if args.no_sigma else SigmaOracle(args.orbits)
    tol = dict(pos_mas=args.pos_tol_mas, rate_deg_day=args.rate_tol, vmag=args.vmag_tol)
    rows, summary = compare_to_ssobservation(nss, sss, dia, reason_lookup(orbits), sigma_fn, tol)
    report_comparison(rep, summary, tol)
    if sigma_fn is not None and sigma_fn.note:
        rep(sigma_fn.note)
    disc = discrepancies(rows)
    ok = summary["n_fail"] == 0
    verdict = "PASS" if ok and not summary["n_unknown"] else ("FAIL" if not ok else "INCOMPLETE")
    rep(f"discrepancies exported for horizons-adjudicate: {len(disc):,}")
    rep(f"GATE (no value mismatches, no wrong/unexplained misses): {verdict}")
    rep.write(rows, discrepancies=disc)
    return 0 if verdict == "PASS" else (1 if verdict == "FAIL" else 3)


# ---------------------------------------------------------------------------
# 2. DP2 intersection
# ---------------------------------------------------------------------------

def ssobjectid_to_packed(ids):
    """Inverse of ssp.util.packed_ascii_to_uint64_le: the packed
    designation (spaces stripped) of each ssObjectId."""
    b = np.asarray(ids, dtype="<u8").view("S8")
    return np.array([x.decode("ascii", "replace").strip(" \x00") for x in b], dtype=object)


def reconcile_designations(des, ident):
    """Map designations to their current primary provisional designation
    via current_identifications (unpacked secondary -> unpacked primary);
    designations that are already primary, or unknown, stay."""
    m = dict(zip(ident["unpacked_secondary_provisional_designation"],
                 ident["unpacked_primary_provisional_designation"]))
    return np.array([m.get(d, d) for d in des], dtype=object)


def cmd_dp2_intersection(args):
    rep = Report("dp2-intersection", args.out)
    rep("# NearbySSO vs DP2 SSSource (different orbits and cuts): REPORT ONLY, not gated")
    nss, sss = _read_nss(args.nearbysso), _read_sss(args.ssobservation, processing=args.sss_processing)
    ident = pd.read_parquet(args.identifications, columns=[
        "unpacked_primary_provisional_designation", "unpacked_secondary_provisional_designation",
        "packed_secondary_provisional_designation"])
    if "designation" not in sss:
        packed = ssobjectid_to_packed(sss["ssObjectId"].to_numpy())
        pmap = dict(zip(ident["packed_secondary_provisional_designation"],
                        ident["unpacked_primary_provisional_designation"]))
        sss["designation"] = [pmap.get(p) for p in packed]
        rep(f"designations from ssObjectId: {pd.isna(sss['designation']).sum():,} unresolved")
        sss = sss[pd.notna(sss["designation"])]
    before = sss["designation"].to_numpy(dtype=object)
    sss["designation"] = reconcile_designations(before, ident)
    nss["designation"] = reconcile_designations(nss["designation"].to_numpy(dtype=object), ident)
    rep(f"DP2 designations re-mapped to a current primary: {(sss['designation'] != before).sum():,}")
    orbits = read_orbits(args.orbits, designations=set(sss["designation"]) | set(nss["designation"]))
    lookup = reason_lookup(orbits)
    r = reasons_for(sss["designation"].to_numpy(), lookup)
    obj = pd.DataFrame({"designation": sss["designation"], "reason": r}).drop_duplicates("designation")
    rep(f"DP2 objects: {len(obj):,}; excluded by our filter: "
        f"{obj['reason'].replace('', np.nan).value_counts().to_dict()}")
    dp2_objects = set(obj["designation"])
    both = sss[r == ""]
    nss_both = nss[nss["designation"].isin(dp2_objects)]
    rep(f"restricted to objects both keep: {both['designation'].nunique():,} objects, "
        f"{len(both):,} DP2 rows, {len(nss_both):,} NearbySSO rows")
    dia = read_dia_subset(args.dia, ids=np.union1d(both["diaSourceId"], nss_both["diaSourceId"]))
    sigma_fn = SigmaOracle(args.orbits) if args.sigma else None
    rows, summary = compare_to_ssobservation(nss_both, both, dia, lookup, sigma_fn)
    report_comparison(rep, summary, TOL)
    rep("d position [mas] of matched rows:", _stats(rows["d_pos_mas"], "mas"))
    rep("(different orbit snapshots: differences are expected; not gated)")
    rep.write(rows, discrepancies=discrepancies(rows))
    return 0


# ---------------------------------------------------------------------------
# 3. Horizons adjudication
# ---------------------------------------------------------------------------

def adjudicate_rows(disc, h_ra, h_dec, tol_mas=1.0, radius=None):
    """Which side of each discrepancy agrees with Horizons (pure logic).
    Adds sep_{sss,nss,dia}_h and ``verdict``. ``radius``: None for each
    row's object's match radius (`radius_of`), or one for all."""
    d = disc.copy()

    def sep(ra, dec):
        ra, dec = np.asarray(ra, float), np.asarray(dec, float)
        out = np.full(len(d), np.nan)
        m = np.isfinite(ra) & np.isfinite(dec) & np.isfinite(h_ra)
        out[m] = util.sky_separation_arcsec(ra[m], dec[m], h_ra[m], h_dec[m])
        return out
    d["h_ra"], d["h_dec"] = h_ra, h_dec
    d["sep_sss_h_mas"] = sep(d["sss_ephRa"], d["sss_ephDec"]) * 1e3
    same = (d["nss_designation"].astype(str) == d["designation"].astype(str)).to_numpy()
    s = sep(d["nss_ephRa"], d["nss_ephDec"]) * 1e3
    d["sep_nss_h_mas"] = np.where(same, s, np.nan)
    d["sep_dia_h_arcsec"] = sep(d["dia_ra"], d["dia_dec"])
    ss, ns = d["sep_sss_h_mas"].to_numpy() < tol_mas, d["sep_nss_h_mas"].to_numpy() < tol_mas
    within = d["sep_dia_h_arcsec"].to_numpy() <= radius_of(d["designation"].to_numpy(), radius)
    v = np.full(len(d), "no_horizons", dtype=object)
    have = np.isfinite(h_ra)
    v[have & same & ss & ns] = "both_match"
    v[have & same & ss & ~ns] = "sss_matches: NearbySSO bug"
    v[have & same & ~ss & ns] = "nss_matches: SSObservation issue"
    v[have & same & ~ss & ~ns] = "neither_matches"
    miss = have & ~same
    v[miss & ss & within] = "sss_matches, within radius: NearbySSO missed it"
    v[miss & ss & ~within] = "sss_matches, outside radius per Horizons"
    v[miss & ~ss] = "sss_disagrees: SSObservation issue"
    d["verdict"] = v
    return d


def cmd_horizons_adjudicate(args):
    rep = Report("horizons-adjudicate", args.out)
    polite = Polite(args.min_interval, args.max_queries, args.query_log)
    disc = pd.read_parquet(args.discrepancies)
    if args.max_rows and len(disc) > args.max_rows:
        disc = disc.sample(args.max_rows, random_state=args.seed).reset_index(drop=True)
    rep(f"# Horizons adjudication of {len(disc)} discrepancies")
    orbits = read_orbits(args.orbits, designations=set(disc["designation"]))
    by = orbits.set_index("designation", drop=False)
    h_ra, h_dec = np.full(len(disc), np.nan), np.full(len(disc), np.nan)
    for d, idx in disc.groupby("designation").indices.items():
        if d not in by.index:
            continue
        try:
            cols = horizons_own_elements(by.loc[d], disc["midpointMjdTai"].to_numpy()[idx], polite)
        except QueryBudgetExceededError as exc:
            rep(f"stopping: {exc}")
            break
        except Exception as exc:
            rep(f"  {d}: Horizons failed: {exc}")
            continue
        h_ra[idx], h_dec[idx] = _col(cols, "R.A."), _col(cols, "DEC")
    res = adjudicate_rows(disc, h_ra, h_dec, args.tol_mas)
    exc = res["designation"].isin(list(KNOWN_EXCEPTIONS)).to_numpy()
    res.loc[exc, "verdict"] = "known exception: " + res.loc[exc, "verdict"]
    for d in sorted(set(res.loc[exc, "designation"])):
        rep(f"  {d}: known exception, not gated ({KNOWN_EXCEPTIONS[d]})")
    for k, v in res["verdict"].value_counts().items():
        rep(f"  {k:52s} {v:>6}")
    rep(f"Horizons requests: {polite.n}")
    rep.write(res)
    bugs = (res["verdict"].str.contains("NearbySSO") & ~exc).sum()
    return 1 if bugs else 0


# ---------------------------------------------------------------------------
# 4. Stratified Horizons position spot check
# ---------------------------------------------------------------------------

DEFAULT_STRATA = (
    ("main_belt", 15), ("neo", 14), ("neo_close", 1), ("trojan", 14), ("tno", 14),
    ("short_arc", 14), ("near_radius", 14), ("near_sigma", 14),
)


def strata_masks(nss, orbits, short_arc_days=30.0):
    """The strata (boolean masks over nss rows) and their ordering keys."""
    o = orbits.set_index("designation")
    q = o["q"].reindex(nss["designation"]).to_numpy(dtype=float)
    e = o["e"].reindex(nss["designation"]).to_numpy(dtype=float)
    arc = arc_days(o["arc_text"].reindex(nss["designation"]).to_numpy(dtype=object))
    cls = dynamical_class(q, e)
    sig = sigma_major_arcsec(nss["ephRaErr"].to_numpy(float), nss["ephDecErr"].to_numpy(float),
                             nss["ephRa_ephDec_Cov"].to_numpy(float))
    rate = np.hypot(nss["ephRateRa"].to_numpy(float), nss["ephRateDec"].to_numpy(float))
    masks = {
        "main_belt": cls == "main_belt", "neo": cls == "neo", "trojan": cls == "trojan",
        "tno": cls == "tno", "short_arc": arc < short_arc_days,
        "near_radius": nss["ephOffset"].to_numpy(float) > 0.9 * radius_of(nss["designation"].to_numpy()),
        "near_sigma": sig > 0.8 * SIGMA_MAX,
    }
    # the NEO rows moving fastest on the sky: a proxy for a close approach
    masks["neo_close"] = masks["neo"]
    keys = {"neo_close": -rate}
    return masks, keys


def stratified_sample(n_rows, masks, strata=DEFAULT_STRATA, keys=None, rng=None, group=None):
    """Pick row indices per stratum, never the same row twice (strata in
    the given order). With ``keys[name]``, the lowest-keyed rows are taken
    instead of random ones. With ``group`` (e.g. designations), at most one
    row per group per stratum. Returns a DataFrame(row, stratum)."""
    rng = np.random.default_rng(rng)
    keys = keys or {}
    taken = np.zeros(n_rows, bool)
    out = []
    for name, n in strata:
        cand = np.flatnonzero(np.asarray(masks.get(name, np.zeros(n_rows, bool))) & ~taken)
        if name in keys:
            cand = cand[np.argsort(np.asarray(keys[name])[cand], kind="stable")]
        else:
            cand = rng.permutation(cand)
        if group is not None:
            _, first = np.unique(np.asarray(group)[cand], return_index=True)
            cand = cand[np.sort(first)]
        pick = cand[:n]
        taken[pick] = True
        out += [(int(i), name) for i in pick]
    return pd.DataFrame(out, columns=["row", "stratum"])


def cmd_horizons_positions(args):
    rep = Report("horizons-positions", args.out)
    polite = Polite(args.min_interval, args.max_queries, args.query_log)
    nss = _read_nss(args.nearbysso)
    orbits = read_orbits(args.orbits, designations=set(nss["designation"]))
    masks, keys = strata_masks(nss, orbits)
    strata = [(s, int(round(n * args.scale))) for s, n in DEFAULT_STRATA]
    strata = [(s, max(n, 1)) for s, n in strata]
    samp = stratified_sample(len(nss), masks, strata, keys, args.seed, group=nss["designation"].to_numpy())
    if args.max_objects:
        keep = pd.unique(nss["designation"].to_numpy()[samp["row"]])[:args.max_objects]
        samp = samp[np.isin(nss["designation"].to_numpy()[samp["row"]], keep)]
    s = nss.iloc[samp["row"]].reset_index(drop=True)
    s["stratum"] = samp["stratum"].to_numpy()
    rep(f"# Horizons spot check: {len(s)} rows, {s['designation'].nunique()} objects")
    rep("  per stratum: " + ", ".join(f"{k}={v}" for k, v in s["stratum"].value_counts().items()))
    dia = read_dia_subset(args.dia, ids=s["diaSourceId"].to_numpy())
    s = s.merge(dia[["diaSourceId", "midpointMjdTai", "ra", "dec"]], on="diaSourceId", how="left")
    by = orbits.set_index("designation", drop=False)
    half = 60.0 / 86400.0
    n = len(s)
    h = {k: np.full(n, np.nan) for k in ("ra", "dec", "ra0", "dec0", "ra1", "dec1", "vmag")}
    for d, idx in s.groupby("designation").indices.items():
        t = s["midpointMjdTai"].to_numpy()[idx]
        tt = np.concatenate([t, t - half, t + half])
        try:
            cols = horizons_own_elements(by.loc[d], tt, polite, quantities="1,9", with_hg=True)
        except QueryBudgetExceededError as exc:
            rep(f"stopping: {exc}")
            break
        except Exception as exc:
            rep(f"  {d}: Horizons failed: {exc}")
            continue
        ra, dec = _col(cols, "R.A."), _col(cols, "DEC")
        k = len(t)
        h["ra"][idx], h["dec"][idx] = ra[:k], dec[:k]
        h["ra0"][idx], h["dec0"][idx] = ra[k:2 * k], dec[k:2 * k]
        h["ra1"][idx], h["dec1"][idx] = ra[2 * k:], dec[2 * k:]
        if "APmag" in cols:
            h["vmag"][idx] = cols["APmag"][:k]
    res = horizons_position_residuals(s, h, half)
    res["known_exception"] = res["designation"].isin(list(KNOWN_EXCEPTIONS))
    rep(f"Horizons requests: {polite.n}")
    for _, r in res[res["known_exception"]].iterrows():
        rep(f"  known exception, not gated: {r['designation']} ({KNOWN_EXCEPTIONS[r['designation']]}): "
            f"d position {r['d_pos_mas']:.4g} mas")
    ok = np.isfinite(res["d_pos_mas"]) & ~res["known_exception"]
    rms = float(np.sqrt(np.mean(res["d_pos_mas"][ok] ** 2))) if ok.any() else np.nan
    for st, g in res[~res["known_exception"]].groupby("stratum"):
        rep(f"  {st:12s} n={len(g):3d}  pos: {_stats(g['d_pos_mas'], 'mas')}")
    rep("d rate [arcsec/h] (vs central difference of Horizons astrometric, +-60 s):",
        _stats(res["d_rate_arcsec_h"]))
    rep("d Vmag (ephVmag - Horizons APmag, printed to 1e-3):", _stats(res["d_vmag"]))
    rep("d ephOffset [arcsec] (vs Horizons position):", _stats(res["d_ephOffset"]))
    verdict = "PASS" if ok.sum() and rms < 1.0 else ("FAIL" if ok.sum() else "INCOMPLETE")
    rep(f"GATE: position RMS = {rms:.4f} mas over {ok.sum()} rows (< 1 mas): {verdict}")
    rep.write(res)
    return {"PASS": 0, "FAIL": 1}.get(verdict, 3)


def horizons_position_residuals(s, h, half):
    """Per-row residuals of NearbySSO against Horizons (pure)."""
    r = s.copy()
    for k, v in h.items():
        r["h_" + k] = v
    m = np.isfinite(r["h_ra"].to_numpy())
    dpos = np.full(len(r), np.nan)
    dpos[m] = util.sky_separation_arcsec(r["ephRa"][m], r["ephDec"][m], r["h_ra"][m], r["h_dec"][m]) * 1e3
    r["d_pos_mas"] = dpos
    dmid = np.deg2rad(0.5 * (r["h_dec0"] + r["h_dec1"]))
    dra = ((r["h_ra1"] - r["h_ra0"] + 540.0) % 360.0) - 180.0
    h_lon = dra * np.cos(dmid) / (2 * half)
    h_lat = (r["h_dec1"] - r["h_dec0"]) / (2 * half)
    r["d_rateRa_arcsec_h"] = (r["ephRateRa"] - h_lon) * 150.0
    r["d_rateDec_arcsec_h"] = (r["ephRateDec"] - h_lat) * 150.0
    r["d_rate_arcsec_h"] = np.hypot(r["d_rateRa_arcsec_h"], r["d_rateDec_arcsec_h"])
    r["d_vmag"] = r["ephVmag"] - r["h_vmag"]
    off = np.full(len(r), np.nan)
    mm = m & np.isfinite(r["ra"].to_numpy(float))
    off[mm] = util.sky_separation_arcsec(r["ra"][mm], r["dec"][mm], r["h_ra"][mm], r["h_dec"][mm])
    r["d_ephOffset"] = r["ephOffset"] - off
    return r


# ---------------------------------------------------------------------------
# 5. Horizons uncertainty check (JPL orbit and covariance)
# ---------------------------------------------------------------------------

def compare_sigma(ours, hz, gate_below_arcsec=60.0, rel_tol=0.10):
    """Compare our 1-sigma ellipses with Horizons' 3-sigma ones (pure).
    ``ours``: ra_err, dec_err [deg], ra_dec_cov [deg^2]; ``hz``: SMAA_3sig,
    SMIA_3sig [arcsec], Theta [deg], RA_3sigma, DEC_3sigma [arcsec]."""
    smaa, smia, th = ellipse_axes(ours["ra_err"], ours["dec_err"], ours["ra_dec_cov"])
    r = pd.DataFrame({
        "our_smaa_3sig": 3 * smaa * 3600, "our_smia_3sig": 3 * smia * 3600, "our_theta": th,
        "our_ra_3sig": 3 * np.asarray(ours["ra_err"]) * 3600,
        "our_dec_3sig": 3 * np.asarray(ours["dec_err"]) * 3600,
    })
    for k in ("SMAA_3sig", "SMIA_3sig", "Theta", "RA_3sigma", "DEC_3sigma"):
        r["h_" + k] = np.asarray(hz.get(k, np.full(len(r), np.nan)), float)
    r["ratio_smaa"] = r["our_smaa_3sig"] / r["h_SMAA_3sig"]
    r["ratio_smia"] = r["our_smia_3sig"] / r["h_SMIA_3sig"]
    r["ratio_ra"] = r["our_ra_3sig"] / r["h_RA_3sigma"]
    r["ratio_dec"] = r["our_dec_3sig"] / r["h_DEC_3sigma"]
    # Horizons' Theta is measured from the +RA axis towards +Dec, i.e.
    # 90 deg - the position angle (east of north). Established empirically
    # (2026-09-27, 9 epochs of 3 objects: our PA = 90 - Theta to < 1 deg
    # wherever the ellipse isn't near-circular); a sign error in our
    # RA/Dec covariance would show up as PA = 180 - (90 - Theta) instead.
    r["h_pa"] = np.mod(90.0 - r["h_Theta"], 180.0)
    dth = (r["our_theta"] - r["h_pa"]) % 180.0
    r["d_theta"] = np.where(dth > 90, dth - 180, dth)
    r["elongated"] = r["h_SMAA_3sig"] > 1.2 * r["h_SMIA_3sig"]
    r["gated"] = (r["our_smaa_3sig"] / 3 < gate_below_arcsec) & np.isfinite(r["h_SMAA_3sig"])
    r["pass"] = ~r["gated"] | (np.abs(r["ratio_smaa"] - 1) <= rel_tol)
    return r


def jpl_command(info):
    des = str(info.get("des") or "")
    return f"'{des};'" if des.isdigit() else f"'DES={des};'"


def sky_jacobian(rho):
    """d (RA cos(dec), Dec) / d position [rad/AU], (N, 2, 3), for
    topocentric vectors rho (N, 3)."""
    rho = np.atleast_2d(rho)
    d = np.linalg.norm(rho, axis=1)
    ra, dec = np.arctan2(rho[:, 1], rho[:, 0]), np.arcsin(rho[:, 2] / d)
    e_ra = np.stack([-np.sin(ra), np.cos(ra), np.zeros_like(ra)], axis=1)
    e_dec = np.stack([-np.sin(dec) * np.cos(ra), -np.sin(dec) * np.sin(ra), np.cos(dec)], axis=1)
    return np.stack([e_ra, e_dec], axis=1) / d[:, None, None]


def fd_track(orbit, mjd_tai, ephem, step_sigma=0.1):
    """An independent reference for coarse(): the state-transition matrix by
    central differences of our own ASSIST propagation
    (ssp.ephem_assist._propagate_one, no variational equations), steps of
    ``step_sigma`` x each state component's 1-sigma, C(t) = Phi C0 Phi^T
    projected through `sky_jacobian` (geometric, no light time). Returns a
    dict like `coarse_at`'s."""
    from ssp.ephem_assist import _propagate_one
    mjd_tai = np.asarray(mjd_tai, float)
    t = tai_to_assist(mjd_tai)
    obs_pos, _ = observer_states(mjd_tai)
    s0, c0 = np.asarray(orbit["state0"], float), np.asarray(orbit["cov0"], float)
    X, _ = _propagate_one(s0[:3], s0[3:], float(orbit["epoch"]), t, ephem)
    phi = np.empty((len(t), 3, 6))
    for k in range(6):
        h = step_sigma * np.sqrt(c0[k, k])
        d = np.zeros(6)
        d[k] = h
        Xp, _ = _propagate_one((s0 + d)[:3], (s0 + d)[3:], float(orbit["epoch"]), t, ephem)
        Xm, _ = _propagate_one((s0 - d)[:3], (s0 - d)[3:], float(orbit["epoch"]), t, ephem)
        phi[:, :, k] = ((Xp - Xm) / (2 * h)).T
    rho = X.T - obs_pos
    J = sky_jacobian(rho)
    Cs = np.einsum("nij,njk,nlk->nil", J @ phi, c0[None], J @ phi) * np.degrees(1) ** 2
    ra, dec = vec_to_radec(rho)
    ra_err, dec_err = np.sqrt(Cs[:, 0, 0]), np.sqrt(Cs[:, 1, 1])
    return {"ra": ra, "dec": dec, "ra_err": ra_err, "dec_err": dec_err, "ra_dec_cov": Cs[:, 0, 1],
            "sigma_major": sigma_major_arcsec(ra_err, dec_err, Cs[:, 0, 1]),
            "ok": np.ones(len(t), bool)}


def cmd_horizons_sigma(args):
    rep = Report("horizons-sigma", args.out)
    polite = Polite(args.min_interval, args.max_queries, args.query_log)
    ephem = open_ephem()
    epochs = np.array([float(x) for x in args.epochs.split(",")])
    rep(f"# Horizons plane-of-sky uncertainty vs ours, JPL orbits; epochs (TAI MJD) {list(epochs)}")
    rep("  'coarse': ssp.nearbysso.propagate.coarse (WP2, gated); 'fd': finite-difference reference")
    out, have_coarse = [], True
    for des in args.designations.split(","):
        des = des.strip()
        try:
            js = sbdb_query(des, polite)
            rec, info = sbdb_orbit_record(js, ephem)
        except QueryBudgetExceededError as exc:
            rep(f"stopping: {exc}")
            break
        except Exception as exc:
            rep(f"  {des}: SBDB failed: {exc}")
            continue
        rep(f"  {des}: {info['fullname']} orbit {info['orbit_id']} epoch(TT MJD) "
            f"{rec['epoch_mjd']:.4f} has_cov={rec['has_cov']} {info['note']}")
        if not rec["has_cov"]:
            continue
        tracks = {"fd": fd_track(rec, epochs, ephem)}
        tr = coarse_at(rec, epochs, ephem)
        if tr is None:
            have_coarse = False
        else:
            tracks["coarse"] = tr
        try:
            hz = horizons_jpl_orbit(jpl_command(info), epochs, polite)
        except QueryBudgetExceededError as exc:
            rep(f"stopping: {exc}")
            break
        except Exception as exc:
            rep(f"  {des}: Horizons failed: {exc}")
            continue
        h_ra, h_dec = _col(hz, "R.A."), _col(hz, "DEC")
        for name, trk in tracks.items():
            r = compare_sigma(trk, hz, args.gate_below, args.rel_tol)
            r.insert(0, "designation", des)
            r.insert(1, "mjd_tai", epochs)
            r.insert(2, "method", name)
            r["orbit_id"] = info["orbit_id"]
            r["d_pos_arcsec_geometric"] = util.sky_separation_arcsec(trk["ra"], trk["dec"], h_ra, h_dec)
            out.append(r)
    res = pd.concat(out, ignore_index=True) if out else pd.DataFrame()
    if len(res):
        with pd.option_context("display.width", 250, "display.max_columns", 30,
                               "display.float_format", "{:.4g}".format):
            rep(res[["designation", "mjd_tai", "method", "our_smaa_3sig", "h_SMAA_3sig", "ratio_smaa",
                     "ratio_smia", "d_theta", "elongated", "ratio_ra", "ratio_dec",
                     "d_pos_arcsec_geometric",
                     "gated", "pass"]].to_string(index=False))
        el = res[res["elongated"]]
        rep("orientation: |d_theta| where SMAA > 1.2 SMIA:", _stats(el["d_theta"], "deg"))
        rep("(positions are geometric here and astrometric in Horizons: d_pos is info only)")
    rep(f"SBDB+Horizons requests: {polite.n}")
    for name in ("fd", "coarse"):
        g = res[(res["method"] == name) & res["gated"]] if len(res) else res
        if not len(g):
            rep(f"GATE [{name}]: nothing gated")
            continue
        rep(f"GATE [{name}]: SMAA within {args.rel_tol:.0%} where 1-sigma < {args.gate_below} arcsec: "
            f"{'PASS' if g['pass'].all() else 'FAIL'} ({len(g)} gated)")
    if not have_coarse:
        rep("propagate.coarse not implemented: WP2 not checked (INCOMPLETE)")
    rep.write(res)
    g = res[(res["method"] == "coarse") & res["gated"]] if len(res) else res
    fd_ok = bool(res.loc[(res["method"] == "fd") & res["gated"], "pass"].all()) if len(res) else True
    if not len(g):
        return 3 if fd_ok else 1
    return 0 if g["pass"].all() and fd_ok else 1


# ---------------------------------------------------------------------------
# 6. Brute force: coarse-pass safety
# ---------------------------------------------------------------------------

def twobody_state_helio_ecl(q, e, inc, node, peri, dt_peri, mu=GM_SUN):
    """Vectorized heliocentric ecliptic state (X [AU], V [AU/day]), each
    (N, 3), from cometary elements (angles in deg; dt_peri = t -
    peri_time [day]); e != 1. Used for the prefilter's states at epoch."""
    q, e, dt = (np.asarray(x, float) for x in (q, e, dt_peri))
    a = q / (1.0 - e)
    n = np.sqrt(mu / np.abs(a) ** 3)
    M = n * dt
    x, y, vx, vy = (np.empty_like(q) for _ in range(4))
    ell = e < 1
    if ell.any():
        Me = np.mod(M[ell] + np.pi, 2 * np.pi) - np.pi
        ee = e[ell]
        E = Me + 0.85 * ee * np.sign(np.sin(Me))
        for _ in range(60):
            dE = (E - ee * np.sin(E) - Me) / (1 - ee * np.cos(E))
            E -= dE
            if np.all(np.abs(dE) < 1e-13):
                break
        ae, b_ = a[ell], a[ell] * np.sqrt(1 - ee * ee)
        Edot = n[ell] / (1 - ee * np.cos(E))
        x[ell], y[ell] = ae * (np.cos(E) - ee), b_ * np.sin(E)
        vx[ell], vy[ell] = -ae * np.sin(E) * Edot, b_ * np.cos(E) * Edot
    hyp = ~ell
    if hyp.any():
        Mh, eh = M[hyp], e[hyp]
        H = np.sign(Mh) * np.log(2 * np.abs(Mh) / eh + 1.8)
        for _ in range(200):
            dH = (eh * np.sinh(H) - H - Mh) / (eh * np.cosh(H) - 1)
            H -= dH
            if np.all(np.abs(dH) < 1e-13 * np.maximum(1, np.abs(H))):
                break
        ah = -a[hyp]
        b_ = ah * np.sqrt(eh * eh - 1)
        Hdot = n[hyp] / (eh * np.cosh(H) - 1)
        x[hyp], y[hyp] = ah * (eh - np.cosh(H)), b_ * np.sinh(H)
        vx[hyp], vy[hyp] = -ah * np.sinh(H) * Hdot, b_ * np.cosh(H) * Hdot
    i, Om, w = (np.deg2rad(np.asarray(v, float)) for v in (inc, node, peri))
    cO, sO, ci, si, cw, sw = np.cos(Om), np.sin(Om), np.cos(i), np.sin(i), np.cos(w), np.sin(w)
    P = np.stack([cO * cw - sO * sw * ci, sO * cw + cO * sw * ci, sw * si], axis=-1)
    Q = np.stack([-cO * sw - sO * cw * ci, -sO * sw + cO * cw * ci, cw * si], axis=-1)
    return x[:, None] * P + y[:, None] * Q, vx[:, None] * P + vy[:, None] * Q


def angle_between(a, b):
    """Angle [rad] between vectors (..., 3), accurate at small angles too
    (atan2 of |a x b| and a.b, not arccos of the dot product)."""
    a, b = np.asarray(a, float), np.asarray(b, float)
    return np.arctan2(np.linalg.norm(np.cross(a, b), axis=-1), np.sum(a * b, axis=-1))


def bary_states_at_epoch(orbits, ephem):
    """Barycentric ICRF states (N, 6) at each orbit's epoch, vectorized (the
    same conversion as ssp.ephem_assist.elements_row_to_bary_icrf), and the
    epochs in ASSIST time."""
    ep = orbits["epoch_mjd"].to_numpy(float)
    X, Vv = twobody_state_helio_ecl(orbits["q"], orbits["e"], orbits["i"], orbits["node"],
                                    orbits["argperi"], ep - orbits["peri_time"].to_numpy(float))
    t0 = np.empty(len(ep))
    sun = np.empty((len(ep), 6))
    uniq, inv = np.unique(ep, return_inverse=True)
    t_u = tt_to_assist(uniq)
    for k, t in enumerate(t_u):
        sp = ephem.get_particle(ASSIST_SUN, float(t))
        sun[inv == k] = (sp.x, sp.y, sp.z, sp.vx, sp.vy, sp.vz)
        t0[inv == k] = t
    return np.hstack([X @ R_ECL2EQ.T, Vv @ R_ECL2EQ.T]) + sun, t0


_PF = {}


def _pf_chunk(b0, b1):
    """Worker: plain ASSIST integration of prefilter batches b0..b1 (test
    particles sharing an epoch) to the visit times. Returns, per batch, the
    orbit indices and their geometric topocentric unit vectors (n, K, 3)
    and barycentric speeds (n, K) [AU/day]."""
    global _BF_EPHEM
    import assist
    import rebound
    if _BF_EPHEM is None:
        _BF_EPHEM = open_ephem()
    t, obs = _PF["t"], _PF["obs_pos"]
    out = []
    for idx in _PF["batches"][b0:b1]:
        st, t0 = _PF["state"][idx], float(_PF["t0"][idx[0]])
        u_ = np.full((len(idx), len(t), 3), np.nan)
        spd = np.full((len(idx), len(t)), np.nan)
        for side in (np.flatnonzero(t >= t0), np.flatnonzero(t < t0)[::-1]):
            if not len(side):
                continue
            sim = rebound.Simulation()
            sim.t = t0
            ax = assist.Extras(sim, _BF_EPHEM)
            for x in st:
                sim.add(x=x[0], y=x[1], z=x[2], vx=x[3], vy=x[4], vz=x[5])
            sim.ri_ias15.adaptive_mode = 2
            xyz = np.empty((len(idx), 3))
            vxyz = np.empty((len(idx), 3))
            for j in side[np.argsort(np.abs(t[side] - t0), kind="stable")]:
                try:
                    ax.integrate_or_interpolate(float(t[j]))
                except Exception:
                    break
                sim.serialize_particle_data(xyz=xyz, vxvyvz=vxyz)
                rho = xyz - obs[j]
                u_[:, j] = rho / np.linalg.norm(rho, axis=1)[:, None]
                spd[:, j] = np.linalg.norm(vxyz, axis=1)
            del ax, sim
        out.append((idx, u_, spd))
    return out


def assist_prefilter(orbits, state, t0, t_assist, obs_pos, ephem, workers, batch=64, neo_batch=8):
    """Geometric topocentric directions (N, K, 3) and barycentric speeds
    (N, K) of every orbit at the K times, by plain ASSIST integrations of
    ``batch`` test particles sharing an epoch (``neo_batch`` for q < 1.3,
    whose close approaches force small steps on the whole batch). NaN
    where an integration failed."""
    key = pd.DataFrame({"t0": t0, "neo": orbits["q"].to_numpy(float) < 1.3})
    batches = []
    for (_, neo), g in key.groupby(["t0", "neo"], sort=True):
        ix = g.index.to_numpy()
        n = neo_batch if neo else batch
        batches += [ix[s:s + n] for s in range(0, len(ix), n)]
    _PF.update(batches=batches, state=state, t0=t0, t=np.asarray(t_assist, float), obs_pos=obs_pos)
    try:
        chunks = util.balanced_chunks(np.array([len(b) for b in batches]), 16 * max(workers, 1))
        if workers > 1 and util.fork_context() is not None:
            res = util.run_chunks(_pf_chunk, chunks, workers, "prefilter")
        else:
            res = [_pf_chunk(a, b) for a, b in chunks]
    finally:
        _PF.clear()
    U = np.full((len(orbits), len(t_assist), 3), np.nan)
    S = np.full((len(orbits), len(t_assist)), np.nan)
    for part in res:
        for idx, u_, spd in part:
            U[idx], S[idx] = u_, spd
    return U, S


def match_within(pred_ra, pred_dec, dia_ra, dia_dec, radius=RADIUS):
    """Every (prediction k, DiaSource h) pair within ``radius`` arcsec, by a
    KD tree on unit vectors (independent of WP3's HEALPix index); returns
    (k, h, sep [arcsec]), separations as ssp.util.sky_separation_arcsec.
    ``radius``: one radius, or one per prediction (each prediction's
    object's match radius)."""
    from scipy.spatial import cKDTree
    if len(pred_ra) == 0 or len(dia_ra) == 0:
        return np.zeros(0, int), np.zeros(0, int), np.zeros(0)
    radius = np.broadcast_to(np.asarray(radius, dtype=np.float64), np.shape(pred_ra))
    chord = 2 * np.sin(np.deg2rad(radius / 3600.0) / 2) * (1 + 1e-6)
    tree = cKDTree(radec_to_vec(dia_ra, dia_dec))
    hits = tree.query_ball_point(radec_to_vec(pred_ra, pred_dec), chord)
    k = np.repeat(np.arange(len(hits)), [len(x) for x in hits])
    h = np.fromiter((i for x in hits for i in x), dtype=np.int64, count=len(k))
    sep = util.sky_separation_arcsec(np.asarray(pred_ra)[k], np.asarray(pred_dec)[k],
                                     np.asarray(dia_ra)[h], np.asarray(dia_dec)[h])
    m = sep <= radius[k]
    return k[m], h[m], sep[m]


def expected_nearest(pairs, sigma_max=SIGMA_MAX, radius=None):
    """From all (diaSourceId, designation, sep, sigma) pairs within each
    object's match radius (``radius``: None for `radius_of` per object, or
    one for all), the nearest *eligible* object per DiaSource (sigma <= max;
    nearest in arcsec, whatever the radii; ties by designation). Where some
    candidate's sigma is unknown (NaN) and no eligible one is nearer, the
    row is flagged ``sigma_unknown``."""
    p = pd.DataFrame(pairs)
    p = p[p["sep"].to_numpy(float) <= radius_of(p["designation"].to_numpy(), radius)]
    unk = ~np.isfinite(p["sigma"].to_numpy(float))
    keep = unk | (p["sigma"].to_numpy(float) <= sigma_max)
    p = p[keep].assign(sigma_unknown=unk[keep])
    p = p.sort_values(["diaSourceId", "sep", "designation"], kind="stable")
    return p.drop_duplicates("diaSourceId").reset_index(drop=True)


def compare_expected(expected, nss):
    """Expected nearest matches vs NearbySSO rows of the same DiaSources.
    Status: found, missing, wrong_object (NearbySSO has another), and for
    NearbySSO rows not expected, extra (or extra_beyond_radius: its
    ephOffset exceeds its object's match radius, so it's wrong)."""
    e = expected.merge(nss[["diaSourceId", "designation", "ephOffset"]].rename(
        columns={"designation": "nss_designation", "ephOffset": "nss_ephOffset"}),
        on="diaSourceId", how="left")
    has = pd.notna(e["nss_designation"]).to_numpy()
    same = has & (e["nss_designation"].to_numpy(dtype=object) == e["designation"].to_numpy(dtype=object))
    st = np.where(same, "found", np.where(has, "wrong_object", "missing")).astype(object)
    unk = e["sigma_unknown"].to_numpy(bool)
    st[unk & ~same] = [s + "_sigma_unknown" for s in st[unk & ~same]]
    e["status"] = st
    extra = nss[~nss["diaSourceId"].isin(expected["diaSourceId"])].assign(status="extra")
    extra.loc[beyond_radius(extra), "status"] = "extra_beyond_radius"
    return pd.concat([e, extra], ignore_index=True)


_BF = {}
_BF_EPHEM = None


def _bf_chunk(k0, k1):
    """Worker: exact ephemerides for candidate objects k0..k1."""
    global _BF_EPHEM
    if _BF_EPHEM is None:
        _BF_EPHEM = open_ephem()
    orbits, cand = _BF["orbits"], _BF["cand"]
    t, pos, vel = _BF["t"], _BF["obs_pos"], _BF["obs_vel"]
    out = []
    for k in range(k0, k1):
        oi, vis = cand[k]
        row = orbits.iloc[oi]
        try:
            e = compute_ephemerides_one(row["designation"], Time(t[vis], format="mjd", scale="tai"),
                                        None, _BF_EPHEM, row=row, obs_pos=pos[vis].T, obs_vel=vel[vis].T)
        except Exception:
            continue
        for j, v in enumerate(vis):
            out.append((oi, v, e.ra_deg[j], e.dec_deg[j]))
    return out


def cmd_brute_force(args):
    rep = Report("brute-force", args.out)
    rng = np.random.default_rng(args.seed)
    vis_all = np.unique(pq.read_table(args.dia, columns=["visit"]).column(0).to_numpy())
    if args.visits:
        visits = np.array([int(v) for v in args.visits.split(",")])
    else:
        visits = np.sort(rng.choice(vis_all, size=min(args.n_visits, len(vis_all)), replace=False))
    dia = read_dia_subset(args.dia, visits=visits)
    V, dia = derive_visits(dia)
    rep(f"# brute force over {len(V)} visits ({len(dia):,} DiaSources): {list(V['visit'])}")
    obs_pos, obs_vel = observer_states(V["t_tai_mjd"].to_numpy())
    ephem = open_ephem()
    t_vis = tai_to_assist(V["t_tai_mjd"].to_numpy())

    orbits = read_orbits(args.orbits)
    reason = filter_reason(orbits)
    orbits = orbits[reason == ""].reset_index(drop=True)
    rep(f"orbits kept by the filter: {len(orbits):,}")
    e = orbits["e"].to_numpy(float)
    near_parabolic = np.abs(1 - e) < 1e-6
    t0_clock = time.monotonic()
    state, t0 = bary_states_at_epoch(orbits.assign(e=np.where(near_parabolic, 0.5, e)), ephem)
    # ASSIST's own perturbers (Pluto and the 16 asteroids; all have H < 8)
    # get special handling in compute_ephemerides_one: always exact
    from ssp.ephem_assist import self_perturber
    big = np.flatnonzero(orbits["h"].to_numpy(float) < 8)
    selfp = np.zeros(len(orbits), bool)
    selfp[big] = [self_perturber(state[k, :3], state[k, 3:], t0[k], ephem) is not None for k in big]
    always = near_parabolic | selfp
    rep(f"always exact: {int(near_parabolic.sum())} near-parabolic, {int(selfp.sum())} ASSIST perturbers")

    # The prefilter: plain ASSIST integrations (the exact pass's force
    # model) to the visit times, geometric. Its only difference from the
    # exact (light-time corrected) direction is the light-time shift,
    # |V_perp| tau / Delta <= |V| / c (plus O(tau^2)), bounded per orbit
    # from its speed; --margin-deg covers the rest, and the calibration
    # below measures it.
    pf = np.flatnonzero(~always)
    U, S = assist_prefilter(orbits.iloc[pf].reset_index(drop=True), state[pf], t0[pf], t_vis, obs_pos,
                            ephem, args.workers, args.batch)
    lt_bound = S / C_AU_PER_DAY * 1.01           # rad
    failed = ~np.isfinite(U[:, :, 0])
    rep(f"prefilter: {len(pf):,} orbits x {len(V)} visits in {time.monotonic() - t0_clock:.0f} s; "
        f"{int(failed.any(axis=1).sum())} failed integrations (-> exact); light-time bound max "
        f"{np.degrees(np.nanmax(lt_bound)):.3g} deg")
    margin = np.deg2rad(args.margin_deg)
    cand = {int(oi): list(range(len(V))) for oi in np.flatnonzero(always)}
    for j in range(len(V)):
        cen = np.asarray(V["center"].iloc[j], float)
        ang = angle_between(U[:, j], cen[None, :])
        sel = (ang - np.nan_to_num(lt_bound[:, j]) < V["radius"].iloc[j] + margin) | failed[:, j]
        for oi in pf[sel]:
            cand.setdefault(int(oi), []).append(j)
        rep(f"  visit {V['visit'].iloc[j]}: {int(sel.sum()):,} prefilter candidates")

    # calibration: random orbits of the kind the prefilter handles, exact at
    # every sampled visit time, against the prefilter's directions
    cal_k = rng.choice(len(pf), size=min(args.calibrate, len(pf)), replace=False)
    items = sorted(cand.items()) + [(int(pf[k]), list(range(len(V)))) for k in cal_k]
    _BF.update(orbits=orbits, cand=items, t=V["t_tai_mjd"].to_numpy(), obs_pos=obs_pos, obs_vel=obs_vel)
    chunks = util.balanced_chunks(np.array([len(v) for _, v in items]), 8 * args.workers)
    t1_clock = time.monotonic()
    try:
        if args.workers > 1 and util.fork_context() is not None:
            res = util.run_chunks(_bf_chunk, chunks, args.workers, "exact ephemerides")
        else:
            res = [_bf_chunk(a, b) for a, b in chunks]
    finally:
        _BF.clear()
    exact = pd.DataFrame([r for part in res for r in part], columns=["oi", "vj", "ra", "dec"])
    exact = exact.drop_duplicates(["oi", "vj"]).reset_index(drop=True)
    rep(f"exact ephemerides: {len(items):,} objects in {time.monotonic() - t1_clock:.0f} s")
    pos_of = np.full(len(orbits), -1)
    pos_of[pf] = np.arange(len(pf))
    cal = exact[np.isin(exact["oi"].to_numpy(), pf[cal_k])]
    k_ = pos_of[cal["oi"].to_numpy()]
    j_ = cal["vj"].to_numpy()
    ue = radec_to_vec(cal["ra"].to_numpy(), cal["dec"].to_numpy())
    err = np.degrees(angle_between(U[k_, j_], ue))
    excess = err - np.degrees(lt_bound[k_, j_])      # what the margin must cover
    ok = np.isfinite(err)
    q = orbits["q"].to_numpy(float)[cal["oi"].to_numpy()]
    rep(f"prefilter calibration on {len(cal_k)} random orbits x {len(V)} visits ({ok.sum():,} points): "
        f"error median {np.median(err[ok]):.3g} deg, 99.9% {np.percentile(err[ok], 99.9):.3g}, "
        f"max {err[ok].max():.3g} deg (q < 1.3: max {np.max(err[ok & (q < 1.3)], initial=0):.3g}); "
        f"beyond the light-time bound: max {np.max(excess[ok], initial=-np.inf):.3g} deg; "
        f"margin {args.margin_deg} deg")
    margin_ok = bool(ok.any() and np.max(excess[ok]) < args.margin_deg / 3)
    if not margin_ok:
        rep("WARNING: prefilter error beyond the light-time bound exceeds margin/3: "
            "the prefilter may not be conservative")
    # (calibration-only objects are matched too; that only adds coverage)

    # match exact predictions to DiaSources (KD tree on unit vectors)
    parts = []
    des_all = orbits["designation"].to_numpy(dtype=object)
    rad_all = radius_of(des_all)
    for j in range(len(V)):
        s, e_ = V["dia_start"].iloc[j], V["dia_end"].iloc[j]
        dv = dia.iloc[s:e_]
        ex = exact[exact["vj"] == j]
        k, h, sep = match_within(ex["ra"].to_numpy(), ex["dec"].to_numpy(),
                                 dv["ra"].to_numpy(), dv["dec"].to_numpy(),
                                 rad_all[ex["oi"].to_numpy()])
        parts.append(pd.DataFrame({
            "diaSourceId": dv["diaSourceId"].to_numpy(dtype=np.int64)[h],
            "designation": des_all[ex["oi"].to_numpy()[k]], "sep": sep,
            "midpointMjdTai": dv["midpointMjdTai"].to_numpy(dtype=np.float64)[h]}))
    pairs = pd.concat(parts, ignore_index=True)
    pairs = pairs[pairs["sep"].to_numpy(float) <= radius_of(pairs["designation"].to_numpy())]
    pairs = pairs.reset_index(drop=True)
    comet = radius_of(pairs["designation"].to_numpy()) > RADIUS
    rep(f"(object, DiaSource) pairs within the object's match radius ({RADIUS:g}\", comets and ISOs "
        f"{C.MATCH_RADIUS_COMET_ARCSEC:g}\"): {len(pairs):,} ({pairs['designation'].nunique():,} objects; "
        f"comets and ISOs: {int(comet.sum()):,} pairs, {pairs['designation'][comet].nunique():,} objects, "
        f"{int((pairs['sep'][comet] > RADIUS).sum()):,} pairs beyond {RADIUS:g}\")")
    oracle = SigmaOracle(args.orbits, ephem)
    pairs["sigma"] = oracle(pairs["designation"].to_numpy(), pairs["midpointMjdTai"].to_numpy())
    if oracle.note:
        rep(oracle.note)
    exp = expected_nearest(pairs)
    nss = _read_nss(args.nearbysso) if args.nearbysso else pd.DataFrame(
        {"diaSourceId": pd.Series([], "i8"), "designation": [], "ephOffset": []})
    nss_v = nss[nss["diaSourceId"].isin(dia["diaSourceId"])]
    cmp = compare_expected(exp, nss_v)
    for k, v in cmp["status"].value_counts().items():
        rep(f"  {k:28s} {v:>8,}")
    n_bad = int(cmp["status"].isin(["missing", "wrong_object", "extra_beyond_radius"]).sum())
    n_unk = int(cmp["status"].str.endswith("_sigma_unknown").sum())
    verdict = "FAIL" if n_bad else ("INCOMPLETE" if n_unk or not margin_ok else "PASS")
    rep(f"GATE: every eligible nearest match is in NearbySSO: {verdict}")
    bad = cmp[cmp["status"].isin(["missing", "wrong_object"])]
    if len(bad):
        rep("missing / wrong_object by object (the first 20):")
        g = bad.groupby("designation").agg(n=("diaSourceId", "size"), sep_min=("sep", "min"),
                                           sep_max=("sep", "max")).sort_values("n", ascending=False)
        for des, r in g.head(20).iterrows():
            rep(f"  {des:24s} {int(r['n']):>6,}  sep {r['sep_min']:.2f}-{r['sep_max']:.2f}\" "
                f"(radius {radius_of([des])[0]:g}\")")
    rep.write(cmp, pairs=pairs)
    return {"PASS": 0, "FAIL": 1}.get(verdict, 3)


# ---------------------------------------------------------------------------
# 7. diaDistanceRank by brute force
# ---------------------------------------------------------------------------

RANK = "diaDistanceRank"
RANK_WITHIN_GATE = (f"the row's DiaSource is within its object's match radius ({C.MATCH_RADIUS_ARCSEC:g}\", "
                    f"comets and ISOs {C.MATCH_RADIUS_COMET_ARCSEC:g}\") of ephRa/ephDec")
RANK_FAULTS = {
    "plus1": "one row's rank increased by 1",
    "swap": "the ranks of two rows of one (object, visit) swapped",
    "rank1": "a rank-2 row set to rank 1",
    "zero": "one rank set to 0",
}


def read_dia_all(path, columns=("diaSourceId", "visit", "ra", "dec")):
    """Every DiaSource row (only ``columns``), sorted by (visit,
    diaSourceId). Rows repeating a diaSourceId (PPDB has some) are kept;
    `brute_force_ranks` counts each id once, at its smallest separation.
    ``attrs``: ``dup_ids`` (the ids on several rows) and ``n_exact_dup``
    (rows identical to an earlier one), for the report."""
    t = pq.read_table(path, columns=list(columns))
    d = pd.DataFrame({c: t[c].to_numpy() for c in columns})
    ids = d["diaSourceId"].to_numpy()
    dup_ids = np.unique(ids[pd.Series(ids).duplicated().to_numpy()])
    n_exact = int(d.duplicated().sum())
    d = d.sort_values(["visit", "diaSourceId"], kind="stable").reset_index(drop=True)
    d.attrs.update(n_exact_dup=n_exact, dup_ids=dup_ids)
    return d


def brute_force_ranks(visit, pred_ra, pred_dec, own_id, dia, radius=RADIUS, chunk=512):
    """For predictions (visit[k], pred_ra[k], pred_dec[k]) and the DiaSource
    each names (own_id[k]): the separation of EVERY DiaSource of that visit
    (``dia``: from `read_dia_all`, sorted by (visit, diaSourceId)), by
    ssp.util.sky_separation_arcsec. A diaSourceId on several rows is one
    DiaSource, at its smallest separation (the contract). Returns a
    DataFrame with, per k:

    - ``own_sep`` [arcsec]: of the named DiaSource (NaN: not in that visit);
    - ``n_closer``: the visit's DiaSources nearer than it, or as near with a
      lower diaSourceId, so its rank is ``n_closer + 1``;
    - ``n_within``: the visit's DiaSources within ``radius`` (one radius,
      or one per prediction: its object's match radius), the rank's pool;
    - ``nearest_id``, ``nearest_sep``: the visit's nearest DiaSource (ties:
      the lower diaSourceId);
    - ``near_dup``: the named DiaSource, or one at most as far, is on
      several rows (so the dedup matters).
    """
    visit = np.asarray(visit, dtype=np.int64)
    pred_ra, pred_dec = np.asarray(pred_ra, np.float64), np.asarray(pred_dec, np.float64)
    own_id = np.asarray(own_id, dtype=np.int64)
    n = len(visit)
    radius = np.broadcast_to(np.asarray(radius, dtype=np.float64), (n,))
    out = {"own_sep": np.full(n, np.nan), "n_closer": np.full(n, -1, np.int64),
           "n_within": np.zeros(n, np.int64), "nearest_id": np.full(n, -1, np.int64),
           "nearest_sep": np.full(n, np.nan), "near_dup": np.zeros(n, bool)}
    dv = dia["visit"].to_numpy(np.int64)
    ids_all = dia["diaSourceId"].to_numpy(np.int64)
    ra_all, dec_all = dia["ra"].to_numpy(np.float64), dia["dec"].to_numpy(np.float64)
    order = np.argsort(visit, kind="stable")
    uv, start = np.unique(visit[order], return_index=True)
    end = np.append(start[1:], n)
    lo, hi = np.searchsorted(dv, uv, "left"), np.searchsorted(dv, uv, "right")
    for v_lo, v_hi, s, e in zip(lo, hi, start, end):
        if v_hi == v_lo:
            continue
        ids_rows, ra, dec = ids_all[v_lo:v_hi], ra_all[v_lo:v_hi], dec_all[v_lo:v_hi]
        ids, first, mult = np.unique(ids_rows, return_index=True, return_counts=True)   # ids_rows sorted
        dup = mult > 1
        for c0 in range(s, e, chunk):
            k = order[c0:min(e, c0 + chunk)]
            S = util.sky_separation_arcsec(pred_ra[k][:, None], pred_dec[k][:, None],
                                           ra[None, :], dec[None, :])
            if dup.any():
                S = np.minimum.reduceat(S, first, axis=1)       # one column per distinct id
            pos = np.minimum(np.searchsorted(ids, own_id[k]), len(ids) - 1)
            found = ids[pos] == own_id[k]
            own = np.where(found, S[np.arange(len(k)), pos], np.nan)
            closer = (S < own[:, None]) | ((S == own[:, None]) & (ids[None, :] < own_id[k][:, None]))
            out["own_sep"][k] = own
            out["n_closer"][k] = np.where(found, closer.sum(axis=1), -1)
            out["n_within"][k] = (S <= radius[k][:, None]).sum(axis=1)
            out["near_dup"][k] = ((S <= own[:, None]) & dup[None, :]).any(axis=1)
            j = np.argmin(S, axis=1)      # the first minimum: the lowest id (ids ascending)
            out["nearest_id"][k] = ids[j]
            out["nearest_sep"][k] = S[np.arange(len(k)), j]
    return pd.DataFrame(out)


def rank_order_violations(designation, visit, sep, did, rank):
    """Within each (object, visit), ranks must be distinct and increasing in
    (sep, diaSourceId). Returns a boolean mask of the rows of violating
    groups."""
    df = pd.DataFrame({"des": designation, "visit": visit, "sep": sep, "id": did, "rank": rank})
    df = df.sort_values(["des", "visit", "sep", "id"], kind="stable")
    same = (df["des"].to_numpy() == np.roll(df["des"].to_numpy(), 1)) & \
           (df["visit"].to_numpy() == np.roll(df["visit"].to_numpy(), 1))
    if len(same):
        same[0] = False
    r = df["rank"].to_numpy()
    bad_row = same & (r <= np.roll(r, 1))
    g = df.groupby(["des", "visit"], sort=False).ngroup().to_numpy()
    bad_groups = np.unique(g[bad_row])
    mask = np.zeros(len(df), bool)
    mask[df.index.to_numpy()[np.isin(g, bad_groups)]] = True
    return mask


def _nss_with_visits(path, dia):
    """NearbySSO rows (with the rank, if present) and each row's visit and
    DiaSource position, from the DiaSource table ``dia``."""
    present = pq.read_schema(path).names
    cols = [c for c in ("diaSourceId", "designation", "ephRa", "ephDec", "ephOffset", RANK) if c in present]
    t = pq.read_table(path, columns=cols)
    nss = pd.DataFrame({c: t[c].to_numpy(zero_copy_only=False) for c in cols if c != RANK})
    rank_type = t.schema.field(RANK).type if RANK in cols else None
    if rank_type is not None:
        r = t[RANK]
        nss[RANK] = pc.fill_null(r, -1).to_numpy()
        nss["rank_null"] = r.is_null().to_numpy(zero_copy_only=False)
    # every DiaSource row of each NearbySSO row's id; the separation is the
    # smallest over them (an id on several rows is one DiaSource)
    ids = dia["diaSourceId"].to_numpy()
    q = nss["diaSourceId"].to_numpy()
    by_id = np.argsort(ids, kind="stable")
    ids_sorted = ids[by_id]
    lo, hi = np.searchsorted(ids_sorted, q, "left"), np.searchsorted(ids_sorted, q, "right")
    cnt = hi - lo
    nss["n_dia_rows"] = cnt
    ok = cnt > 0
    row = np.repeat(np.arange(len(nss)), cnt)
    offs = np.arange(len(row)) - np.repeat(np.cumsum(cnt) - cnt, cnt)
    drow = by_id[lo[row] + offs]
    sep_rows = util.sky_separation_arcsec(nss["ephRa"].to_numpy()[row], nss["ephDec"].to_numpy()[row],
                                          dia["ra"].to_numpy()[drow], dia["dec"].to_numpy()[drow])
    sep = np.full(len(nss), np.inf)
    np.minimum.at(sep, row, sep_rows)
    nss["sep"] = np.where(ok, sep, np.nan)
    visit = np.full(len(nss), -1, np.int64)
    visit[row] = dia["visit"].to_numpy()[drow]
    nss["visit"] = visit
    nss["dia_row"] = np.where(ok, 0, -1)
    return nss, rank_type


def _hist(values, top=12):
    vc = pd.Series(values).value_counts().sort_index()
    head = vc.iloc[:top]
    s = ", ".join(f"{k}: {v:,}" for k, v in head.items())
    if len(vc) > top:
        s += f", ... ({int(vc.iloc[top:].sum()):,} more, max {vc.index.max()})"
    return s


def check_rank(nss_path, dia_path, orbits_path=None, n=100_000, seed=42, rep=None, sep_rtol=1e-6):
    """The rank checks (see the `rank` subcommand). Returns (verdict, the
    Report, the sampled rows with their brute-force values, or None); the
    Report's ``gates`` lists (name, passed)."""
    rep = rep or Report("rank", None)
    gates = rep.gates = []

    def gate(name, ok, detail=""):
        gates.append((name, bool(ok)))
        rep(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f": {detail}" if detail else ""))

    rep("# NearbySSO diaDistanceRank by brute force against the DiaSources")
    rep(f"nearbysso={nss_path}\ndia={dia_path}" + (f"\norbits={orbits_path}" if orbits_path else ""))
    dia = read_dia_all(dia_path)
    nss, rank_type = _nss_with_visits(nss_path, dia)
    rep(f"NearbySSO: {len(nss):,} rows; DiaSources: {len(dia):,} in {dia['visit'].nunique():,} visits")
    dup_ids = dia.attrs["dup_ids"]
    if len(dup_ids):
        rep(f"DiaSource rows repeating a diaSourceId: {len(dup_ids):,} ids "
            f"({dia.attrs['n_exact_dup']:,} rows exact repeats); {int((nss['n_dia_rows'] > 1).sum()):,} "
            f"NearbySSO rows name one. Each id counts once, at its smallest separation.")
    if rank_type is None:
        gate(f"{RANK} column present", False, "missing")
        return "FAIL", rep, None
    r = nss[RANK].to_numpy()
    gate(f"{RANK} is int16 (short), non-null, >= 1",
         rank_type == pa.int16() and not nss["rank_null"].any() and (r[~nss["rank_null"]] >= 1).all(),
         f"type {rank_type}, {int(nss['rank_null'].sum()):,} NULL, "
         f"{int(np.sum((r < 1) & ~nss['rank_null'].to_numpy())):,} < 1")
    rep(f"ranks in the file: {_hist(r)}")
    found = nss["dia_row"].to_numpy() >= 0
    gate("every row's DiaSource is in the DiaSource file", found.all(), f"{int((~found).sum()):,} missing")

    # every row: in each (object, visit), distinct ranks in (sep, id) order
    f = nss[found]
    viol = rank_order_violations(f["designation"].to_numpy(), f["visit"].to_numpy(), f["sep"].to_numpy(),
                                 f["diaSourceId"].to_numpy(), f[RANK].to_numpy())
    n_groups = f.groupby(["designation", "visit"]).ngroups
    multi = int((f.groupby(["designation", "visit"]).size() > 1).sum())
    gate("within each (object, visit), ranks distinct and increasing with (separation, diaSourceId)",
         not viol.any(), f"{n_groups:,} (object, visit) groups, {multi:,} with several rows; "
         f"{int(viol.sum()):,} rows in violating groups"
         + (f" (e.g. {f['diaSourceId'].to_numpy()[viol][:3].tolist()})" if viol.any() else ""))

    # the brute-force sample
    rng = np.random.default_rng(seed)
    cand = np.flatnonzero(found)
    pick = np.sort(cand if not n or n >= len(cand) else rng.choice(cand, n, replace=False))
    s = nss.iloc[pick].reset_index(drop=True)
    rep(f"sample: {len(s):,} rows (seed {seed}) in {s['visit'].nunique():,} visits")
    t0 = time.time()
    rad = radius_of(s["designation"].to_numpy())
    bf = brute_force_ranks(s["visit"], s["ephRa"], s["ephDec"], s["diaSourceId"], dia, radius=rad)
    rep(f"brute force: every DiaSource of each sampled row's visit, {time.time() - t0:.1f} s")
    s = pd.concat([s, bf], axis=1)
    s["expected_rank"] = s["n_closer"] + 1
    nd = s["near_dup"].to_numpy()
    if nd.any():
        rep(f"sampled rows whose rank involves a repeated diaSourceId: {int(nd.sum()):,} "
            f"(rank as brute force: {int((s[RANK][nd] == s['expected_rank'][nd]).sum()):,})")

    within = s["own_sep"].to_numpy() <= rad
    eo = s["ephOffset"].to_numpy(np.float64)
    with np.errstate(invalid="ignore"):
        eo_ok = np.abs(eo - s["own_sep"].to_numpy()) <= sep_rtol * np.maximum(s["own_sep"].to_numpy(), 1e-3)
    gate(RANK_WITHIN_GATE, within.all(),
         f"{int((~within).sum()):,} not" + (f" (e.g. {s['diaSourceId'][~within].head(3).tolist()})"
                                            if not within.all() else ""))
    ex = list(zip(s["diaSourceId"][~eo_ok].head(3), eo[~eo_ok][:3], s["own_sep"][~eo_ok].head(3)))
    gate(f"ephOffset == its separation (rel. {sep_rtol:g})", eo_ok.all(),
         f"{int((~eo_ok).sum()):,} differ" + (f" (e.g. {ex})" if ex else ""))

    got, want = s[RANK].to_numpy(), s["expected_rank"].to_numpy()
    bad = got != want
    detail = f"{len(s):,} rows; {int(bad.sum()):,} differ"
    if bad.any():
        b = s[bad].head(5)
        detail += " (diaSourceId designation: file vs brute force) " + "; ".join(
            f"{r.diaSourceId} {r.designation}: {r[RANK]} vs {r.expected_rank}" for _, r in b.iterrows())
    gate(f"{RANK} == 1 + the DiaSources of the visit nearer the prediction (ties: lower diaSourceId)",
         not bad.any(), detail)
    one = got == 1
    not_nearest = one & ((s["nearest_id"].to_numpy() != s["diaSourceId"].to_numpy()))
    gate("rank-1 rows: no DiaSource of the visit is nearer the prediction", not not_nearest.any(),
         f"{int(one.sum()):,} rank-1 rows sampled; {int(not_nearest.sum()):,} with a nearer one"
         + (f" (e.g. {s['diaSourceId'][not_nearest].head(3).tolist()})" if not_nearest.any() else ""))
    rep(f"brute-force ranks (sample): {_hist(want)}")
    rep(f"DiaSources within the object's match radius of the prediction (sample): {_hist(s['n_within'])}")
    comet = rad > RADIUS
    if comet.any():
        rep(f"sampled rows of comets and ISOs (radius {C.MATCH_RADIUS_COMET_ARCSEC:g}\"): "
            f"{int(comet.sum()):,}; ranks {_hist(want[comet])}; "
            f"beyond {RADIUS:g}\": {int((s['own_sep'][comet] > RADIUS).sum()):,}")
    rep(f"rows whose rank > 1 (their object has a nearer DiaSource, named by another row or not "
        f"in NearbySSO): {int(np.sum(want > 1)):,} of {len(s):,} ({100 * np.mean(want > 1):.2f}%)")

    if orbits_path:
        orbits = read_orbits(orbits_path, designations=set(s["designation"]))
        reasons = reasons_for(s["designation"].to_numpy(), reason_lookup(orbits))
        rep(f"sampled rows' objects by orbit-filter reason ('' kept): "
            f"{pd.Series(reasons).value_counts().to_dict()}")

    ok = all(g for _, g in gates)
    rep(f"GATE (every rank check): {'PASS' if ok else 'FAIL'} "
        f"({sum(not g for _, g in gates)} of {len(gates)} failed)")
    return ("PASS" if ok else "FAIL"), rep, s


def cmd_rank(args):
    rep = Report("rank", args.out)
    verdict, rep, s = check_rank(args.nearbysso, args.dia, args.orbits, args.n, args.seed, rep)
    if s is not None:
        rep.write(s.drop(columns=["dia_row", "rank_null"], errors="ignore"))
    else:
        rep.write()
    return 0 if verdict == "PASS" else 1


def add_ranks(nss_path, dia_path, out_path, faults=(), seed=42):
    """(development) The NearbySSO file with diaDistanceRank computed by
    brute force (`brute_force_ranks`), in the schema's column order, with
    optional injected RANK_FAULTS. Returns (table, faults DataFrame)."""
    for f in faults:
        if f not in RANK_FAULTS:
            raise ValueError(f"unknown fault {f}; known: {sorted(RANK_FAULTS)}")
    dia = read_dia_all(dia_path)
    t = pq.read_table(nss_path)
    nss, _ = _nss_with_visits(nss_path, dia)
    if (nss["dia_row"] < 0).any():
        raise ValueError("NearbySSO rows whose DiaSource is not in the DiaSource file")
    bf = brute_force_ranks(nss["visit"], nss["ephRa"], nss["ephDec"], nss["diaSourceId"], dia)
    rank = (bf["n_closer"].to_numpy() + 1).astype(np.int16)
    ok = np.ones(len(rank), bool)
    rng = np.random.default_rng(seed)
    injected = []
    if "plus1" in faults:
        i = int(rng.choice(np.flatnonzero(ok)))
        rank[i] += 1
        injected.append(("plus1", i))
    if "rank1" in faults:
        i = int(rng.choice(np.flatnonzero(ok & (rank == 2))))
        rank[i] = 1
        injected.append(("rank1", i))
    if "zero" in faults:
        i = int(rng.choice(np.flatnonzero(ok)))
        rank[i] = 0
        injected.append(("zero", i))
    if "swap" in faults:
        g = nss.assign(r=rank).groupby(["designation", "visit"])
        groups = [ix for ix in g.indices.values() if len(ix) > 1 and ok[ix].all()]
        ix = groups[int(rng.integers(len(groups)))]
        a, b = int(ix[0]), int(ix[1])
        rank[a], rank[b] = rank[b], rank[a]
        injected += [("swap", a), ("swap", b)]
    if RANK in t.column_names:
        t = t.drop_columns([RANK])
    t = t.append_column(pa.field(RANK, pa.int16(), nullable=False), pa.array(rank, pa.int16()))
    t = t.select([c for c in NSS_COLUMNS if c in t.column_names]
                 + [c for c in t.column_names if c not in NSS_COLUMNS])
    pq.write_table(t, out_path, compression="zstd")
    fl = pd.DataFrame([(f, int(nss["diaSourceId"].iloc[i])) for f, i in injected],
                      columns=["fault", "diaSourceId"])
    return t, fl


def cmd_mock_rank(args):
    t, faults = add_ranks(args.nearbysso, args.dia, args.output, args.fault, args.seed)
    faults.to_parquet(args.output + ".faults.parquet", index=False)
    print(f"wrote {len(t):,} rows to {args.output}; faults: {faults.to_dict('records') or 'none'}")
    return 0


# ---------------------------------------------------------------------------
# Development: a NearbySSO faked from SSObservation, with injected faults
# ---------------------------------------------------------------------------

def mock_from_ssobservation(sss, dia, reason_of, rng=None, n_drop=0, n_perturb=0):
    """A NEARBYSSO_DTYPE table from SSObservation rows that NearbySSO should
    contain (object kept by the filter, prediction within its match
    radius), nearest per DiaSource, zero ellipses; then ``n_drop`` rows
    removed and ``n_perturb`` positions shifted by 1-10 mas. Returns
    (table, injected faults)."""
    rng = np.random.default_rng(rng)
    d = pd.DataFrame(sss).merge(pd.DataFrame(dia)[["diaSourceId", "ra", "dec"]], on="diaSourceId")
    d = d[np.isfinite(d["ephRa"].to_numpy(float))]
    d["ephOffset"] = util.sky_separation_arcsec(d["ephRa"], d["ephDec"], d["ra"], d["dec"])
    d = d[(reasons_for(d["designation"].to_numpy(), reason_of) == "")
          & (d["ephOffset"].to_numpy(float) <= radius_of(d["designation"].to_numpy()))]
    d = d.sort_values(["diaSourceId", "ephOffset", "designation"]).drop_duplicates("diaSourceId")
    out = pd.DataFrame({c: d[c].to_numpy() if c in d else 0 for c in NSS_COLUMNS})
    for c in ("ephRaErr", "ephDecErr", "ephRa_ephDec_Cov"):
        out[c] = np.float32(0)
    out = out.astype({n: C.NEARBYSSO_DTYPE[n].str if C.NEARBYSSO_DTYPE[n].kind != "U" else object
                      for n in NSS_COLUMNS})
    faults = []
    idx = rng.choice(len(out), size=min(n_drop + n_perturb, len(out)), replace=False)
    for k in idx[n_drop:]:
        out.loc[k, "ephDec"] += rng.uniform(1, 10) / 3.6e6
        faults.append(("perturb", int(out.loc[k, "diaSourceId"])))
    faults += [("drop", int(out.loc[k, "diaSourceId"])) for k in idx[:n_drop]]
    out = out.drop(index=idx[:n_drop]).reset_index(drop=True)
    return out, pd.DataFrame(faults, columns=["fault", "diaSourceId"])


def cmd_mock(args):
    sss = _read_sss(args.ssobservation, processing=args.sss_processing)
    dia = read_dia_subset(args.dia, ids=sss["diaSourceId"].to_numpy())
    orbits = read_orbits(args.orbits, designations=set(sss["designation"].dropna()))
    out, faults = mock_from_ssobservation(sss, dia, reason_lookup(orbits), args.seed, args.drop, args.perturb)
    out.to_parquet(args.output, index=False)
    faults.to_parquet(args.output + ".faults.parquet", index=False)
    print(f"wrote {len(out):,} rows to {args.output}; {len(faults)} faults injected")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _horizons_args(p, max_queries):
    p.add_argument("--max-queries", type=int, default=max_queries,
                   help="hard cap on JPL requests this run")
    p.add_argument("--min-interval", type=float, default=1.5, help="seconds between JPL requests (>= 1)")
    p.add_argument("--query-log", default=None, help="append one line per JPL request here")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p, nss=True, dia=True, orbits=True):
        if nss:
            p.add_argument("--nearbysso", required=True)
        if dia:
            p.add_argument("--dia", required=True, help="DiaSource Parquet")
        if orbits:
            p.add_argument("--orbits", required=True, help="mpc_orbits Parquet")
        p.add_argument("--out", required=True, help="report directory")
        p.add_argument("--seed", type=int, default=42)

    p = sub.add_parser("same-orbits", help="vs SSObservation from the same mpc_orbits")
    common(p)
    p.add_argument("--ssobservation", required=True,
                   help="SSObservation: the delivery directory, its manifest, or a single Parquet file")
    p.add_argument("--no-sigma", action="store_true", help="don't compute sigma (classify as unknown)")
    p.add_argument("--pos-tol-mas", type=float, default=TOL["pos_mas"])
    p.add_argument("--rate-tol", type=float, default=TOL["rate_deg_day"], help="deg/day")
    p.add_argument("--vmag-tol", type=float, default=TOL["vmag"])
    p.add_argument("--sss-processing", default=SSS_PROCESSING,
                   help="SSObservation: the processing to compare (default: %(default)s; 'all')")
    p.set_defaults(func=cmd_same_orbits)

    p = sub.add_parser("dp2-intersection", help="vs DP2 SSSource on the objects both keep (report)")
    common(p)
    p.add_argument("--ssobservation", required=True, help="DP2 SSSource (designation or ssObjectId)")
    p.add_argument("--identifications", required=True, help="current_identifications Parquet")
    p.add_argument("--sigma", action="store_true", help="compute sigma for unexplained misses")
    p.add_argument("--sss-processing", default=SSS_PROCESSING,
                   help="SSObservation: the processing to compare (default: %(default)s; 'all')")
    p.set_defaults(func=cmd_dp2_intersection)

    p = sub.add_parser("horizons-adjudicate", help="Horizons verdict on discrepancies")
    common(p, nss=False, dia=False)
    p.add_argument("--discrepancies", required=True, help="*.discrepancies.parquet")
    p.add_argument("--max-rows", type=int, default=50)
    p.add_argument("--tol-mas", type=float, default=1.0)
    _horizons_args(p, 20)
    p.set_defaults(func=cmd_horizons_adjudicate)

    p = sub.add_parser("horizons-positions", help="stratified Horizons spot check (< 1 mas RMS)")
    common(p)
    p.add_argument("--scale", type=float, default=1.0, help="scale the per-stratum sample sizes")
    p.add_argument("--max-objects", type=int, default=0, help="cap the number of objects (queries)")
    _horizons_args(p, 120)
    p.set_defaults(func=cmd_horizons_positions)

    p = sub.add_parser("horizons-sigma", help="our ellipse vs Horizons 3-sigma, JPL orbits")
    p.add_argument("--designations", required=True, help="comma-separated SBDB search strings")
    p.add_argument("--epochs", default="60797.1,61300.1,62000.1", help="TAI MJDs, comma-separated")
    p.add_argument("--gate-below", type=float, default=60.0, help="gate where 1-sigma < this [arcsec]")
    p.add_argument("--rel-tol", type=float, default=0.10)
    p.add_argument("--out", required=True)
    _horizons_args(p, 20)
    p.set_defaults(func=cmd_horizons_sigma)

    p = sub.add_parser("brute-force", help="coarse-pass safety vs all orbits, exact ephemerides")
    common(p, nss=False)
    p.add_argument("--nearbysso", default=None)
    p.add_argument("--n-visits", type=int, default=4)
    p.add_argument("--visits", default=None, help="comma-separated visit ids (instead of random)")
    p.add_argument("--margin-deg", type=float, default=0.1,
                   help="prefilter margin beyond the per-orbit light-time bound; must exceed 3x the "
                        "calibrated excess")
    p.add_argument("--batch", type=int, default=64, help="test particles per prefilter simulation")
    p.add_argument("--calibrate", type=int, default=2000, help="orbits used to calibrate the prefilter")
    p.add_argument("--workers", type=int, default=8)
    p.set_defaults(func=cmd_brute_force)

    p = sub.add_parser("rank", help="diaDistanceRank by brute force against the DiaSources")
    common(p, orbits=False)
    p.add_argument("--orbits", default=None,
                   help="mpc_orbits Parquet (optional: reports the sampled objects' filter reasons)")
    p.add_argument("--n", type=int, default=100_000, help="rows sampled (0: all)")
    p.set_defaults(func=cmd_rank)

    p = sub.add_parser("mock-rank", help="(development) add diaDistanceRank by brute force")
    p.add_argument("--nearbysso", required=True)
    p.add_argument("--dia", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--fault", action="append", default=[], choices=sorted(RANK_FAULTS))
    p.add_argument("--seed", type=int, default=42)
    p.set_defaults(func=cmd_mock_rank)

    p = sub.add_parser("mock-nearbysso", help="(development) fake NearbySSO from SSObservation")
    p.add_argument("--ssobservation", required=True)
    p.add_argument("--dia", required=True)
    p.add_argument("--orbits", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--drop", type=int, default=5)
    p.add_argument("--perturb", type=int, default=5)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--sss-processing", default=SSS_PROCESSING,
                   help="SSObservation: the processing to compare (default: %(default)s; 'all')")
    p.set_defaults(func=cmd_mock)

    args = ap.parse_args(argv)
    # (future epochs: astropy falls back to mean polar motion, sub-mas here)
    warnings.filterwarnings("ignore", message="Tried to get polar motions")
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
