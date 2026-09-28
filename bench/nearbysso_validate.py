"""Validation harness for the NearbySSO builder (WP5 of
docs/design/nearbysso.md, "Validation").

Black-box checks of the builder's *outputs* (``nearbysso.parquet``, of
``_contract.NEARBYSSO_DTYPE``) against its inputs (the DiaSource Parquet,
``mpc_orbits``) and independent references (SSSource, JPL Horizons, the JPL
SBDB). It is written from the design and the contract only: the orbit
filter, the visits, the 2-body prefilter and the matching are
re-implemented here, and the builder's code is called only through the
contract (``propagate.coarse`` for sigma), so a shared bug is unlikely.

Subcommands (each writes ``<out>/<name>.txt`` and ``<out>/<name>.parquet``)::

  same-orbits          NearbySSO vs SSSource built from the SAME mpc_orbits
  dp2-intersection     vs DP2 SSSource (other orbits/cuts); reported only
  horizons-adjudicate  which side of each discrepancy matches Horizons
  horizons-positions   stratified Horizons spot check, < 1 mas RMS gate
  horizons-sigma       our ellipse (SBDB orbit+covariance) vs Horizons 3-sigma
  brute-force          coarse-pass safety: all orbits, exact ephemerides
  mock-nearbysso       (development) a NearbySSO file faked from SSSource,
                       with injected faults, to exercise the checks

Exit status: 0 pass (or report-only), 1 a gate failed, 3 incomplete (e.g.
sigma unknown because propagate.coarse isn't implemented yet).

HORIZONS / SBDB ETIQUETTE: every request goes through one `Polite`
instance: strictly serial, >= --min-interval s (default 1.5) apart, <= 60
epochs per request, and at most --max-queries requests per run. Never run
these subcommands in parallel or from the test suite.

Example::

  python -m bench.nearbysso_validate same-orbits \\
      --nearbysso nearbysso.parquet --sssource sssource.parquet \\
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
import pyarrow.parquet as pq
from astropy.time import Time
import astropy.units as u

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from bench.ephem_bench import horizons_observer, horizons_request  # noqa: E402
from ssp import util  # noqa: E402
from ssp.ephem_assist import (  # noqa: E402
    ASSIST_SUN,
    GM_SUN,
    MJD_J2000,
    cometary_to_helio_ecliptic,
    compute_ephemerides_one,
    ecliptic_to_equatorial,
    elements_row_to_bary_icrf,
    open_ephem,
)
from ssp.nearbysso import _contract as C  # noqa: E402

RADIUS = C.MATCH_RADIUS_ARCSEC
SIGMA_MAX = C.SIGMA_MAX_ARCSEC
NSS_COLUMNS = list(C.NEARBYSSO_DTYPE.names)
EPH_COMPARED = ["ephRa", "ephDec", "ephRateRa", "ephRateDec", "ephVmag"]
AU_DAY_TO_KM_S = (1.0 * u.au / u.day).to_value(u.km / u.s)

#: mpc_orbits columns read (renamed: designation, packed)
ORBIT_COLUMNS = [
    "unpacked_primary_provisional_designation", "packed_primary_provisional_designation",
    "q", "e", "i", "node", "argperi", "peri_time", "epoch_mjd", "h", "g",
    "arc_length_total", "normalized_rms",
]
ELEMENTS = ["q", "e", "i", "node", "argperi", "peri_time"]

#: Default tolerances for "identical" eph* values (same-orbits). Identical
#: code and inputs should agree bitwise; these allow for integrator step
#: choices that depend on the set of requested times (numerical noise).
TOL = dict(pos_mas=0.1, rate_deg_day=1e-7, vmag=1e-4)

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

def read_orbits(path, designations=None, with_json=False):
    """mpc_orbits columns as a DataFrame with ``designation`` and ``packed``,
    optionally only the given designations, optionally with mpc_orb_jsonb."""
    cols = ORBIT_COLUMNS + (["mpc_orb_jsonb"] if with_json else [])
    present = set(pq.read_schema(path).names)
    cols = [c for c in cols if c in present]
    filters = None
    if designations is not None:
        filters = [("unpacked_primary_provisional_designation", "in", sorted(set(designations)))]
        if not len(filters[0][2]):
            return _empty_orbits(with_json)
    df = pq.read_table(path, columns=cols, filters=filters).to_pandas()
    df = df.rename(columns={"unpacked_primary_provisional_designation": "designation",
                            "packed_primary_provisional_designation": "packed"})
    return df.sort_values("designation", kind="stable").reset_index(drop=True)


def _empty_orbits(with_json):
    cols = ["designation", "packed"] + ORBIT_COLUMNS[2:] + (["mpc_orb_jsonb"] if with_json else [])
    return pd.DataFrame({c: [] for c in cols})


#: Whether an unknown (NaN) arc_length_total passes the "> 2 days" rule.
#: The contract's rule, read literally, fails it; ~1/3 of mpc_orbits
#: (2026-09-26: 512,797 rows, mostly multi-opposition orbits) has NaN
#: there, so the choice matters. Set by --keep-unknown-arc.
KEEP_UNKNOWN_ARC = False


def filter_reason(orbits, keep_unknown_arc=None):
    """Why each orbit is excluded by NearbySSO's rules ('' if kept): the
    first of 'comet' (designation with '/', or packed starting with '_'),
    'missing_elements' (any of q, e, i, node, argperi, peri_time NaN),
    'short_arc' (arc_length_total <= 2 days) and 'unknown_arc'
    (arc_length_total NaN, unless keep_unknown_arc)."""
    if keep_unknown_arc is None:
        keep_unknown_arc = KEEP_UNKNOWN_ARC
    des = orbits["designation"].astype(str).to_numpy()
    packed = orbits["packed"].fillna("").astype(str).to_numpy()
    comet = np.char.find(des.astype(str), "/") >= 0
    comet |= np.char.startswith(packed.astype(str), "_")
    missing = np.zeros(len(orbits), bool)
    for c in ELEMENTS:
        missing |= ~np.isfinite(orbits[c].to_numpy(dtype=np.float64))
    arc = orbits["arc_length_total"].to_numpy(dtype=np.float64)
    unknown = np.isnan(arc) & (not keep_unknown_arc)
    short = arc <= 2.0
    return np.select([comet, missing, short, unknown],
                     ["comet", "missing_elements", "short_arc", "unknown_arc"], "")


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
    return {k: np.asarray(getattr(tr, k))[inv] for k in C.CoarseTrack._fields}


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
# 1/2. Comparison against an SSSource
# ---------------------------------------------------------------------------

def compare_to_sssource(nss, sss, dia, reason_of, sigma_fn=None, tol=TOL,
                        radius=RADIUS, sigma_max=SIGMA_MAX):
    """Compare NearbySSO rows (``nss``) with SSSource rows (``sss``).

    ``dia``: diaSourceId, midpointMjdTai, ra, dec for the SSSource rows'
    DiaSources. ``reason_of``: designation -> filter reason ('' kept).
    ``sigma_fn(designations, mjd_tai) -> sigma_major [arcsec]`` (NaN:
    unknown), called only for rows no cheaper reason explains.

    Returns (rows, summary): one row per SSSource row with its status:

    - ``match``: same designation, eph* within ``tol``;
    - ``value_mismatch``: same designation, an eph* value outside ``tol``;
    - ``filtered:<reason>``: the object fails NearbySSO's orbit filter;
    - ``no_diasource`` / ``sss_no_ephemeris``: incomparable inputs;
    - ``separation``: SSSource's prediction is > radius from the DiaSource;
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
    d = pd.DataFrame(sss)[["diaSourceId", "designation"] + EPH_COMPARED].copy()
    d = d.merge(pd.DataFrame(dia)[["diaSourceId", "midpointMjdTai", "ra", "dec"]].rename(
        columns={"ra": "dia_ra", "dec": "dia_dec"}), on="diaSourceId", how="left")
    d = d.merge(nsub, on="diaSourceId", how="left")
    n = len(d)

    des = d["designation"].astype(str).to_numpy()
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

    # value differences (where the designation matches)
    dpos = np.full(n, np.nan)
    if same.any():
        dpos[same] = util.sky_separation_arcsec(
            d["ephRa"].to_numpy()[same], d["ephDec"].to_numpy()[same],
            d["nss_ephRa"].to_numpy(dtype=float)[same], d["nss_ephDec"].to_numpy(dtype=float)[same]) * 1e3
    d["d_pos_mas"] = dpos
    for c in ("ephRateRa", "ephRateDec", "ephVmag"):
        d["d_" + c] = np.where(same, d["nss_" + c].to_numpy(dtype=float) - d[c].to_numpy(dtype=float),
                               np.nan)
    bad = (np.nan_to_num(dpos) > tol["pos_mas"])
    bad |= np.abs(np.nan_to_num(d["d_ephRateRa"])) > tol["rate_deg_day"]
    bad |= np.abs(np.nan_to_num(d["d_ephRateDec"])) > tol["rate_deg_day"]
    bad |= np.abs(np.nan_to_num(d["d_ephVmag"])) > tol["vmag"]
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
        (np.nan_to_num(sep, nan=np.inf) > radius, "separation"),
    ):
        m = todo & cond
        status[m] = ["filtered:" + r for r in reason[m]] if label is None else label
        todo &= ~m
    # a nearer object took the DiaSource?
    other_off = d["nss_ephOffset"].to_numpy(dtype=float) if "nss_ephOffset" in d else np.full(n, np.nan)
    eps = 1e-4   # arcsec: the float32 ephOffset's resolution
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
    d["borderline"] = (np.abs(np.nan_to_num(sep, nan=1e9) - radius) < 1e-3) | (
        np.abs(sigma - sigma_max) < 0.01 * sigma_max)
    d["identical"] = identical

    # NearbySSO rows for DiaSources not in this SSSource at all
    in_sss = np.isin(nss["diaSourceId"].to_numpy(), d["diaSourceId"].to_numpy())
    counts = pd.Series(status).value_counts().to_dict()
    ms = d[same]
    summary = {
        "n_sssource": n, "n_nearbysso": len(nss), "n_nearbysso_duplicate_ids": n_dup,
        "n_nearbysso_not_in_sssource": int((~in_sss).sum()),
        "status_counts": {k: int(v) for k, v in sorted(counts.items())},
        "n_identical": int(identical.sum()),
        "max_d_pos_mas": float(np.nanmax(ms["d_pos_mas"])) if len(ms) else np.nan,
        "max_d_rateRa_deg_day": float(np.nanmax(np.abs(ms["d_ephRateRa"]))) if len(ms) else np.nan,
        "max_d_rateDec_deg_day": float(np.nanmax(np.abs(ms["d_ephRateDec"]))) if len(ms) else np.nan,
        "max_d_vmag": float(np.nanmax(np.abs(ms["d_ephVmag"]))) if len(ms) else np.nan,
        "n_fail": int(sum(counts.get(s, 0) for s in FAIL_STATUSES)) + n_dup,
        "n_unknown": int(counts.get("sigma_unknown", 0)),
        "n_borderline_fail": int((d["borderline"] & np.isin(status, FAIL_STATUSES)).sum()),
    }
    return d, summary


def report_comparison(rep, summary, tol=None):
    rep(f"SSSource rows:          {summary['n_sssource']:,}")
    rep(f"NearbySSO rows:         {summary['n_nearbysso']:,} "
        f"({summary['n_nearbysso_not_in_sssource']:,} for DiaSources not in SSSource; "
        f"{summary['n_nearbysso_duplicate_ids']} duplicate diaSourceIds)")
    rep("status of each SSSource row:")
    for k, v in summary["status_counts"].items():
        rep(f"  {k:32s} {v:>10,}")
    rep(f"bitwise-identical eph* rows: {summary['n_identical']:,}")
    rep(f"max |d position|  = {summary['max_d_pos_mas']:.3g} mas")
    rep(f"max |d rateRa|    = {summary['max_d_rateRa_deg_day']:.3g} deg/day, "
        f"|d rateDec| = {summary['max_d_rateDec_deg_day']:.3g} deg/day")
    rep(f"max |d Vmag|      = {summary['max_d_vmag']:.3g} mag")
    if tol:
        rep(f"tolerances: {tol}")
    if summary["n_borderline_fail"]:
        rep(f"({summary['n_borderline_fail']} failing rows are within 1e-3\" of the radius or "
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


def _read_sss(path, extra=()):
    present = set(pq.read_schema(path).names)
    cols = [c for c in ["diaSourceId", "designation", "ssObjectId"] + EPH_COMPARED + ["ephOffset"]
            + list(extra) if c in present]
    return pd.read_parquet(path, columns=cols)


def cmd_same_orbits(args):
    rep = Report("same-orbits", args.out)
    rep("# NearbySSO vs SSSource built from the same mpc_orbits")
    rep(f"nearbysso={args.nearbysso}\nsssource={args.sssource}\ndia={args.dia}\norbits={args.orbits}")
    nss, sss = _read_nss(args.nearbysso), _read_sss(args.sssource)
    dia = read_dia_subset(args.dia, ids=np.union1d(sss["diaSourceId"], nss["diaSourceId"]))
    orbits = read_orbits(args.orbits, designations=set(sss["designation"].dropna()))
    sigma_fn = None if args.no_sigma else SigmaOracle(args.orbits)
    tol = dict(pos_mas=args.pos_tol_mas, rate_deg_day=args.rate_tol, vmag=args.vmag_tol)
    rows, summary = compare_to_sssource(nss, sss, dia, reason_lookup(orbits), sigma_fn, tol)
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
    nss, sss = _read_nss(args.nearbysso), _read_sss(args.sssource)
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
    rows, summary = compare_to_sssource(nss_both, both, dia, lookup, sigma_fn)
    report_comparison(rep, summary, TOL)
    rep("d position [mas] of matched rows:", _stats(rows["d_pos_mas"], "mas"))
    rep("(different orbit snapshots: differences are expected; not gated)")
    rep.write(rows, discrepancies=discrepancies(rows))
    return 0


# ---------------------------------------------------------------------------
# 3. Horizons adjudication
# ---------------------------------------------------------------------------

def adjudicate_rows(disc, h_ra, h_dec, tol_mas=1.0, radius=RADIUS):
    """Which side of each discrepancy agrees with Horizons (pure logic).
    Adds sep_{sss,nss,dia}_h and ``verdict``."""
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
    within = d["sep_dia_h_arcsec"].to_numpy() <= radius
    v = np.full(len(d), "no_horizons", dtype=object)
    have = np.isfinite(h_ra)
    v[have & same & ss & ns] = "both_match"
    v[have & same & ss & ~ns] = "sss_matches: NearbySSO bug"
    v[have & same & ~ss & ns] = "nss_matches: SSSource issue"
    v[have & same & ~ss & ~ns] = "neither_matches"
    miss = have & ~same
    v[miss & ss & within] = "sss_matches, within radius: NearbySSO missed it"
    v[miss & ss & ~within] = "sss_matches, outside radius per Horizons"
    v[miss & ~ss] = "sss_disagrees: SSSource issue"
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
    for k, v in res["verdict"].value_counts().items():
        rep(f"  {k:52s} {v:>6}")
    rep(f"Horizons requests: {polite.n}")
    rep.write(res)
    bugs = res["verdict"].str.contains("NearbySSO").sum()
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
    arc = o["arc_length_total"].reindex(nss["designation"]).to_numpy(dtype=float)
    cls = dynamical_class(q, e)
    sig = sigma_major_arcsec(nss["ephRaErr"].to_numpy(float), nss["ephDecErr"].to_numpy(float),
                             nss["ephRa_ephDec_Cov"].to_numpy(float))
    rate = np.hypot(nss["ephRateRa"].to_numpy(float), nss["ephRateDec"].to_numpy(float))
    masks = {
        "main_belt": cls == "main_belt", "neo": cls == "neo", "trojan": cls == "trojan",
        "tno": cls == "tno", "short_arc": arc < short_arc_days,
        "near_radius": nss["ephOffset"].to_numpy(float) > 0.9 * RADIUS,
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
    rep(f"Horizons requests: {polite.n}")
    ok = np.isfinite(res["d_pos_mas"])
    rms = float(np.sqrt(np.mean(res["d_pos_mas"][ok] ** 2))) if ok.any() else np.nan
    for st, g in res.groupby("stratum"):
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

def twobody_helio_ecl(q, e, inc, node, peri, dt_peri, mu=GM_SUN):
    """Vectorized heliocentric ecliptic position [AU] (N, 3) from cometary
    elements (angles in deg; dt_peri = t - peri_time [day]); e != 1."""
    q, e, dt = (np.asarray(x, float) for x in (q, e, dt_peri))
    a = q / (1.0 - e)
    n = np.sqrt(mu / np.abs(a) ** 3)
    M = n * dt
    x, y = np.empty_like(q), np.empty_like(q)
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
        ae = a[ell]
        x[ell] = ae * (np.cos(E) - ee)
        y[ell] = ae * np.sqrt(1 - ee * ee) * np.sin(E)
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
        x[hyp] = ah * (eh - np.cosh(H))
        y[hyp] = ah * np.sqrt(eh * eh - 1) * np.sinh(H)
    i, Om, w = (np.deg2rad(np.asarray(v, float)) for v in (inc, node, peri))
    cO, sO, ci, si, cw, sw = np.cos(Om), np.sin(Om), np.cos(i), np.sin(i), np.cos(w), np.sin(w)
    P = np.stack([cO * cw - sO * sw * ci, sO * cw + cO * sw * ci, sw * si], axis=-1)
    Q = np.stack([-cO * sw - sO * cw * ci, -sO * sw + cO * cw * ci, cw * si], axis=-1)
    return x[:, None] * P + y[:, None] * Q


def twobody_directions(orbits, mjd_tai, obs_pos, sun_pos):
    """Unit vectors (N, 3) from the observer to each orbit's 2-body
    position at mjd_tai (no light time; TT ~ TAI + 32.184 s)."""
    t_tt = mjd_tai + 32.184 / 86400.0
    X = twobody_helio_ecl(orbits["q"], orbits["e"], orbits["i"], orbits["node"], orbits["argperi"],
                          t_tt - orbits["peri_time"].to_numpy(float))
    X = X @ R_ECL2EQ.T + sun_pos - obs_pos
    return X / np.linalg.norm(X, axis=1)[:, None]


def match_within(pred_ra, pred_dec, dia_ra, dia_dec, radius=RADIUS):
    """Every (prediction k, DiaSource h) pair within ``radius`` arcsec, by a
    KD tree on unit vectors (independent of WP3's HEALPix index); returns
    (k, h, sep [arcsec]), separations as ssp.util.sky_separation_arcsec."""
    from scipy.spatial import cKDTree
    if len(pred_ra) == 0 or len(dia_ra) == 0:
        return np.zeros(0, int), np.zeros(0, int), np.zeros(0)
    chord = 2 * np.sin(np.deg2rad(radius / 3600.0) / 2) * (1 + 1e-6)
    tree = cKDTree(radec_to_vec(dia_ra, dia_dec))
    hits = tree.query_ball_point(radec_to_vec(pred_ra, pred_dec), chord)
    k = np.repeat(np.arange(len(hits)), [len(x) for x in hits])
    h = np.fromiter((i for x in hits for i in x), dtype=np.int64, count=len(k))
    sep = util.sky_separation_arcsec(np.asarray(pred_ra)[k], np.asarray(pred_dec)[k],
                                     np.asarray(dia_ra)[h], np.asarray(dia_dec)[h])
    m = sep <= radius
    return k[m], h[m], sep[m]


def expected_nearest(pairs, sigma_max=SIGMA_MAX, radius=RADIUS):
    """From all (diaSourceId, designation, sep, sigma) pairs within the
    radius, the nearest *eligible* object per DiaSource (sigma <= max; ties
    by designation). Where some candidate's sigma is unknown (NaN) and no
    eligible one is nearer, the row is flagged ``sigma_unknown``."""
    p = pd.DataFrame(pairs)
    p = p[p["sep"] <= radius]
    unk = ~np.isfinite(p["sigma"].to_numpy(float))
    keep = unk | (p["sigma"].to_numpy(float) <= sigma_max)
    p = p[keep].assign(sigma_unknown=unk[keep])
    p = p.sort_values(["diaSourceId", "sep", "designation"], kind="stable")
    return p.drop_duplicates("diaSourceId").reset_index(drop=True)


def compare_expected(expected, nss):
    """Expected nearest matches vs NearbySSO rows of the same DiaSources.
    Status: found, missing, wrong_object (NearbySSO has another), and for
    NearbySSO rows not expected, extra."""
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
    sun_pos = np.array([[s.x, s.y, s.z] for s in (ephem.get_particle(ASSIST_SUN, float(x))
                                                   for x in tai_to_assist(V["t_tai_mjd"]))])

    orbits = read_orbits(args.orbits)
    reason = filter_reason(orbits)
    orbits = orbits[reason == ""].reset_index(drop=True)
    rep(f"orbits kept by the filter: {len(orbits):,}")
    q, e = orbits["q"].to_numpy(float), orbits["e"].to_numpy(float)
    ep = orbits["epoch_mjd"].to_numpy(float)
    near_parabolic = np.abs(1 - e) < 1e-6
    safe = orbits.assign(e=np.where(near_parabolic, 0.5, e))
    margin = np.deg2rad(args.margin_deg)

    # candidates: always exact for NEOs, near-parabolic, far epochs
    cand = {}
    for j in range(len(V)):
        t = V["t_tai_mjd"].iloc[j]
        always = (q < args.always_exact_q) | near_parabolic | (np.abs(ep - t) > args.max_epoch_gap)
        u_ = twobody_directions(safe, t, obs_pos[j], sun_pos[j])
        ang = np.arccos(np.clip(u_ @ V["center"].iloc[j], -1, 1))
        sel = np.flatnonzero(always | (ang < V["radius"].iloc[j] + margin))
        for oi in sel:
            cand.setdefault(oi, []).append(j)
        rep(f"  visit {V['visit'].iloc[j]}: {len(sel):,} candidates "
            f"({int((ang < V['radius'].iloc[j] + margin).sum()):,} by 2-body, {int(always.sum()):,} always)")

    # calibration of the 2-body prefilter against exact ephemerides: random
    # orbits of the kind the prefilter handles, at every sampled visit time
    pool = np.flatnonzero((q >= args.always_exact_q) & ~near_parabolic)
    cal_idx = rng.choice(pool, size=min(args.calibrate, len(pool)), replace=False)
    items = sorted(cand.items()) + [(int(oi), list(range(len(V)))) for oi in cal_idx]
    _BF.update(orbits=orbits, cand=items, t=V["t_tai_mjd"].to_numpy(), obs_pos=obs_pos, obs_vel=obs_vel)
    chunks = util.balanced_chunks(np.array([len(v) for _, v in items]), 8 * args.workers)
    try:
        if args.workers > 1 and util.fork_context() is not None:
            res = util.run_chunks(_bf_chunk, chunks, args.workers, "exact ephemerides")
        else:
            res = [_bf_chunk(a, b) for a, b in chunks]
    finally:
        _BF.clear()
    exact = pd.DataFrame([r for part in res for r in part], columns=["oi", "vj", "ra", "dec"])
    exact = exact.drop_duplicates(["oi", "vj"]).reset_index(drop=True)
    cal = exact[exact["oi"].isin(set(int(x) for x in cal_idx))]
    errs = []
    for j in range(len(V)):
        c = cal[cal["vj"] == j]
        if not len(c):
            continue
        u2 = twobody_directions(safe.iloc[c["oi"].to_numpy()], V["t_tai_mjd"].iloc[j], obs_pos[j],
                                sun_pos[j])
        ue = radec_to_vec(c["ra"].to_numpy(), c["dec"].to_numpy())
        errs.append(np.degrees(np.arccos(np.clip(np.sum(u2 * ue, axis=1), -1, 1))))
    errs = np.concatenate(errs) if errs else np.array([np.nan])
    rep(f"2-body prefilter calibration on {len(cal_idx)} random orbits with q >= "
        f"{args.always_exact_q} x {len(V)} visits: error median {np.nanmedian(errs):.3g} deg, "
        f"99.9% {np.nanpercentile(errs, 99.9):.3g}, max {np.nanmax(errs):.3g} deg; "
        f"margin {args.margin_deg} deg")
    margin_ok = bool(np.nanmax(errs) < args.margin_deg / 3)
    if not margin_ok:
        rep("WARNING: 2-body error exceeds margin/3: the prefilter may not be conservative")
    # (calibration-only objects are matched too; that only adds coverage)

    # match exact predictions to DiaSources (KD tree on unit vectors)
    parts = []
    des_all = orbits["designation"].to_numpy(dtype=object)
    for j in range(len(V)):
        s, e_ = V["dia_start"].iloc[j], V["dia_end"].iloc[j]
        dv = dia.iloc[s:e_]
        ex = exact[exact["vj"] == j]
        k, h, sep = match_within(ex["ra"].to_numpy(), ex["dec"].to_numpy(),
                                 dv["ra"].to_numpy(), dv["dec"].to_numpy(), RADIUS)
        parts.append(pd.DataFrame({
            "diaSourceId": dv["diaSourceId"].to_numpy(dtype=np.int64)[h],
            "designation": des_all[ex["oi"].to_numpy()[k]], "sep": sep,
            "midpointMjdTai": dv["midpointMjdTai"].to_numpy(dtype=np.float64)[h]}))
    pairs = pd.concat(parts, ignore_index=True)
    pairs = pairs[pairs["sep"] <= RADIUS].reset_index(drop=True)
    rep(f"(object, DiaSource) pairs within {RADIUS}\": {len(pairs):,} "
        f"({pairs['designation'].nunique():,} objects)")
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
    n_bad = int(cmp["status"].isin(["missing", "wrong_object"]).sum())
    n_unk = int(cmp["status"].str.endswith("_sigma_unknown").sum())
    verdict = "FAIL" if n_bad else ("INCOMPLETE" if n_unk or not margin_ok else "PASS")
    rep(f"GATE: every eligible nearest match is in NearbySSO: {verdict}")
    rep.write(cmp, pairs=pairs)
    return {"PASS": 0, "FAIL": 1}.get(verdict, 3)


# ---------------------------------------------------------------------------
# Development: a NearbySSO faked from SSSource, with injected faults
# ---------------------------------------------------------------------------

def mock_from_sssource(sss, dia, reason_of, rng=None, n_drop=0, n_perturb=0):
    """A NEARBYSSO_DTYPE table from SSSource rows that NearbySSO should
    contain (object kept by the filter, prediction within 5"), nearest per
    DiaSource, zero ellipses; then ``n_drop`` rows removed and ``n_perturb``
    positions shifted by 1-10 mas. Returns (table, injected faults)."""
    rng = np.random.default_rng(rng)
    d = pd.DataFrame(sss).merge(pd.DataFrame(dia)[["diaSourceId", "ra", "dec"]], on="diaSourceId")
    d = d[np.isfinite(d["ephRa"].to_numpy(float))]
    d["ephOffset"] = util.sky_separation_arcsec(d["ephRa"], d["ephDec"], d["ra"], d["dec"])
    d = d[(reasons_for(d["designation"].to_numpy(), reason_of) == "") & (d["ephOffset"] <= RADIUS)]
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
    sss = _read_sss(args.sssource)
    dia = read_dia_subset(args.dia, ids=sss["diaSourceId"].to_numpy())
    orbits = read_orbits(args.orbits, designations=set(sss["designation"].dropna()))
    out, faults = mock_from_sssource(sss, dia, reason_lookup(orbits), args.seed, args.drop, args.perturb)
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

    p = sub.add_parser("same-orbits", help="vs SSSource from the same mpc_orbits")
    common(p)
    p.add_argument("--sssource", required=True)
    p.add_argument("--no-sigma", action="store_true", help="don't compute sigma (classify as unknown)")
    p.add_argument("--pos-tol-mas", type=float, default=TOL["pos_mas"])
    p.add_argument("--rate-tol", type=float, default=TOL["rate_deg_day"], help="deg/day")
    p.add_argument("--vmag-tol", type=float, default=TOL["vmag"])
    p.set_defaults(func=cmd_same_orbits)

    p = sub.add_parser("dp2-intersection", help="vs DP2 SSSource on the objects both keep (report)")
    common(p)
    p.add_argument("--sssource", required=True, help="DP2 SSSource (designation or ssObjectId)")
    p.add_argument("--identifications", required=True, help="current_identifications Parquet")
    p.add_argument("--sigma", action="store_true", help="compute sigma for unexplained misses")
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
    p.add_argument("--margin-deg", type=float, default=1.5,
                   help="2-body prefilter margin; must exceed 3x the calibrated 2-body error")
    p.add_argument("--always-exact-q", type=float, default=1.3, help="q below which it's always exact")
    p.add_argument("--max-epoch-gap", type=float, default=1500.0, help="days; farther epochs are exact")
    p.add_argument("--calibrate", type=int, default=2000, help="orbits used to calibrate the 2-body error")
    p.add_argument("--workers", type=int, default=8)
    p.set_defaults(func=cmd_brute_force)

    p = sub.add_parser("mock-nearbysso", help="(development) fake NearbySSO from SSSource")
    p.add_argument("--sssource", required=True)
    p.add_argument("--dia", required=True)
    p.add_argument("--orbits", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--drop", type=int, default=5)
    p.add_argument("--perturb", type=int, default=5)
    p.add_argument("--seed", type=int, default=42)
    p.set_defaults(func=cmd_mock)

    for p in sub.choices.values():
        p.add_argument("--keep-unknown-arc", action="store_true",
                       help="treat a NaN arc_length_total as passing the > 2 d rule")
    args = ap.parse_args(argv)
    # (future epochs: astropy falls back to mean polar motion, sub-mas here)
    warnings.filterwarnings("ignore", message="Tried to get polar motions")
    global KEEP_UNKNOWN_ARC
    KEEP_UNKNOWN_ARC = args.keep_unknown_arc
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
