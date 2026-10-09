"""The comparison with JPL (WP N5 of docs/design/nongrav.md).

Three checks, on the non-grav fixture (``/sdf/data/rubin/user/mjuric/nongrav/
fixtures/2026-10-01/``) and an SSObservation built from it:

  orbits      (a report) the MPC orbits, through our SSObservation, against
              JPL's own orbits, through Horizons, at the Rubin observation
              times: the separation, in arcsec and in combined sigma; which
              orbit fits Rubin's positions better; JPL's 3-sigma against our
              error ellipse; and flags (non-standard g(r), DT, non-grav
              model mismatches, > 3 sigma disagreements, 2025 QH138).
  integrator  (pass/fail) JPL's SBDB elements and non-gravs, run through our
              ASSIST code, against Horizons for the same JPL solution:
              (a) barycentric geometric vectors (the integration alone) and
              (b) X05 astrometric RA/Dec (light time, aberration, observer).
              The initial elements are those Horizons integrates from (its
              header, at the solution epoch; SBDB's for comets).
  user        (check 2b) the MPC's own elements and A's sent to Horizons as
              user elements (COMMAND=';'), against our SSObservation.

Subcommands::

  fetch [--stage sbdb|horizons|user|all] [--dry-run [--show-url]]
      the ONLY code that talks to JPL. Cache-first: a request already in the
      cache is never sent again.
  orbits      the check-1 report, from the cache alone
  integrator  the check-2 report, from the cache alone
  user        the check-2b report, from the cache alone
  status      the cache and the request count

Outputs (``--out``, default WORK): orbits_report.txt, orbits_objects.csv,
orbits_rows.csv; integrator_report.txt, integrator_objects.csv,
integrator_rows.csv; user_elements_report.txt and its two CSVs.

JPL ETIQUETTE (hard rules, enforced by ``JPLClient``): requests are strictly
serial (one process, holding a lock on the cache) and at least
``MIN_INTERVAL_S`` apart; at most ``BUDGET`` requests in total, counted from
``requests.log``; every raw response is cached under ``cache/``, keyed by the
request (service + sorted parameters), and every request is logged (time,
URL, bytes). Never run ``fetch`` from tests or CI. Never contact the MPC.

Time scales: SSObservation times are ``midpointMjdTai``. Horizons observer
tables are requested in TT (TT = TAI + 32.184 s exactly, so no conversion
model is involved) and vector tables in TDB (astropy's TT -> TDB). The times
are sent as MJD with 10 decimals (~9 us), and our side uses exactly the
times sent.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import numpy as np

# ---------------------------------------------------------------------------
# Paths and constants
# ---------------------------------------------------------------------------

FIXTURE = "/sdf/data/rubin/user/mjuric/nongrav/fixtures/2026-10-01"
WORK = "/sdf/data/rubin/user/mjuric/nongrav/work/n5"
CACHE = os.path.join(WORK, "cache")
SSOBSERVATION = os.path.join(WORK, "sssource", "sssource.parquet")   # (fixture, pre-rename name)

HORIZONS_URL = "https://ssd.jpl.nasa.gov/api/horizons.api"
SBDB_URL = "https://ssd-api.jpl.nasa.gov/sbdb.api"
SERVICES = {
    "horizons": HORIZONS_URL,
    "sbdb": SBDB_URL,
    "sbdb_query": "https://ssd-api.jpl.nasa.gov/sbdb_query.api",
}

#: Seconds between two JPL requests (the rule is >= 1 s).
MIN_INTERVAL_S = 1.5
#: Total requests allowed, over all runs (requests.log).
BUDGET = 100

#: TT - TAI [day], exact.
TT_MINUS_TAI_DAY = 32.184 / 86400.0

#: DE440/441's GM_sun [au^3/day^2] (JPL's small-body integrations use it).
GM_SUN_DE440 = 2.9591220828411951e-04

#: JPL's default comet g(r) (Marsden et al. 1973), as SBDB names its
#: parameters (ALN = alpha, R0 in au).
JPL_DEFAULT_GR = {"ALN": 0.1112620426, "NM": 2.15, "NN": 5.093, "NK": 4.6142, "R0": 2.808}
#: The g(r) = (r / 1 au)^-2 of Yarkovsky fits, in the same names.
JPL_YARKOVSKY_GR = {"ALN": 1.0, "NM": 2.0, "NN": 5.093, "NK": 0.0, "R0": 1.0}
#: SBDB name -> ASSIST Extras attribute.
GR_TO_ASSIST = {"ALN": "alpha", "NM": "nm", "NN": "nn", "NK": "nk", "R0": "r0"}

# Check 1's objects beyond the fixture's comet_ng and yarkovsky classes: five
# gravity-only comets (two with the largest MPC residuals, 3I, two ordinary)
# and five control asteroids (numbered and unnumbered, many observations).
CHECK1_COMET_GRAV = ("P/1994 P1", "P/1999 RO28", "C/2025 N1", "P/2000 WT168", "C/2023 H1")
CHECK1_CONTROL = ("2007 GK33", "2016 PR243", "6120 P-L", "2025 MA232", "2013 BV67")

# Check 2's objects, chosen from the cached SBDB answers (see
# ``check2_candidates``): comets with a JPL non-grav fit (no DT), Yarkovsky
# asteroids with a JPL A2, and gravity-only controls.
# Comets: five with the standard g(r) and no DT (Encke at q = 0.34 au,
# 47P's large A's, 78P's negative A1, 210P with A2 only, 32P with A3), and
# 228P with JPL's non-standard g(r) (set in ASSIST's general form). 67P and
# 3I fit DT, which ASSIST lacks. Yarkovsky: four candidates looked up in
# SBDB, of which those with a JPL A2 are used. Controls: three of check 1's.
CHECK2_COMETS = ("P/1818 W1", "P/1948 Q1", "P/1973 S1", "P/2003 K2", "P/1926 V1", "P/2001 YX127")
CHECK2_YARKOVSKY = ("1999 JU3", "1936 CA", "2008 NP3", "1998 FW4")
CHECK2_CONTROL = ("2007 GK33", "2016 PR243", "6120 P-L")

# Check 2b (added on N4's reading of the Horizons API: COMMAND=';' takes
# user elements with A1-A3 and the g(r) constants): the MPC's own elements
# and A's sent to Horizons, against our SSObservation at the Rubin times. Three
# comets where the non-gravs move SSObservation most, 2025 QH138 (the largest
# Yarkovsky A2) and 2008 NP3; and one comet again without its A's, to see
# that Horizons applies them.
CHECK2B_OBJECTS = ("P/2003 K2", "P/1973 S1", "P/2005 N3", "2025 QH138", "2008 NP3")
CHECK2B_NOGRAV = ("P/2003 K2",)

#: The check-2 times beyond the Rubin ones: epoch + these offsets [day]...
CHECK2_OFFSETS_D = tuple(np.linspace(-730.0, 730.0, 21))
#: ... and, for comets, perihelion + these.
CHECK2_PERI_OFFSETS_D = (-30.0, -10.0, 0.0, 10.0, 30.0)


# ---------------------------------------------------------------------------
# The JPL client: cache-first, serial, paced, budgeted, logged
# ---------------------------------------------------------------------------


class NotCachedError(RuntimeError):
    """An offline lookup of a request that isn't in the cache."""


class BudgetExceededError(RuntimeError):
    pass


def request_key(service, params):
    """The cache key of a request: a hash of the service and its sorted
    parameters (so parameter order doesn't matter)."""
    canon = service + "?" + urllib.parse.urlencode(sorted((str(k), str(v)) for k, v in params.items()))
    return hashlib.sha256(canon.encode()).hexdigest()[:16]


def request_url(service, params):
    return (
        SERVICES[service] + "?" + urllib.parse.urlencode(sorted((str(k), str(v)) for k, v in params.items()))
    )


def _slug(label):
    return re.sub(r"[^A-Za-z0-9]+", "_", label).strip("_")[:40]


class JPLClient:
    """Cache-first access to JPL. ``offline=True`` (the analysis default)
    never touches the network: a request missing from the cache raises
    NotCachedError. ``opener`` (url -> bytes) replaces urllib in tests."""

    def __init__(
        self,
        cache_dir=CACHE,
        offline=True,
        min_interval=MIN_INTERVAL_S,
        budget=BUDGET,
        opener=None,
        sleep=time.sleep,
        clock=time.time,
    ):
        self.cache_dir = cache_dir
        self.offline = offline
        self.min_interval = min_interval
        self.budget = budget
        self.opener = opener or _urlopen
        self.sleep = sleep
        self.clock = clock
        self._last = None
        self._lock = None
        self.sent = 0

    # -- the cache -------------------------------------------------------
    def path(self, service, params, label):
        ext = "json" if service.startswith("sbdb") else "txt"
        return os.path.join(
            self.cache_dir, f"{service}__{_slug(label)}__{request_key(service, params)}.{ext}"
        )

    def cached(self, service, params, label):
        return os.path.exists(self.path(service, params, label))

    def log_path(self):
        return os.path.join(self.cache_dir, "requests.log")

    def n_logged(self):
        try:
            with open(self.log_path()) as f:
                return sum(1 for ln in f if ln.strip())
        except FileNotFoundError:
            return 0

    def get(self, service, params, label):
        """The raw response text of a request, from the cache, or (online
        only) fetched, cached and logged."""
        p = self.path(service, params, label)
        if os.path.exists(p):
            with open(p) as f:
                return f.read()
        if self.offline:
            raise NotCachedError(f"{service} {label}: not in the cache ({p})")
        return self._fetch(service, params, label, p)

    # -- the network -------------------------------------------------------
    def _acquire(self):
        if self._lock is None:
            os.makedirs(self.cache_dir, exist_ok=True)
            self._lock = open(os.path.join(self.cache_dir, ".lock"), "w")
            try:
                fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                raise RuntimeError("another process holds the JPL cache lock: requests must be serial")

    def _fetch(self, service, params, label, p):
        self._acquire()
        if self.n_logged() >= self.budget:
            raise BudgetExceededError(f"the JPL request budget ({self.budget}) is used up")
        last = self._last if self._last is not None else self._last_logged()
        if last is not None:
            wait = self.min_interval - (self.clock() - last)
            if wait > 0:
                self.sleep(wait)
        url = request_url(service, params)
        t0 = self.clock()
        status = "ok"
        try:
            body = self.opener(url)
        except Exception as e:  # logged, then re-raised
            body = None
            status = f"error {type(e).__name__}: {e}"
        self._last = self.clock()
        self.sent += 1
        stamp = datetime.fromtimestamp(t0, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        with open(self.log_path(), "a") as f:
            f.write(f"{stamp}\t{0 if body is None else len(body)}\t{status}\t{label}\t{url}\n")
        if body is None:
            raise RuntimeError(f"{service} {label}: {status}")
        text = body.decode("utf-8", errors="replace")
        tmp = p + ".tmp"
        with open(tmp, "w") as f:
            f.write(text)
        os.replace(tmp, p)
        with open(p + ".request", "w") as f:
            json.dump(
                {
                    "service": service,
                    "label": label,
                    "params": params,
                    "url": url,
                    "time": stamp,
                    "bytes": len(body),
                },
                f,
                indent=1,
            )
        return text

    def _last_logged(self):
        """The time of the last logged request (pacing across runs)."""
        try:
            with open(self.log_path()) as f:
                lines = [ln for ln in f if ln.strip()]
        except FileNotFoundError:
            return None
        if not lines:
            return None
        stamp = lines[-1].split("\t", 1)[0]
        return datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp() + 1.0


def _urlopen(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ssp-tools-bench-jpl-compare/0.1"})
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.read()
    except urllib.error.HTTPError as e:  # keep JPL's explanation for the log
        body = e.read().decode("utf-8", errors="replace").replace("\n", " ")[:300]
        raise RuntimeError(f"HTTP {e.code}: {body}") from None


# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------

#: Objects SBDB's search string matches more than once (141P has
#: fragments), looked up by exact designation instead.
SBDB_BY_DES = ("P/1994 P1",)


def sbdb_params(sstr, exact=False):
    """One object: full-precision elements, the solution, model_pars;
    ``exact`` looks the designation up exactly (``des``), not by search."""
    return {"des" if exact else "sstr": sstr, "full-prec": "1"}


def fmt_mjd(t):
    return f"{float(t):.10f}"


def horizons_command(pdes, is_comet):
    """Horizons' COMMAND for a JPL primary designation: comets with the
    closest-apparition and no-fragment options."""
    if is_comet:
        return f"'DES={pdes};CAP;NOFRAG'"
    if pdes.isdigit():
        return f"'{pdes};'"
    return f"'DES={pdes};'"


def horizons_observer_params(command, mjd_tt):
    """An X05 observer table at TT MJDs: astrometric RA/Dec (1), the RA/Dec
    3-sigma (36) and the plane-of-sky 3-sigma ellipse (37)."""
    return {
        "format": "text",
        "COMMAND": command,
        "OBJ_DATA": "YES",
        "MAKE_EPHEM": "YES",
        "EPHEM_TYPE": "OBSERVER",
        "CENTER": "'X05'",
        "TLIST": ",".join(fmt_mjd(t) for t in mjd_tt),
        "TLIST_TYPE": "MJD",
        "TIME_TYPE": "TT",
        "QUANTITIES": "'1,36,37'",
        "ANG_FORMAT": "DEG",
        "EXTRA_PREC": "YES",
        "CSV_FORMAT": "YES",
        "CAL_FORMAT": "JD",
        "REF_SYSTEM": "ICRF",
    }


def horizons_user_params(row, ng, mjd_tt, with_ng=True):
    """An X05 observer table (astrometric RA/Dec) for user-supplied
    heliocentric ecliptic J2000 elements: an mpc_orbits row (epoch and
    peri_time TT MJD, sent as TDB JD) and its ssp.nongrav.NonGrav (A1-A3 and
    the g(r) of ssp.nongrav.G_OF_R, when ``with_ng``)."""
    from ssp.nongrav import G_OF_R

    def tdb_jd(tt_mjd):
        return float(tt_to_tdb(float(tt_mjd))) + 2400000.5

    p = {
        "format": "text",
        "COMMAND": "';'",
        "OBJ_DATA": "YES",
        "MAKE_EPHEM": "YES",
        "EPHEM_TYPE": "OBSERVER",
        "ECLIP": "J2000",
        "EPOCH": f"{tdb_jd(row['epoch_mjd']):.10f}",
        "EC": f"{float(row['e']):.16e}",
        "QR": f"{float(row['q']):.16e}",
        "TP": f"{tdb_jd(row['peri_time']):.10f}",
        "OM": f"{float(row['node']):.16e}",
        "W": f"{float(row['argperi']):.16e}",
        "IN": f"{float(row['i']):.16e}",
        "CENTER": "'X05'",
        "TLIST": ",".join(fmt_mjd(t) for t in mjd_tt),
        "TLIST_TYPE": "MJD",
        "TIME_TYPE": "TT",
        "QUANTITIES": "'1'",
        "ANG_FORMAT": "DEG",
        "EXTRA_PREC": "YES",
        "CSV_FORMAT": "YES",
        "CAL_FORMAT": "JD",
        "REF_SYSTEM": "ICRF",
    }
    if with_ng and ng.model:
        for k, v in zip(("A1", "A2", "A3"), ng.A):
            p[k] = f"{float(v):.16e}"
        gr = G_OF_R[ng.model]
        for k, a in GR_TO_ASSIST.items():
            p[k] = f"{float(gr[a]):.10g}"
    return p


def horizons_vectors_params(command, mjd_tdb):
    """Barycentric (500@0) geometric ICRF state vectors at TDB MJDs."""
    return {
        "format": "text",
        "COMMAND": command,
        "OBJ_DATA": "YES",
        "MAKE_EPHEM": "YES",
        "EPHEM_TYPE": "VECTORS",
        "CENTER": "'500@0'",
        "REF_PLANE": "FRAME",
        "REF_SYSTEM": "ICRF",
        "VEC_TABLE": "2",
        "VEC_CORR": "NONE",
        "OUT_UNITS": "AU-D",
        "CSV_FORMAT": "YES",
        "VEC_LABELS": "NO",
        "TLIST": ",".join(fmt_mjd(t) for t in mjd_tdb),
        "TLIST_TYPE": "MJD",
        "TIME_TYPE": "TDB",
        "CAL_FORMAT": "JD",
    }


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def _float(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return np.nan


def parse_sbdb(text):
    """An SBDB answer -> dict: des, fullname, spkid, kind, orbit_id, epoch
    (JD TDB), elements {name: float} (e, q, tp, om, w, i, a, ma), the
    model_pars {name: (value, sigma, kind)}, producer, soln_date, n_obs,
    data_arc. Raises ValueError for an answer without an orbit (e.g. a
    list of matches)."""
    j = json.loads(text)
    if "orbit" not in j:
        raise ValueError(f"SBDB: no orbit in the answer: {str(j)[:300]}")
    o, ob = j["object"], j["orbit"]
    el = {e["name"]: _float(e["value"]) for e in ob["elements"]}
    mp = {
        m["name"]: (_float(m["value"]), _float(m.get("sigma")), m.get("kind"))
        for m in ob.get("model_pars") or []
    }
    return {
        "des": o.get("des"),
        "fullname": o.get("fullname"),
        "spkid": o.get("spkid"),
        "kind": o.get("kind"),
        "prefix": o.get("prefix"),
        "orbit_id": ob.get("orbit_id"),
        "epoch": _float(ob.get("epoch")),
        "elements": el,
        "model_pars": mp,
        "producer": ob.get("producer"),
        "soln_date": ob.get("soln_date"),
        "n_obs": ob.get("n_obs_used"),
        "data_arc": ob.get("data_arc"),
        "sb_used": ob.get("sb_used"),
        "pe_used": ob.get("pe_used"),
    }


def _table(text):
    """(header list, row lists) of a Horizons CSV table."""
    try:
        soe, eoe = text.index("$$SOE"), text.index("$$EOE")
    except ValueError:
        raise ValueError("Horizons: no $$SOE/$$EOE in the answer:\n" + text[:2000])
    pre = [ln for ln in text[:soe].splitlines() if ln.strip() and not ln.startswith("*")]
    header = [h.strip() for h in pre[-1].split(",")]
    rows = [[c.strip() for c in ln.split(",")] for ln in text[soe + 5 : eoe].splitlines() if ln.strip()]
    return header, rows


def horizons_meta(text):
    """The solution of a Horizons answer: source (e.g. 'JPL#63'), the
    record number, the solution date, the target name, and the non-grav
    parameters it prints (A1, A2, A3, DT, ALN, NM, NN, NK, R0)."""
    head = text[: text.index("$$SOE")] if "$$SOE" in text else text
    meta = {}
    m = re.search(r"Target body name:\s*(.+?)\s*\{source:\s*([^}]+)\}", head)
    if m:
        meta["target"], meta["source"] = m.group(1).strip(), m.group(2).strip()
    m = re.search(r"Rec #:\s*(\d+)", head)
    if m:
        meta["rec"] = m.group(1)
    m = re.search(r"Soln\.date:\s*([0-9]{4}-[A-Za-z0-9-]+\s+[0-9:]+)", head)
    if m:
        meta["soln_date"] = m.group(1)
    for name in ("A1", "A2", "A3", "DT", "ALN", "NM", "NN", "NK", "R0", "AMRAT"):
        m = re.search(rf"(?<![A-Za-z0-9_]){name}=\s*([-+0-9.Ee]+)", head)
        if m:
            meta[name] = float(m.group(1))
    return meta


def parse_horizons_observer(text):
    """An observer table -> dict of arrays: jd (as printed), ra, dec [deg],
    ra3s, dec3s, smaa3s, smia3s [arcsec], theta [deg] (NaN where Horizons
    prints n.a.), plus 'meta'."""
    header, rows = _table(text)

    def col(pred):
        for j, h in enumerate(header):
            if pred(h):
                return np.array([_float(r[j]) if j < len(r) else np.nan for r in rows])
        return np.full(len(rows), np.nan)

    return {
        "jd": col(lambda h: h.startswith("Date")),
        "ra": col(lambda h: h.startswith("R.A.")),
        "dec": col(lambda h: h.startswith("DEC_") and "sig" not in h),
        "ra3s": col(lambda h: h.startswith("RA_3sigma")),
        "dec3s": col(lambda h: h.startswith("DEC_3sigma")),
        "smaa3s": col(lambda h: h.startswith("SMAA_3sig")),
        "smia3s": col(lambda h: h.startswith("SMIA_3sig")),
        "theta": col(lambda h: h.startswith("Theta")),
        "meta": horizons_meta(text),
    }


def parse_horizons_vectors(text):
    """A vector table -> dict: jd (TDB), X (3, N) [au], V (3, N) [au/day],
    and 'meta'."""
    header, rows = _table(text)
    idx = {h: j for j, h in enumerate(header)}
    get = lambda name: np.array([_float(r[idx[name]]) for r in rows])  # noqa: E731
    return {
        "jd": get("JDTDB"),
        "X": np.array([get("X"), get("Y"), get("Z")]),
        "V": np.array([get("VX"), get("VY"), get("VZ")]),
        "meta": horizons_meta(text),
    }


# ---------------------------------------------------------------------------
# Non-grav models
# ---------------------------------------------------------------------------


def jpl_gr(model_pars):
    """JPL's g(r) parameters (SBDB names) for a solution's model_pars:
    SBDB's defaults (Marsden's; "normalizing factor [default
    0.1112620426]") overridden by whatever it lists (Yarkovsky fits list
    ALN 1, NM 2, NK 0, R0 1)."""
    base = dict(JPL_DEFAULT_GR)
    for n in base:
        if n in model_pars and np.isfinite(model_pars[n][0]):
            base[n] = model_pars[n][0]
    return base


def gr_kind(gr):
    """'marsden', '1/r2' or 'other' for a g(r) parameter dict."""

    def same(ref):
        return all(np.isclose(gr[k], ref[k], rtol=1e-6, atol=1e-12) for k in ("ALN", "NM", "R0", "NK")) and (
            gr["NK"] == 0 or np.isclose(gr["NN"], ref["NN"], rtol=1e-6)
        )

    if same(JPL_DEFAULT_GR):
        return "marsden"
    if same(JPL_YARKOVSKY_GR):
        return "1/r2"
    return "other"


def jpl_nongrav_summary(model_pars):
    """(A (3,), has_ng, has_dt, gr dict, gr kind, other estimated params)."""
    A = np.array([model_pars.get(n, (0.0,))[0] for n in ("A1", "A2", "A3")], dtype=float)
    A = np.where(np.isfinite(A), A, 0.0)
    has_ng = bool(np.any(A != 0))
    has_dt = "DT" in model_pars and np.isfinite(model_pars["DT"][0]) and model_pars["DT"][0] != 0
    gr = jpl_gr(model_pars)
    known = {"A1", "A2", "A3", "DT", "ALN", "NM", "NN", "NK", "R0"}
    other = sorted(set(model_pars) - known)
    return A, has_ng, has_dt, gr, (gr_kind(gr) if has_ng else ""), other


# ---------------------------------------------------------------------------
# Geometry
# ---------------------------------------------------------------------------


def unit(ra_deg, dec_deg):
    ra, dec = np.radians(ra_deg), np.radians(dec_deg)
    return np.array([np.cos(dec) * np.cos(ra), np.cos(dec) * np.sin(ra), np.sin(dec)])


def separation_arcsec(ra1, dec1, ra2, dec2):
    """Great-circle separation [arcsec] (atan2 form, exact at small angles)."""
    u1, u2 = unit(ra1, dec1), unit(ra2, dec2)
    c = np.cross(u1, u2, axis=0)
    return np.degrees(np.arctan2(np.linalg.norm(c, axis=0), np.sum(u1 * u2, axis=0))) * 3600.0


def tangent_offset_arcsec(ra0, dec0, ra1, dec1):
    """(east, north) [arcsec] of (ra1, dec1) from (ra0, dec0) on the
    tangent plane at (ra0, dec0) (gnomonic)."""
    u0, u1 = unit(ra0, dec0), unit(ra1, dec1)
    ra, dec = np.radians(ra0), np.radians(dec0)
    e = np.array([-np.sin(ra), np.cos(ra), np.zeros_like(ra)])
    n = np.array([-np.sin(dec) * np.cos(ra), -np.sin(dec) * np.sin(ra), np.cos(dec)])
    d = np.sum(u0 * u1, axis=0)
    k = np.degrees(1.0) * 3600.0
    return np.sum(u1 * e, axis=0) / d * k, np.sum(u1 * n, axis=0) / d * k


def ellipse_cov(a, b, theta_deg, theta_from="north_east"):
    """The 2x2 (east, north) covariance [same units squared] of a 1-sigma
    ellipse with semi-axes a >= b, its major axis at angle theta measured
    from north through east (``north_east``) or west (``north_west``), or
    from east through north (``east_north``) or south (``east_south``).
    Arrays broadcast; shape (..., 2, 2)."""
    th = np.radians(theta_deg)
    if theta_from == "north_east":
        ue, un = np.sin(th), np.cos(th)
    elif theta_from == "north_west":
        ue, un = -np.sin(th), np.cos(th)
    elif theta_from == "east_north":
        ue, un = np.cos(th), np.sin(th)
    elif theta_from == "east_south":
        ue, un = np.cos(th), -np.sin(th)
    else:
        raise ValueError(theta_from)
    ve, vn = -un, ue
    a2, b2 = np.asarray(a) ** 2, np.asarray(b) ** 2
    cee = a2 * ue * ue + b2 * ve * ve
    cnn = a2 * un * un + b2 * vn * vn
    cen = a2 * ue * un + b2 * ve * vn
    return np.stack([np.stack([cee, cen], -1), np.stack([cen, cnn], -1)], -2)


def sigma_along(C, de, dn):
    """The 1-sigma of covariance C (..., 2, 2) along the direction (de, dn);
    NaN for a zero direction."""
    r = np.hypot(de, dn)
    with np.errstate(invalid="ignore", divide="ignore"):
        ue, un = de / r, dn / r
    return np.sqrt(C[..., 0, 0] * ue * ue + 2 * C[..., 0, 1] * ue * un + C[..., 1, 1] * un * un)


def state_to_cometary(X, V, mu):
    """Heliocentric ecliptic state -> (q, e, i, node, argperi [deg], time
    since perihelion [day]); the inverse of ssp.ephem_assist's
    cometary_to_helio_ecliptic, for the round trip (e != 1)."""
    X, V = np.asarray(X, float), np.asarray(V, float)
    r, v2 = np.linalg.norm(X), V @ V
    h = np.cross(X, V)
    hn = np.linalg.norm(h)
    evec = np.cross(V, h) / mu - X / r
    e = np.linalg.norm(evec)
    inc = np.arccos(h[2] / hn)
    nvec = np.array([-h[1], h[0], 0.0])
    nn = np.linalg.norm(nvec)
    node = np.arctan2(nvec[1], nvec[0]) % (2 * np.pi)
    argp = np.arctan2(np.dot(np.cross(nvec, evec), h) / hn, np.dot(nvec, evec)) % (2 * np.pi)
    nu = np.arctan2(np.dot(np.cross(evec, X), h) / hn, np.dot(evec, X))
    p = hn * hn / mu
    q = p / (1 + e)
    a = 1.0 / (2.0 / r - v2 / mu)
    n = np.sqrt(mu / abs(a) ** 3)
    if e < 1:
        E = 2 * np.arctan2(np.sqrt(1 - e) * np.sin(nu / 2), np.sqrt(1 + e) * np.cos(nu / 2))
        M = E - e * np.sin(E)
    else:
        H = 2 * np.arctanh(np.sqrt((e - 1) / (e + 1)) * np.tan(nu / 2))
        M = e * np.sinh(H) - H
    del nn
    return q, e, np.degrees(inc), np.degrees(node), np.degrees(argp), M / n


# ---------------------------------------------------------------------------
# Inputs: objects, times
# ---------------------------------------------------------------------------


def load_objects(fixture=FIXTURE):
    """{designation: class} of objects.txt."""
    out = {}
    with open(os.path.join(fixture, "objects.txt")) as f:
        for ln in f:
            if ln.strip():
                c, d = ln.rstrip("\n").split("\t")
                out[d] = c
    return out


def check1_objects(fixture=FIXTURE):
    """[(designation, class)] of check 1."""
    obj = load_objects(fixture)
    sel = [(d, c) for d, c in obj.items() if c in ("comet_ng", "yarkovsky")]
    sel += [(d, "comet_grav") for d in CHECK1_COMET_GRAV]
    sel += [(d, "control") for d in CHECK1_CONTROL]
    for d, c in sel:
        if obj.get(d) != c:
            raise ValueError(f"{d}: class {obj.get(d)} in objects.txt, expected {c}")
    return sel


def is_comet_des(des):
    return bool(re.match(r"^[PCDXIA]/", des))


def permids(designations, fixture=FIXTURE):
    """{designation: permid} from numbered_identifications (numbered ones)."""
    import pyarrow.parquet as pq

    t = pq.read_table(
        os.path.join(fixture, "in", "numbered_identifications.parquet"),
        columns=["unpacked_primary_provisional_designation", "permid"],
        filters=[("unpacked_primary_provisional_designation", "in", list(designations))],
    )
    return dict(zip(t.column(0).to_pylist(), t.column(1).to_pylist()))


def sbdb_sstr(des, permid):
    """What we ask SBDB for: the permanent designation if numbered."""
    return permid if permid else des


def ssobservation_rows(path=SSOBSERVATION, designations=None):
    """SSObservation rows (the partitioned SSObservation's directory or
    manifest, or a single Parquet file), optionally only ``designations``."""
    from ssp import ssobservation_parts as SP

    cols = [
        "designation",
        "midpointMjdTai",
        "ra",
        "dec",
        "ephRa",
        "ephDec",
        "ephRaErr",
        "ephDecErr",
        "ephRa_ephDec_Cov",
        "ephOffset",
        "obsid",
    ]
    filt = [("designation", "in", list(designations))] if designations is not None else None
    return SP.read_table(path, columns=cols, filters=filt).to_pandas()


def rubin_times_tai(df, des):
    """Unique sorted midpointMjdTai of one object's rows with an orbit."""
    s = df[(df.designation == des) & df.ephRa.notna()]
    return np.unique(s.midpointMjdTai.to_numpy())


def tai_to_tt(mjd_tai):
    return np.asarray(mjd_tai, float) + TT_MINUS_TAI_DAY


def tt_to_tdb(mjd_tt):
    from astropy.time import Time

    return Time(np.asarray(mjd_tt, float), format="mjd", scale="tt").tdb.mjd


def tdb_to_tt(mjd_tdb):
    from astropy.time import Time

    return Time(np.asarray(mjd_tdb, float), format="mjd", scale="tdb").tt.mjd


def roundtrip(mjd):
    """The times as sent (10 decimals)."""
    return np.array([float(fmt_mjd(t)) for t in np.atleast_1d(mjd)])


def check2_extra_tdb(sb, is_comet):
    """Check 2's extra times [TDB MJD]: epoch + CHECK2_OFFSETS_D and, for
    comets, perihelion + CHECK2_PERI_OFFSETS_D."""
    ep = sb["epoch"] - 2400000.5
    t = [ep + o for o in CHECK2_OFFSETS_D]
    if is_comet:
        tp = sb["elements"]["tp"] - 2400000.5
        t += [tp + o for o in CHECK2_PERI_OFFSETS_D]
    return np.unique(np.round(np.array(t), 6))


# ---------------------------------------------------------------------------
# The plan: every request, derived from the inputs and the cached SBDB
# ---------------------------------------------------------------------------


class Plan:
    """The requests of both checks. SBDB requests depend on the inputs
    only; Horizons requests need the SBDB answers (the JPL designation and,
    for check 2, the epoch and perihelion)."""

    def __init__(self, client, fixture=FIXTURE, ssobservation=SSOBSERVATION):
        self.client = client
        self.fixture = fixture
        self.objects = check1_objects(fixture)
        self.cls = dict(self.objects)
        des = [d for d, _ in self.objects]
        self.permid = permids(des, fixture)
        self.df = ssobservation_rows(ssobservation, des)
        self.comets = [d for d in des if is_comet_des(d)]
        self.asteroids = [d for d in des if not is_comet_des(d)]

    def asteroid_pdes(self, d):
        """An asteroid's JPL primary designation: its MPC number, or its
        provisional designation."""
        return self.permid.get(d) or d

    def has_sbdb(self, d):
        """Comets and check-2 asteroids get an SBDB lookup; the other
        asteroids' JPL solution (its ID and non-grav parameters) is read
        from the Horizons header (an sbdb_query for them all was refused,
        HTTP 400, and one lookup each would break the budget)."""
        return is_comet_des(d) or d in CHECK2_YARKOVSKY + CHECK2_CONTROL

    def sbdb_sstr(self, d):
        return sbdb_sstr(d, self.permid.get(d))

    # -- SBDB --------------------------------------------------------------
    def sbdb_requests(self):
        """[(service, params, label)]: one SBDB lookup per comet and per
        check-2 asteroid."""
        return [
            ("sbdb", sbdb_params(self.sbdb_sstr(d), d in SBDB_BY_DES), f"sbdb {d}")
            for d, _ in self.objects
            if self.has_sbdb(d)
        ]

    def sbdb(self, d):
        """parse_sbdb of one object's cached SBDB answer."""
        return parse_sbdb(
            self.client.get("sbdb", sbdb_params(self.sbdb_sstr(d), d in SBDB_BY_DES), f"sbdb {d}")
        )

    def jpl_model_pars(self, d):
        """JPL's model_pars for one object ({name: (value, sigma, kind)}):
        SBDB's, or (asteroids without a lookup) the non-grav parameters the
        Horizons header prints (no sigmas)."""
        if self.has_sbdb(d):
            return self.sbdb(d)["model_pars"]
        meta = self.observer(d)["meta"]
        out = {
            n: (meta[n], np.nan, "HZN")
            for n in ("A1", "A2", "A3", "ALN", "NM", "NN", "NK", "R0")
            if n in meta
        }
        for n in ("DT", "AMRAT"):  # printed as 0. when not used
            if meta.get(n, 0.0) != 0.0:
                out[n] = (meta[n], np.nan, "HZN")
        return out

    # -- Horizons -----------------------------------------------------------
    def check2(self):
        return tuple(CHECK2_COMETS) + tuple(CHECK2_YARKOVSKY) + tuple(CHECK2_CONTROL)

    def times(self, d):
        """(Rubin TAI MJDs, TT MJDs as sent for the observer table, TDB MJDs
        as sent for vectors or None, flags of which are Rubin times)."""
        tai = rubin_times_tai(self.df, d)
        tt = roundtrip(tai_to_tt(tai))
        rubin = np.ones(len(tt), bool)
        tdb = None
        if d in self.check2():
            extra_tdb = check2_extra_tdb(self.sbdb(d), is_comet_des(d))
            extra_tt = roundtrip(tdb_to_tt(extra_tdb))
            tt = np.concatenate([tt, extra_tt])
            rubin = np.concatenate([rubin, np.zeros(len(extra_tt), bool)])
            o = np.argsort(tt, kind="stable")
            tt, rubin = tt[o], rubin[o]
            tdb = roundtrip(tt_to_tdb(tt))
        return tai, tt, tdb, rubin

    def command(self, d):
        if is_comet_des(d):
            return horizons_command(self.sbdb(d)["des"], True)
        return horizons_command(self.asteroid_pdes(d), False)

    def horizons_requests(self):
        out = []
        for d, _ in self.objects:
            _, tt, tdb, _ = self.times(d)
            cmd = self.command(d)
            out.append(("horizons", horizons_observer_params(cmd, tt), f"obs {d}"))
            if tdb is not None:
                out.append(("horizons", horizons_vectors_params(cmd, tdb), f"vec {d}"))
        return out

    def mpc_rows(self):
        """{designation: mpc_orbits row (dict)} of the check-2b objects."""
        if not hasattr(self, "_mpc_rows"):
            import pyarrow.parquet as pq

            t = pq.read_table(
                os.path.join(self.fixture, "in", "mpc_orbits.parquet"),
                columns=[
                    "unpacked_primary_provisional_designation",
                    "q",
                    "e",
                    "i",
                    "node",
                    "argperi",
                    "peri_time",
                    "epoch_mjd",
                    "h",
                    "g",
                ],
                filters=[("unpacked_primary_provisional_designation", "in", list(CHECK2B_OBJECTS))],
            )
            self._mpc_rows = {r["unpacked_primary_provisional_designation"]: r for r in t.to_pylist()}
        return self._mpc_rows

    def user_requests(self):
        """Check 2b's requests: [(service, params, label)]."""
        ng = mpc_nongravs(CHECK2B_OBJECTS, self.fixture)
        out = []
        for d in CHECK2B_OBJECTS:
            _, tt, _, _ = self.times(d)
            tt = tt[np.isin(tt, roundtrip(tai_to_tt(rubin_times_tai(self.df, d))))]
            row = self.mpc_rows()[d]
            out.append(("horizons", horizons_user_params(row, ng[d], tt), f"user {d}"))
            if d in CHECK2B_NOGRAV:
                out.append(
                    ("horizons", horizons_user_params(row, ng[d], tt, with_ng=False), f"user-grav {d}")
                )
        return out

    def observer(self, d):
        _, tt, _, _ = self.times(d)
        return parse_horizons_observer(
            self.client.get("horizons", horizons_observer_params(self.command(d), tt), f"obs {d}")
        )

    def vectors(self, d):
        _, _, tdb, _ = self.times(d)
        return parse_horizons_vectors(
            self.client.get("horizons", horizons_vectors_params(self.command(d), tdb), f"vec {d}")
        )


# ---------------------------------------------------------------------------
# fetch / status
# ---------------------------------------------------------------------------


def cmd_fetch(args):
    client = JPLClient(args.cache, offline=args.dry_run)
    plan = Plan(client, args.fixture, args.ssobservation)
    stages = ["sbdb", "horizons", "user"] if args.stage == "all" else [args.stage]
    for st in stages:
        reqs = {"sbdb": plan.sbdb_requests, "horizons": plan.horizons_requests, "user": plan.user_requests}[
            st
        ]()
        if args.only:
            reqs = [r for r in reqs if any(o == r[2].split(" ", 1)[1] for o in args.only)]
        todo = [r for r in reqs if not client.cached(*r)]
        print(
            f"[{st}] {len(reqs)} requests, {len(todo)} not cached; {client.n_logged()} sent so far "
            f"(budget {client.budget})"
        )
        if args.limit is not None:
            todo = todo[: args.limit]
        for service, params, label in todo:
            if args.dry_run:
                print(f"  would send: {label}  ({len(request_url(service, params))} chars)")
                if args.show_url:
                    print("    " + request_url(service, params))
                continue
            text = client.get(service, params, label)
            print(f"  {label}: {len(text)} bytes")
    print(f"sent this run: {client.sent}; total logged: {client.n_logged()}")
    return 0


def cmd_status(args):
    client = JPLClient(args.cache)
    files = [f for f in os.listdir(args.cache) if not f.endswith(".request") and not f.startswith(".")]
    print(
        f"cache {args.cache}: {len(files)} files; {client.n_logged()} requests logged "
        f"(budget {client.budget})"
    )
    return 0


# ---------------------------------------------------------------------------
# Check 1: orbits (MPC through SSObservation against JPL through Horizons)
# ---------------------------------------------------------------------------

#: The ways Horizons' 3-sigma quantities could be meant: Theta measured
#: from north or from east; RA_3sigma on the sky or in RA (needing
#: cos(dec)). Chosen by the data (``jpl_convention``): 36 against 37 fixes
#: the axis Theta starts from and whether RA_3sigma is on the sky; the
#: sense of Theta (which only flips the correlation's sign) is chosen by
#: agreement with our own ellipses' correlations.
CONVENTIONS = [(th, cd) for th in ("north_east", "east_north") for cd in (True, False)]
MIRROR = {"north_east": "north_west", "east_north": "east_south"}


def jpl_convention(obs_rows):
    """The (theta_from, ra3s_on_sky) convention under which Horizons'
    ellipse (37) reproduces its RA/Dec 3-sigma (36) best, with the rms
    relative mismatch of each, from rows with sigmas > 0.05" (the table
    prints 3 decimals). ``obs_rows``: dicts of parse_horizons_observer
    arrays plus 'dec'."""
    out = {}
    for th, on_sky in CONVENTIONS:
        r = []
        for o in obs_rows:
            ok = np.isfinite(o["smaa3s"]) & (o["smia3s"] > 0.05) & (o["ra3s"] > 0.05) & (o["dec3s"] > 0.05)
            if not ok.any():
                continue
            C = ellipse_cov(o["smaa3s"][ok], o["smia3s"][ok], o["theta"][ok], th)
            ra_sky = o["ra3s"][ok] * (1.0 if on_sky else np.cos(np.radians(o["dec"][ok])))
            r += list(np.sqrt(C[:, 0, 0]) / ra_sky - 1) + list(np.sqrt(C[:, 1, 1]) / o["dec3s"][ok] - 1)
        out[(th, on_sky)] = float(np.sqrt(np.mean(np.square(r)))) if r else np.nan
    best = min(out, key=lambda k: np.inf if np.isnan(out[k]) else out[k])
    return best, out


def jpl_correlation_sense(pairs, convention):
    """(convention, fraction): Theta's sense (``convention`` or its mirror)
    under which the sign of JPL's ellipse correlation agrees with ours more
    often, over rows where both |rho| > 0.3, and that fraction. ``pairs``:
    [(observer dict, our (K, 2, 2) covariance)] on the same rows."""
    th, on_sky = convention
    agree = {th: [], MIRROR[th]: []}
    for o, Co in pairs:
        ro = Co[:, 0, 1] / np.sqrt(Co[:, 0, 0] * Co[:, 1, 1])
        for t in agree:
            C = ellipse_cov(o["smaa3s"], o["smia3s"], o["theta"], t)
            rj = C[:, 0, 1] / np.sqrt(C[:, 0, 0] * C[:, 1, 1])
            ok = np.isfinite(rj) & np.isfinite(ro) & (np.abs(rj) > 0.3) & (np.abs(ro) > 0.3)
            agree[t] += list(np.sign(rj[ok]) == np.sign(ro[ok]))
    frac = {t: (np.mean(v) if v else np.nan) for t, v in agree.items()}
    best = max(frac, key=lambda t: -1 if np.isnan(frac[t]) else frac[t])
    return (best, on_sky), frac


def jpl_cov_arcsec2(o, convention):
    """JPL's 1-sigma (east, north) covariance [arcsec^2] per row: from the
    ellipse (37) where it's given, else from the RA/Dec 3-sigma (36),
    uncorrelated; NaN where neither is."""
    th, on_sky = convention
    C = ellipse_cov(o["smaa3s"] / 3.0, o["smia3s"] / 3.0, o["theta"], th)
    ra = o["ra3s"] / 3.0 * (1.0 if on_sky else np.cos(np.radians(o["dec"])))
    alt = np.zeros_like(C)
    alt[:, 0, 0], alt[:, 1, 1] = ra**2, (o["dec3s"] / 3.0) ** 2
    use = ~np.isfinite(C[:, 0, 0])
    C[use] = alt[use]
    return C


def our_cov_arcsec2(df):
    """Our 1-sigma (east, north) covariance [arcsec^2] (ephRaErr is on the
    sky; the contract's DiaSource convention)."""
    k = 3600.0
    C = np.empty((len(df), 2, 2))
    C[:, 0, 0] = (df.ephRaErr.to_numpy(float) * k) ** 2
    C[:, 1, 1] = (df.ephDecErr.to_numpy(float) * k) ** 2
    C[:, 0, 1] = C[:, 1, 0] = df.ephRa_ephDec_Cov.to_numpy(float) * k * k
    return C


def mpc_nongravs(designations, fixture=FIXTURE):
    """{designation: ssp.nongrav.NonGrav} of the MPC orbits."""
    import pyarrow.parquet as pq
    from ssp.nongrav import nongrav_params

    t = pq.read_table(
        os.path.join(fixture, "in", "mpc_orbits.parquet"),
        columns=["unpacked_primary_provisional_designation", "mpc_orb_jsonb"],
        filters=[("unpacked_primary_provisional_designation", "in", list(designations))],
    )
    return {d: nongrav_params(j) for d, j in zip(t.column(0).to_pylist(), t.column(1).to_pylist())}


def orbit_flags(d, cls, mpc_ng, jpl_mp, source):
    """The check-1 flags of one object's models (list of strings)."""
    flags = []
    A, has_ng, has_dt, gr, kind, other = jpl_nongrav_summary(jpl_mp)
    comet = is_comet_des(d)
    if not str(source).startswith("JPL#"):
        flags.append(f"JPL uses a non-JPL solution ({source})")
    if bool(mpc_ng.model) and not has_ng:
        flags.append("MPC fits non-gravs, JPL is gravity-only")
    if has_ng and not mpc_ng.model:
        flags.append("JPL fits non-gravs, MPC is gravity-only")
    if has_dt:
        flags.append(f"JPL fits DT ({jpl_mp['DT'][0]:.4g} d)")
    if has_ng and comet and kind != "marsden":
        flags.append(
            "JPL g(r) non-standard: " + ", ".join(f"{k} {gr[k]:.6g}" for k in ("ALN", "R0", "NM", "NN", "NK"))
        )
    if has_ng and not comet and kind != "1/r2":
        flags.append(f"JPL asteroid g(r) is {kind}")
    if other:
        flags.append("JPL also uses " + ", ".join(other))
    if bool(mpc_ng.model) and has_ng:
        fitted = [f"A{i + 1}" for i in range(3) if mpc_ng.fitted[i]]
        jfit = [f"A{i + 1}" for i in range(3) if A[i] != 0]
        if fitted != jfit:
            flags.append(f"fitted A's differ: MPC {'+'.join(fitted)}, JPL {'+'.join(jfit)}")
    return flags


def check1_rows(plan, d):
    """(SSObservation rows with an orbit, the Horizons observer arrays on those
    rows) of one object."""
    df = plan.df[(plan.df.designation == d) & plan.df.ephRa.notna()].sort_values("midpointMjdTai")
    o = plan.observer(d)
    _, tt, _, _ = plan.times(d)
    tt_rows = roundtrip(tai_to_tt(df.midpointMjdTai.to_numpy()))
    idx = np.searchsorted(tt, tt_rows)
    if not np.all(np.abs(tt[np.clip(idx, 0, len(tt) - 1)] - tt_rows) < 1e-9):
        raise RuntimeError(f"{d}: SSObservation times missing from the Horizons table")
    for k in ("ra", "dec", "ra3s", "dec3s", "smaa3s", "smia3s", "theta"):
        o[k] = o[k][idx]
    return df, o


def check1_object(plan, d, cls, mpc_ng, convention):
    """Per-row and per-object check-1 results for one object."""
    import pandas as pd

    df, o = check1_rows(plan, d)
    meta = o["meta"]
    ra_o, dec_o = df.ephRa.to_numpy(float), df.ephDec.to_numpy(float)
    ra_m, dec_m = df.ra.to_numpy(float), df.dec.to_numpy(float)
    sep = separation_arcsec(ra_o, dec_o, o["ra"], o["dec"])
    de, dn = tangent_offset_arcsec(ra_o, dec_o, o["ra"], o["dec"])
    Co, Cj = our_cov_arcsec2(df), jpl_cov_arcsec2(o, convention)
    s_c = sigma_along(Co + Cj, de, dn)
    off_jpl = separation_arcsec(ra_m, dec_m, o["ra"], o["dec"])
    rows = pd.DataFrame(
        {
            "designation": d,
            "class": cls,
            "obsid": df.obsid.to_numpy(),
            "midpointMjdTai": df.midpointMjdTai.to_numpy(),
            "sep_arcsec": sep,
            "sep_east_arcsec": de,
            "sep_north_arcsec": dn,
            "sigma_comb_arcsec": s_c,
            "sep_nsigma": sep / s_c,
            "ephOffset_mpc": df.ephOffset.to_numpy(float),
            "offset_jpl": off_jpl,
            "ours_raErr_arcsec": np.sqrt(Co[:, 0, 0]),
            "ours_decErr_arcsec": np.sqrt(Co[:, 1, 1]),
            "jpl_ra_1sig_arcsec": np.sqrt(Cj[:, 0, 0]),
            "jpl_dec_1sig_arcsec": np.sqrt(Cj[:, 1, 1]),
            "jpl_ra3s": o["ra3s"],
            "jpl_dec3s": o["dec3s"],
            "jpl_smaa3s": o["smaa3s"],
            "jpl_smia3s": o["smia3s"],
            "jpl_theta": o["theta"],
        }
    )
    mp = plan.jpl_model_pars(d)
    flags = orbit_flags(d, cls, mpc_ng, mp, meta.get("source"))
    A, has_ng, has_dt, gr, kind, _ = jpl_nongrav_summary(mp)
    with np.errstate(invalid="ignore", divide="ignore"):
        sig_ratio = np.sqrt((Co[:, 0, 0] + Co[:, 1, 1]) / (Cj[:, 0, 0] + Cj[:, 1, 1]))
    nanmax = lambda x: np.nanmax(x) if np.isfinite(x).any() else np.nan  # noqa: E731
    nanmed = lambda x: np.nanmedian(x) if np.isfinite(x).any() else np.nan  # noqa: E731
    obj = {
        "designation": d,
        "class": cls,
        "jpl_source": meta.get("source"),
        "jpl_target": meta.get("target"),
        "n": len(df),
        "sep_med_arcsec": np.median(sep),
        "sep_max_arcsec": np.max(sep),
        "nsig_med": nanmed(rows.sep_nsigma.to_numpy()),
        "nsig_max": nanmax(rows.sep_nsigma.to_numpy()),
        "ephOffset_mpc_med": np.median(rows.ephOffset_mpc),
        "offset_jpl_med": np.median(off_jpl),
        "better": "MPC" if np.median(rows.ephOffset_mpc) < np.median(off_jpl) else "JPL",
        "ours_sig_med_arcsec": np.median(np.sqrt(Co[:, 0, 0] + Co[:, 1, 1])),
        "jpl_sig_med_arcsec": nanmed(np.sqrt(Cj[:, 0, 0] + Cj[:, 1, 1])),
        "sig_ratio_ours_jpl_med": nanmed(sig_ratio),
        "mpc_model": mpc_ng.model or "grav",
        "mpc_A": ";".join(f"{a:.4g}" for a in mpc_ng.A) if mpc_ng.model else "",
        "jpl_model": ("ng/" + kind + ("+DT" if has_dt else "")) if has_ng else "grav",
        "jpl_A": ";".join(f"{a:.4g}" for a in A) if has_ng else "",
    }
    om, oj = obj["ephOffset_mpc_med"], obj["offset_jpl_med"]
    if max(om, oj) > 1.0 and max(om, oj) > 2 * min(om, oj):
        flags.append(
            f"{'JPL' if oj < om else 'MPC'}'s orbit fits Rubin far better (median offset MPC {om:.2f}\", "
            f'JPL {oj:.2f}")'
        )
    if obj["nsig_max"] > 3:
        flags.append(
            f"MPC and JPL positions disagree by > 3 sigma combined (max {obj['nsig_max']:.1f}, "
            f"median {obj['nsig_med']:.1f})"
        )
    if not np.isfinite(o["ra3s"]).any():
        flags.append("Horizons gives no uncertainty (no covariance)")
    if d == "2025 QH138":
        sA = np.sqrt(mpc_ng.cov[6, 6]) if mpc_ng.cov is not None and mpc_ng.cov.shape[0] > 6 else np.nan
        flags.append(
            f"2025 QH138: MPC Yarkovsky A2 {mpc_ng.A[1]:.3g} au/d^2 (sigma {sA:.2g}), ~1000x typical; "
            f"JPL: {'A2 %.3g' % A[1] if has_ng else 'gravity-only'} ({meta.get('source')})"
        )
    obj["flag_text"] = " | ".join(flags)
    return rows, obj


def cmd_orbits(args):
    import pandas as pd

    client = JPLClient(args.cache)
    plan = Plan(client, args.fixture, args.ssobservation)
    des = [d for d, _ in plan.objects]
    mng = mpc_nongravs(des, args.fixture)
    pairs = []
    for d in des:
        df, o = check1_rows(plan, d)
        pairs.append((o, our_cov_arcsec2(df)))
    convention, conv_rms = jpl_convention([o for o, _ in pairs])
    convention, sense = jpl_correlation_sense(pairs, convention)
    rows, objs = [], []
    for d, c in plan.objects:
        r, ob = check1_object(plan, d, c, mng[d], convention)
        rows.append(r)
        objs.append(ob)
    rows = pd.concat(rows, ignore_index=True)
    objs = pd.DataFrame(objs)
    os.makedirs(args.out, exist_ok=True)
    rows.to_csv(os.path.join(args.out, "orbits_rows.csv"), index=False)
    objs.to_csv(os.path.join(args.out, "orbits_objects.csv"), index=False)
    lines = orbits_report(objs, rows, convention, conv_rms, sense, client.n_logged(), args)
    text = "\n".join(lines) + "\n"
    with open(os.path.join(args.out, "orbits_report.txt"), "w") as f:
        f.write(text)
    print(text)
    return 0


def orbits_report(objs, rows, convention, conv_rms, sense, n_req, args):
    L = [
        "# Check 1 (orbits): MPC orbits (our SSObservation) against JPL orbits (Horizons), X05",
        f"SSObservation: {args.ssobservation}",
        f"JPL requests so far (all of WP N5): {n_req}",
        "Horizons times: midpointMjdTai + 32.184 s, requested as TT MJD; astrometric RA/Dec (quantity 1),",
        "  3-sigma RA/Dec (36) and plane-of-sky ellipse (37).",
        "Horizons 3-sigma convention, chosen by the data (rms relative mismatch of 37 against 36):",
    ]
    for k, v in conv_rms.items():
        L.append(
            f"  theta from {k[0].split('_')[0]:6s} "
            f"RA_3sigma {'on the sky' if k[1] else 'in RA     '}: {v:.4f}"
            + (
                "   <- used"
                if k[1] == convention[1] and k[0].split("_")[0] == convention[0].split("_")[0]
                else ""
            )
        )
    L.append(
        "  theta's sense, by agreement of the correlation's sign with our ellipses\n"
        "  (rows with |rho| > 0.3 in both):"
    )
    for t, f in sense.items():
        L.append(
            f"    from {t.replace('_', ' through '):20s}: {f:.3f}"
            + ("   <- used" if t == convention[0] else "")
        )
    L += [
        "",
        "sep = |ours - JPL| [arcsec]; nsig = sep / combined 1-sigma along the separation",
        "  (our ellipse + JPL's);",
        "off = the observed position's offset from each prediction [arcsec, median]; sig = median",
        "sqrt(sigma_E^2 + sigma_N^2) [arcsec], ours / JPL's 1-sigma.",
        "",
    ]
    hdr = (
        f"{'class':10s} {'object':13s} {'JPL soln':12s} {'n':>3s} {'sep med':>8s} {'sep max':>8s} "
        f"{'nsig med':>8s} {'nsig max':>8s} {'off MPC':>8s} {'off JPL':>8s} {'sig ours':>8s} "
        f"{'sig JPL':>8s} {'ratio':>6s}  models (MPC / JPL)"
    )
    L.append(hdr)
    for _, r in objs.iterrows():
        L.append(
            f"{r['class']:10s} {r.designation:13s} {str(r.jpl_source):12s} {r.n:3d} {r.sep_med_arcsec:8.3f} "
            f"{r.sep_max_arcsec:8.3f} {r.nsig_med:8.2f} {r.nsig_max:8.2f} {r.ephOffset_mpc_med:8.3f} "
            f"{r.offset_jpl_med:8.3f} {r.ours_sig_med_arcsec:8.3f} {r.jpl_sig_med_arcsec:8.3f} "
            f"{r.sig_ratio_ours_jpl_med:6.2f}  {r.mpc_model} / {r.jpl_model}"
        )
    L += [
        "",
        "## By class (medians over objects; 'JPL better' counts objects whose median offset",
        "## is smaller with JPL's orbit)",
    ]
    for c, g in objs.groupby("class", sort=False):
        L.append(
            f'{c:10s} {len(g):3d} objects: sep med {g.sep_med_arcsec.median():.3f}" (max of maxes '
            f'{g.sep_max_arcsec.max():.3f}"), nsig med {g.nsig_med.median():.2f}; off MPC '
            f'{g.ephOffset_mpc_med.median():.3f}" vs JPL {g.offset_jpl_med.median():.3f}" '
            f"(JPL better for {int((g.better == 'JPL').sum())}/{len(g)}); sigma ours/JPL "
            f"{g.sig_ratio_ours_jpl_med.median():.2f}"
        )
    L += ["", "## Flags"]
    for _, r in objs.iterrows():
        if r.flag_text:
            L.append(f"{r.designation:13s} {r.flag_text}")
    return L


# ---------------------------------------------------------------------------
# Check 2: integrator (JPL's orbit through our ASSIST code against Horizons)
# ---------------------------------------------------------------------------

#: Pass thresholds. Gravity-only: (a) and (b) within PASS_GRAV_MAS ("the
#: mas level"). Everything: (b, geometry only) within PASS_GEOM_MAS. With
#: non-gravs: the integration residual's growth, max |dX| / max(|t -
#: epoch|, 1 yr), within PASS_NG_RATE_FACTOR x the largest of the
#: gravity-only controls' (their residual is ASSIST's and JPL's force-model
#: floor: it grows ~0.13 km/yr with or without non-gravs, is unchanged by
#: IAS15's tolerance, and is far below what the non-gravs move).
PASS_GRAV_MAS = 2.0
PASS_GEOM_MAS = 0.5
PASS_NG_RATE_FACTOR = 2.0
YEAR_D = 365.25


def jpl_nongrav(sb, d):
    """ssp.nongrav.NonGrav for a JPL solution, registering its g(r) under a
    model name of its own in ssp.nongrav.G_OF_R when it's neither the
    standard comet one nor 1/r^2 (a runtime entry, not a code change: the
    general g(r) is ASSIST's, which ``ssp.nongrav.apply`` sets)."""
    from ssp import nongrav as N

    A, has_ng, has_dt, gr, kind, other = jpl_nongrav_summary(sb["model_pars"])
    if not has_ng:
        return N.NONE, ""
    if kind == "marsden":
        model = "comet"
    elif kind == "1/r2":
        model = "yarkovsky"
    else:
        model = f"jpl:{d}"
        N.G_OF_R[model] = {GR_TO_ASSIST[k]: float(v) for k, v in gr.items()}
    note = ("DT ignored; " if has_dt else "") + (f"also {other}; " if other else "")
    return N.NonGrav(np.asarray(A, float), model, A != 0, None), note + f"g(r) {kind}"


def jpl_row(sb):
    """An mpcorb-like row of JPL's elements for compute_ephemerides_one:
    the epoch converted TDB -> TT (that path assumes TT epochs and converts
    back), and peri_time placed so that epoch - peri_time is JPL's TDB
    interval exactly."""
    el = sb["elements"]
    ep_tdb = sb["epoch"] - 2400000.5
    tp_tdb = el["tp"] - 2400000.5
    ep_tt = float(tdb_to_tt(ep_tdb))
    return {
        "q": el["q"],
        "e": el["e"],
        "i": el["i"],
        "node": el["om"],
        "argperi": el["w"],
        "epoch_mjd": ep_tt,
        "peri_time": ep_tt - (ep_tdb - tp_tdb),
        "h": np.nan,
        "g": 0.15,
    }


def jpl_initial_state(sb, ephem, mu):
    """JPL's elements -> (helio ecliptic X, V; bary ICRF X, V; t0 ASSIST)."""
    from ssp.ephem_assist import ASSIST_SUN, MJD_J2000, cometary_to_helio_ecliptic, ecliptic_to_equatorial

    el = sb["elements"]
    ep = sb["epoch"] - 2400000.5
    Xe, Ve = cometary_to_helio_ecliptic(
        el["q"],
        el["e"],
        np.radians(el["i"]),
        np.radians(el["om"]),
        np.radians(el["w"]),
        sb["epoch"] - el["tp"],
        mu=mu,
    )
    t0 = ep - MJD_J2000
    s = ephem.get_particle(ASSIST_SUN, t0)
    Xb = ecliptic_to_equatorial(Xe) + np.array([s.x, s.y, s.z])
    Vb = ecliptic_to_equatorial(Ve) + np.array([s.vx, s.vy, s.vz])
    return Xe, Ve, Xb, Vb, t0


def horizons_helio_state(text):
    """The 'Equivalent ICRF heliocentric cartesian coordinates' of the
    Horizons header (the state JPL derives from its elements), (6,)."""
    head = text[: text.index("$$SOE")]
    i = head.index("Equivalent ICRF heliocentric cartesian")
    out = []
    for k in ("X", "Y", "Z", "VX", "VY", "VZ"):
        m = re.search(rf"(?<![A-Z]){k}=\s*([-+0-9.E]+)", head[i:])
        out.append(float(m.group(1)))
    return np.array(out)


AU_KM = 149597870.7


def horizons_initial_elements(text):
    """The 'Initial IAU76/J2000 heliocentric ecliptic osculating elements'
    of a Horizons header (the elements its integration starts from, at the
    solution epoch), in parse_sbdb's form ('epoch' JD TDB, 'elements')."""
    head = text[: text.index("$$SOE")]
    blk = head[head.index("Initial IAU76/J2000") :]
    blk = blk[: blk.index("Equivalent ICRF")]

    def g(k):
        return float(re.search(rf"(?<![A-Z]){k}=\s*([-+0-9.E]+)", blk).group(1))

    return {
        "epoch": g("EPOCH"),
        "elements": {"e": g("EC"), "q": g("QR"), "tp": g("TP"), "om": g("OM"), "w": g("W"), "i": g("IN")},
    }


def check2_object(plan, d, ephem):
    """Check-2 results for one object: (summary dict, per-time DataFrame)."""
    import pandas as pd
    from astropy.time import Time
    from ssp import ephem_assist as EA

    sb = plan.sbdb(d)
    ng, ng_note = jpl_nongrav(sb, d)
    is_comet = is_comet_des(d)
    _, tt, tdb, rubin = plan.times(d)
    obs = plan.observer(d)
    vec = plan.vectors(d)
    obs_text = plan.client.get("horizons", horizons_observer_params(plan.command(d), tt), f"obs {d}")
    # The initial elements: those Horizons integrates from (its header, at
    # the solution epoch). For comets they are SBDB's; for asteroids SBDB
    # gives elements re-osculated at a standard epoch instead, checked
    # separately below (sbdb_epoch_km).
    ic = horizons_initial_elements(obs_text)
    if is_comet:
        for k, v in ic["elements"].items():
            if not np.isclose(v, sb["elements"][k], rtol=1e-12, atol=1e-9):
                raise RuntimeError(f"{d}: Horizons' initial {k} {v} != SBDB's {sb['elements'][k]}")
    # The initial state with DE440's GM_sun (JPL's: it reproduces the state
    # Horizons prints to < 1.2 m, k^2 to < 3.1 m), and with ssp's k^2 (what
    # compute_ephemerides_one uses, in (b)).
    Xe, Ve, Xb, Vb, t0 = jpl_initial_state(ic, ephem, GM_SUN_DE440)
    Xe2, Ve2, Xb2, Vb2, _ = jpl_initial_state(ic, ephem, EA.GM_SUN)
    _, _, Xs, _, _ = jpl_initial_state(sb, ephem, GM_SUN_DE440)
    hz = horizons_helio_state(obs_text)
    Xeq, Veq = EA.ecliptic_to_equatorial(Xe), EA.ecliptic_to_equatorial(Ve)
    conv_km = np.linalg.norm(Xeq - hz[:3]) * AU_KM
    conv_mm_s = np.linalg.norm(Veq - hz[3:]) * AU_KM * 1e6 / 86400
    gm_km = np.linalg.norm(Xb - Xb2) * AU_KM
    gm_mm_s = np.linalg.norm(Vb - Vb2) * AU_KM * 1e6 / 86400
    q, e, inc, node, argp, dtp = state_to_cometary(Xe, Ve, GM_SUN_DE440)
    el = ic["elements"]
    rt = {
        "dq_au": q - el["q"],
        "de": e - el["e"],
        "di_deg": inc - el["i"],
        "dnode_deg": (node - el["om"] + 180) % 360 - 180,
        "dargp_deg": (argp - el["w"] + 180) % 360 - 180,
        "dtp_day": (ic["epoch"] - dtp) - el["tp"],
    }
    # (a) vectors
    t_assist = tdb - EA.MJD_J2000
    X, V = EA._propagate_one(Xb, Vb, t0, t_assist, ephem, nongrav=ng)
    dX = (X - vec["X"]) * AU_KM
    # What the non-gravs move: the same integration without them
    if ng.model:
        X0, _ = EA._propagate_one(Xb, Vb, t0, t_assist, ephem)
        ng_km = np.linalg.norm(X0 - vec["X"], axis=0) * AU_KM
    else:
        ng_km = np.zeros(len(t_assist))
    earth = np.array([[p.x, p.y, p.z] for p in (ephem.get_particle("Earth", float(t)) for t in t_assist)]).T
    geo = np.linalg.norm(vec["X"] - earth, axis=0) * AU_KM
    a_mas = np.degrees(np.linalg.norm(dX, axis=0) / geo) * 3.6e6

    def at_epoch(epoch_jd, X0):
        """|X0 - Horizons' vector| [km] at an epoch in the vector table."""
        k = int(np.argmin(np.abs(tdb - (epoch_jd - 2400000.5))))
        return (
            np.linalg.norm(X0 - vec["X"][:, k]) * AU_KM
            if abs(tdb[k] - (epoch_jd - 2400000.5)) < 1e-6
            else np.nan
        )

    ep_km = at_epoch(ic["epoch"], Xb)
    sbdb_ep_km = at_epoch(sb["epoch"], Xs)
    # (b) observer RA/Dec through compute_ephemerides_one
    row = jpl_row(ic)
    res = EA.compute_ephemerides_one(d, Time(tt, format="mjd", scale="tt"), None, ephem, row=row, nongrav=ng)
    b_mas = separation_arcsec(res.ra_deg, res.dec_deg, obs["ra"], obs["dec"]) * 1e3
    # (b, geometry only) Horizons' vectors through our light time and X05
    # observer (ssp.ephem_assist._emission_state, as compute_ephemerides_one
    # uses it): isolates the light-time, aberration and observer conventions
    sun = np.array(
        [[p.x, p.y, p.z] for p in (ephem.get_particle(EA.ASSIST_SUN, float(t)) for t in t_assist)]
    ).T
    rho = EA._emission_state(vec["X"], vec["V"], sun, res.obs)[0] - res.obs
    ra_g, dec_g = EA._vector_to_radec(rho)
    bg_mas = separation_arcsec(ra_g, dec_g, obs["ra"], obs["dec"]) * 1e3
    rows = pd.DataFrame(
        {
            "designation": d,
            "mjd_tt": tt,
            "mjd_tdb": tdb,
            "rubin": rubin,
            "dt_from_epoch_d": tdb - (ic["epoch"] - 2400000.5),
            "a_dX_km": np.linalg.norm(dX, axis=0),
            "a_mas": a_mas,
            "b_mas": b_mas,
            "b_geom_mas": bg_mas,
            "without_ng_km": ng_km,
            "r_helio_au": np.linalg.norm(res.helio_pos, axis=0),
        }
    )
    kind = "grav" if not ng.model else ("comet" if is_comet else "yarkovsky")
    yr = np.maximum(np.abs(rows.dt_from_epoch_d.to_numpy()) / YEAR_D, 1.0)
    summ = {
        "designation": d,
        "kind": kind,
        "jpl_soln": sb["orbit_id"],
        "horizons_source": obs["meta"].get("source"),
        "vec_source": vec["meta"].get("source"),
        "epoch_tdb_mjd": ic["epoch"] - 2400000.5,
        "sbdb_epoch_tdb_mjd": sb["epoch"] - 2400000.5,
        "sbdb_epoch_km": sbdb_ep_km,
        "A": ";".join(f"{a:.4g}" for a in ng.A) if ng.model else "",
        "model": ng.model,
        "note": ng_note,
        "conv_vs_horizons_km": conv_km,
        "conv_vs_horizons_mm_s": conv_mm_s,
        "epoch_vec_km": ep_km,
        "gm_k2_vs_de440_km": gm_km,
        "gm_k2_vs_de440_mm_s": gm_mm_s,
        "roundtrip_max_rel": max(
            abs(rt["dq_au"]) / el["q"],
            abs(rt["de"]),
            abs(np.radians(rt["di_deg"])),
            abs(np.radians(rt["dnode_deg"])),
            abs(np.radians(rt["dargp_deg"])),
        ),
        "roundtrip_dtp_s": rt["dtp_day"] * 86400,
        "n_times": len(tt),
        "n_rubin": int(rubin.sum()),
        "span_d": f"{rows.dt_from_epoch_d.min():.0f}..{rows.dt_from_epoch_d.max():.0f}",
        "a_max_km": rows.a_dX_km.max(),
        "a_max_mas": rows.a_mas.max(),
        "a_rubin_max_mas": rows.a_mas[rubin].max(),
        "b_max_mas": rows.b_mas.max(),
        "b_rubin_max_mas": rows.b_mas[rubin].max(),
        "b_med_mas": rows.b_mas.median(),
        "b_geom_max_mas": rows.b_geom_mas.max(),
        "rate_km_yr": float(np.max(rows.a_dX_km / yr)),
        "without_ng_max_km": rows.without_ng_km.max(),
    }
    return summ, rows


def check2_pass(summ):
    """PASS per object (see PASS_GRAV_MAS etc.); returns the non-grav rate
    threshold [km/yr]."""
    grav = summ.kind == "grav"
    rate_thr = PASS_NG_RATE_FACTOR * summ.rate_km_yr[grav].max()
    ok_geom = summ.b_geom_max_mas <= PASS_GEOM_MAS
    ok_grav = (summ.a_max_mas <= PASS_GRAV_MAS) & (summ.b_max_mas <= PASS_GRAV_MAS)
    ok_ng = summ.rate_km_yr <= rate_thr
    summ["pass"] = ok_geom & np.where(grav, ok_grav, ok_ng)
    return rate_thr


def cmd_integrator(args):
    import pandas as pd
    from ssp.ephem_assist import open_ephem

    client = JPLClient(args.cache)
    plan = Plan(client, args.fixture, args.ssobservation)
    ephem = open_ephem()
    summ, rows = [], []
    for d in plan.check2():
        s, r = check2_object(plan, d, ephem)
        summ.append(s)
        rows.append(r)
    summ = pd.DataFrame(summ)
    rate_thr = check2_pass(summ)
    rows = pd.concat(rows, ignore_index=True)
    os.makedirs(args.out, exist_ok=True)
    summ.to_csv(os.path.join(args.out, "integrator_objects.csv"), index=False)
    rows.to_csv(os.path.join(args.out, "integrator_rows.csv"), index=False)
    text = "\n".join(integrator_report(summ, rows, client.n_logged(), rate_thr)) + "\n"
    with open(os.path.join(args.out, "integrator_report.txt"), "w") as f:
        f.write(text)
    print(text)
    return 0 if summ["pass"].all() else 1


def integrator_report(summ, rows, n_req, rate_thr):
    grav = summ.kind == "grav"
    L = [
        "# Check 2 (integrator): JPL's orbits and non-gravs through our ASSIST code, against Horizons",
        f"JPL requests so far (all of WP N5): {n_req}",
        "Initial conditions: the elements Horizons integrates from (its header; heliocentric ecliptic J2000,",
        "  epoch and tp in TDB, at the solution epoch). For comets they are SBDB's to the last digit; for",
        "  asteroids SBDB's elements are re-osculated at a standard epoch, compared in 'SBDB ep km'.",
        "  Converted with DE440's GM_sun and the IAU76 obliquity; 'conv m' is the distance from",
        "  the ICRF state",
        "  Horizons prints for them, 'ep m' from Horizons' barycentric vector at the epoch (comets).",
        "Non-gravs: JPL's A1, A2, A3 and g(r) through ssp.nongrav.apply (a non-standard g(r) as a runtime",
        "  G_OF_R entry: ASSIST's g(r) is the general form). DT is not supported (none chosen has one).",
        "(a)  barycentric geometric ICRF vectors (VECTORS @0, TDB) against ssp.ephem_assist._propagate_one:",
        "     max |dX| [km], its angle from the geocentre [mas], and its growth",
        "     max |dX| / max(|t - epoch|, 1 yr).",
        "(b)  X05 astrometric RA/Dec (quantity 1, TT) against compute_ephemerides_one",
        "     (its row path, k^2) [mas].",
        "(bg) Horizons' own vectors through our light-time and X05 observer code [mas]: the geometry alone.",
        "'no-ng km': max |dX| of the same integration without the non-gravs (what they move).",
        "",
        f"PASS: (bg) <= {PASS_GEOM_MAS} mas for all; gravity-only (a), (b) <= {PASS_GRAV_MAS} mas;",
        "  with non-gravs",
        f"  growth <= {PASS_NG_RATE_FACTOR:g} x the gravity-only controls' largest "
        f"({summ.rate_km_yr[grav].max():.3f} km/yr) = {rate_thr:.3f} km/yr.",
        "",
    ]
    L.append(
        f"{'object':13s} {'kind':9s} {'soln':8s} {'n':>3s} {'t-epoch [d]':>11s} {'a km':>7s} {'a mas':>6s} "
        f"{'km/yr':>6s} {'b mas':>6s} {'b Rub':>6s} {'bg mas':>6s} {'no-ng km':>9s} {'conv m':>6s} "
        f"{'ep m':>6s} {'SBDB ep km':>10s}  result  model"
    )
    for _, s in summ.iterrows():
        L.append(
            f"{s.designation:13s} {s.kind:9s} {s.jpl_soln:8s} {s.n_times:3d} {s.span_d:>11s} "
            f"{s.a_max_km:7.3f} "
            f"{s.a_max_mas:6.3f} {s.rate_km_yr:6.3f} {s.b_max_mas:6.3f} {s.b_rubin_max_mas:6.3f} "
            f"{s.b_geom_max_mas:6.3f} {s.without_ng_max_km:9.1f} {s.conv_vs_horizons_km * 1e3:6.2f} "
            f"{s.epoch_vec_km * 1e3:6.2f} {s.sbdb_epoch_km:10.3f}  {'PASS' if s['pass'] else 'FAIL'}    "
            f"{s.model or 'grav'} A=({s.A}) {s.note}"
        )
    L += [
        "",
        "Round trip (elements -> state -> elements): max relative "
        f"{summ.roundtrip_max_rel.max():.1e}, tp {summ.roundtrip_dtp_s.abs().max():.1e} s.",
        f"k^2 against DE440's GM_sun in the initial state: up to {summ.gm_k2_vs_de440_km.max() * 1e3:.1f} m.",
        "Horizons' solution IDs equal SBDB's for all: "
        f"{bool((summ.horizons_source == 'JPL#' + summ.jpl_soln).all())} "
        f"(observer) / {bool((summ.vec_source == 'JPL#' + summ.jpl_soln).all())} (vectors).",
        "",
    ]
    L.append(f"Overall: {int(summ['pass'].sum())}/{len(summ)} PASS")
    return L


# ---------------------------------------------------------------------------
# Check 2b: the MPC's elements and A's in Horizons, against our SSObservation
# ---------------------------------------------------------------------------


def check2b_object(plan, d, mng, ephem):
    """Check-2b rows of one object: SSObservation ephRa/ephDec against Horizons
    run on the MPC's own elements and A's; for CHECK2B_NOGRAV objects also
    the Horizons gravity-only run, and our own with and without the A's."""
    import pandas as pd
    from astropy.time import Time
    from ssp import ephem_assist as EA

    reqs = {
        lab.split(" ", 1)[0]: (svc, prm, lab)
        for svc, prm, lab in plan.user_requests()
        if lab.split(" ", 1)[1] == d
    }
    hz = parse_horizons_observer(plan.client.get(*reqs["user"]))
    df = plan.df[(plan.df.designation == d) & plan.df.ephRa.notna()].sort_values("midpointMjdTai")
    tt_rows = roundtrip(tai_to_tt(df.midpointMjdTai.to_numpy()))
    tt = np.array([float(t) for t in reqs["user"][1]["TLIST"].split(",")])
    idx = np.searchsorted(tt, tt_rows)
    ra_h, dec_h = hz["ra"][idx], hz["dec"][idx]
    out = pd.DataFrame(
        {
            "designation": d,
            "obsid": df.obsid.to_numpy(),
            "midpointMjdTai": df.midpointMjdTai.to_numpy(),
            "sep_mas": separation_arcsec(df.ephRa.to_numpy(float), df.ephDec.to_numpy(float), ra_h, dec_h)
            * 1e3,
        }
    )
    hdr = hz["meta"]
    summ = {
        "designation": d,
        "n": len(df),
        "model": mng[d].model,
        "A": ";".join(f"{a:.4g}" for a in mng[d].A),
        "horizons_echo": ";".join(f"{hdr.get(k, np.nan):.4g}" for k in ("A1", "A2", "A3")),
        "sep_med_mas": out.sep_mas.median(),
        "sep_max_mas": out.sep_mas.max(),
    }
    if "user-grav" in reqs:
        hg = parse_horizons_observer(plan.client.get(*reqs["user-grav"]))
        hz_ng = separation_arcsec(hz["ra"], hz["dec"], hg["ra"], hg["dec"]) * 1e3
        row = plan.mpc_rows()[d]
        t = Time(tt, format="mjd", scale="tt")
        ours = EA.compute_ephemerides_one(d, t, None, ephem, row=row, nongrav=mng[d])
        ours0 = EA.compute_ephemerides_one(d, t, None, ephem, row=row)
        our_ng = separation_arcsec(ours.ra_deg, ours.dec_deg, ours0.ra_deg, ours0.dec_deg) * 1e3
        grav_sep = separation_arcsec(ours0.ra_deg, ours0.dec_deg, hg["ra"], hg["dec"]) * 1e3
        summ.update(
            {
                "ng_shift_horizons_mas_med": float(np.median(hz_ng)),
                "ng_shift_ours_mas_med": float(np.median(our_ng)),
                "ng_shift_diff_mas_max": float(np.max(np.abs(hz_ng - our_ng))),
                "grav_only_sep_mas_max": float(np.max(grav_sep)),
            }
        )
    return summ, out


def cmd_user(args):
    import pandas as pd
    from ssp.ephem_assist import open_ephem

    client = JPLClient(args.cache)
    plan = Plan(client, args.fixture, args.ssobservation)
    mng = mpc_nongravs(CHECK2B_OBJECTS, args.fixture)
    ephem = open_ephem()
    summ, rows = [], []
    for d in CHECK2B_OBJECTS:
        try:
            s_, r = check2b_object(plan, d, mng, ephem)
        except NotCachedError as e:
            print(f"{d}: skipped ({e})")
            continue
        summ.append(s_)
        rows.append(r)
    summ = pd.DataFrame(summ)
    os.makedirs(args.out, exist_ok=True)
    summ.to_csv(os.path.join(args.out, "user_elements_objects.csv"), index=False)
    pd.concat(rows, ignore_index=True).to_csv(os.path.join(args.out, "user_elements_rows.csv"), index=False)
    L = [
        "# Check 2b: the MPC's own elements and A's sent to Horizons (COMMAND=';'), "
        "against our SSObservation",
        f"JPL requests so far (all of WP N5): {client.n_logged()}",
        "Elements: mpc_orbits q, e, i, node, argperi, peri_time, epoch (TT, sent as TDB JD), ECLIP=J2000;",
        "  A1-A3 and the g(r) constants of ssp.nongrav (Marsden for comets, 1/r^2 for Yarkovsky).",
        "sep: SSObservation ephRa/ephDec against Horizons' astrometric RA/Dec at the Rubin times [mas].",
        "'echo': the A's Horizons prints back. ng shift: how far the A's move the position, Horizons'",
        "  (with against without) and ours (compute_ephemerides_one with against without) [mas, median].",
        "",
    ]
    for _, r in summ.iterrows():
        line = (
            f"{r.designation:13s} n={r.n:3d} {r.model:9s} A=({r.A}) echo=({r.horizons_echo})  "
            f"sep med {r.sep_med_mas:7.3f} max {r.sep_max_mas:7.3f} mas"
        )
        if "ng_shift_horizons_mas_med" in r and np.isfinite(r.get("ng_shift_horizons_mas_med", np.nan)):
            line += (
                f"\n{'':13s} ng shift: Horizons {r.ng_shift_horizons_mas_med:.1f} mas, ours "
                f"{r.ng_shift_ours_mas_med:.1f} mas (max difference {r.ng_shift_diff_mas_max:.3f} mas); "
                f"gravity-only ours against Horizons max {r.grav_only_sep_mas_max:.3f} mas"
            )
        L.append(line)
    text = "\n".join(L) + "\n"
    with open(os.path.join(args.out, "user_elements_report.txt"), "w") as f:
        f.write(text)
    print(text)
    return 0


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cache", default=CACHE)
    ap.add_argument("--fixture", default=FIXTURE)
    ap.add_argument("--ssobservation", default=SSOBSERVATION)
    ap.add_argument("--out", default=WORK, help="report directory")
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch")
    f.add_argument("--stage", choices=["sbdb", "horizons", "user", "all"], default="all")
    f.add_argument("--dry-run", action="store_true")
    f.add_argument("--limit", type=int, default=None, help="send at most this many requests")
    f.add_argument("--only", nargs="*", help="only these designations")
    f.add_argument("--show-url", action="store_true", help="with --dry-run, print the URLs")
    sub.add_parser("status")
    sub.add_parser("orbits")
    sub.add_parser("integrator")
    sub.add_parser("user", help="check 2b: the MPC's elements and A's in Horizons")
    args = ap.parse_args(argv)
    return {
        "fetch": cmd_fetch,
        "status": cmd_status,
        "orbits": cmd_orbits,
        "integrator": cmd_integrator,
        "user": cmd_user,
    }[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
