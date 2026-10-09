"""Validation harness for the non-gravitational forces (WP N4 of
docs/design/nongrav.md, "Validation").

Black-box checks, written from the design, the contracts
(``ssp/nongrav.py``, ``ssp/nearbysso/_contract.py``,
``ssp/ssobservation_contract.py``) and ``sso_base.yaml`` only. The MPC ``CAR``
block is parsed here independently of ``ssp.nongrav``; the frames, the
g(r) and the units come from the design doc. The implementation is called
only through public entry points: the ``ssp-build-*`` outputs, and
``ssp.ssobservation_ellipse.load_orbit_covariances`` + ``ssp.nearbysso.
propagate.coarse`` / ``ellipse_at`` for the published error ellipse.

Classes (``--objects objects.txt``, ``class<TAB>designation``, or derived
from ``mpc_orbits``' ``mpc_orb_jsonb.CAR.coefficient_names``):

  comet_ng     a comet-style designation with A1/A2/A3 fitted
  yarkovsky    one "yarkovski"/"yarkovsky" coefficient
  ng_unknown   other coefficients beyond x..vz (unexpected; non-grav)
  comet_grav   a comet-style designation (P/ C/ D/ X/ I/, numbered P/D/I)
               without a non-grav fit
  control      everything else with an orbit (asteroids, A/ objects)
  satellite    S/ natural satellites
  no_orbit     not in mpc_orbits

Subcommands (each prints a text report, also written to ``--out FILE``;
exit 0 on PASS, 1 on FAIL)::

  offsets SSOBSERVATION_NEW SSOBSERVATION_REF MPC_ORBITS
          [--objects objects.txt] [--ng-errors any|unchanged|changed]
      SSObservation with non-gravs (NEW) against a gravity-only build of the
      same inputs (REF). Gates: gravity-only objects bitwise identical in every
      ephemeris, geometry and error column; non-grav objects' ephemerides
      changed; the comet_ng median ephOffset improves and no comet_ng object
      gets much worse; no Yarkovsky object gets worse beyond a small
      tolerance. ``--ng-errors``: what the non-grav objects' error columns
      must do (``unchanged``: no ephRaErr/ephDecErr moves by more than
      ERR_CHANGE_REL, as before WP N2; ``changed``: some object of every
      non-grav class does, as after it; ``any``, the default, only
      reports). Per-class and per-object tables (median/max ephOffset, the
      ephemeris shift, the sigma ratio).

  nearbysso NEARBYSSO SSOBSERVATION MPC_ORBITS DIA_SOURCES
          [--objects objects.txt]
      NearbySSO built from the same mpc_orbits as SSObservation, and
      DIA_SOURCES the NearbySSO input. Each SSObservation row with an orbit
      whose DiaSource is in the input (by diaSourceId; by (visit, ra, dec)
      where SSObservation's diaSourceId is NULL, i.e. measuredOn = science)
      must have a NearbySSO row for the same object with the same ephemeris, or
      a miss explained by: offset > radius (the object's match radius: 5", 15"
      for comets and ISOs, C/ P/ D/ I/), sigma > 10" (or no covariance), orbit
      filtered, sungrazer (q < 0.02 au), a nearer object, or (asked of
      propagate.coarse for what's left) sigma > 10" within +-0.34 d of the
      observation, where NearbySSO's nightly sample may gate the night.
      Gates: no unexplained misses; no NearbySSO row beyond its object's
      match radius; most comet rows matched (when the input
      has any; otherwise noted as not applicable); agreement of
      the matched rows; no S/ objects; no NearbySSO row for an orbit the
      filter drops. ``--table FILE``: the per-row table (Parquet).
      Time shift (docs/design/shutter-timing.md): SSObservation predicts at its
      own (shutter-corrected) midpointMjdTai, NearbySSO at the DiaSource's
      (DIA_SOURCES' midpointMjdTai). Where they differ by dt, the
      agreement tolerances and the offset-radius and nearer-object
      explanations add bench/time_shift.py's allowances (position and
      ephOffset: |rate| |dt| plus a margin); where dt = 0 they are
      unchanged. |dt| > TS.DT_MAX_S (10 s, a sanity limit) fails.

  uncertainty MPC_ORBITS [--ssobservation SSOBSERVATION]
          [--objects objects.txt]
          [--n-orbits 10] [--draws 1000] [--times 3] [--workers 8]
      Monte Carlo of the published error ellipse. For a sample of comet_ng
      and Yarkovsky orbits, draws (state, A) from the CAR covariance (MPC
      units: comets' A's in au/d^2, the Yarkovsky coefficient x 1e-10
      au/d^2), converts the heliocentric ecliptic J2000 state to barycentric
      ICRF here, integrates every draw with ASSIST directly (particle_params
      per draw, the design doc's g(r)), and projects on the sky from X05
      (geometric, no light time, as the coarse pass). The sample covariance
      is compared with ``propagate.coarse`` + ``ellipse_at`` (gate) and with
      the SSObservation ellipse columns at the same rows (gate, with
      --ssobservation). A state-only Monte Carlo (A fixed at its value) is
      reported alongside, as a diagnostic: an ellipse that matches it but
      not the full one is missing the A partials. Times: the object's
      SSObservation observation times (--ssobservation), else epoch +- 60/300
      days. ``--table FILE``: the per-(orbit, time) table (Parquet).

JPL comparisons (Horizons with the MPC elements plus non-gravs) are not
part of this harness: they belong to WP N5, so that only one agent talks
to JPL at a time. This module makes no network requests.

Examples (the 2026-10-01 fixture)::

  F=/sdf/data/rubin/user/mjuric/nongrav/fixtures/2026-10-01
  python -m bench.nongrav_validate offsets new/ssobservation.parquet \\
      $F/ref_gravity/ssobservation.parquet $F/in/mpc_orbits.parquet \\
      --objects $F/objects.txt --out offsets.txt
  python -m bench.nongrav_validate uncertainty $F/in/mpc_orbits.parquet \\
      --ssobservation new/ssobservation.parquet --objects $F/objects.txt \\
      --out unc.txt

The ASSIST files come from SSP_ASSIST_PLANETS / SSP_ASSIST_ASTEROIDS; set
OMP_NUM_THREADS=1.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections import Counter

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from bench.ssobservation_validate import bitwise_mismatch, to_np  # noqa: E402
from bench import time_shift as TS  # noqa: E402
from ssp.nearbysso import _contract as _C  # noqa: E402

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

#: The ephemeris, geometry and error columns of SSObservation (every computed
#: column, block 6 of the SSObservation contract): by prefix and by name.
EPH_PREFIXES = ("ecl", "gal", "topo", "helio", "eph")
EPH_EXACT = ("elongation", "phaseAngle")
ERROR_COLUMNS = ("ephRaErr", "ephDecErr", "ephRa_ephDec_Cov")

NONGRAV_CLASSES = ("comet_ng", "yarkovsky", "ng_unknown")
GRAVITY_CLASSES = ("comet_grav", "control", "satellite", "no_orbit")
CLASS_ORDER = ("comet_ng", "comet_grav", "yarkovsky", "ng_unknown", "control", "satellite", "no_orbit")

#: Comet-style designations: P/ C/ D/ X/ I/ provisional, and numbered
#: periodic/defunct/interstellar (e.g. "1P", "73P-B"); A/ objects are
#: asteroids on comet-like orbits and S/ natural satellites (both apart).
COMET_RE = re.compile(r"^([PCDXI]/|\d+[PDI](-[A-Z]+)?(/|$))")

# offsets ------------------------------------------------------------------

#: A comet_ng object is "much worse" when its median ephOffset grows by more
#: than both COMET_WORSE_ABS_ARCSEC and COMET_WORSE_REL x its gravity-only
#: median. The comets' gravity-only offsets are ~0.65" (median, up to 6",
#: design doc); with the right A's they should shrink. 0.2" is ~3x the
#: asteroids' median astrometric residual (0.06"), so coma/centroiding noise
#: on a handful of rows can't trip it, while a wrong unit (1e-10 vs 1), a
#: sign error or the wrong g(r), which move comets by arcseconds, do.
COMET_WORSE_ABS_ARCSEC = 0.2
COMET_WORSE_REL = 0.5

#: A Yarkovsky object "gets worse" when its median ephOffset grows by more
#: than YARKO_TOL_ABS_ARCSEC + YARKO_TOL_REL x its gravity-only median. The
#: Yarkovsky drift between the MPC epoch and the observations (months) is
#: mas-level for nearly all of them, so the correct change is ~0; 10 mas is
#: 1/6 of the median residual (60 mas) and well above float32 rounding of
#: ephOffset (~1e-7 relative), and catches a coefficient applied without the
#: 1e-10 unit (a 1e10 times larger acceleration) or along the wrong axis.
YARKO_TOL_ABS_ARCSEC = 0.01
YARKO_TOL_REL = 0.10

#: A non-grav object's error columns "changed" (--ng-errors) when ephRaErr
#: or ephDecErr moves by more than this, relatively. The ellipse is
#: projected on the precise pass's line of sight, which the A's move by up
#: to ~6" (~3e-5 rad), so a gravity-only covariance changes its float32
#: errors at the ~1e-5 level; adding the A's to the covariance changes them
#: by >1% wherever they matter (bitwise comparison stays the gate for
#: gravity-only objects).
ERR_CHANGE_REL = 1e-3

# nearbysso ----------------------------------------------------------------

#: The NearbySSO contract's radii and sigma cut, and its precise pass's
#: object filter (``_contract.MATCH_RADIUS_ARCSEC``/``SIGMA_MAX_ARCSEC``).
#: The match radius is per object: ``_contract.match_radius(designation)``,
#: MATCH_RADIUS_COMET_ARCSEC for comets and ISOs (docs/design/comet-radius.md).
MATCH_RADIUS_ARCSEC = _C.MATCH_RADIUS_ARCSEC
MATCH_RADIUS_COMET_ARCSEC = _C.MATCH_RADIUS_COMET_ARCSEC
SIGMA_MAX_ARCSEC = 10.0
#: Sungrazers: perihelion below this [au] is an accepted miss category.
SUNGRAZER_Q_AU = 0.02
#: Rows within this of a cut (the match radius, 10" sigma) are
#: "borderline": the two tables compute their values separately (float32
#: output, NearbySSO's ellipse from the nightly samples, SSObservation's at the
#: observation), so a miss within 2% of a cut is explained by the cut.
CUT_BORDER_REL = 0.02
#: (visit, ra, dec) matching of SSObservation rows without a diaSourceId to the
#: NearbySSO input: both copy the same measurement, so any real difference
#: is float rounding; 1 mas is far below the source density.
POSITION_MATCH_MAS = 1.0
#: A miss SSObservation's ellipse doesn't explain is asked of propagate.coarse
#: at these offsets [days] from the observation: NearbySSO gates on the
#: track's nightly sample, which the contract puts within 0.338 d of every
#: visit of the night (VisitIndex.candidates), so a sigma > 10" anywhere in
#: +-0.34 d may have gated the night.
NIGHT_SAMPLE_OFFSETS_DAYS = (-0.34, -0.17, 0.0, 0.17, 0.34)
#: Agreement of a matched row's ephemeris: SSObservation and NearbySSO run the
#: same precise pass on the same orbit, so they agree to integrator noise
#: (the step sequence depends on the requested times): 0.1 mas observed for
#: asteroids (bench/nearbysso_validate.py's TOL); comets near perihelion get
#: 1 mas. Rates and V as there.
POS_TOL_MAS = 1.0
RATE_TOL_DEG_DAY = 1e-6
VMAG_TOL = 1e-3
#: ephOffset (float32 in both; the same DiaSource) agrees to this [arcsec].
OFFSET_TOL_ARCSEC = 1e-3
#: SSObservation columns for the time-shift allowances (bench/time_shift.py).
SHIFT_COLUMNS = ("topoRange", "topoRangeRate", "helioRange", "helioRangeRate")
#: "Most" comet rows: at least this fraction of the comet SSObservation rows
#: with an orbit and their DiaSource in the input must be matched (the cuts
#: legitimately drop some: large offsets of comets without non-gravs,
#: large sigmas of short arcs).
COMET_MATCH_MIN_FRAC = 0.5

# uncertainty --------------------------------------------------------------

#: The comet g(r) (Marsden, Sekanina & Yeomans 1973) and the Yarkovsky one
#: (1/r^2), as ASSIST Extras attributes, from the design doc.
GOFR = {
    "comet": dict(alpha=0.1112620426, r0=2.808, nm=2.15, nn=5.093, nk=4.6142),
    "yarkovsky": dict(alpha=1.0, r0=1.0, nm=2.0, nn=5.093, nk=0.0),
}
#: The MPC's Yarkovsky coefficient is A2 in units of this [au/d^2]
#: (the design doc's units check).
YARKOVSKY_UNIT = 1e-10
#: The CAR block's obliquity if system_data doesn't say [arcsec] (IAU 1976).
OBLIQUITY_ARCSEC = 84381.448
#: sigma ratios (published / Monte Carlo) must be within MC_Z x the
#: sampling error of a standard deviation, 1/sqrt(2(N-1)), plus
#: MC_MARGIN. MC_Z = 3.5 keeps the false-alarm rate below ~1 in 2000 per
#: comparison (a run has ~100); MC_MARGIN covers the linearization of a
#: mildly non-linear map over the arc (orbits with the largest sigmas) and
#: the float32 published values. With 1000 draws the band is +-11%.
MC_Z = 3.5
MC_MARGIN = 0.03
MC_DRAWS = 1000
#: The sample's minimum size per class (comet_ng, yarkovsky).
MC_N_ORBITS = 10

AU_KM = 149597870.7
ASSIST_JD_REF = 2451545.0


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------

class Report:
    """PASS/FAIL checks and informational lines (as ssobservation_validate)."""

    def __init__(self, title):
        self.title = title
        self.lines = [title, "=" * len(title)]
        self.results = []

    def check(self, name, ok, detail=""):
        ok = bool(ok)
        self.results.append((name, ok))
        self.lines.append(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f": {detail}" if detail else ""))
        return ok

    def info(self, text=""):
        for line in str(text).splitlines() or [""]:
            self.lines.append(f"       {line}" if line else "")

    @property
    def failed(self):
        return [n for n, ok in self.results if not ok]

    @property
    def ok(self):
        return not self.failed

    def text(self):
        n = len(self.results)
        tail = (f"RESULT: PASS ({n} checks)" if self.ok
                else f"RESULT: FAIL ({len(self.failed)} of {n} checks failed: {', '.join(self.failed)})")
        return "\n".join(self.lines + ["", tail]) + "\n"

    def finish(self, out=None):
        text = self.text()
        sys.stdout.write(text)
        if out:
            os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
            with open(out, "w") as f:
                f.write(text)
        return 0 if self.ok else 1


def _table(df, floatfmt="{:.4g}"):
    """A plain fixed-width text table of a DataFrame."""
    if df is None or len(df) == 0:
        return "(none)"
    cells = [[str(c) for c in df.columns]]
    for row in df.itertuples(index=False):
        cells.append([(floatfmt.format(v) if isinstance(v, (float, np.floating)) and np.isfinite(v)
                       else ("-" if isinstance(v, (float, np.floating)) else str(v))) for v in row])
    w = [max(len(r[k]) for r in cells) for k in range(len(cells[0]))]
    lines = ["  ".join(c.rjust(w[k]) if k else c.ljust(w[k]) for k, c in enumerate(r)) for r in cells]
    lines.insert(1, "  ".join("-" * x for x in w))
    return "\n".join(lines)


# --------------------------------------------------------------------------
# The CAR block (own parse; independent of ssp.nongrav)
# --------------------------------------------------------------------------

def _json(j):
    if j is None:
        return None
    if isinstance(j, (str, bytes)):
        try:
            return json.loads(j)
        except ValueError:
            return None
    return j


def car_parse(mpc_orb_jsonb):
    """The CAR block of one ``mpc_orb_jsonb``: a dict with ``names``,
    ``values`` and ``cov`` (n x n, None if any entry is missing) in the
    MPC's own units, ``kind`` ("" none, "comet", "yarkovsky", "unknown"),
    ``scale`` (n,) converting values/cov rows to au, au/d and au/d^2,
    ``A_index`` (3,) the CAR index of A1/A2/A3 (-1 if not fitted),
    ``epoch_tt_mjd`` and ``obliquity_arcsec``. None without a CAR block."""
    j = _json(mpc_orb_jsonb)
    if not isinstance(j, dict) or not isinstance(j.get("CAR"), dict):
        return None
    car = j["CAR"]
    names = [str(n) for n in (car.get("coefficient_names") or [])]
    values = np.array([float(v) for v in (car.get("coefficient_values") or [])], dtype=np.float64)
    if len(names) < 6 or len(values) != len(names):
        return None
    n = len(names)
    covd = car.get("covariance") or {}
    cov = np.empty((n, n))
    for a in range(n):
        for b in range(a, n):
            v = covd.get(f"cov{a}{b}")
            if v is None:
                cov = None
                break
            cov[a, b] = cov[b, a] = float(v)
        if cov is None:
            break
    extra = names[6:]
    scale = np.ones(n)
    A_index = np.full(3, -1)
    if not extra:
        kind = ""
    elif all(x in ("A1", "A2", "A3") for x in extra):
        kind = "comet"
        for k, x in enumerate(extra):
            A_index[int(x[1]) - 1] = 6 + k
    elif len(extra) == 1 and extra[0].lower() in ("yarkovski", "yarkovsky"):
        kind = "yarkovsky"
        A_index[1] = 6
        scale[6] = YARKOVSKY_UNIT
    else:
        kind = "unknown"
    ep = j.get("epoch_data") or {}
    sysd = j.get("system_data") or {}
    try:
        obl = float(sysd.get("EclipticObliquityArcseconds") or OBLIQUITY_ARCSEC)
    except ValueError:
        obl = OBLIQUITY_ARCSEC
    return dict(names=names, values=values, cov=cov, kind=kind, scale=scale, A_index=A_index,
                epoch_tt_mjd=float(ep["epoch"]) if ep.get("epoch") is not None else np.nan,
                timesystem=ep.get("timesystem"), obliquity_arcsec=obl)


_NAMES_RE = r'"coefficient_names"\s*:\s*\[(?P<n>[^\]]*)\]'


def coefficient_extras(json_col):
    """For an array of mpc_orb_jsonb strings, the CAR coefficient names
    beyond the first six, as tuples (None where there is no list). A regex,
    not json.loads, so the whole catalog is fast."""
    arr = json_col
    if isinstance(arr, pa.ChunkedArray):
        parts = [coefficient_extras(c) for c in arr.chunks]
        return np.concatenate(parts) if parts else np.zeros(0, dtype=object)
    if not isinstance(arr, pa.Array):
        arr = pa.array([x if isinstance(x, str) else (json.dumps(x) if isinstance(x, dict) else None)
                        for x in arr], type=pa.string())
    m = pc.extract_regex(arr, _NAMES_RE)
    lists = pc.struct_field(m, "n").to_numpy(zero_copy_only=False)
    valid = pc.is_valid(m).to_numpy(zero_copy_only=False)
    out = np.empty(len(lists), dtype=object)
    for k, (s, ok) in enumerate(zip(lists, valid)):
        if not ok or s is None:
            out[k] = None
            continue
        names = tuple(x.strip().strip('"') for x in s.split(",") if x.strip())
        out[k] = names[6:]
    return out


def classify(designations, extras, has_orbit=None):
    """The class of each object (see the module docstring) from its
    designation and its CAR names beyond x..vz (`coefficient_extras`)."""
    des = np.asarray(designations, dtype=object)
    out = np.empty(len(des), dtype=object)
    for k, (d, ex) in enumerate(zip(des, extras)):
        d = str(d)
        if has_orbit is not None and not has_orbit[k]:
            out[k] = "no_orbit"
        elif d.startswith("S/"):
            out[k] = "satellite"
        elif ex and all(x in ("A1", "A2", "A3") for x in ex):
            out[k] = "comet_ng"
        elif ex and len(ex) == 1 and ex[0].lower() in ("yarkovski", "yarkovsky"):
            out[k] = "yarkovsky"
        elif ex:
            out[k] = "ng_unknown"
        elif COMET_RE.match(d):
            out[k] = "comet_grav"
        else:
            out[k] = "control"
    return out


def read_objects(path):
    """objects.txt (class<TAB>designation) as {designation: class}."""
    out = {}
    with open(path) as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line.strip() or line.startswith("#"):
                continue
            cls, des = line.split("\t", 1)
            out[des.strip()] = cls.strip()
    return out


ORBIT_KEY = "unpacked_primary_provisional_designation"


def read_mpc(path, designations, columns=()):
    """mpc_orbits rows for the given designations (DataFrame, ``designation``
    column), with mpc_orb_jsonb and the requested columns."""
    want = sorted({str(d) for d in designations if d is not None})
    present = set(pq.read_schema(path).names)
    cols = [ORBIT_KEY, "mpc_orb_jsonb", *[c for c in columns if c in present and c != ORBIT_KEY]]
    if not want:
        return pd.DataFrame({c: [] for c in ["designation", *cols[1:]]})
    t = pq.read_table(path, columns=cols, filters=[(ORBIT_KEY, "in", want)])
    df = t.to_pandas().rename(columns={ORBIT_KEY: "designation"})
    return df.drop_duplicates("designation").reset_index(drop=True)


def classes_for(designations, mpc_orbits=None, objects=None, orbits_df=None):
    """{designation: class}: objects.txt where given (its classes win),
    else from mpc_orbits (path) or an already-read DataFrame."""
    des = sorted({str(d) for d in designations if d is not None and d == d})
    out = {}
    if objects:
        out.update({d: objects[d] for d in des if d in objects})
    rest = [d for d in des if d not in out]
    if rest:
        if orbits_df is None:
            orbits_df = read_mpc(mpc_orbits, rest)
        sub = orbits_df[orbits_df["designation"].isin(rest)]
        ex = dict(zip(sub["designation"], coefficient_extras(sub["mpc_orb_jsonb"].to_numpy(dtype=object))))
        has = np.array([d in ex for d in rest])
        cls = classify(rest, [ex.get(d) for d in rest], has_orbit=has)
        out.update(dict(zip(rest, cls)))
    return out


# --------------------------------------------------------------------------
# 1. offsets
# --------------------------------------------------------------------------

def eph_columns(names):
    return [c for c in names if c.startswith(EPH_PREFIXES) or c in EPH_EXACT]


def _read_sss(path, columns):
    have = pq.read_schema(path).names
    return pq.read_table(path, columns=[c for c in columns if c in have])


def per_object_offsets(des, cls, off_new, off_ref):
    """One row per object: class, rows, median ephOffset before/after."""
    df = pd.DataFrame({"designation": des, "class": cls, "before": off_ref, "after": off_new})
    df = df[df["designation"].notna()]
    g = df.groupby("designation", sort=True)
    out = pd.DataFrame({
        "class": g["class"].first(),
        "rows": g.size(),
        "before": g["before"].median(),
        "after": g["after"].median(),
        "max_before": g["before"].max(),
        "max_after": g["after"].max(),
    }).reset_index()
    out["delta"] = out["after"] - out["before"]
    out["ratio"] = out["after"] / out["before"]
    return out


def class_summary(obj, rows):
    """Per-class: objects, rows, median of the per-object medians and the
    rows' p90/max, before and after."""
    out = []
    for c in CLASS_ORDER:
        o = obj[obj["class"] == c]
        r = rows[rows["class"] == c]
        if not len(o):
            continue
        b, a = r["before"].dropna(), r["after"].dropna()
        out.append(dict(
            **{"class": c, "objects": len(o), "rows": len(r)},
            med_before=float(o["before"].median()), med_after=float(o["after"].median()),
            p90_before=float(np.percentile(b, 90)) if len(b) else np.nan,
            p90_after=float(np.percentile(a, 90)) if len(a) else np.nan,
            max_before=float(b.max()) if len(b) else np.nan,
            max_after=float(a.max()) if len(a) else np.nan))
    return pd.DataFrame(out)


def check_offsets(new, ref, mpc_orbits=None, objects=None, ng_errors="any", rep=None, orbits_df=None):
    rep = rep or Report(f"Non-grav offsets: {new} vs {ref}")
    ref_names = pq.read_schema(ref).names
    new_names = pq.read_schema(new).names
    cols = eph_columns(ref_names)
    missing = [c for c in cols if c not in new_names]
    rep.check("every reference ephemeris/geometry/error column present", not missing,
              f"{len(cols)} columns" + (f"; missing {missing}" if missing else ""))
    cols = [c for c in cols if c in new_names]
    rt = _read_sss(ref, ["obsid", "designation", *cols])
    nt = _read_sss(new, ["obsid", "designation", *cols])
    r_obsid = np.asarray(to_np(rt["obsid"])[0], dtype=object)
    n_obsid = np.asarray(to_np(nt["obsid"])[0], dtype=object)
    idx = pd.Index(r_obsid).get_indexer(n_obsid)
    same = (len(rt) == len(nt)) and (idx >= 0).all() and len(np.unique(idx)) == len(idx)
    rep.check("same row set (obsid)", same, f"new {len(nt):,}, reference {len(rt):,}, "
              f"{int((idx < 0).sum()):,} new-only")
    keep = idx >= 0
    nt = nt.filter(pa.array(keep))
    rt = rt.take(pa.array(idx[keep]))
    dn, dnv, _ = to_np(nt["designation"])
    dr, drv, _ = to_np(rt["designation"])
    des = np.where(dnv, dn, np.where(drv, dr, None)).astype(object)

    cmap = classes_for([d for d in des if d is not None], mpc_orbits, objects, orbits_df)
    cls = np.array([cmap.get(d, "no_orbit") if d is not None else "no_orbit" for d in des], dtype=object)
    counts = Counter(cmap.values())
    rep.info("classes (objects): " + ", ".join(f"{c} {counts[c]}" for c in CLASS_ORDER if counts[c]))
    nongrav = np.isin(cls, NONGRAV_CLASSES)

    # gravity-only rows: bitwise identical in every column
    mism_any = np.zeros(len(des), bool)
    err_changed = np.zeros(len(des), bool)
    eph_changed = np.zeros(len(des), bool)
    bad = []
    for c in cols:
        if nt.schema.field(c).type != rt.schema.field(c).type:
            bad.append(f"{c}: type {nt.schema.field(c).type} vs {rt.schema.field(c).type}")
            continue
        m, _ = bitwise_mismatch(nt[c], rt[c])
        mism_any |= m
        if c in ("ephRaErr", "ephDecErr"):
            # changed beyond the line-of-sight effect (see ERR_CHANGE_REL)
            a = np.asarray(to_np(nt[c])[0], np.float64)
            b = np.asarray(to_np(rt[c])[0], np.float64)
            with np.errstate(divide="ignore", invalid="ignore"):
                rel = np.abs(a / b - 1.0)
            err_changed |= (np.isfinite(a) != np.isfinite(b)) | (np.isfinite(rel) & (rel > ERR_CHANGE_REL))
        if c in ("ephRa", "ephDec"):
            eph_changed |= m
        g = m & ~nongrav
        if g.any():
            ex = ", ".join(f"{des[i]} ({_obsid_str(n_obsid[keep][i])})" for i in np.flatnonzero(g)[:3])
            bad.append(f"{c}: {int(g.sum()):,} rows ({ex})")
    grav_rows = int((~nongrav).sum())
    rep.check("gravity-only rows bitwise identical (ephemeris, geometry, error columns)", not bad,
              f"{len(cols)} columns x {grav_rows:,} rows" + ("; differ:\n" + "\n".join(bad) if bad else ""))

    # non-grav objects: ephemerides changed
    ng_obj = sorted({d for d, c in cmap.items() if c in NONGRAV_CLASSES})
    has_eph = ~np.isnan(np.asarray(to_np(nt["ephRa"])[0], dtype=np.float64)) if "ephRa" in nt.column_names \
        else np.zeros(len(des), bool)
    unchanged = []
    for d in ng_obj:
        sel = (des == d) & has_eph
        if sel.any() and not eph_changed[sel].any():
            unchanged.append(d)
    rep.check("every non-grav object's ephemeris changed", not unchanged,
              f"{len(ng_obj)} non-grav objects"
              + (f"; unchanged: {', '.join(unchanged[:10])}" if unchanged else ""))

    # error columns of the non-grav objects
    ng_err = sorted({des[i] for i in np.flatnonzero(err_changed & nongrav)})
    ng_with = sorted({des[i] for i in np.flatnonzero(nongrav & has_eph)})
    detail = (f"{len(ng_err)} of {len(ng_with)} non-grav objects have error columns changed by "
              f"> {ERR_CHANGE_REL:g} relative")
    if ng_errors == "unchanged":
        rep.check("non-grav objects' error columns unchanged (--ng-errors unchanged)", not ng_err, detail)
    elif ng_errors == "changed":
        # An object whose A's barely act over its observations (far from the
        # Sun, or near the epoch) may keep bitwise the same float32 errors,
        # so the gate is per class: some object of each non-grav class must
        # have changed errors (the A's are in the published covariance at
        # all); whether they are right is the uncertainty subcommand's job.
        nochg = sorted(set(ng_with) - set(ng_err))
        classes = sorted({cmap[d] for d in ng_with})
        dead = [c for c in classes if not any(cmap[d] == c for d in ng_err)]
        rep.check("non-grav error columns changed, in every non-grav class (--ng-errors changed)", not dead,
                  detail + (f"; no object changed in: {dead}" if dead else "")
                  + (f"; unchanged (see shift_mas below): {', '.join(nochg[:10])}" if nochg else ""))
    else:
        rep.info(detail + " (not gated: --ng-errors any)")

    # offsets
    off_n = np.asarray(to_np(nt["ephOffset"])[0], dtype=np.float64)
    off_r = np.asarray(to_np(rt["ephOffset"])[0], dtype=np.float64)
    rows = pd.DataFrame({"designation": des, "class": cls, "before": off_r, "after": off_n})
    obj = per_object_offsets(des, cls, off_n, off_r)
    # the ephemeris shift [mas] and the sigma_major ratio (new / reference)
    extra = pd.DataFrame({"designation": des})
    if all(c in nt.column_names for c in ("ephRa", "ephDec")):
        extra["shift_mas"] = sky_sep_arcsec(to_np(nt["ephRa"])[0], to_np(nt["ephDec"])[0],
                                            to_np(rt["ephRa"])[0], to_np(rt["ephDec"])[0]) * 1000.0
    if all(c in nt.column_names for c in ERROR_COLUMNS):
        sn = sigma_major_arcsec(*(to_np(nt[c])[0] for c in ERROR_COLUMNS))
        sr = sigma_major_arcsec(*(to_np(rt[c])[0] for c in ERROR_COLUMNS))
        with np.errstate(divide="ignore", invalid="ignore"):
            extra["sig_ratio"] = sn / sr
    g = extra[extra["designation"].notna()].groupby("designation")
    agg = {}
    if "shift_mas" in extra:
        agg["shift_mas"] = g["shift_mas"].max()
    if "sig_ratio" in extra:
        agg["sig_ratio"] = g["sig_ratio"].median()
    if agg:
        obj = obj.merge(pd.DataFrame(agg).reset_index(), on="designation", how="left")

    cn = obj[obj["class"] == "comet_ng"]
    if len(cn):
        mb, ma = float(cn["before"].median()), float(cn["after"].median())
        rep.check("comet_ng median ephOffset improves", ma < mb,
                  f"median of {len(cn)} per-object medians {mb:.3f}\" -> {ma:.3f}\"")
        worse = cn[(cn["delta"] > COMET_WORSE_ABS_ARCSEC) & (cn["delta"] > COMET_WORSE_REL * cn["before"])]
        rep.check("no comet_ng object much worse", worse.empty,
                  f"worse = median up by > {COMET_WORSE_ABS_ARCSEC}\" and > {COMET_WORSE_REL:.0%}"
                  + (": " + ", ".join(f"{r.designation} {r.before:.3f}->{r.after:.3f}\""
                                      for r in worse.itertuples()) if len(worse) else ""))
    else:
        rep.check("comet_ng objects present", False, "no comet_ng object in the rows")
    yk = obj[obj["class"] == "yarkovsky"]
    if len(yk):
        tol = YARKO_TOL_ABS_ARCSEC + YARKO_TOL_REL * yk["before"]
        worse = yk[yk["delta"] > tol]
        rep.check("no Yarkovsky object worse beyond tolerance", worse.empty,
                  f"{len(yk)} objects, tolerance {YARKO_TOL_ABS_ARCSEC}\" + {YARKO_TOL_REL:.0%}; median "
                  f"{yk['before'].median():.4f}\" -> {yk['after'].median():.4f}\""
                  + (": " + ", ".join(f"{r.designation} {r.before:.4f}->{r.after:.4f}\""
                                      for r in worse.itertuples()) if len(worse) else ""))
    else:
        rep.info("no Yarkovsky object in the rows")

    rep.info("")
    rep.info("Per class (ephOffset [arcsec]; med = median of the per-object medians; p90/max over rows):")
    rep.info(_table(class_summary(obj, rows)))
    for c in ("comet_ng", "yarkovsky", "ng_unknown", "comet_grav"):
        o = obj[obj["class"] == c]
        if not len(o):
            continue
        rep.info("")
        rep.info(f"Per object, {c} (median/max ephOffset [arcsec], before = reference; shift_mas = max "
                 f"ephemeris shift; sig_ratio = median sigma_major new/reference):")
        o = o.copy()
        o["err_changed"] = [d in ng_err for d in o["designation"]]
        show = ("designation", "rows", "before", "after", "delta", "ratio", "max_before", "max_after",
                "shift_mas", "sig_ratio", "err_changed")
        rep.info(_table(o[[k for k in show if k in o]]))
    return rep


def _obsid_str(x):
    return str(x)


# --------------------------------------------------------------------------
# 2. nearbysso
# --------------------------------------------------------------------------

def sigma_major_arcsec(ra_err, dec_err, cov):
    """1-sigma semi-major axis [arcsec] of (ra_err, dec_err [deg],
    cov [deg^2])."""
    a, b, c = (np.asarray(x, np.float64) for x in (ra_err, dec_err, cov))
    a, b = a * a, b * b
    l1 = 0.5 * (a + b) + np.sqrt(np.maximum(0.25 * (a - b) ** 2 + c * c, 0.0))
    return np.sqrt(l1) * 3600.0


def sky_sep_arcsec(ra1, dec1, ra2, dec2):
    """Great-circle separation [arcsec] (Vincenty), inputs in degrees."""
    r1, d1, r2, d2 = (np.deg2rad(np.asarray(x, np.float64)) for x in (ra1, dec1, ra2, dec2))
    dra = r2 - r1
    num = np.hypot(np.cos(d2) * np.sin(dra), np.cos(d1) * np.sin(d2) - np.sin(d1) * np.cos(d2) * np.cos(dra))
    den = np.sin(d1) * np.sin(d2) + np.cos(d1) * np.cos(d2) * np.cos(dra)
    return np.rad2deg(np.arctan2(num, den)) * 3600.0


def orbit_filter_reason(orbits):
    """NearbySSO's orbit filter (the contract's load_orbits), re-implemented:
    '' if kept, else 'satellite', 'missing_elements' or 'arc' (the JSON
    orbit_fit_statistics.arc_length_total *text* is '0 days', '1 days',
    '2 days' or null/absent). ``orbits``: designation, the six elements,
    mpc_orb_jsonb."""
    from bench.nearbysso_validate import arc_text_from_json
    des = orbits["designation"].astype(str).to_numpy()
    missing = np.zeros(len(orbits), bool)
    for c in ("q", "e", "i", "node", "argperi", "peri_time"):
        missing |= ~np.isfinite(pd.to_numeric(orbits[c], errors="coerce").to_numpy(np.float64))
    arc = arc_text_from_json(orbits["mpc_orb_jsonb"].to_numpy(dtype=object)) if len(orbits) else []
    bad_arc = np.array([a is None or a in ("0 days", "1 days", "2 days") for a in arc], dtype=bool)
    return np.select([np.char.startswith(des.astype(str), "S/"), missing, bad_arc],
                     ["satellite", "missing_elements", "arc"], "").astype(object)


def match_radius(designations):
    """The match radius [arcsec] of each designation (the contract's
    ``match_radius``), as a float64 array."""
    des = [str(d) for d in designations]
    if not des:
        return np.zeros(0)
    return np.asarray(_C.match_radius(np.array(des)), dtype=np.float64)


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return np.nan


def _INT_MAPPER(t):
    """Arrow -> pandas: 64-bit ids stay exact (nullable Int64, not float64)."""
    return pd.Int64Dtype() if t == pa.int64() else None


def map_to_dia(sss, dia, tol_mas=POSITION_MATCH_MAS):
    """The NearbySSO-input diaSourceId of each SSObservation row (-1 if its
    DiaSource isn't in the input) and how it was found ('id', 'position',
    ''): by diaSourceId where SSObservation has one, else by (visit, ra, dec)
    within tol_mas."""
    out = np.full(len(sss), -1, dtype=np.int64)
    how = np.full(len(sss), "", dtype=object)
    sid = sss["diaSourceId"]
    if sid.dtype.kind == "f":
        raise TypeError("diaSourceId must be read as an exact integer (pandas Int64), not float64")
    has_id = sid.notna().to_numpy()
    sid_i = sid.fillna(-1).astype("int64").to_numpy()
    hit = has_id & np.isin(sid_i, dia["diaSourceId"].to_numpy(dtype=np.int64))
    out[hit], how[hit] = sid_i[hit], "id"
    rest = np.flatnonzero(~has_id)
    if len(rest):
        by_visit = {v: g for v, g in dia.groupby("visit")}
        visit = sss["visit"].to_numpy()
        ra, dec = sss["ra"].to_numpy(np.float64), sss["dec"].to_numpy(np.float64)
        for k in rest:
            g = by_visit.get(int(visit[k]))
            if g is None:
                continue
            sep = sky_sep_arcsec(ra[k], dec[k], g["ra"].to_numpy(), g["dec"].to_numpy())
            j = int(np.argmin(sep))
            if sep[j] * 1000.0 <= tol_mas:
                out[k], how[k] = int(g["diaSourceId"].iloc[j]), "position"
    return out, how


def nearbysso_compare(nss, sss, dia, orbits, cmap, rep, coarse_sigma=None):
    """The comparison (DataFrames): ``nss`` NearbySSO rows, ``sss``
    SSObservation rows (designation, diaSourceId, visit, midpointMjdTai, ra,
    dec, eph*, optionally SHIFT_COLUMNS), ``dia`` the NearbySSO input
    (diaSourceId, visit, ra, dec, optionally midpointMjdTai: without it the
    time shift is taken as 0, i.e. strict), ``orbits``
    mpc_orbits rows (designation, q, e, i, node, argperi, peri_time,
    mpc_orb_jsonb), ``cmap`` {designation: class}. ``coarse_sigma(
    designation, tai) -> sigma_major [arcsec]`` (optional) is asked about
    the otherwise unexplained misses: NearbySSO gates on its coarse track's
    nightly sigma, which can exceed 10" within the night even where
    SSObservation's ellipse at the observation is small. Returns the per-row
    table."""
    # no natural satellites
    nd = nss["designation"].astype(str).to_numpy()
    sat = np.char.startswith(nd.astype(str), "S/")
    rep.check("no natural satellites (S/) in NearbySSO", not sat.any(),
              f"{int(sat.sum())} rows" + (f": {sorted(set(nd[sat]))[:5]}" if sat.any() else ""))

    reason = dict(zip(orbits["designation"], orbit_filter_reason(orbits)))
    qmap = dict(zip(orbits["designation"], orbits["q"].astype(float)))
    named = sorted(set(nd))
    dropped = [d for d in named if reason.get(d, "") != ""]
    rep.check("no NearbySSO row names an orbit the filter drops", not dropped,
              f"{len(named)} objects named" + (f"; dropped by the filter: {dropped[:10]}" if dropped else ""))
    if "ephOffset" in nss:
        with np.errstate(invalid="ignore"):
            far = nss["ephOffset"].to_numpy(np.float64) > match_radius(nd) + OFFSET_TOL_ARCSEC
        rep.check("no NearbySSO row beyond its object's match radius", not far.any(),
                  f"{int(far.sum())} rows" + (": " + "; ".join(
                      f"{d} {o:.3f}\"" for d, o in zip(nd[far][:5], nss["ephOffset"].to_numpy()[far][:5]))
                      if far.any() else ""))

    # SSObservation rows with an orbit and a DiaSource in the input
    sss = sss[sss["designation"].notna() & sss["ephRa"].notna()].reset_index(drop=True)
    dsid, how = map_to_dia(sss, dia)
    in_input = dsid >= 0
    rep.info(f"SSObservation rows with an orbit: {len(sss):,}; DiaSource in the NearbySSO input: "
             f"{int(in_input.sum()):,} (by id {int((how == 'id').sum()):,}, by position "
             f"{int((how == 'position').sum()):,})")
    s = sss[in_input].reset_index(drop=True)
    n = len(s)
    nb = nss.drop_duplicates("diaSourceId").set_index("diaSourceId")
    j = nb.index.get_indexer(dsid[in_input])
    has = j >= 0
    jj = np.where(has, j, 0)

    def col(c, kind="f"):
        v = nb[c].to_numpy()[jj] if len(nb) else np.zeros(n)
        if kind == "f":
            return np.where(has, np.asarray(v, np.float64), np.nan)
        return np.where(has, v.astype(object), None)

    def f(c):
        return s[c].to_numpy(np.float64) if c in s else np.full(n, np.nan)

    # the time shift: SSObservation at its own time, NearbySSO at the
    # DiaSource's
    if "midpointMjdTai" in dia and len(dia):
        dtime = dia.drop_duplicates("diaSourceId").set_index("diaSourceId")["midpointMjdTai"]
        t_dia = dtime.reindex(dsid[in_input]).to_numpy(np.float64)
    else:
        t_dia = np.full(n, np.nan)
    dt = TS.dt_days(f("midpointMjdTai"), t_dia)
    rate = TS.rate_deg_day(f("ephRateRa"), f("ephRateDec"), col("ephRateRa"), col("ephRateDec"))
    pos_allow = TS.position_mas(rate, dt)

    des = s["designation"].astype(str).to_numpy().astype(object)
    nss_des = col("designation", "s")
    df = pd.DataFrame({"designation": des, "class": [cmap.get(d, "no_orbit") for d in des],
                       "dia_id": dsid[in_input], "tai": f("midpointMjdTai"),
                       "sss_offset": f("ephOffset"),
                       "sss_sigma": sigma_major_arcsec(f("ephRaErr"), f("ephDecErr"), f("ephRa_ephDec_Cov")),
                       "nss_designation": nss_des, "nss_offset": col("ephOffset"),
                       "dt_s": dt * TS.SECONDS_PER_DAY,
                       "allow_pos_mas": pos_allow,
                       "allow_rate": TS.rate_allowance(dt, rate, f("topoRange"), f("topoRangeRate"),
                                                       f("helioRange"), f("ephDec")),
                       "allow_vmag": TS.vmag_allowance(dt, rate, f("topoRange"), f("topoRangeRate"),
                                                       f("helioRange"), f("helioRangeRate"))})
    match = has & (nss_des == des)
    with np.errstate(invalid="ignore"):
        df["pos_mas"] = np.where(match, sky_sep_arcsec(f("ephRa"), f("ephDec"), col("ephRa"), col("ephDec"))
                                 * 1000.0, np.nan)
        # where dt != 0: what's left once the motion over dt is taken out
        mean_ra = np.where(np.isfinite(col("ephRateRa")), (f("ephRateRa") + col("ephRateRa")) / 2,
                           f("ephRateRa"))
        mean_dec = np.where(np.isfinite(col("ephRateDec")), (f("ephRateDec") + col("ephRateDec")) / 2,
                            f("ephRateDec"))
        resid = TS.motion_residual_mas(f("ephRa"), f("ephDec"), col("ephRa"), col("ephDec"),
                                       mean_ra, mean_dec, dt)
        df["pos_resid_mas"] = np.where(match & (dt != 0), resid, df["pos_mas"].to_numpy())
        df["allow_pos_margin_mas"] = TS.position_margin_mas(rate, dt, TS.angular_acceleration(
            rate, f("topoRange"), f("topoRangeRate"), f("helioRange"), f("ephDec")))
        for name, c in (("rate_ra", "ephRateRa"), ("rate_dec", "ephRateDec"), ("vmag", "ephVmag"),
                        ("offset", "ephOffset")):
            df[name] = np.where(match, np.abs(f(c) - col(c)), np.nan)
    border = 1.0 + CUT_BORDER_REL
    rsn = np.array([reason.get(d, "not in mpc_orbits") for d in des], dtype=object)
    q = np.array([qmap.get(d, np.nan) for d in des], dtype=np.float64)
    sig, off = df["sss_sigma"].to_numpy(), df["sss_offset"].to_numpy()
    df["match_radius"] = rad = match_radius(des)
    with np.errstate(invalid="ignore"):
        conds = [match, rsn != "", np.isfinite(q) & (q < SUNGRAZER_Q_AU), ~np.isfinite(sig),
                 sig * border > SIGMA_MAX_ARCSEC, off * border > rad - pos_allow / 1e3,
                 has & (df["nss_offset"].to_numpy() <= off * border + pos_allow / 1e3)]
    labels = ["match", "orbit filtered", "sungrazer (q < 0.02 au)", "no covariance", "sigma > 10\"",
              "offset > radius", "nearer object"]
    status = np.select(conds, labels, "UNEXPLAINED").astype(object)
    filt = status == "orbit filtered"
    status[filt] = [f"orbit filtered ({r})" for r in rsn[filt]]
    df["status"] = status
    if coarse_sigma is not None and len(df):
        un = df.index[df["status"] == "UNEXPLAINED"]
        for d in sorted(set(df.loc[un, "designation"])):
            ii = [i for i in un if df.at[i, "designation"] == d]
            tai = np.array([df.at[i, "tai"] for i in ii])
            offs = np.array(NIGHT_SAMPLE_OFFSETS_DAYS)
            sig = np.asarray(coarse_sigma(d, (tai[:, None] + offs[None, :]).ravel()), np.float64)
            sig = sig.reshape(len(ii), len(offs))
            for k, i in enumerate(ii):
                fin = np.isfinite(sig[k])
                df.at[i, "coarse_sigma_max"] = float(np.nanmax(sig[k])) if fin.any() else np.inf
                if not np.isfinite(sig[k]).all() or np.nanmax(sig[k]) > SIGMA_MAX_ARCSEC:
                    df.at[i, "status"] = "sigma > 10\" (coarse, within the night)"
    return df


def check_nearbysso(nearbysso, ssobservation, mpc_orbits, dia_sources, objects=None, rep=None):
    rep = rep or Report(f"Non-grav NearbySSO: {nearbysso} vs {ssobservation}")
    nss = pq.read_table(nearbysso, columns=["diaSourceId", "designation", "ephRa", "ephDec", "ephOffset",
                                            "ephVmag", "ephRateRa", "ephRateDec"]).to_pandas()
    present = set(pq.read_schema(ssobservation).names)
    sss = _read_sss(ssobservation, ["designation", "diaSourceId", "visit", "midpointMjdTai", "ra", "dec",
                               "ephRa", "ephDec", "ephOffset", "ephVmag", "ephRateRa", "ephRateDec",
                               *ERROR_COLUMNS, *(c for c in SHIFT_COLUMNS if c in present)]
                    ).to_pandas(types_mapper=_INT_MAPPER)
    visits = np.unique(sss["visit"].astype("int64").to_numpy())
    from bench.nearbysso_validate import read_dia_subset
    dia = read_dia_subset(dia_sources, visits=visits,
                          columns=("diaSourceId", "visit", "ra", "dec", "midpointMjdTai"))
    des = set(sss["designation"].dropna()) | set(nss["designation"].dropna())
    orbits = read_mpc(mpc_orbits, des, columns=("q", "e", "i", "node", "argperi", "peri_time"))
    cmap = classes_for(des, objects=objects, orbits_df=orbits)

    def coarse_sigma(d, tai):
        e = published_ellipses(mpc_orbits, [d], {d: tai}).get(d)
        return np.full(len(tai), np.inf) if e is None else sigma_major_arcsec(*e)
    df = nearbysso_compare(nss, sss, dia, orbits, cmap, rep, coarse_sigma=coarse_sigma)
    _nearbysso_gates(df, rep)
    return rep, df


def _nearbysso_gates(df, rep):
    if not len(df):
        rep.check("SSObservation rows in the NearbySSO input", False, "none")
        return
    comets = df["class"].isin(("comet_ng", "comet_grav"))
    nc = int(comets.sum())
    mc = int((comets & (df["status"] == "match")).sum())
    if nc:
        rep.check("most comet rows matched", mc >= COMET_MATCH_MIN_FRAC * nc,
                  f"{mc:,} of {nc:,} comet rows ({mc / nc:.1%}; >= {COMET_MATCH_MIN_FRAC:.0%})")
    else:
        # e.g. the daily PPDB DiaSources, which hold none of the comets'
        # SSObservation DiaSources (2026-10-01): nothing to match, not a
        # failure
        rep.info("most comet rows matched: not applicable, no comet SSObservation row's DiaSource is in "
                 "the NearbySSO input")
    un = df[df["status"] == "UNEXPLAINED"]
    rep.check("every miss explained", un.empty,
              f"{len(un):,} unexplained" + (": " + "; ".join(
                  f"{r.designation} dia {r.dia_id} offset {r.sss_offset:.3f}\" sigma {r.sss_sigma:.3g}\" "
                  f"NearbySSO {r.nss_designation}" for r in un.head(10).itertuples()) if len(un) else ""))
    m = df[df["status"] == "match"]
    if "dt_s" in df:
        dt_s = df["dt_s"].to_numpy(np.float64)
        big = np.abs(dt_s) > TS.DT_MAX_S
        nz = np.abs(dt_s[dt_s != 0])
        rep.check(f"time shift SSObservation - DiaSource within {TS.DT_MAX_S} s", not big.any(),
                  f"{int((dt_s != 0).sum()):,} of {len(df):,} rows shifted"
                  + (f", max |dt| {nz.max():.3f} s" if len(nz) else "")
                  + (f"; {int(big.sum())} beyond" if big.any() else "")
                  + "; shifted rows get bench/time_shift.py's allowances, the others are strict")
    zero = np.zeros(len(m))

    def allow(c):
        return m[c].to_numpy(np.float64) if c in m else zero
    for name, col, tol, extra in (
            ("position", "pos_resid_mas", POS_TOL_MAS, allow("allow_pos_margin_mas")),
            ("ephRateRa", "rate_ra", RATE_TOL_DEG_DAY, allow("allow_rate")),
            ("ephRateDec", "rate_dec", RATE_TOL_DEG_DAY, allow("allow_rate")),
            ("ephVmag", "vmag", VMAG_TOL, allow("allow_vmag")),
            ("ephOffset", "offset", OFFSET_TOL_ARCSEC, allow("allow_pos_mas") / 1e3)):
        v = m[col].to_numpy(dtype=np.float64)
        bad = np.isfinite(v) & (v > tol + extra)
        worst = m.iloc[int(np.nanargmax(v))] if np.isfinite(v).any() else None
        rep.check(f"matched rows agree: {name}", not bad.any(),
                  f"{len(m):,} rows, max {np.nanmax(v) if np.isfinite(v).any() else float('nan'):.3g} "
                  f"(tol {tol:g}" + (" + the time-shift allowance" if np.any(extra) else "")
                  + (f"; {int(bad.sum())} beyond, worst {worst['designation']}" if bad.any() else "") + ")")
    night = df[df["status"].str.contains("within the night", regex=False)]
    if len(night):
        rep.info("")
        rep.info("Misses gated by the coarse track's sigma within the night, though SSObservation's ellipse "
                 "at the observation is small (sigma_major [arcsec]):")
        t = night[["designation", "class", "dia_id", "tai", "sss_sigma", "coarse_sigma_max"]]
        rep.info(_table(t.assign(dia_id=t["dia_id"].astype(str), tai=t["tai"].map("{:.5f}".format))))
    tab = (df.assign(n=1)
           .pivot_table(index="class", columns="status", values="n", aggfunc="sum", fill_value=0)
           .reindex([c for c in CLASS_ORDER if c in set(df["class"])]))
    tab.insert(0, "rows", tab.sum(axis=1))
    rep.info("")
    rep.info("Rows per class and status:")
    rep.info(_table(tab.reset_index()))
    for c in ("comet_ng", "comet_grav", "yarkovsky"):
        sub = df[df["class"] == c]
        if not len(sub):
            continue
        g = sub.groupby("designation")
        t = pd.DataFrame({"rows": g.size(), "match": g["status"].apply(lambda s: int((s == "match").sum())),
                          "misses": g["status"].apply(lambda s: ", ".join(
                              f"{k} {v}" for k, v in Counter(x for x in s if x != "match").items()) or "-"),
                          "max_pos_mas": g["pos_mas"].max()}).reset_index()
        rep.info("")
        rep.info(f"Per object, {c}:")
        rep.info(_table(t))


# --------------------------------------------------------------------------
# 3. uncertainty (Monte Carlo)
# --------------------------------------------------------------------------

def ecl_to_eq_matrix(obliquity_arcsec=OBLIQUITY_ARCSEC):
    """Rotation from ecliptic J2000 to equatorial (ICRF-aligned) axes."""
    e = np.deg2rad(obliquity_arcsec / 3600.0)
    c, s = np.cos(e), np.sin(e)
    return np.array([[1.0, 0.0, 0.0], [0.0, c, -s], [0.0, s, c]])


def tai_mjd_to_assist(mjd_tai):
    from astropy.time import Time
    return Time(np.asarray(mjd_tai, np.float64), format="mjd", scale="tai").tdb.jd - ASSIST_JD_REF


def tt_mjd_to_assist(mjd_tt):
    from astropy.time import Time
    return Time(np.asarray(mjd_tt, np.float64), format="mjd", scale="tt").tdb.jd - ASSIST_JD_REF


def assist_to_tai_mjd(t):
    from astropy.time import Time
    return Time(np.asarray(t, np.float64) + ASSIST_JD_REF, format="jd", scale="tdb").tai.mjd


def observer_pos(mjd_tai):
    """X05 barycentric ICRF positions [AU], (K, 3)."""
    import astropy.units as u
    from astropy.time import Time
    from ssp import util
    t = Time(np.atleast_1d(np.asarray(mjd_tai, np.float64)), format="mjd", scale="tai")
    r, _ = util.observatory_barycentric_posvel("X05", t)
    return r.to_value(u.au).T.copy()


def open_ephem():
    import assist
    return assist.Ephem(os.environ["SSP_ASSIST_PLANETS"], os.environ["SSP_ASSIST_ASTEROIDS"])


def draw_parameters(car, n, rng, state_only=False):
    """n draws of (helio ecliptic state (n, 6) [au, au/d], A (n, 3)
    [au/d^2]) from the CAR covariance in physical units. Row 0 is the
    nominal. ``state_only``: the 6x6 marginal, A fixed at its value."""
    vals = car["values"] * car["scale"]
    cov = car["cov"] * np.outer(car["scale"], car["scale"])
    if state_only:
        p = np.tile(vals, (n, 1))
        p[:, :6] = rng.multivariate_normal(vals[:6], cov[:6, :6], size=n, method="eigh")
    else:
        p = rng.multivariate_normal(vals, cov, size=n, method="eigh")
    p[0] = vals
    A = np.zeros((n, 3))
    for k, ix in enumerate(car["A_index"]):
        if ix >= 0:
            A[:, k] = p[:, ix]
    return p[:, :6].copy(), A


def helio_ecl_to_bary_icrf(states, sun_state, obliquity_arcsec):
    R = ecl_to_eq_matrix(obliquity_arcsec)
    out = np.empty_like(states)
    out[:, :3] = states[:, :3] @ R.T + sun_state[:3]
    out[:, 3:] = states[:, 3:] @ R.T + sun_state[3:]
    return out


def integrate_draws(ephem, t0, states, A, kind, times):
    """Barycentric positions (n, K, 3) of n test particles (barycentric ICRF
    states at ASSIST time t0, per-particle A's, the class's g(r)) at the K
    ASSIST times, one ASSIST simulation per direction. NaN after a
    failure."""
    import assist
    import rebound
    times = np.asarray(times, np.float64)
    out = np.full((len(states), len(times), 3), np.nan)
    for side in (np.flatnonzero(times >= t0), np.flatnonzero(times < t0)):
        if not len(side):
            continue
        order = side[np.argsort(np.abs(times[side] - t0), kind="stable")]
        sim = rebound.Simulation()
        sim.t = t0
        ex = assist.Extras(sim, ephem)
        if kind in GOFR:
            for k, v in GOFR[kind].items():
                setattr(ex, k, v)
        for s in states:
            sim.add(x=s[0], y=s[1], z=s[2], vx=s[3], vy=s[4], vz=s[5])
        ex.particle_params = np.ascontiguousarray(A, dtype=np.float64).ravel()
        sim.ri_ias15.adaptive_mode = 2
        xyz = np.empty((len(states), 3))
        for j in order:
            try:
                ex.integrate_or_interpolate(float(times[j]))
            except Exception:
                break
            sim.serialize_particle_data(xyz=xyz)
            out[:, j] = xyz
        del ex, sim
    return out


def tangent_offsets(rho, rho0):
    """Gnomonic offsets [arcsec] (xi along +RA on the sky, eta along +Dec)
    of the directions rho (n, 3) about rho0 (3,)."""
    u0 = rho0 / np.linalg.norm(rho0)
    ra = np.arctan2(u0[1], u0[0])
    dec = np.arcsin(u0[2])
    e_ra = np.array([-np.sin(ra), np.cos(ra), 0.0])
    e_dec = np.array([-np.sin(dec) * np.cos(ra), -np.sin(dec) * np.sin(ra), np.cos(dec)])
    d = rho @ u0
    return np.rad2deg(rho @ e_ra / d) * 3600.0, np.rad2deg(rho @ e_dec / d) * 3600.0


def mc_ellipse(xi, eta):
    """(sigma_ra*, sigma_dec [arcsec], rho) of the samples."""
    ok = np.isfinite(xi) & np.isfinite(eta)
    c = np.cov(np.vstack([xi[ok], eta[ok]]))
    sr, sd = np.sqrt(c[0, 0]), np.sqrt(c[1, 1])
    return sr, sd, c[0, 1] / (sr * sd), int(ok.sum())


def _nanmedian(x):
    x = np.asarray(x, np.float64)
    x = x[np.isfinite(x)]
    return float(np.median(x)) if len(x) else np.nan


def sigma_band(n):
    """The allowed |ratio - 1| for a sample of n."""
    return MC_Z / np.sqrt(2.0 * (n - 1)) + MC_MARGIN


def compare_sigmas(mc_ra, mc_dec, n, pub_ra, pub_dec):
    """(ratio_ra, ratio_dec, ok) of published over Monte Carlo sigmas."""
    rr, rd = pub_ra / mc_ra, pub_dec / mc_dec
    band = sigma_band(n)
    ok = bool(np.isfinite(rr) and np.isfinite(rd) and abs(rr - 1) <= band and abs(rd - 1) <= band)
    return rr, rd, ok


def _mc_one(task):
    """Worker: one orbit's Monte Carlo. Returns a list of per-time dicts."""
    des, car, kind, tai, draws, seed = task
    ephem = open_ephem()
    rng = np.random.default_rng(seed)
    t0 = float(tt_mjd_to_assist(car["epoch_tt_mjd"]))
    sun = ephem.get_particle(0, t0)
    sun_state = np.array([sun.x, sun.y, sun.z, sun.vx, sun.vy, sun.vz])
    t = tai_mjd_to_assist(tai)
    obs = observer_pos(tai)
    res = {}
    for label, so in (("full", False), ("state_only", True)):
        st, A = draw_parameters(car, draws, rng, state_only=so)
        bary = helio_ecl_to_bary_icrf(st, sun_state, car["obliquity_arcsec"])
        pos = integrate_draws(ephem, t0, bary, A, kind, t)
        res[label] = pos
    out = []
    for k in range(len(t)):
        rec = dict(designation=des, kind=kind, tai=float(tai[k]), dt_epoch=float(t[k] - t0))
        for label, pos in res.items():
            rho = pos[:, k] - obs[k]
            xi, eta = tangent_offsets(rho, rho[0])
            sr, sd, rho_c, n = mc_ellipse(xi, eta)
            rec[f"{label}_ra"], rec[f"{label}_dec"], rec[f"{label}_rho"], rec[f"{label}_n"] = sr, sd, rho_c, n
            # the sample mean's offset from the nominal [sigma]: non-linearity
            rec[f"{label}_bias"] = float(max(abs(np.nanmean(xi)) / sr, abs(np.nanmean(eta)) / sd))
        rec["nominal_rho"] = res["full"][0, k] - obs[k]
        out.append(rec)
    return out


def published_ellipses(mpc_orbits, designations, tai_by_des):
    """``propagate.coarse`` + ``ellipse_at`` at each object's times, for
    ORBIT_DTYPE rows from ``ssp.ssobservation_ellipse.load_orbit_covariances``.
    {designation: (ra_err, dec_err, cov) [deg, deg, deg^2] arrays}."""
    from ssp.nearbysso import propagate
    from ssp.ssobservation_ellipse import load_orbit_covariances
    ephem = open_ephem()
    rows = load_orbit_covariances(mpc_orbits, list(designations), ephem)
    out = {}
    for d in designations:
        if d not in rows:
            continue
        # coarse wants sorted times
        tai, inv = np.unique(np.asarray(tai_by_des[d], np.float64), return_inverse=True)
        t = tai_mjd_to_assist(tai)
        obs = observer_pos(tai)
        tr = propagate.coarse(rows[d], t, obs, ephem)
        ra, dec = np.deg2rad(tr.ra), np.deg2rad(tr.dec)
        u = np.stack([np.cos(dec) * np.cos(ra), np.cos(dec) * np.sin(ra), np.sin(dec)], axis=1)
        topo = u * np.asarray(tr.delta)[:, None]
        e = propagate.ellipse_at(tr, t, topo_pos=topo)
        out[d] = tuple(np.asarray(x, np.float64)[inv] for x in e[:3])
    return out


def pick_times(mjd, k):
    """Up to k distinct times spread over the object's observations."""
    u = np.unique(np.asarray(mjd, np.float64))
    if len(u) <= k:
        return u
    return u[np.unique(np.round(np.linspace(0, len(u) - 1, k)).astype(int))]


def check_uncertainty(mpc_orbits, ssobservation=None, objects=None, n_orbits=MC_N_ORBITS, draws=MC_DRAWS,
                      n_times=3, workers=8, seed=20261001, rep=None):
    rep = rep or Report(f"Non-grav uncertainty (Monte Carlo): {mpc_orbits}")
    rng = np.random.default_rng(seed)
    sss = None
    if ssobservation:
        sss = _read_sss(ssobservation, ["designation", "midpointMjdTai", "ephRa", *ERROR_COLUMNS]).to_pandas()
        sss = sss[sss["designation"].notna() & sss["ephRa"].notna()]
    if objects:
        cands = {c: sorted(d for d, k in objects.items() if k == c) for c in ("comet_ng", "yarkovsky")}
    elif sss is not None:
        cm = classes_for(set(sss["designation"]), mpc_orbits)
        cands = {c: sorted(d for d, k in cm.items() if k == c) for c in ("comet_ng", "yarkovsky")}
    else:
        t = pq.read_table(mpc_orbits, columns=[ORBIT_KEY, "mpc_orb_jsonb"])
        ex = coefficient_extras(t.column("mpc_orb_jsonb"))
        des = t.column(ORBIT_KEY).to_numpy(zero_copy_only=False)
        cl = classify(des, ex)
        cands = {c: sorted(des[cl == c].tolist()) for c in ("comet_ng", "yarkovsky")}
    if sss is not None:
        have = set(sss["designation"])
        cands = {c: [d for d in v if d in have] for c, v in cands.items()}
    sample = []
    for c, v in cands.items():
        pick = sorted(rng.choice(v, size=min(n_orbits, len(v)), replace=False).tolist()) if v else []
        sample += pick
        rep.info(f"{c}: {len(pick)} of {len(v)} candidate orbits")
    orbits = read_mpc(mpc_orbits, sample)
    cars = {r.designation: car_parse(r.mpc_orb_jsonb) for r in orbits.itertuples(index=False)}
    tasks, tai_by = [], {}
    skipped = []
    for k, d in enumerate(sample):
        car = cars.get(d)
        if car is None or car["cov"] is None or car["kind"] not in GOFR:
            skipped.append(d)
            continue
        if sss is not None:
            tai = pick_times(sss.loc[sss["designation"] == d, "midpointMjdTai"].to_numpy(), n_times)
        else:
            ep = float(assist_to_tai_mjd(tt_mjd_to_assist(car["epoch_tt_mjd"])))
            tai = ep + np.array([-300.0, -60.0, 60.0, 300.0])[:max(n_times, 1)]
        tai_by[d] = tai
        tasks.append((d, car, car["kind"], tai, draws, seed + k))
    rep.check("every sampled orbit has a CAR covariance and a known model", not skipped,
              f"{len(tasks)} orbits" + (f"; skipped {skipped}" if skipped else ""))
    t_start = time.time()
    results = []
    if workers > 1 and len(tasks) > 1:
        import multiprocessing as mp
        with mp.get_context("fork").Pool(min(workers, len(tasks))) as pool:
            for r in pool.imap(_mc_one, tasks):
                results += r
    else:
        for task in tasks:
            results += _mc_one(task)
    rep.info(f"Monte Carlo: {len(tasks)} orbits x {draws} draws (full and state-only) in "
             f"{time.time() - t_start:.0f} s; X05 topocentric, geometric")
    df = pd.DataFrame(results)
    pub = published_ellipses(mpc_orbits, list(tai_by), tai_by)
    df["coarse_ra"] = np.nan
    df["coarse_dec"] = np.nan
    df["sss_ra"] = np.nan
    df["sss_dec"] = np.nan
    for d, (re_, de_, _) in pub.items():
        sel = np.flatnonzero(df["designation"] == d)
        for j, i in enumerate(sel):
            df.loc[i, "coarse_ra"] = re_[j] * 3600.0
            df.loc[i, "coarse_dec"] = de_[j] * 3600.0
    if sss is not None:
        for i, r in df.iterrows():
            s = sss[(sss["designation"] == r["designation"]) & (sss["midpointMjdTai"] == r["tai"])]
            if len(s):
                df.loc[i, "sss_ra"] = float(s["ephRaErr"].iloc[0]) * 3600.0
                df.loc[i, "sss_dec"] = float(s["ephDecErr"].iloc[0]) * 3600.0
    _uncertainty_gates(df, rep, draws, with_sss=sss is not None)
    return rep, df


def _uncertainty_gates(df, rep, draws, with_sss):
    band = sigma_band(draws)
    rep.info(f"sigma ratio band: |published / Monte Carlo - 1| <= {band:.3f} "
             f"({MC_Z} x 1/sqrt(2(N-1)) + {MC_MARGIN}, N = {draws})")
    if not len(df):
        rep.check("Monte Carlo results", False, "none")
        return
    for src, label in (("coarse", "propagate.coarse + ellipse_at"),
                       ("sss", "SSObservation ephRaErr/ephDecErr")):
        if src == "sss" and not with_sss:
            continue
        for mc in ("full", "state_only"):
            rr = df[f"{src}_ra"] / df[f"{mc}_ra"]
            rd = df[f"{src}_dec"] / df[f"{mc}_dec"]
            df[f"r_{src}_{mc}_ra"], df[f"r_{src}_{mc}_dec"] = rr, rd
        rr, rd = df[f"r_{src}_full_ra"], df[f"r_{src}_full_dec"]
        ok = np.isfinite(rr) & np.isfinite(rd) & ((rr - 1).abs() <= band) & ((rd - 1).abs() <= band)
        bad = df[~ok]
        per_kind = "; ".join(
            f"{k}: median ratio RA {_nanmedian(rr[df['kind'] == k]):.3f}, "
            f"Dec {_nanmedian(rd[df['kind'] == k]):.3f}"
            for k in sorted(set(df["kind"])))
        rep.check(f"{label} sigma vs the full Monte Carlo", bad.empty,
                  f"{int(ok.sum())} of {len(df)} (orbit, time) within the band; {per_kind}"
                  + (f"; outside: {', '.join(sorted(set(bad['designation'])))}" if len(bad) else ""))
        so_r, so_d = df[f"r_{src}_state_only_ra"], df[f"r_{src}_state_only_dec"]
        so_ok = (np.isfinite(so_r) & np.isfinite(so_d)
                 & ((so_r - 1).abs() <= band) & ((so_d - 1).abs() <= band))
        rep.info(f"(diagnostic) {label} vs the state-only Monte Carlo: {int(so_ok.sum())} of {len(df)} "
                 f"within the band; median ratio RA {_nanmedian(so_r):.3f}, Dec {_nanmedian(so_d):.3f}")
    rep.info("")
    rep.info("Per (orbit, time): sigmas [arcsec] (RA on the sky; MC full, MC state-only, coarse, "
             "SSObservation) and published/MC-full ratios:")
    df = df.assign(tai=df["tai"].map(lambda x: f"{x:.4f}"))
    rep.info(f"max |sample mean - nominal| / sigma (full MC): {df['full_bias'].max():.3f}")
    cols = ["designation", "kind", "tai", "dt_epoch", "full_ra", "full_dec", "full_rho", "full_bias",
            "state_only_ra", "state_only_dec", "coarse_ra", "coarse_dec", "r_coarse_full_ra",
            "r_coarse_full_dec"]
    if with_sss:
        cols += ["sss_ra", "sss_dec", "r_sss_full_ra", "r_sss_full_dec"]
    rep.info(_table(df[cols].sort_values(["kind", "designation", "tai"])))


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main(argv=None):
    ap = argparse.ArgumentParser(prog="python -m bench.nongrav_validate",
                                 description="Black-box validation of the non-gravitational forces.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def add(name, help_):
        p = sub.add_parser(name, help=help_)
        p.add_argument("--out", help="also write the report to this file")
        p.add_argument("--objects", default=None, help="objects.txt (class<TAB>designation)")
        return p

    p = add("offsets", "SSObservation with non-gravs vs a gravity-only reference")
    p.add_argument("new")
    p.add_argument("ref")
    p.add_argument("mpc_orbits")
    p.add_argument("--ng-errors", choices=("any", "unchanged", "changed"), default="any",
                   help="what the non-grav objects' error columns must do (default: only report)")
    p = add("nearbysso", "NearbySSO vs SSObservation for the same DiaSources")
    p.add_argument("nearbysso")
    p.add_argument("ssobservation")
    p.add_argument("mpc_orbits")
    p.add_argument("dia_sources", help="the NearbySSO input DiaSources")
    p.add_argument("--table", default=None, help="also write the per-row table (Parquet)")
    p = add("uncertainty", "Monte Carlo of the published error ellipse")
    p.add_argument("mpc_orbits")
    p.add_argument("--ssobservation", default=None,
                   help="SSObservation: observation times and its ellipse columns")
    p.add_argument("--n-orbits", type=int, default=MC_N_ORBITS, help="orbits per class (comet_ng, yarkovsky)")
    p.add_argument("--draws", type=int, default=MC_DRAWS)
    p.add_argument("--times", type=int, default=3, help="times per orbit")
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--seed", type=int, default=20261001)
    p.add_argument("--table", default=None, help="also write the per-(orbit, time) table (Parquet)")
    a = ap.parse_args(argv)
    objects = read_objects(a.objects) if a.objects else None
    df = None
    if a.cmd == "offsets":
        rep = check_offsets(a.new, a.ref, a.mpc_orbits, objects, a.ng_errors)
    elif a.cmd == "nearbysso":
        rep, df = check_nearbysso(a.nearbysso, a.ssobservation, a.mpc_orbits, a.dia_sources, objects)
    elif a.cmd == "uncertainty":
        if not 1 <= a.workers <= 32:
            ap.error("--workers must be 1..32 (shared nodes)")
        rep, df = check_uncertainty(a.mpc_orbits, a.ssobservation, objects, a.n_orbits, a.draws, a.times,
                                    a.workers, a.seed)
    if df is not None and getattr(a, "table", None):
        df = df.copy()
        for c in df.columns:
            if df[c].dtype == object and len(df) and isinstance(df[c].iloc[0], np.ndarray):
                df[c] = df[c].map(list)
        df.to_parquet(a.table, index=False)
    return rep.finish(a.out)


if __name__ == "__main__":
    sys.exit(main())
