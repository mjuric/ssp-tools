"""Black-box validation of the tail position angles (WP T2 of
docs/design/tail-angles.md): ``ephAntiSunPA`` and ``ephAntiMotionPA`` in
SSSource and NearbySSO.

Written from the design, the contract (``ssp/sssource_contract.py``, "Tail
position angles"; ``ssp/nearbysso/_contract.py``) and ``sso_base.yaml``
only. The angles are recomputed here by this module's own implementation of
the contract's definition (``position_angle``); the production function
(``ssp.ephem_assist.tail_position_angles``) is never called.

Subcommands::

  fetch [--dry-run [--show-url]] [--limit N] [--only DES ...]
      the ONLY code here that talks to JPL: one Horizons X05 observer table
      per object of OBJECTS, at its Rubin times (quantities 1, 19, 20, 24,
      27: astrometric RA/Dec, r, delta, the phase angle, PsAng/PsAMV), for
      JPL's default solution. Cache-first, serial, paced, budgeted (BUDGET),
      logged, through bench.jpl_compare.JPLClient.
  jpl         (pass/fail) from the cache alone: JPL's own elements (the
              Horizons header) integrated by compute_ephemerides_one, the
              angles computed from the EphResult in the ICRS, the mean
              equator of date and the true equator of date, against PsAng /
              PsAMV. Settles which pole Horizons uses and checks ours.
  consistency SSSOURCE NEARBYSSO [--dia-sources DIA]
              (pass/fail, no network) the two tables' angles equal at the
              same (designation, diaSourceId): bitwise, or within
              max(1 float32 ulp, 1e-4 deg), since the two passes'
              integrations may differ at ~1e-11 deg (both counts
              reported); non-null exactly where there is an orbit; in
              [0, 360); and SSSource's angles recomputed from its own
              float32 helio_* / topo_* columns. With DIA (the NearbySSO
              input, ppdb_dia_sources.parquet), a pair whose SSSource
              midpointMjdTai differs from its DiaSource's (the shutter
              correction, docs/design/shutter-timing.md) also gets the
              time-shift allowance of bench/time_shift.py on top; pairs at
              the same time keep the strict rule. Without DIA every pair is
              held to the strict rule.

Outputs of ``jpl`` (``--out``, default WORK): tail_angles_jpl_report.txt and
tail_angles_jpl_rows.csv.

JPL ETIQUETTE (enforced by bench.jpl_compare.JPLClient): strictly serial (a
lock on the cache), >= 1.5 s apart, at most BUDGET requests in total
(counted from this cache's requests.log), every response cached and every
request logged. Never run ``fetch`` from tests or CI. Never contact the MPC.

Time scales: the Rubin times are SSSource's midpointMjdTai; Horizons is
asked for TT (TAI + 32.184 s), sent with 10 decimals, and our side uses
exactly the times sent (as bench.jpl_compare does).
"""

from __future__ import annotations

import argparse
import os
import sys

import numpy as np

from bench import jpl_compare as J

# ---------------------------------------------------------------------------
# Paths, objects, limits
# ---------------------------------------------------------------------------

WORK = "/sdf/data/rubin/user/mjuric/tail-angles/work/t2"
CACHE = os.path.join(WORK, "cache")
FIXTURE = J.FIXTURE
#: The fixture's reference SSSource (midpointMjdTai of each object's rows).
REF_SSSOURCE = os.path.join(FIXTURE, "ref_gravity", "sssource.parquet")
#: Horizons requests allowed for this WP, over all runs.
BUDGET = 15

#: (fixture designation, Horizons COMMAND, kind, why). The COMMANDs are
#: JPL's designations as N5 looked them up (its cache, read-only); the
#: phase-angle spans are those of the fixture's Rubin times.
OBJECTS = (
    ("P/1970 Y1", "'DES=70P;CAP;NOFRAG'", "comet", "near opposition: phase 0.23-7.1 deg, r 5.3 au"),
    ("P/2005 N3", "'DES=261P;CAP;NOFRAG'", "comet", "phase 0.8-9.9 deg"),
    ("P/1818 W1", "'DES=2P;CAP;NOFRAG'", "comet", "2P/Encke, phase 12-13 deg, r 4.1 au"),
    ("P/2003 K2", "'DES=210P;CAP;NOFRAG'", "comet", "phase 16-18 deg, dec -22"),
    ("C/2023 H1", "'DES=2023 H1;CAP;NOFRAG'", "comet", "long-period, phase 1.7-4.5 deg"),
    ("2002 VU114", "'DES=2002 VU114;'", "asteroid", "phase 0.8-16 deg"),
    ("2007 GK33", "'324765;'", "asteroid", "main belt, phase 1.7-15 deg"),
    ("1994 CJ1", "'847431;'", "asteroid", "NEO, phase 15-58 deg"),
    ("2018 BY6", "'DES=2018 BY6;'", "asteroid", "NEO at 0.05 au, phase 41-49 deg"),
    ("2023 YO1", "'DES=2023 YO1;'", "asteroid", "NEO, phase 73 deg"),
)

#: Pass threshold against Horizons [deg] (the design's 0.01 deg)...
PASS_DEG = 0.01
#: ... applied to PsAng only where the phase angle is at least this [deg]
#: (closer to opposition the anti-Sun direction is ill-conditioned).
PSANG_MIN_PHASE_DEG = 1.0

# ---------------------------------------------------------------------------
# The contract's definition, implemented independently
# ---------------------------------------------------------------------------


def position_angle(w, u, R=None):
    """Position angle [deg, in [0, 360)] of the (3, N) vectors ``w``
    projected on the sky at the (3, N) directions ``u`` (any length),
    measured from north through east. North is the ICRS pole, or, with
    ``R`` ((3, 3) or (N, 3, 3), ICRS -> another equatorial frame), that
    frame's pole. NaN where w's projection is exactly zero or an input is
    NaN.

    With u at (alpha, delta), north = (-sin d cos a, -sin d sin a, cos d)
    and east = (-sin a, cos a, 0); PA = atan2(w . east, w . north). Written
    here without trigonometry: east = z x u / |z x u|, north = u x east."""
    w = np.asarray(w, float)
    u = np.asarray(u, float)
    if R is not None:
        R = np.asarray(R, float)
        if R.ndim == 2:
            w, u = R @ w, R @ u
        else:
            w = np.einsum("nij,jn->in", R, w)
            u = np.einsum("nij,jn->in", R, u)
    u = u / np.linalg.norm(u, axis=0)
    east = np.array([-u[1], u[0], np.zeros_like(u[0])])
    ne = np.linalg.norm(east, axis=0)
    # At the exact pole alpha = atan2(0, 0) = 0, so east = (0, 1, 0).
    east = np.where(ne > 0, east / np.where(ne > 0, ne, 1.0), np.array([[0.0], [1.0], [0.0]]))
    north = np.cross(u.T, east.T).T
    e, n = np.sum(w * east, axis=0), np.sum(w * north, axis=0)
    with np.errstate(invalid="ignore"):
        pa = np.degrees(np.arctan2(e, n)) % 360.0
        pa = np.where(pa >= 360.0, 0.0, pa)  # -tiny % 360 rounds to 360.0
    return np.where((e == 0) & (n == 0), np.nan, pa)


def tail_angles(helio_pos, helio_vel, topo_pos, R=None):
    """(anti-Sun PA, anti-motion PA) [deg] per the contract: w = helio_pos
    and w = -helio_vel, projected at topo_pos."""
    return (
        position_angle(helio_pos, topo_pos, R),
        position_angle(-np.asarray(helio_vel, float), topo_pos, R),
    )


def dangle(a, b):
    """a - b wrapped to [-180, 180) [deg]."""
    return (np.asarray(a, float) - np.asarray(b, float) + 180.0) % 360.0 - 180.0


def sky_fraction(w, u):
    """|w projected on the sky at u| / |w|: sin of the angle between w and
    the line of sight (small near opposition for w = helio_pos)."""
    w = np.asarray(w, float)
    u = np.asarray(u, float) / np.linalg.norm(u, axis=0)
    wn = np.linalg.norm(w, axis=0)
    along = np.sum(w * u, axis=0)
    return np.sqrt(np.maximum(wn**2 - along**2, 0.0)) / wn


def frame_matrices(mjd_tt, kind):
    """(N, 3, 3) ICRS -> equator-of-date rotations: 'mean' (frame bias +
    IAU 2006 precession, erfa.pmat06) or 'true' (+ IAU 2000A nutation,
    erfa.pnm06a)."""
    import erfa

    jd1 = np.full(np.shape(mjd_tt), 2400000.5)
    f = {"mean": erfa.pmat06, "true": erfa.pnm06a}[kind]
    return f(jd1, np.asarray(mjd_tt, float))


FRAMES = ("icrs", "mean", "true")

# ---------------------------------------------------------------------------
# Horizons requests and parsing
# ---------------------------------------------------------------------------


def horizons_tail_params(command, mjd_tt):
    """An X05 observer table at TT MJDs: astrometric RA/Dec (1), r (19),
    delta (20), the Sun-target-observer angle (24) and PsAng/PsAMV (27)."""
    p = J.horizons_observer_params(command, mjd_tt)
    p["QUANTITIES"] = "'1,19,20,24,27'"
    return p


def parse_tail_table(text):
    """A quantity-27 observer table -> dict of arrays: jd, ra, dec [deg], r,
    delta [au], phase (S-T-O) [deg], psang, psamv [deg]; NaN where Horizons
    prints n.a.; plus 'meta' and 'notes' (its explanation of PsAng/PsAMV)."""
    header, rows = J._table(text)

    def col(pred):
        for j, h in enumerate(header):
            if pred(h):
                return np.array([J._float(r[j]) if j < len(r) else np.nan for r in rows])
        raise ValueError(f"Horizons: no column matching in {header}")

    return {
        "jd": col(lambda h: h.startswith("Date")),
        "ra": col(lambda h: h.startswith("R.A.")),
        "dec": col(lambda h: h.startswith("DEC_")),
        "r": col(lambda h: h == "r"),
        "delta": col(lambda h: h == "delta"),
        "phase": col(lambda h: h == "S-T-O"),
        "psang": col(lambda h: h == "PsAng"),
        "psamv": col(lambda h: h == "PsAMV"),
        "meta": J.horizons_meta(text),
        "notes": psang_notes(text),
    }


def psang_notes(text):
    """Horizons' own explanation of quantity 27 (the 'Column meaning' block
    after $$EOE), or ''."""
    tail = text[text.index("$$EOE") :] if "$$EOE" in text else text
    i = tail.find("PsAng")
    while i >= 0 and "=" not in tail[i : i + 40]:
        i = tail.find("PsAng", i + 1)
    if i < 0:
        return ""
    block = tail[i:]
    end = block.find("Units:")
    if end >= 0:
        end = block.find("\n", end)
    return block[: end if end > 0 else 1000].strip()


def header_model_pars(meta):
    """JPL's non-grav parameters as Horizons' header prints them, in
    parse_sbdb's model_pars form (bench.jpl_compare.Plan.jpl_model_pars)."""
    out = {
        n: (meta[n], np.nan, "HZN") for n in ("A1", "A2", "A3", "ALN", "NM", "NN", "NK", "R0") if n in meta
    }
    for n in ("DT", "AMRAT"):
        if meta.get(n, 0.0) != 0.0:
            out[n] = (meta[n], np.nan, "HZN")
    return out


# ---------------------------------------------------------------------------
# The plan
# ---------------------------------------------------------------------------


def rubin_times_tt(designations, sssource=REF_SSSOURCE):
    """{designation: TT MJDs as sent}: the unique midpointMjdTai of each
    object's rows with an orbit, + 32.184 s, rounded as sent."""
    import pyarrow.parquet as pq

    df = pq.read_table(
        sssource,
        columns=["designation", "midpointMjdTai", "ephRa"],
        filters=[("designation", "in", list(designations))],
    ).to_pandas()
    return {d: J.roundtrip(J.tai_to_tt(J.rubin_times_tai(df, d))) for d in designations}


class Plan:
    def __init__(self, client, sssource=REF_SSSOURCE, objects=OBJECTS):
        self.client = client
        self.objects = objects
        self.tt = rubin_times_tt([o[0] for o in objects], sssource)

    def requests(self):
        return [
            ("horizons", horizons_tail_params(cmd, self.tt[d]), f"pa {d}") for d, cmd, _, _ in self.objects
        ]

    def text(self, d):
        cmd = dict((o[0], o[1]) for o in self.objects)[d]
        return self.client.get("horizons", horizons_tail_params(cmd, self.tt[d]), f"pa {d}")


# ---------------------------------------------------------------------------
# fetch
# ---------------------------------------------------------------------------


def cmd_fetch(args):
    client = J.JPLClient(args.cache, offline=args.dry_run, budget=BUDGET)
    plan = Plan(client, args.sssource)
    reqs = plan.requests()
    if args.only:
        reqs = [r for r in reqs if r[2].split(" ", 1)[1] in args.only]
    todo = [r for r in reqs if not client.cached(*r)]
    print(f"{len(reqs)} requests, {len(todo)} not cached; {client.n_logged()} sent so far (budget {BUDGET})")
    if args.limit is not None:
        todo = todo[: args.limit]
    for service, params, label in todo:
        if args.dry_run:
            print(f"  would send: {label}  ({len(J.request_url(service, params))} chars)")
            if args.show_url:
                print("    " + J.request_url(service, params))
            continue
        text = client.get(service, params, label)
        print(f"  {label}: {len(text)} bytes")
    print(f"sent this run: {client.sent}; total logged: {client.n_logged()}")
    return 0


# ---------------------------------------------------------------------------
# jpl: the comparison
# ---------------------------------------------------------------------------


def our_side(d, hz, tt, ephem, is_comet):
    """compute_ephemerides_one of JPL's own solution (the elements its
    integration starts from, from the Horizons header, and its non-gravs)
    at the TT times sent."""
    from astropy.time import Time
    from ssp import ephem_assist as EA

    ic = J.horizons_initial_elements(hz["text"])
    ng, note = J.jpl_nongrav({"model_pars": header_model_pars(hz["meta"])}, d)
    res = EA.compute_ephemerides_one(
        d, Time(tt, format="mjd", scale="tt"), None, ephem, row=J.jpl_row(ic), nongrav=ng
    )
    return res, note


def compare_object(plan, d, kind, ephem):
    """Per-time DataFrame of one object: Horizons' and our angles in the
    three frames, the residuals, the phase angle and the sky fractions."""
    import pandas as pd

    text = plan.text(d)
    hz = parse_tail_table(text)
    hz["text"] = text
    tt = plan.tt[d]
    if len(hz["jd"]) != len(tt) or np.max(np.abs(hz["jd"] - 2400000.5 - tt)) > 1e-8:
        raise RuntimeError(f"{d}: Horizons' times differ from those sent")
    res, note = our_side(d, hz, tt, ephem, kind == "comet")
    out = {
        "designation": d,
        "kind": kind,
        "mjd_tt": tt,
        "source": hz["meta"].get("source", ""),
        "ng": note,
        "phase_hz": hz["phase"],
        "phase_ours": res.phase_angle,
        "radec_mas": J.separation_arcsec(res.ra_deg, res.dec_deg, hz["ra"], hz["dec"]) * 1e3,
        "dec": res.dec_deg,
        "psang_hz": hz["psang"],
        "psamv_hz": hz["psamv"],
        "sky_frac_sun": sky_fraction(res.helio_pos, res.topo_pos),
        "sky_frac_vel": sky_fraction(res.helio_vel, res.topo_pos),
    }
    for fr in FRAMES:
        R = None if fr == "icrs" else frame_matrices(tt, fr)
        a, m = tail_angles(res.helio_pos, res.helio_vel, res.topo_pos, R)
        out[f"psang_{fr}"] = a
        out[f"psamv_{fr}"] = m
        out[f"d_psang_{fr}"] = dangle(a, hz["psang"])
        out[f"d_psamv_{fr}"] = dangle(m, hz["psamv"])
    # Horizons' angles moved to the ICRS pole, assuming they refer to the
    # true equator of date: the rotation of the pole is the difference of
    # our true-of-date and ICRS angles (the same vectors, two poles).
    for q in ("psang", "psamv"):
        rot = dangle(out[f"{q}_true"], out[f"{q}_icrs"])
        out[f"{q}_hz_icrs"] = (hz[q] - rot) % 360.0
        out[f"pole_rot_{q}"] = rot
    return pd.DataFrame(out), hz["notes"]


def frame_verdict(rows, frame_col_fmt="d_psamv_{}"):
    """{frame: RMS residual [deg]} over rows where both angles are
    well-conditioned, and the best frame."""
    ok = (rows.phase_hz >= PSANG_MIN_PHASE_DEG).to_numpy()
    rms = {}
    for fr in FRAMES:
        x = np.concatenate([rows[f"d_psang_{fr}"].to_numpy()[ok], rows[f"d_psamv_{fr}"].to_numpy()])
        x = x[np.isfinite(x)]
        rms[fr] = float(np.sqrt(np.mean(x**2))) if len(x) else np.nan
    return rms, min(rms, key=lambda k: rms[k] if np.isfinite(rms[k]) else np.inf)


def summarize(rows, frame):
    """Per-object table (our angles in ``frame`` against Horizons')."""
    import pandas as pd

    out = []
    for d, g in rows.groupby("designation", sort=False):
        far = g.phase_hz >= PSANG_MIN_PHASE_DEG
        a = np.abs(g[f"d_psang_{frame}"])
        m = np.abs(g[f"d_psamv_{frame}"])
        out.append(
            {
                "designation": d,
                "kind": g.kind.iloc[0],
                "source": g.source.iloc[0],
                "n": len(g),
                "phase_min": g.phase_hz.min(),
                "phase_max": g.phase_hz.max(),
                "radec_max_mas": g.radec_mas.max(),
                "psang_max": a[far].max() if far.any() else np.nan,
                "psang_max_lowphase": a[~far].max() if (~far).any() else np.nan,
                "n_lowphase": int((~far).sum()),
                "psamv_max": m.max(),
                "pole_rot_max": np.abs(g.pole_rot_psang).max(),
            }
        )
    return pd.DataFrame(out)


def cmd_jpl(args):
    import pandas as pd
    from ssp.ephem_assist import open_ephem

    client = J.JPLClient(args.cache, offline=True, budget=BUDGET)
    plan = Plan(client, args.sssource)
    ephem = open_ephem(os.environ.get("SSP_ASSIST_PLANETS"), os.environ.get("SSP_ASSIST_ASTEROIDS"))
    parts, notes = [], ""
    for d, _, kind, _ in plan.objects:
        try:
            r, n = compare_object(plan, d, kind, ephem)
        except J.NotCachedError as e:
            print(f"skip {d}: {e}")
            continue
        parts.append(r)
        notes = notes or n
    if not parts:
        print("nothing cached; run fetch first")
        return 1
    rows = pd.concat(parts, ignore_index=True)
    rms, best = frame_verdict(rows)
    summ = summarize(rows, "icrs")
    # The pass: our ICRS angles against Horizons' moved to the ICRS pole
    # (from the frame it was found to use; 'icrs' = unchanged).
    if best == "icrs":
        hz_a, hz_m = rows.psang_hz, rows.psamv_hz
    else:
        hz_a, hz_m = rows.psang_hz_icrs, rows.psamv_hz_icrs
        if best == "mean":  # (not expected) correct by the mean-of-date rotation instead
            hz_a = (rows.psang_hz - dangle(rows.psang_mean, rows.psang_icrs)) % 360
            hz_m = (rows.psamv_hz - dangle(rows.psamv_mean, rows.psamv_icrs)) % 360
    rows["res_psang"] = dangle(rows.psang_icrs, hz_a)
    rows["res_psamv"] = dangle(rows.psamv_icrs, hz_m)
    far = rows.phase_hz >= PSANG_MIN_PHASE_DEG
    ok_a = (np.abs(rows.res_psang[far]) < PASS_DEG).all()
    ok_m = (np.abs(rows.res_psamv) < PASS_DEG).all()
    passed = bool(ok_a and ok_m and np.isfinite(rows.res_psamv).all())
    os.makedirs(args.out, exist_ok=True)
    rows.to_csv(os.path.join(args.out, "tail_angles_jpl_rows.csv"), index=False)
    text = jpl_report(rows, summ, rms, best, notes, passed, client.n_logged())
    with open(os.path.join(args.out, "tail_angles_jpl_report.txt"), "w") as f:
        f.write(text)
    print(text)
    return 0 if passed else 1


def jpl_report(rows, summ, rms, best, notes, passed, n_req):
    import pandas as pd

    L = []
    L.append("Tail position angles against JPL Horizons quantity 27 (PsAng, PsAMV), X05, Rubin times")
    L.append(
        f"objects {rows.designation.nunique()}, rows {len(rows)}, "
        f"JPL requests logged {n_req} (budget {BUDGET})"
    )
    L.append("")
    L.append("Horizons' explanation of quantity 27 (from its answer):")
    L.extend("  " + ln for ln in notes.splitlines())
    L.append("")
    L.append(
        f"Frame: RMS of (ours - Horizons) [deg] over PsAMV and PsAng at phase >= {PSANG_MIN_PHASE_DEG} deg:"
    )
    for fr in FRAMES:
        L.append(f"  {fr:5s} {rms[fr]:.5f}")
    L.append(f"  -> Horizons' pole: {best} ('icrs' ICRF; 'mean'/'true' the mean/true equator of date)")
    pr = np.abs(rows.pole_rot_psang.to_numpy())
    L.append(
        f"  ICRS vs true-of-date pole: PA differs by |rot| median {np.median(pr):.4f}, max {pr.max():.4f} deg"
    )
    L.append("")
    L.append(
        f"Residuals, ours (ICRS) - Horizons (moved to the ICRS pole) [deg]; PASS < {PASS_DEG}, "
        f"PsAng only at phase >= {PSANG_MIN_PHASE_DEG} deg"
    )
    s = []
    for d, g in rows.groupby("designation", sort=False):
        far = g.phase_hz >= PSANG_MIN_PHASE_DEG
        s.append(
            {
                "designation": d,
                "kind": g.kind.iloc[0],
                "solution": g.source.iloc[0],
                "n": len(g),
                "phase": f"{g.phase_hz.min():.2f}-{g.phase_hz.max():.2f}",
                "radec_mas": g.radec_mas.max(),
                "PsAng_max": np.abs(g.res_psang[far]).max() if far.any() else np.nan,
                "PsAng_lowphase(n)": (
                    f"{np.abs(g.res_psang[~far]).max():.4f} ({(~far).sum()})" if (~far).any() else "-"
                ),
                "PsAMV_max": np.abs(g.res_psamv).max(),
                "pole_rot_max": np.abs(g.pole_rot_psang).max(),
            }
        )
    with pd.option_context("display.width", 250, "display.max_columns", 30):
        L.append(pd.DataFrame(s).to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    L.append("")
    L.append("Residuals vs phase angle (all objects):")
    bins = [0, 0.5, 1, 2, 5, 10, 20, 40, 90]
    cut = pd.cut(rows.phase_hz, bins)
    t = rows.groupby(cut, observed=True).agg(
        n=("res_psang", "size"),
        psang_max=("res_psang", lambda x: np.abs(x).max()),
        psang_rms=("res_psang", lambda x: np.sqrt(np.mean(x**2))),
        psamv_max=("res_psamv", lambda x: np.abs(x).max()),
        sky_frac_sun_min=("sky_frac_sun", "min"),
    )
    L.append(t.to_string(float_format=lambda x: f"{x:.5f}"))
    L.append("")
    L.append("Horizons prints PsAng/PsAMV to 0.001 deg; residuals below ~0.0005 deg are its rounding.")
    L.append(f"RESULT: {'PASS' if passed else 'FAIL'}")
    return "\n".join(L) + "\n"


# ---------------------------------------------------------------------------
# consistency: SSSource and NearbySSO
# ---------------------------------------------------------------------------

PA_COLS = ("ephAntiSunPA", "ephAntiMotionPA")
#: float32 unit roundoff (half an ulp, relative).
EPS32 = 2.0**-24
#: The recomputation tolerance's floor [deg] (see recompute_tolerance).
RECOMP_FLOOR_DEG = 1e-3
#: Safety factor on the float32 error bound.
RECOMP_SAFETY = 4.0
#: SSSource vs NearbySSO: a pair that isn't bitwise equal must agree
#: within max(1 float32 ulp, this) [deg].
PAIR_TOL_DEG = 1e-4


def recompute_tolerance(sky_frac, dec_deg):
    """Per-row tolerance [deg] of the recomputation from float32 columns.

    Each float32 component carries a relative rounding error <= EPS32 of
    the vector's length, so the direction of w (helio_* or -helio_v*) is
    off by <= sqrt(3) EPS32 rad, and its PA, after projection, by that over
    the sky fraction |w_sky| / |w| (which -> 0 near opposition for the
    anti-Sun vector). The direction of u (topo_*) is off by the same, which
    turns the local north by <= sqrt(3) EPS32 (1 + |tan dec|) rad. The
    stored PA is itself float32 (half an ulp <= 2^-16 deg below 512).
    Bound = sqrt(3) EPS32 (1 / sky_frac + 1 + |tan dec|) [rad] + 2^-16 deg;
    tolerance = max(RECOMP_FLOOR_DEG, RECOMP_SAFETY x bound). Away from
    opposition and the poles the bound is ~1e-5 deg, so the 1e-3 deg floor
    is ~100x looser than float32 needs while still ~10x tighter than the
    JPL pass (0.01 deg) and far tighter than any wrong convention (a sign,
    a swapped basis vector, a velocity at the wrong time or the wrong pole:
    >= 0.1 deg)."""
    with np.errstate(divide="ignore"):
        b = np.sqrt(3.0) * EPS32 * (1.0 / sky_frac + 1.0 + np.abs(np.tan(np.radians(dec_deg))))
    return np.maximum(RECOMP_FLOOR_DEG, RECOMP_SAFETY * (np.degrees(b) + 2.0**-16))


def _f(t, name):
    """A float column as float64 numpy with NULL -> NaN."""
    return t.column(name).to_numpy(zero_copy_only=False).astype(np.float64)


def check_angle_columns(pa_sun, pa_mot, has_orbit, label):
    """[(ok, message)]: non-null exactly where there's an orbit; in
    [0, 360)."""
    out = []
    for name, v in zip(PA_COLS, (pa_sun, pa_mot)):
        fin = np.isfinite(v)
        miss = int(np.sum(has_orbit & ~fin))
        extra = int(np.sum(~has_orbit & ~np.isnan(v)))
        out.append(
            (miss == 0, f"{label}.{name}: null/NaN on {miss} of {int(has_orbit.sum())} rows with an orbit")
        )
        out.append((extra == 0, f"{label}.{name}: non-null on {extra} rows without an orbit"))
        bad = fin & ~((v >= 0.0) & (v < 360.0))
        msg = f"{label}.{name}: {int(bad.sum())} values outside [0, 360)"
        if bad.any():
            msg += f" (e.g. {v[bad][:5].tolist()})"
        out.append((not bad.any(), msg))
    return out


def _dia_times(dia_sources, ids):
    """{diaSourceId: midpointMjdTai} of the DiaSource input, for ``ids``."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(dia_sources)
    want = pa.array(np.unique(np.asarray(ids, np.int64)))
    parts = []
    for rg in range(pf.num_row_groups):
        t = pf.read_row_group(rg, columns=["diaSourceId", "midpointMjdTai"])
        t = t.filter(pc.is_in(t.column("diaSourceId"), value_set=want))
        if t.num_rows:
            parts.append(t)
    if not parts:
        return {}
    t = pa.concat_tables(parts)
    return dict(zip(t.column("diaSourceId").to_numpy(), t.column("midpointMjdTai").to_numpy()))


def consistency(sssource, nearbysso, dia_sources=None):
    """(passed, report lines) of the SSSource/NearbySSO angle checks.
    ``dia_sources``: the NearbySSO input, for the time-shift allowance
    (bench/time_shift.py); None holds every pair to the strict rule."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    from bench import time_shift as TS

    L, checks = [], []
    cols = ["designation", "diaSourceId", "ephRa", "ephDec", "phaseAngle", *PA_COLS]
    cols += [f"helio_{c}" for c in ("x", "y", "z", "vx", "vy", "vz")] + [f"topo_{c}" for c in "xyz"]
    present = set(pq.read_schema(sssource).names)
    cols += [c for c in ("midpointMjdTai", "ephRate", "ephRateRa", "ephRateDec") if c in present]
    s = pq.read_table(sssource, columns=cols)
    s = s.append_column("_row", pa.array(np.arange(s.num_rows, dtype=np.int64)))
    n = pq.read_table(nearbysso, columns=["designation", "diaSourceId", "ephRa", *PA_COLS])
    for name, t in (("sssource", s), ("nearbysso", n)):
        for c in PA_COLS:
            ty = t.schema.field(c).type
            checks.append((str(ty) == "float", f"{name}.{c}: type {ty} (float32 expected)"))
    L.append(f"SSSource {s.num_rows:,} rows; NearbySSO {n.num_rows:,} rows")

    # 1. NULL rules and range
    s_orbit = s.column("ephRa").is_valid().to_numpy(zero_copy_only=False)
    n_orbit = n.column("ephRa").is_valid().to_numpy(zero_copy_only=False)
    s_sun, s_mot = _f(s, PA_COLS[0]), _f(s, PA_COLS[1])
    checks += check_angle_columns(s_sun, s_mot, s_orbit, "sssource")
    checks += check_angle_columns(_f(n, PA_COLS[0]), _f(n, PA_COLS[1]), n_orbit, "nearbysso")
    checks.append((bool(n_orbit.all()), f"nearbysso: {int((~n_orbit).sum())} rows without ephRa"))

    # 2. agreement at the same (designation, diaSourceId): bitwise, or
    #    within max(1 float32 ulp, PAIR_TOL_DEG)
    def keyed(t, prefix):
        t = t.filter(pc.is_valid(t.column("diaSourceId")))
        keep = ["designation", "diaSourceId", *PA_COLS] + (["_row"] if "_row" in t.column_names else [])
        df = t.select(keep).to_pandas()
        for c in PA_COLS:  # float32 bit patterns; NULL -> a sentinel
            v = t.column(c)
            bits = (
                v.fill_null(np.float32(np.nan))
                .to_numpy(zero_copy_only=False)
                .astype(np.float32)
                .view(np.uint32)
            )
            df[prefix + c] = np.where(v.is_valid().to_numpy(zero_copy_only=False), bits.astype(np.int64), -1)
            del df[c]
        return df

    a, b = keyed(s, "s_"), keyed(n, "n_")
    for name, df in (("sssource", a), ("nearbysso", b)):
        dup = int(df.duplicated(["designation", "diaSourceId"]).sum())
        L.append(f"{name}: {dup} repeated (designation, diaSourceId) keys")
    m = a.merge(b, on=["designation", "diaSourceId"], how="inner")
    unmatched = int(
        len(b)
        - b.set_index(["designation", "diaSourceId"])
        .index.isin(a.set_index(["designation", "diaSourceId"]).index)
        .sum()
    )
    L.append(
        f"matched (designation, diaSourceId): {len(m):,} rows; NearbySSO rows without an SSSource match: "
        f"{unmatched:,} (expected: NearbySSO also covers unattributed DiaSources)"
    )
    checks.append((len(m) > 0, f"matched rows: {len(m):,} (> 0 expected)"))

    # the time shift of each pair (docs/design/shutter-timing.md): SSSource
    # at its midpointMjdTai, NearbySSO at its DiaSource's; 0 -> strict
    row = m["_row"].to_numpy()
    dt = np.zeros(len(m))
    if dia_sources is not None and "midpointMjdTai" in present:
        tmap = _dia_times(dia_sources, m["diaSourceId"].to_numpy())
        t_dia = np.array([tmap.get(i, np.nan) for i in m["diaSourceId"].to_numpy()], np.float64)
        dt = TS.dt_days(_f(s, "midpointMjdTai")[row], t_dia)
        missing = int(np.isnan(t_dia).sum())
        L.append(TS.summary_line(dt) + (f"; {missing:,} pairs without their DiaSource in {dia_sources} "
                                        "(strict)" if missing else ""))
        big = TS.too_large(dt)
        checks.append((not big.any(), f"time shift: {int(big.sum())} pairs with |dt| > {TS.DT_MAX_S} s"))
    else:
        L.append("time shift: not computed (no --dia-sources, or no SSSource midpointMjdTai); every pair "
                 "held to the strict rule")
    if "ephRate" in present:
        rate = _f(s, "ephRate")
    elif "ephRateRa" in present:
        rate = TS.rate_deg_day(_f(s, "ephRateRa"), _f(s, "ephRateDec"))
    else:
        rate = np.full(s.num_rows, np.nan)
    hp_all = np.array([_f(s, f"helio_{c}") for c in "xyz"])
    hv_all = np.array([_f(s, f"helio_v{c}") for c in "xyz"])
    tp_all = np.array([_f(s, f"topo_{c}") for c in "xyz"])
    with np.errstate(invalid="ignore", divide="ignore"):
        shift_tol = {
            PA_COLS[0]: TS.pa_allowance_deg(dt, rate[row], sky_fraction(hp_all[:, row], tp_all[:, row]),
                                            _f(s, "ephDec")[row],
                                            TS.anti_sun_direction_rate(hp_all[:, row], hv_all[:, row])),
            PA_COLS[1]: TS.pa_allowance_deg(dt, rate[row], sky_fraction(hv_all[:, row], tp_all[:, row]),
                                            _f(s, "ephDec")[row],
                                            TS.anti_motion_direction_rate(hp_all[:, row], hv_all[:, row])),
        }
    shifted = dt != 0
    for c in PA_COLS:
        sb, nb = m[f"s_{c}"].to_numpy(), m[f"n_{c}"].to_numpy()
        diff = sb != nb
        # The two tables' integrations may differ at the ~1e-11 deg level
        # (integrator noise between the two passes), which can move a
        # float32 value across a rounding boundary: a differing pair passes
        # within one float32 ulp or PAIR_TOL_DEG, whichever is larger.
        sv = np.where(sb >= 0, sb, 0).astype(np.uint32).view(np.float32).astype(np.float64)
        nv = np.where(nb >= 0, nb, 0).astype(np.uint32).view(np.float32).astype(np.float64)
        both = (sb >= 0) & (nb >= 0)
        sv, nv = np.where(both, sv, np.nan), np.where(both, nv, np.nan)
        ulp = np.spacing(np.maximum(np.abs(sv), np.abs(nv)).astype(np.float32)).astype(np.float64)
        close = both & (np.abs(dangle(sv, nv)) <= np.maximum(ulp, PAIR_TOL_DEG) + shift_tol[c])
        bad = diff & ~close
        dd = np.abs(dangle(sv, nv))[diff & both]
        dmax = float(dd.max()) if len(dd) else 0.0
        L.append(
            f"{c}: {int((~diff).sum()):,} of {len(m):,} matched pairs bitwise equal, "
            f"{int(diff.sum()):,} not (max |diff| {dmax:.2e} deg)"
        )
        if shifted.any():
            ds = np.abs(dangle(sv, nv))[shifted & both]
            L.append(
                f"{c}: at a nonzero time shift ({int(shifted.sum()):,} pairs) max |diff| "
                f"{(float(ds.max()) if len(ds) else 0.0):.2e} deg; allowance median "
                f"{float(np.median(shift_tol[c][shifted])):.2e} deg"
            )
        msg = (
            f"pairs {c}: {int(bad.sum())} of {len(m):,} matched rows differ by more than "
            f"max(1 float32 ulp, {PAIR_TOL_DEG} deg) (+ the time-shift allowance where dt != 0) "
            f"or in NULL-ness ({int(diff.sum())} not bitwise equal)"
        )
        if bad.any():
            k = np.flatnonzero(bad)[:3]
            msg += f" (e.g. SSSource {sv[k].tolist()} vs NearbySSO {nv[k].tolist()})"
        checks.append((not bad.any(), msg))

    # 3. recomputation from SSSource's own float32 vectors
    hp = np.array([_f(s, f"helio_{c}") for c in "xyz"])
    hv = np.array([_f(s, f"helio_v{c}") for c in "xyz"])
    tp = np.array([_f(s, f"topo_{c}") for c in "xyz"])
    dec = _f(s, "ephDec")
    use = s_orbit & np.isfinite(hp).all(0) & np.isfinite(hv).all(0) & np.isfinite(tp).all(0)
    checks.append(
        (
            bool((use == s_orbit).all()),
            f"sssource: {int((s_orbit & ~use).sum())} rows with an orbit but a NULL helio_/topo_ column",
        )
    )
    sun, mot = tail_angles(hp[:, use], hv[:, use], tp[:, use])
    for c, ours, theirs, frac in (
        (PA_COLS[0], sun, s_sun[use], sky_fraction(hp[:, use], tp[:, use])),
        (PA_COLS[1], mot, s_mot[use], sky_fraction(hv[:, use], tp[:, use])),
    ):
        tol = recompute_tolerance(frac, dec[use])
        r = np.abs(dangle(ours, theirs))
        bad = ~(r <= tol)
        q = np.nanpercentile(r, [50, 99, 100]) if len(r) else [np.nan] * 3
        worst = float(np.nanmax(r / tol)) if len(r) else np.nan
        msg = (
            f"recomputed {c}: {int(bad.sum())} of {len(r):,} rows beyond tolerance; |diff| median "
            f"{q[0]:.2e}, p99 {q[1]:.2e}, max {q[2]:.2e} deg; max |diff|/tolerance {worst:.3f}; "
            f"tolerance floor {RECOMP_FLOOR_DEG} deg, {int((tol > RECOMP_FLOOR_DEG).sum())} rows above "
            "it (near opposition)"
        )
        if bad.any():
            k = np.flatnonzero(bad)[:3]
            msg += f"; e.g. ours {ours[k].tolist()} vs table {theirs[k].tolist()}"
        checks.append((not bad.any(), msg))
    phase = _f(s, "phaseAngle")[use]
    low = phase < PSANG_MIN_PHASE_DEG
    L.append(f"rows at phase < {PSANG_MIN_PHASE_DEG} deg (ephAntiSunPA ill-conditioned): {int(low.sum()):,}")

    L.append("")
    for ok, msg in checks:
        L.append(("PASS  " if ok else "FAIL  ") + msg)
    passed = all(ok for ok, _ in checks)
    L.append(f"RESULT: {'PASS' if passed else 'FAIL'}")
    return passed, L


def cmd_consistency(args):
    passed, lines = consistency(args.sssource_file, args.nearbysso_file, args.dia_sources)
    text = "\n".join(lines) + "\n"
    print(text)
    if args.report:
        with open(args.report, "w") as f:
            f.write(text)
    return 0 if passed else 1


def cmd_status(args):
    client = J.JPLClient(args.cache, offline=True, budget=BUDGET)
    plan = Plan(client, args.sssource)
    for service, params, label in plan.requests():
        print(
            f"{'cached ' if client.cached(service, params, label) else 'missing'}  {label}  "
            f"({len(params['TLIST'].split(','))} times)"
        )
    print(f"requests logged: {client.n_logged()} (budget {BUDGET})")
    return 0


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main(argv=None):
    ap = argparse.ArgumentParser(
        prog="python -m bench.tail_angles_validate",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--cache", default=CACHE)
    ap.add_argument(
        "--sssource", default=REF_SSSOURCE, help="the SSSource giving the Rubin times (fetch, jpl)"
    )
    ap.add_argument("--out", default=WORK, help="report directory (jpl)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch")
    f.add_argument("--dry-run", action="store_true")
    f.add_argument("--limit", type=int, default=None, help="send at most this many requests")
    f.add_argument("--only", nargs="*", help="only these designations")
    f.add_argument("--show-url", action="store_true")
    sub.add_parser("status")
    sub.add_parser("jpl")
    c = sub.add_parser("consistency", help="SSSource vs NearbySSO angles, no network")
    c.add_argument("sssource_file")
    c.add_argument("nearbysso_file")
    c.add_argument("--report", help="also write the report to this file")
    c.add_argument("--dia-sources", help="the NearbySSO input (ppdb_dia_sources.parquet): the DiaSource "
                                         "times, for the shutter-correction time-shift allowance")
    args = ap.parse_args(argv)
    return {"fetch": cmd_fetch, "status": cmd_status, "jpl": cmd_jpl, "consistency": cmd_consistency}[
        args.cmd
    ](args)


if __name__ == "__main__":
    sys.exit(main())
