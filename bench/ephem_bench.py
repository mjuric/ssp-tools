"""Benchmark and accuracy gate: ASSIST vs jorbit vs JPL Horizons.

Three sections, each independently runnable:

  1. Accuracy gate (ASSIST vs Horizons, same elements). Pass criterion:
     <1 mas RMS great-circle separation.

  2. Drop-in equivalence (ASSIST vs current ssp.ephem.jorbit path). Just
     reports residuals; does not gate on them.

  3. Performance (jorbit vs ASSIST vs two-body Kepler). Reports object·epoch
     throughput end-to-end including the ~500 ms ASSIST cold start.

Run with --help for options. Sensible defaults:

  python -m bench.ephem_bench \\
      --mpcorb analysis/inputs/mpc_orbits.parquet \\
      --planets-path /data/linux_p1550p2650.440 \\
      --asteroids-path /data/sb441-n16.bsp \\
      --n-objects 10 --n-epochs 30 --section all
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
from astropy.time import Time
import astropy.units as u

from ssp.ephem_assist import (
    GM_SUN,
    compute_ephemerides_one,
    elements_row_to_bary_icrf,
    kepler_to_helio_ecliptic,
    ecliptic_to_equatorial,
    _light_time_correct,
    _vector_to_radec,
    MJD_J2000,
)
from ssp import util


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@dataclass
class Residuals:
    method: str
    n: int
    ra_arcsec: np.ndarray = field(repr=False)   # (RA·cos(dec)) residual
    dec_arcsec: np.ndarray = field(repr=False)
    sep_arcsec: np.ndarray = field(repr=False)

    def summarize(self) -> str:
        def stats(x, units):
            return (
                f"min={np.min(x):+.3e}, "
                f"med={np.median(x):+.3e}, "
                f"rms={np.sqrt(np.mean(x ** 2)):.3e}, "
                f"max={np.max(np.abs(x)):.3e} {units}"
            )

        return (
            f"[{self.method}] N={self.n}\n"
            f"  Δ(RA·cosδ): {stats(self.ra_arcsec, 'arcsec')}\n"
            f"  ΔDec:       {stats(self.dec_arcsec, 'arcsec')}\n"
            f"  separation: {stats(self.sep_arcsec, 'arcsec')}"
        )


def angular_residual(ra1, dec1, ra2, dec2):
    """Return (Δ(RA·cosδ), ΔDec, great-circle separation), all in arcsec."""
    cosd = np.cos(np.deg2rad(dec1))
    dra = (((ra2 - ra1) + 540.0) % 360.0) - 180.0
    dra_arcsec = dra * cosd * 3600.0
    ddec_arcsec = (dec2 - dec1) * 3600.0
    # Vincenty-style great-circle formula
    phi1 = np.deg2rad(dec1)
    phi2 = np.deg2rad(dec2)
    dphi = phi2 - phi1
    dlam = np.deg2rad(((ra2 - ra1 + 540.0) % 360.0) - 180.0)
    a = np.sin(dphi / 2.0) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlam / 2.0) ** 2
    sep_arcsec = 2.0 * np.arcsin(np.sqrt(np.clip(a, 0, 1))) * 206264.80624709636
    return dra_arcsec, ddec_arcsec, sep_arcsec


def pick_random_objects(mpcorb: pd.DataFrame, n: int, rng) -> pd.DataFrame:
    """Sample n objects from mpcorb, preferring well-conditioned orbits."""
    keep = (
        (mpcorb["e"] < 0.85)
        & (mpcorb["a"] > 0.5) & (mpcorb["a"] < 50.0)
        & mpcorb["epoch_mjd"].notna()
    )
    pool = mpcorb[keep]
    if len(pool) < n:
        n = len(pool)
    idx = rng.choice(len(pool), size=n, replace=False)
    return pool.iloc[idx].reset_index(drop=True)


def make_schedule(rows: pd.DataFrame, n_epochs: int, rng,
                  centre_mjd: float = 60500.0,
                  span_days: float = 30.0) -> dict:
    """For each row, generate n_epochs random TAI-MJD observation times in a
    centre±span/2 window. Returns {provID: astropy.Time}.
    """
    out = {}
    for _, row in rows.iterrows():
        t = centre_mjd + (rng.random(n_epochs) - 0.5) * span_days
        out[row["unpacked_primary_provisional_designation"]] = Time(
            np.sort(t), format="mjd", scale="tai",
        )
    return out


# ---------------------------------------------------------------------------
# Method 1: ASSIST (ssp.ephem_assist)
# ---------------------------------------------------------------------------

def run_assist(rows, schedule, ephem):
    """Runs the ASSIST batch. Returns dict provID -> {ra,dec,t_mjd_tai}."""
    out = {}
    mpcorb = rows.set_index("unpacked_primary_provisional_designation", drop=False)
    for provID, eph_times in schedule.items():
        res = compute_ephemerides_one(provID, eph_times, mpcorb, ephem)
        out[provID] = {
            "ra_deg": res.ra_deg,
            "dec_deg": res.dec_deg,
            "t_mjd_tai": eph_times.tai.mjd,
        }
    return out


# ---------------------------------------------------------------------------
# Method 2: jorbit (current ssp.ephem._aux_compute_ephemerides)
# ---------------------------------------------------------------------------

def run_jorbit(rows, schedule):
    """Runs the jorbit path the way ssp.ephem._aux_compute_ephemerides does:
    fetch the state from JPL Horizons by packed designation, then compute the
    ephemeris at the requested times plus a second call at t+dt for rates.

    Calls jorbit directly rather than through _aux_compute_ephemerides, whose
    ``eph, xx, vv, obs = p.ephemeris(...)`` unpacking does not match any
    released jorbit (1.0-1.7 all return a bare SkyCoord).
    """
    from ssp import ephem as _ephem  # noqa: F401  (enables jax x64)
    from jorbit import Particle

    by_id = rows.set_index("unpacked_primary_provisional_designation", drop=False)
    out = {}
    dt = 1.0 / (3600.0 + 24.0) * u.s
    for provID, eph_times in schedule.items():
        row = by_id.loc[provID]
        p = Particle.from_horizons(
            name=row["packed_primary_provisional_designation"],
            time=Time(float(row["epoch_mjd"]), format="mjd", scale="tdb"),
        )
        eph = p.ephemeris(times=eph_times, observer="rubin")
        p.ephemeris(times=eph_times + dt, observer="rubin")
        out[provID] = {
            "ra_deg": np.asarray(eph.ra.deg),
            "dec_deg": np.asarray(eph.dec.deg),
            "t_mjd_tai": eph_times.tai.mjd,
        }
    return out


# ---------------------------------------------------------------------------
# Method 3: pure two-body Kepler (control)
# ---------------------------------------------------------------------------

def run_two_body(rows, schedule):
    """Pure Keplerian propagation, no perturbations. Coordinates: ICRF
    barycentric, with the Sun's barycentric position assumed = 0 (ie
    treat heliocentric as barycentric — a deliberate simplification for
    a worst-case control measurement).
    """
    out = {}
    for _, row in rows.iterrows():
        provID = row["unpacked_primary_provisional_designation"]
        eph_times = schedule[provID]
        epoch_tt_mjd = float(row["epoch_mjd"])
        epoch_tdb_mjd = Time(epoch_tt_mjd, format="mjd", scale="tt").tdb.mjd
        t_tdb_mjd = eph_times.tdb.mjd

        # Mean motion in rad/day
        a = float(row["a"])
        e = float(row["e"])
        n_rad_day = np.sqrt(GM_SUN / a ** 3)

        M0 = np.deg2rad(float(row["mean_anomaly"]))
        inc = np.deg2rad(float(row["i"]))
        Om = np.deg2rad(float(row["node"]))
        om = np.deg2rad(float(row["argperi"]))

        ras = np.empty(len(t_tdb_mjd))
        decs = np.empty(len(t_tdb_mjd))

        # Observer (Rubin) barycentric position
        r_obs_q, _ = util.observatory_barycentric_posvel("X05", eph_times)
        r_obs = r_obs_q.to(u.au).value  # (3, N)

        for k in range(len(t_tdb_mjd)):
            dt = t_tdb_mjd[k] - epoch_tdb_mjd
            M = M0 + n_rad_day * dt
            X_ecl, V_ecl = kepler_to_helio_ecliptic(a, e, inc, Om, om, M)
            X_eq = ecliptic_to_equatorial(X_ecl)
            V_eq = ecliptic_to_equatorial(V_ecl)
            # Take Sun = barycenter (worst-case control)
            sun_pos = np.zeros((3, 1))
            X_t = X_eq.reshape(3, 1)
            V_t = V_eq.reshape(3, 1)
            r_obs_k = r_obs[:, k:k + 1]
            rho = _light_time_correct(X_t, V_t, sun_pos, r_obs_k)
            ra, dec = _vector_to_radec(rho)
            ras[k] = float(ra[0])
            decs[k] = float(dec[0])

        out[provID] = {
            "ra_deg": ras,
            "dec_deg": decs,
            "t_mjd_tai": eph_times.tai.mjd,
        }
    return out


# ---------------------------------------------------------------------------
# JPL Horizons "ground truth"
# ---------------------------------------------------------------------------

HORIZONS_URL = "https://ssd.jpl.nasa.gov/api/horizons.api"


def horizons_ephem(row, eph_times: Time, observer_code: str = "X05"):
    """Query Horizons for astrometric ICRF RA/Dec at the requested UTC
    epochs, using the row's *own* osculating elements (so the only thing
    being compared between Horizons and ASSIST is the propagator).

    extra_prec=YES is required to get arcsec → microarcsec precision.

    Returns (ra_deg, dec_deg) arrays aligned with eph_times.
    """
    epoch_tt_mjd = float(row["epoch_mjd"])
    epoch_tdb_jd = Time(epoch_tt_mjd, format="mjd", scale="tt").tdb.jd

    # User-supplied heliocentric ecliptic elements need COMMAND=';'. Horizons
    # accepts [TP, QR], [MA, A] or [MA, N] pairs; we pass [MA, A]. EPOCH is
    # JD TDB. ECLIP=J2000 means the IAU76/80 obliquity (84381.448").
    a = float(row["a"])
    e = float(row["e"])

    tlist = ",".join(f"{t:.10f}" for t in eph_times.utc.jd)

    params = {
        "format": "text",
        "EPHEM_TYPE": "OBSERVER",
        "OBJ_DATA": "NO",
        "MAKE_EPHEM": "YES",
        "COMMAND": "';'",
        "OBJECT": "Test",
        "ECLIP": "J2000",
        "EC": f"{e:.16e}",
        "A": f"{a:.16e}",
        "IN": f"{float(row['i']):.16e}",
        "OM": f"{float(row['node']):.16e}",
        "W":  f"{float(row['argperi']):.16e}",
        "MA": f"{float(row['mean_anomaly']):.16e}",
        "EPOCH": f"{epoch_tdb_jd:.10f}",
        "CENTER": f"'{observer_code}'",
        "TLIST": tlist,
        "TIME_TYPE": "UT",
        "QUANTITIES": "1",         # 1 = astrometric RA & Dec
        "ANG_FORMAT": "DEG",
        "extra_prec": "YES",
        "CSV_FORMAT": "YES",
        "REF_PLANE": "FRAME",
        "REF_SYSTEM": "ICRF",
    }
    qs = "&".join(f"{k}={urllib.parse.quote(str(v))}" for k, v in params.items())
    url = HORIZONS_URL + "?" + qs

    req = urllib.request.Request(url, headers={"User-Agent": "ssp-tools-bench/0.1"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = resp.read().decode("utf-8", errors="replace")

    # Parse the CSV between $$SOE and $$EOE
    try:
        soe = body.index("$$SOE")
        eoe = body.index("$$EOE")
    except ValueError:
        raise RuntimeError(
            "Horizons response did not contain $$SOE/$$EOE markers:\n"
            + body[:1000]
        )
    rows_text = [r for r in body[soe + 5:eoe].splitlines() if r.strip()]

    ras = np.empty(len(eph_times))
    decs = np.empty(len(eph_times))
    for i, line in enumerate(rows_text):
        cols = [c.strip() for c in line.split(",")]
        # Columns: Date, , RA, Dec  (with extra_prec the angular precision
        # is high). Schema: "Date_UT", "Sol_pres", "RA", "Dec".
        # Find the two rightmost numeric columns ending with RA, Dec.
        # Robust: take the last two columns with valid floats.
        nums = [c for c in cols if _is_float(c)]
        ras[i] = float(nums[-2])
        decs[i] = float(nums[-1])
    return ras, decs


def _is_float(s: str) -> bool:
    try:
        float(s)
        return True
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# Main: glue + reporting
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        description="Benchmark ASSIST vs jorbit vs two-body for ssp ephemerides.",
    )
    p.add_argument("--mpcorb", required=True,
                   help="Path to mpc_orbits parquet file.")
    p.add_argument("--planets-path", default=None,
                   help="JPL DE440/DE441 planet ephemeris (default: /data/linux_p1550p2650.440 inside ASSIST).")
    p.add_argument("--asteroids-path", default=None,
                   help="ASSIST sb441-n16 asteroid file.")
    p.add_argument("--n-objects", type=int, default=10)
    p.add_argument("--n-epochs", type=int, default=30)
    p.add_argument("--centre-mjd", type=float, default=60500.0,
                   help="Centre TAI-MJD of the synthetic obs window.")
    p.add_argument("--span-days", type=float, default=30.0)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--section", choices=["accuracy", "drop-in", "perf", "all"],
                   default="all")
    p.add_argument("--horizons", action="store_true",
                   help="Hit the JPL Horizons web API (needed for the accuracy section).")
    p.add_argument("--report-json", default=None,
                   help="If set, dump residual/timing summary as JSON.")
    args = p.parse_args()

    rng = np.random.default_rng(args.seed)
    print(f"Loading mpcorb from {args.mpcorb} ...", flush=True)
    mpcorb = pd.read_parquet(args.mpcorb)
    print(f"  {len(mpcorb):,} orbits available", flush=True)

    rows = pick_random_objects(mpcorb, args.n_objects, rng)
    print(f"Sampled {len(rows)} objects", flush=True)

    schedule = make_schedule(rows, args.n_epochs, rng,
                             centre_mjd=args.centre_mjd, span_days=args.span_days)

    print("Initialising ASSIST ephemeris (one-time) ...", flush=True)
    from ssp.ephem_assist import _open_ephem
    t0 = time.perf_counter()
    ephem = _open_ephem(args.planets_path, args.asteroids_path)
    cold_start = time.perf_counter() - t0
    print(f"  cold start: {cold_start * 1000:.1f} ms", flush=True)

    summary = {"args": vars(args), "cold_start_sec": cold_start}

    # ---- Accuracy gate ---------------------------------------------------
    if args.section in ("accuracy", "all"):
        print("\n=== ACCURACY GATE: ASSIST vs JPL Horizons (same elements) ===")
        if not args.horizons:
            print("  --horizons not set; skipping Horizons fetch.")
        else:
            t0 = time.perf_counter()
            assist_out = run_assist(rows, schedule, ephem)
            t_assist = time.perf_counter() - t0

            ra_resid_all, dec_resid_all, sep_all = [], [], []
            n_good = 0
            for _, row in rows.iterrows():
                pid = row["unpacked_primary_provisional_designation"]
                eph_times = schedule[pid]
                try:
                    ra_h, dec_h = horizons_ephem(row, eph_times)
                except Exception as exc:
                    print(f"  Horizons fetch failed for {pid}: {exc}")
                    continue
                a = assist_out[pid]
                dra, dde, sep = angular_residual(
                    ra_h, dec_h, a["ra_deg"], a["dec_deg"]
                )
                ra_resid_all.append(dra)
                dec_resid_all.append(dde)
                sep_all.append(sep)
                n_good += 1

            if n_good > 0:
                ra_resid_all = np.concatenate(ra_resid_all)
                dec_resid_all = np.concatenate(dec_resid_all)
                sep_all = np.concatenate(sep_all)
                r = Residuals(
                    "ASSIST vs Horizons", n_good * args.n_epochs,
                    ra_resid_all, dec_resid_all, sep_all,
                )
                print(r.summarize())
                rms_arcsec = float(np.sqrt(np.mean(sep_all ** 2)))
                rms_mas = rms_arcsec * 1000.0
                gate_ok = rms_mas < 1.0
                print(
                    f"  Gate: separation RMS = {rms_mas:.4f} mas "
                    f"({'PASS' if gate_ok else 'FAIL'}; target <1 mas)"
                )
                summary["accuracy"] = {
                    "n_obj": n_good,
                    "n_total": int(n_good * args.n_epochs),
                    "rms_sep_mas": rms_mas,
                    "max_sep_arcsec": float(np.max(sep_all)),
                    "rms_ra_arcsec": float(np.sqrt(np.mean(ra_resid_all ** 2))),
                    "rms_dec_arcsec": float(np.sqrt(np.mean(dec_resid_all ** 2))),
                    "passed": gate_ok,
                    "assist_seconds": t_assist,
                }

    # ---- Drop-in equivalence (ASSIST vs jorbit) --------------------------
    if args.section in ("drop-in", "all"):
        print("\n=== DROP-IN: ASSIST vs current jorbit path ===")
        try:
            t0 = time.perf_counter()
            jorbit_out = run_jorbit(rows, schedule)
            t_j = time.perf_counter() - t0
        except Exception as exc:
            print(f"  jorbit run failed: {exc}")
            jorbit_out = None
            t_j = None
        if jorbit_out is not None:
            t0 = time.perf_counter()
            assist_out = run_assist(rows, schedule, ephem)
            t_a = time.perf_counter() - t0

            ra_all, dec_all, sep_all = [], [], []
            for pid in jorbit_out:
                a = assist_out[pid]
                j = jorbit_out[pid]
                dra, dde, sep = angular_residual(
                    j["ra_deg"], j["dec_deg"], a["ra_deg"], a["dec_deg"]
                )
                ra_all.append(dra)
                dec_all.append(dde)
                sep_all.append(sep)
            r = Residuals("ASSIST vs jorbit", len(rows) * args.n_epochs,
                          np.concatenate(ra_all),
                          np.concatenate(dec_all),
                          np.concatenate(sep_all))
            print(r.summarize())
            print(f"  jorbit total time:  {t_j:.2f} s")
            print(f"  ASSIST total time:  {t_a:.2f} s")
            summary["drop_in"] = {
                "rms_sep_arcsec": float(np.sqrt(np.mean(np.concatenate(sep_all) ** 2))),
                "jorbit_seconds": t_j,
                "assist_seconds": t_a,
            }

    # ---- Performance ------------------------------------------------------
    if args.section in ("perf", "all"):
        print("\n=== PERFORMANCE: throughput (object · epochs / sec) ===")
        n_total = len(rows) * args.n_epochs

        try:
            t0 = time.perf_counter()
            run_jorbit(rows, schedule)
            t_j = time.perf_counter() - t0
            print(f"  jorbit:    {t_j:.2f}s,  {n_total / t_j:.1f} obj·ep/s")
        except Exception as exc:
            print(f"  jorbit:    skipped ({exc})")
            t_j = None

        t0 = time.perf_counter()
        run_assist(rows, schedule, ephem)
        t_a = time.perf_counter() - t0
        print(f"  ASSIST:    {t_a:.2f}s,  {n_total / t_a:.1f} obj·ep/s")

        t0 = time.perf_counter()
        run_two_body(rows, schedule)
        t_2b = time.perf_counter() - t0
        print(f"  two-body:  {t_2b:.2f}s,  {n_total / t_2b:.1f} obj·ep/s")

        summary["perf"] = {
            "n_total": n_total,
            "jorbit_seconds": t_j,
            "assist_seconds": t_a,
            "two_body_seconds": t_2b,
        }

    if args.report_json:
        with open(args.report_json, "w") as fh:
            json.dump(summary, fh, indent=2, default=float)
        print(f"\nWrote summary to {args.report_json}")


if __name__ == "__main__":
    main()
