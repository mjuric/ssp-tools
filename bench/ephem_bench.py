"""Benchmark and accuracy gate: ASSIST vs jorbit vs JPL Horizons.

Three sections, each independently runnable:

  1. Accuracy gate (ASSIST vs Horizons, same elements). Pass criterion:
     <1 mas RMS great-circle separation.

  2. Geometry (ASSIST light-emission-time r, rdot, delta, deldot, light
     time and phase angle vs Horizons, same elements). Gated.

  3. Drop-in equivalence (ASSIST vs the former jorbit path). Just reports
     residuals; does not gate on them. jorbit is no longer a dependency;
     install it separately (uv pip install jorbit) to run this section.

  4. Performance (jorbit vs ASSIST vs two-body Kepler). Reports object·epoch
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
import re
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
    compute_ephemerides_one,
    elements_row_to_bary_icrf,
    cometary_to_helio_ecliptic,
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
    # Select on the canonical cometary elements (q, e, peri_time); the
    # derived a / mean_anomaly columns are NaN for over half of mpc_orbits.
    keep = (
        (mpcorb["e"] < 0.85)
        & (mpcorb["q"] > 0.3) & (mpcorb["q"] < 50.0)
        & mpcorb["epoch_mjd"].notna()
        & mpcorb["peri_time"].notna()
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
# Method 2: jorbit (the former ssp.ephem._aux_compute_ephemerides path)
# ---------------------------------------------------------------------------

def run_jorbit(rows, schedule):
    """Runs the jorbit path the way the removed ssp.ephem module did: fetch
    the state from JPL Horizons by packed designation, then compute the
    ephemeris at the requested times plus a second call at t+dt for rates.

    jorbit is optional (not a package dependency); raises ImportError if it
    is not installed.
    """
    import jax
    from jorbit import Particle

    jax.config.update("jax_enable_x64", True)

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

        q = float(row["q"])
        e = float(row["e"])
        dt_peri0 = epoch_tt_mjd - float(row["peri_time"])
        inc = np.deg2rad(float(row["i"]))
        Om = np.deg2rad(float(row["node"]))
        om = np.deg2rad(float(row["argperi"]))

        ras = np.empty(len(t_tdb_mjd))
        decs = np.empty(len(t_tdb_mjd))

        # Observer (Rubin) barycentric position
        r_obs_q, _ = util.observatory_barycentric_posvel("X05", eph_times)
        r_obs = r_obs_q.to(u.au).value  # (3, N)

        for k in range(len(t_tdb_mjd)):
            dt_peri = dt_peri0 + (t_tdb_mjd[k] - epoch_tdb_mjd)
            X_ecl, V_ecl = cometary_to_helio_ecliptic(q, e, inc, Om, om, dt_peri)
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


def horizons_observer(row, eph_times: Time, quantities: str = "1",
                      observer_code: str = "X05"):
    """Query a Horizons observer table for the row's *own* osculating
    elements (so the only thing being compared between Horizons and ASSIST
    is the propagator and the geometry).

    Elements are sent as Horizons' native cometary set [TP, QR] (the
    canonical MPC set), with COMMAND=';' and ECLIP=J2000 (IAU76/80
    obliquity). extra_prec=YES is required for sub-mas RA/Dec.

    Returns (columns, state): ``columns`` maps each CSV header label (e.g.
    "R.A.___(ICRF)", "r", "deldot", "S-T-O") to a float ndarray aligned with
    eph_times; ``state`` is the (6,) heliocentric ICRF cartesian state
    [AU, AU/day] that Horizons derived from the input elements.
    """
    def tdb_jd(tt_mjd):
        return Time(float(tt_mjd), format="mjd", scale="tt").tdb.jd

    params = {
        "format": "text",
        "COMMAND": "';'",
        "OBJECT": "'ssp-bench'",
        "EPHEM_TYPE": "OBSERVER",
        "OBJ_DATA": "NO",
        "MAKE_EPHEM": "YES",
        "ECLIP": "J2000",
        "EPOCH": f"{tdb_jd(row['epoch_mjd']):.10f}",
        "EC": f"{float(row['e']):.16e}",
        "QR": f"{float(row['q']):.16e}",
        "TP": f"{tdb_jd(row['peri_time']):.10f}",
        "OM": f"{float(row['node']):.16e}",
        "W": f"{float(row['argperi']):.16e}",
        "IN": f"{float(row['i']):.16e}",
        "CENTER": f"'{observer_code}'",
        "TLIST": ",".join(f"{t:.10f}" for t in eph_times.utc.jd),
        "TIME_TYPE": "UT",
        "QUANTITIES": f"'{quantities}'",
        "ANG_FORMAT": "DEG",
        "extra_prec": "YES",
        "CSV_FORMAT": "YES",
        "REF_PLANE": "FRAME",
        "REF_SYSTEM": "ICRF",
    }
    url = HORIZONS_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "ssp-tools-bench/0.1"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = resp.read().decode("utf-8", errors="replace")

    try:
        soe = body.index("$$SOE")
        eoe = body.index("$$EOE")
    except ValueError:
        raise RuntimeError(
            "Horizons response did not contain $$SOE/$$EOE markers:\n" + body[:1000]
        )

    # The CSV header is the last non-separator line before $$SOE.
    pre = [ln for ln in body[:soe].splitlines() if ln.strip() and not ln.startswith("*")]
    header = [h.strip() for h in pre[-1].split(",")]
    rows_text = [r for r in body[soe + 5:eoe].splitlines() if r.strip()]
    if len(rows_text) != len(eph_times):
        raise RuntimeError(f"expected {len(eph_times)} rows, got {len(rows_text)}")

    columns = {}
    for j, name in enumerate(header):
        if not name or name.startswith("Date"):
            continue
        vals = [ln.split(",")[j].strip() for ln in rows_text]
        if all(_is_float(v) for v in vals):
            columns[name] = np.array([float(v) for v in vals])

    # "Equivalent ICRF heliocentric cartesian coordinates (au, au/d)"
    state = np.full(6, np.nan)
    for key, idx in (("X", 0), ("Y", 1), ("Z", 2), ("VX", 3), ("VY", 4), ("VZ", 5)):
        m = re.search(rf"(?<![A-Z]){key}=\s*([-+0-9.E]+)", body[:soe])
        if m:
            state[idx] = float(m.group(1))
    return columns, state


def horizons_ephem(row, eph_times: Time, observer_code: str = "X05"):
    """Astrometric ICRF (RA, Dec) in degrees from Horizons; see
    `horizons_observer`."""
    cols, _ = horizons_observer(row, eph_times, "1", observer_code)
    return cols["R.A.___(ICRF)"], cols["DEC____(ICRF)"]


def _is_float(s: str) -> bool:
    try:
        float(s)
        return True
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# Emission-time geometry vs Horizons (r, rdot, delta, deldot, LT, S-T-O)
# ---------------------------------------------------------------------------

AU_KM = (1.0 * u.au).to_value(u.km)
C_KM_S = 299792.458


def run_geometry_check(rows, schedule, ephem):
    """Compare ASSIST light-emission-time geometry to Horizons observer
    quantities 19 (r, rdot), 20 (delta, deldot), 21 (light time) and 43
    (true phase angle phi) for the same elements. Returns a dict of
    per-quantity absolute residual arrays.

    Findings this check encodes (verified against Horizons):
      - r/rdot use the *apparent* Sun (at reflection time); using the Sun at
        emission time instead would put r off by ~20 km.
      - deldot is the plain dot product dhat.(V_em - V_obs), without the
        (1 + dhat.V/c) light-time-rate factor (that variant is off by
        ~0.5 m/s).
      - On-sky rates are the rates of change of the astrometric position,
        which include the light-time rate factor (1 - dtau/dt); checked
        against a central difference of Horizons astrometric RA/Dec at
        t +- 60 s. Without that factor NEOs near Earth are off by ~1"/h.
      - Horizons' phi is the phase angle with the Sun direction aberrated by
        the target's heliocentric velocity (sunlight direction in the
        target's rest frame); ssp.ephem_assist.EphResult.phase_angle
        implements that. The purely geometric angle differs by up to v/c.
    """
    res = {k: [] for k in (
        "state_pos_km", "state_vel_mm_s", "r_km", "rdot_mm_s", "delta_km",
        "deldot_dot_mm_s", "deldot_lt_mm_s", "lt_ms",
        "phase_arcsec", "phase_geom_arcsec", "rate_arcsec_h", "rate_rel", "rate_ref_arcsec_h",
    )}
    by_id = rows.set_index("unpacked_primary_provisional_designation", drop=False)
    for pid, eph_times in schedule.items():
        row = by_id.loc[pid]
        try:
            cols, h_state = horizons_observer(row, eph_times, "1,19,20,21,43")
        except Exception as exc:
            print(f"  Horizons fetch failed for {pid}: {exc}")
            continue

        # Initial heliocentric ICRF state from the elements (Sun at origin).
        X0, V0 = elements_row_to_bary_icrf(row, np.zeros(3), np.zeros(3))
        res["state_pos_km"].append([np.linalg.norm(X0 - h_state[:3]) * AU_KM])
        res["state_vel_mm_s"].append(
            [np.linalg.norm(V0 - h_state[3:]) * AU_KM / 86400.0 * 1e6]
        )

        e = compute_ephemerides_one(pid, eph_times, by_id, ephem)
        hp, hv, tp, tv = e.helio_pos, e.helio_vel, e.topo_pos, e.topo_vel
        r = np.linalg.norm(hp, axis=0)
        delta = np.linalg.norm(tp, axis=0)
        rhat = hp / r
        dhat = tp / delta
        rdot = np.sum(rhat * hv, axis=0)
        deldot_dot = np.sum(dhat * tv, axis=0)
        # d|rho|/dt at the observer includes the light-time rate:
        # D (1 + dhat.V_em / c) = dhat.(V_em - V_obs). For the v/c-sized
        # correction factor the geometric velocity e.vv stands in for V_em.
        deldot_lt = deldot_dot / (1.0 + np.sum(dhat * e.vv, axis=0) / C_KM_S)
        cosg = np.sum(hp * tp, axis=0) / (r * delta)
        phase_geom = np.degrees(np.arccos(np.clip(cosg, -1.0, 1.0)))

        res["r_km"].append(np.abs(r - cols["r"]) * AU_KM)
        res["rdot_mm_s"].append(np.abs(rdot - cols["rdot"]) * 1e6)
        res["delta_km"].append(np.abs(delta - cols["delta"]) * AU_KM)
        res["deldot_dot_mm_s"].append(np.abs(deldot_dot - cols["deldot"]) * 1e6)
        res["deldot_lt_mm_s"].append(np.abs(deldot_lt - cols["deldot"]) * 1e6)
        res["lt_ms"].append(np.abs(e.light_time * 86400e3 - cols["1-way_down_LT"] * 60e3))
        # On-sky rates vs a central difference of Horizons astrometric
        # positions at t +- 60 s. Two separate queries, since Horizons
        # returns rows sorted by time and epochs < 120 s apart would
        # otherwise interleave.
        half = 60.0 / 86400.0
        try:
            ra0, dec0 = horizons_ephem(row, Time(eph_times.tai.mjd - half, format="mjd", scale="tai"))
            ra1, dec1 = horizons_ephem(row, Time(eph_times.tai.mjd + half, format="mjd", scale="tai"))
        except Exception as exc:
            print(f"  Horizons rate fetch failed for {pid}: {exc}")
        else:
            dmid = np.deg2rad(0.5 * (dec0 + dec1))
            dra = ((ra1 - ra0 + 540.0) % 360.0) - 180.0
            h_lon = dra * np.cos(dmid) / (2 * half)          # deg/day
            h_lat = (dec1 - dec0) / (2 * half)
            drate = np.hypot(e.mu_lon - h_lon, e.mu_lat - h_lat) * 150.0  # arcsec/h
            res["rate_arcsec_h"].append(drate)
            res["rate_ref_arcsec_h"].append(np.hypot(h_lon, h_lat) * 150.0)
            res["rate_rel"].append(drate / res["rate_ref_arcsec_h"][-1])

        res["phase_arcsec"].append(np.abs(e.phase_angle - cols["phi"]) * 3600.0)
        res["phase_geom_arcsec"].append(np.abs(phase_geom - cols["phi"]) * 3600.0)
    return {k: np.concatenate([np.atleast_1d(x) for x in v]) for k, v in res.items() if v}


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
    p.add_argument("--section", choices=["accuracy", "geometry", "drop-in", "perf", "all"],
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

    # ---- Emission-time geometry vs Horizons --------------------------------
    if args.section in ("geometry", "all"):
        print("\n=== GEOMETRY: ASSIST emission-time r/delta/rates/phase vs Horizons ===")
        if not args.horizons:
            print("  --horizons not set; skipping Horizons fetch.")
        else:
            g = run_geometry_check(rows, schedule, ephem)
            labels = {
                "state_pos_km": "initial helio state |dX| [km]",
                "state_vel_mm_s": "initial helio state |dV| [mm/s]",
                "r_km": "r       [km]",
                "rdot_mm_s": "rdot    [mm/s]",
                "delta_km": "delta   [km]",
                "deldot_dot_mm_s": "deldot (dot product)      [mm/s]",
                "deldot_lt_mm_s": "deldot (with light-time rate) [mm/s]",
                "lt_ms": "light time [ms]",
                "phase_arcsec": "phase_angle vs phi [arcsec]",
                "rate_arcsec_h": "on-sky rate vs Horizons c.d. [arcsec/h]",
                "rate_rel": "on-sky rate, relative",
                "phase_geom_arcsec": "(geometric phase vs phi, info) [arcsec]",
            }
            summary["geometry"] = {}
            for k, lab in labels.items():
                if k not in g:
                    continue
                x = g[k]
                print(f"  {lab:45s} median={np.median(x):.3e}  max={np.max(x):.3e}  (N={len(x)})")
                summary["geometry"][k] = {"median": float(np.median(x)), "max": float(np.max(x))}

            # Gates: an order of magnitude below the float32 storage
            # precision of the SSSource columns (~1e-7 relative), and within
            # Horizons' printed precision where that is coarser.
            gates = [
                ("initial state |dX| < 1 km", np.max(g["state_pos_km"]) < 1.0),
                ("r, delta < 1 km", max(np.max(g["r_km"]), np.max(g["delta_km"])) < 1.0),
                ("rdot, deldot < 10 mm/s",
                 max(np.max(g["rdot_mm_s"]), np.max(g["deldot_dot_mm_s"])) < 10.0),
                ("light time < 1 ms", np.max(g["lt_ms"]) < 1.0),
                # phi is printed to 1e-4 deg (0.36"), so 0.5" allows for rounding.
                ("phase_angle vs phi < 0.5 arcsec", np.max(g["phase_arcsec"]) < 0.5),
                # 1e-3 "/h covers the reference's print noise (3.6 uas
                # positions differenced over 120 s); 1e-5 x rate stays 10x
                # below the ~1e-4 light-time-rate term (dropping it fails
                # this gate at ~half the points).
                ("on-sky rate < 1e-3\"/h + 1e-5 x rate",
                 bool(np.all(g["rate_arcsec_h"] <= 1e-3 + 1e-5 * g["rate_ref_arcsec_h"]))),
            ]
            for name, ok in gates:
                print(f"  Gate: {name:40s} {'PASS' if ok else 'FAIL'}")
            summary["geometry"]["passed"] = all(ok for _, ok in gates)

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
