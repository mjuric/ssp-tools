"""The SSSource/NearbySSO time-shift allowance (bench/time_shift.py,
docs/design/shutter-timing.md) in the three validators that compare the two
tables, on synthetic inputs: at dt = 0 the old strict tolerances hold; at
dt = 0.2 s a rate x dt difference passes; at dt = 0.2 s an extra error
beyond it fails. Network-free."""
import os
import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bench import nearbysso_validate as NV
from bench import nongrav_validate as GV
from bench import tail_angles_validate as T
from bench import time_shift as TS

T0 = 60800.25
DT = 0.2 / 86400.0
DT2 = 2.0 / 86400.0      # header-timed (degraded) visits shift by up to ~2 s
MAS = 1 / 3.6e6


# --------------------------------------------------------------------------
# the helper
# --------------------------------------------------------------------------

def test_allowances_are_zero_at_dt_zero():
    z = np.zeros(3)
    rate = np.array([1.0, 10.0, 100.0])
    one = np.ones(3)
    assert (TS.position_mas(rate, z) == 0).all()
    assert (TS.position_margin_mas(rate, z) == 0).all()
    assert (TS.rate_allowance(z, rate, one * 0.01, one * 10, one, one * 20) == 0).all()
    assert (TS.vmag_allowance(z, rate, one * 0.01, one * 10, one, one) == 0).all()
    assert (TS.pa_allowance_deg(z, rate, one * 1e-6, one * 20, one) == 0).all()


def test_dt_days_unknown_is_zero():
    dt = TS.dt_days([T0 + DT, T0, np.nan], [T0, T0, T0])
    assert dt[0] == pytest.approx(DT) and dt[1] == 0 and dt[2] == 0


def test_position_allowance_is_rate_times_dt():
    # 10 deg/d over 0.2 s = 83.3 mas, plus the margins
    a = TS.position_mas(np.array([10.0]), np.array([DT]))[0]
    assert 10 * DT / MAS < a < 10 * DT / MAS * 1.03 + 0.06


def test_motion_residual_removes_the_motion():
    ra, dec, rra, rdec = 150.0, 40.0, 8.0, -6.0
    ra_n = ra - rra * DT / np.cos(np.radians(dec))
    dec_n = dec - rdec * DT
    assert TS.motion_residual_mas(ra, dec, ra_n, dec_n, rra, rdec, DT) < 1e-6
    # the wrong sign doubles it
    assert TS.motion_residual_mas(ra, dec, ra_n, dec_n, rra, rdec, -DT) == pytest.approx(2 * 10 * DT / MAS)


def test_rate_allowance_main_belt():
    # a main-belt object (1.5 au, 0.25 deg/d): the diurnal acceleration
    # changes its rate by ~2e-7 deg/d in 0.2 s, more than nearbysso's 1e-7
    a = TS.rate_allowance(np.array([DT]), 0.25, 1.5, 10.0, 2.5, 10.0)[0]
    assert 2e-7 < a < 2e-6


# --------------------------------------------------------------------------
# bench/nearbysso_validate same-orbits
# --------------------------------------------------------------------------

def _nss_case(dt=0.0, extra_mas=(0.0, 0.0), rates=(8.0, -6.0)):
    """One object, one DiaSource: SSSource at T0 + dt, NearbySSO at T0 (its
    DiaSource's time), behind by rate x dt, plus ``extra_mas`` (ra, dec)."""
    ra, dec = 150.0, 40.0
    rra, rdec = rates
    dia = pd.DataFrame({"diaSourceId": [1], "midpointMjdTai": [T0], "ra": [ra + 1 * 1 / 3600], "dec": [dec]})
    sss = pd.DataFrame({"diaSourceId": [1], "designation": ["2020 AB"], "ephRa": [ra], "ephDec": [dec],
                        "ephRateRa": np.float32(rra), "ephRateDec": np.float32(rdec),
                        "ephVmag": np.float32(20),
                        "sss_midpointMjdTai": [T0 + dt], "topoRange": [0.05], "topoRangeRate": [10.0],
                        "helioRange": [1.0], "helioRangeRate": [5.0]})
    cosd = np.cos(np.radians(dec))
    nss = pd.DataFrame({"diaSourceId": [1], "designation": ["2020 AB"],
                        "ephRa": [ra - rra * dt / cosd + extra_mas[0] * MAS / cosd],
                        "ephDec": [dec - rdec * dt + extra_mas[1] * MAS],
                        "ephRateRa": np.float32(rra), "ephRateDec": np.float32(rdec),
                        "ephVmag": np.float32(20),
                        "ephOffset": np.float32(1.0)})
    return NV.compare_to_sssource(nss, sss, dia, {"2020 AB": ""}, lambda d, t: np.ones(len(d)))


@pytest.mark.parametrize("dt, extra, status", [
    (0.0, (0.0, 0.0), "match"),
    (0.0, (0.0, 1.0), "value_mismatch"),        # dt = 0: today's 0.1 mas
    (DT, (0.0, 0.0), "match"),                   # rate x dt = 83 mas
    (DT, (0.0, 5.0), "value_mismatch"),          # + 5 mas across
    (DT, (5.0, 0.0), "value_mismatch"),
    (DT, (8 * 5.0 / 10, -6 * 5.0 / 10), "value_mismatch"),   # + 5 mas along the track
    (-DT, (0.0, 0.0), "match"),
    (DT2, (0.0, 0.0), "match"),                  # a header-timed visit: 833 mas
    (-DT2, (0.0, 0.0), "match"),
    (DT2, (0.0, 5.0), "value_mismatch"),
])
def test_nearbysso_same_orbits(dt, extra, status):
    rows, summ = _nss_case(dt, extra)
    assert rows["status"].iloc[0] == status, rows.T
    assert summ["n_time_shifted"] == int(dt != 0)


def test_nearbysso_unshifted_motion_fails_without_the_time():
    """The rate x dt difference with SSSource's time equal to the
    DiaSource's (as before the correction) is a mismatch."""
    nss_like, _ = _nss_case(DT)       # NearbySSO's positions, behind by rate x dt
    sss = pd.DataFrame({"diaSourceId": [1], "designation": ["2020 AB"], "ephRa": [150.0], "ephDec": [40.0],
                        "ephRateRa": np.float32(8), "ephRateDec": np.float32(-6), "ephVmag": np.float32(20),
                        "sss_midpointMjdTai": [T0]})
    nss = pd.DataFrame({"diaSourceId": [1], "designation": ["2020 AB"],
                        "ephRa": [nss_like["nss_ephRa"].iloc[0]], "ephDec": [nss_like["nss_ephDec"].iloc[0]],
                        "ephRateRa": np.float32(8), "ephRateDec": np.float32(-6), "ephVmag": np.float32(20),
                        "ephOffset": np.float32(1)})
    dia = pd.DataFrame({"diaSourceId": [1], "midpointMjdTai": [T0], "ra": [150.0003], "dec": [40.0]})
    rows, _ = NV.compare_to_sssource(nss, sss, dia, {"2020 AB": ""}, lambda d, t: np.ones(len(d)))
    assert rows["status"].iloc[0] == "value_mismatch"


def test_nearbysso_dt_too_large_fails():
    _, summ = _nss_case(12.0 / 86400)
    assert summ["n_dt_too_large"] == 1 and summ["n_fail"] >= 1


def test_nearbysso_rate_and_vmag_allowances():
    rows, summ = _nss_case(DT)
    assert summ["n_fail"] == 0
    # a rate difference of 1e-6 deg/d at 0.05 au and 10 deg/d over 0.2 s is
    # within the acceleration bound; 1e-2 deg/d isn't
    for dr, ok in ((1e-6, True), (1e-2, False)):
        ra, dec = 150.0, 40.0
        dia = pd.DataFrame({"diaSourceId": [1], "midpointMjdTai": [T0], "ra": [ra], "dec": [dec]})
        sss = pd.DataFrame({"diaSourceId": [1], "designation": ["X"], "ephRa": [ra], "ephDec": [dec],
                            "ephRateRa": [0.0], "ephRateDec": [0.0], "ephVmag": [20.0],
                            "sss_midpointMjdTai": [T0 + DT], "topoRange": [0.05], "topoRangeRate": [10.0],
                            "helioRange": [1.0], "helioRangeRate": [5.0]})
        nss = pd.DataFrame({"diaSourceId": [1], "designation": ["X"], "ephRa": [ra], "ephDec": [dec],
                            "ephRateRa": [dr], "ephRateDec": [0.0], "ephVmag": [20.0], "ephOffset": [0.0]})
        rows, _ = NV.compare_to_sssource(nss, sss, dia, {"X": ""}, lambda d, t: np.ones(len(d)))
        assert (rows["status"].iloc[0] == "match") == ok


def test_read_sss_keeps_its_own_time(tmp_path):
    p = tmp_path / "s.parquet"
    pq.write_table(pa.table({"diaSourceId": [1], "designation": ["A"], "ephRa": [1.0], "ephDec": [2.0],
                             "ephRateRa": [0.1], "ephRateDec": [0.1], "ephVmag": [20.0],
                             "midpointMjdTai": [T0 + DT], "topoRange": [1.0]}), p)
    s = NV._read_sss(str(p))
    assert s["sss_midpointMjdTai"].iloc[0] == T0 + DT and "midpointMjdTai" not in s
    assert "topoRange" in s


# --------------------------------------------------------------------------
# bench/nongrav_validate nearbysso
# --------------------------------------------------------------------------

def _ng_case(dt=0.0, extra_mas=0.0, with_dia_time=True):
    ra = np.array([10.0, 12.0, 14.0])
    dec = np.array([-5.0, 0.0, 5.0])
    rra, rdec = np.array([8.0, 0.2, -3.0]), np.array([-6.0, 0.1, 4.0])
    n = len(ra)
    des = ["2000 CA", "2000 CB", "2000 CC"]
    sss = pd.DataFrame({"designation": des, "diaSourceId": pd.array(1000 + np.arange(n), dtype="Int64"),
                        "visit": 2025070100000 + np.arange(n), "midpointMjdTai": T0 + dt,
                        "ra": ra, "dec": dec, "ephRa": ra + 1e-5, "ephDec": dec,
                        "ephRateRa": rra, "ephRateDec": rdec, "ephVmag": 20.0,
                        "ephRaErr": 1e-4, "ephDecErr": 1e-4, "ephRa_ephDec_Cov": 0.0,
                        "topoRange": 0.05, "topoRangeRate": 10.0, "helioRange": 1.0, "helioRangeRate": 5.0})
    cosd = np.cos(np.radians(dec))
    n_ra = sss["ephRa"] - rra * dt / cosd
    n_dec = sss["ephDec"] - rdec * dt + np.array([extra_mas, 0, 0]) * MAS
    sss["ephOffset"] = GV.sky_sep_arcsec(sss["ephRa"], sss["ephDec"], ra, dec)
    dia = pd.DataFrame({"diaSourceId": 1000 + np.arange(n), "visit": sss["visit"], "ra": ra, "dec": dec})
    if with_dia_time:
        dia["midpointMjdTai"] = T0
    nss = pd.DataFrame({"diaSourceId": dia["diaSourceId"], "designation": des, "ephRa": n_ra, "ephDec": n_dec,
                        "ephOffset": GV.sky_sep_arcsec(n_ra, n_dec, ra, dec).astype(np.float32),
                        "ephVmag": 20.0, "ephRateRa": rra, "ephRateDec": rdec})
    orbits = pd.DataFrame({"designation": des, "q": 2.0, "e": 0.1, "i": 5.0, "node": 1.0, "argperi": 2.0,
                           "peri_time": 60000.0,
                           "mpc_orb_jsonb": '{"orbit_fit_statistics": {"arc_length_total": "2014-2024"}}'})
    cmap = {d: "control" for d in des}
    rep = GV.Report("t")
    df = GV.nearbysso_compare(nss, sss, dia, orbits, cmap, rep)
    GV._nearbysso_gates(df, rep)
    return rep, df


@pytest.mark.parametrize("dt, extra, ok", [
    (0.0, 0.0, True),
    (0.0, 2.0, False),           # dt = 0: today's 1 mas
    (DT, 0.0, True),             # rate x dt, up to 83 mas
    (DT, 5.0, False),            # + 5 mas
    (DT2, 0.0, True),            # 2 s: up to 833 mas
    (DT2, 5.0, False),
])
def test_nongrav_nearbysso(dt, extra, ok):
    rep, df = _ng_case(dt, extra)
    assert rep.ok == ok, rep.text()
    assert (df["status"] == "match").all()
    if not ok:
        assert rep.failed == ["matched rows agree: position"], rep.text()


def test_nongrav_nearbysso_without_dia_times_is_strict():
    rep, _ = _ng_case(DT, with_dia_time=False)
    assert not rep.ok and "matched rows agree: position" in rep.failed


def test_nongrav_dt_too_large():
    rep, _ = _ng_case(12.0 / 86400)
    assert any(n.startswith("time shift") for n in rep.failed), rep.text()


# --------------------------------------------------------------------------
# bench/tail_angles_validate consistency
# --------------------------------------------------------------------------

def _radec_unit(ra, dec):
    a, d = np.radians(ra), np.radians(dec)
    return np.array([np.cos(d) * np.cos(a), np.cos(d) * np.sin(a), np.sin(d)])


def _tail_tables(tmp_path, dt_true, dt_recorded, extra=None, n=200, seed=11):
    """SSSource at T0 + dt_recorded (its angles from its own vectors);
    NearbySSO's angles from the vectors moved back by dt_true (the observer
    fixed, the object moving at helio_v); half the objects close and fast.
    ``extra``: (row, deg) added to that NearbySSO angle. The DiaSource file
    has T0."""
    rng = np.random.default_rng(seed)
    ra, dec = rng.uniform(0, 360, n), np.degrees(np.arcsin(rng.uniform(-0.9, 0.9, n)))
    dist = np.where(np.arange(n) % 2 == 0, rng.uniform(0.002, 0.01, n), rng.uniform(1, 3, n))
    topo = _radec_unit(ra, dec) * dist
    helio = rng.normal(size=(3, n)) * 2.0
    vel = rng.normal(size=(3, n)) * 20.0                            # km/s
    sun, mot = T.tail_angles(helio, vel, topo)
    k = TS.KM_S_TO_AU_D
    sun_n, mot_n = T.tail_angles(helio - vel * k * dt_true, vel, topo - vel * k * dt_true)
    u = topo / np.linalg.norm(topo, axis=0)
    vperp = vel - np.sum(vel * u, 0) * u
    rate = np.degrees(np.linalg.norm(vperp, axis=0) * k / dist)
    ids = np.arange(n, dtype=np.int64) + 1
    des = np.array([f"2026 B{i}" for i in range(n)])
    s = {"designation": des, "diaSourceId": ids, "ephRa": ra, "ephDec": dec,
         "phaseAngle": np.full(n, 30.0, np.float32), "ephAntiSunPA": sun.astype(np.float32),
         "ephAntiMotionPA": mot.astype(np.float32), "midpointMjdTai": np.full(n, T0 + dt_recorded),
         "ephRate": rate.astype(np.float32)}
    for j, c in enumerate("xyz"):
        s[f"helio_{c}"] = helio[j].astype(np.float32)
        s[f"helio_v{c}"] = vel[j].astype(np.float32)
        s[f"topo_{c}"] = topo[j].astype(np.float32)
    mot_n = mot_n.copy()
    if extra:
        mot_n[extra[0]] = (mot_n[extra[0]] + extra[1]) % 360
    nb = {"diaSourceId": ids, "designation": des, "ephRa": ra,
          "ephAntiSunPA": sun_n.astype(np.float32), "ephAntiMotionPA": mot_n.astype(np.float32)}
    ps, pn, pd_ = (str(tmp_path / f) for f in ("s.parquet", "n.parquet", "dia.parquet"))
    pq.write_table(pa.table(s), ps)
    pq.write_table(pa.table(nb), pn)
    pq.write_table(pa.table({"diaSourceId": ids, "midpointMjdTai": np.full(n, T0)}), pd_)
    return ps, pn, pd_


def _fails(lines):
    return [ln for ln in lines if ln.startswith("FAIL")]


def test_tail_dt_zero_is_strict(tmp_path):
    ps, pn, pd_ = _tail_tables(tmp_path, 0.0, 0.0)
    ok, lines = T.consistency(ps, pn, pd_)
    assert ok, "\n".join(lines)
    assert any("0 on every compared row" in ln for ln in lines)
    # a real shift with no recorded time difference fails (strict)
    ps, pn, pd_ = _tail_tables(tmp_path, DT, 0.0)
    ok, lines = T.consistency(ps, pn, pd_)
    assert not ok and all("pairs ephAnti" in ln for ln in _fails(lines)), "\n".join(lines)


def test_tail_shift_passes_with_the_time(tmp_path):
    ps, pn, pd_ = _tail_tables(tmp_path, DT, DT)
    ok, lines = T.consistency(ps, pn, pd_)
    assert ok, "\n".join(lines)
    assert any("nonzero on 200" in ln for ln in lines), "\n".join(lines)
    # the shift is real: some pairs differ by more than the strict 1e-4 deg
    assert not T.consistency(ps, pn)[0]          # without the DiaSource times: strict


def test_tail_shift_of_two_seconds(tmp_path):
    ps, pn, pd_ = _tail_tables(tmp_path, DT2, DT2)
    ok, lines = T.consistency(ps, pn, pd_)
    assert ok, "\n".join(lines)
    ps, pn, pd_ = _tail_tables(tmp_path, DT2, DT2, extra=(1, 3e-4))
    assert not T.consistency(ps, pn, pd_)[0]


def test_dt_max_is_a_sanity_limit_above_the_degraded_shifts():
    assert TS.DT_MAX_S >= 3.0
    assert not TS.too_large(np.array([DT2, -DT2, 2.9 / 86400])).any()


def test_tail_shift_plus_an_error_fails(tmp_path):
    ps, pn, pd_ = _tail_tables(tmp_path, DT, DT, extra=(1, 3e-4))     # row 1: a slow object
    ok, lines = T.consistency(ps, pn, pd_)
    f = _fails(lines)
    assert not ok and len(f) == 1 and "pairs ephAntiMotionPA" in f[0], "\n".join(lines)


def test_tail_dt_too_large(tmp_path):
    ps, pn, pd_ = _tail_tables(tmp_path, 0.0, 12.0 / 86400)
    ok, lines = T.consistency(ps, pn, pd_)
    assert not ok and any("time shift: 200 pairs with |dt| >" in ln for ln in _fails(lines)), "\n".join(lines)


def test_tail_cli_dia_sources(tmp_path):
    ps, pn, pd_ = _tail_tables(tmp_path, DT, DT)
    assert T.main(["consistency", ps, pn, "--dia-sources", pd_]) == 0
    assert T.main(["consistency", ps, pn]) == 1


def test_position_margin_with_and_without_ranges():
    rate, dt = np.array([10.0]), np.array([DT2])
    shift = 10 * DT2 / MAS                                         # 833 mas
    with_acc = TS.position_margin_mas(rate, dt, TS.angular_acceleration(rate, 0.05, 10.0, 1.0, 20.0))[0]
    without = TS.position_margin_mas(rate, dt)[0]
    assert with_acc < 0.003 * shift + 0.1 < 5.0
    assert without == pytest.approx(TS.REL_MARGIN * shift + TS.ABS_MARGIN_MAS)
