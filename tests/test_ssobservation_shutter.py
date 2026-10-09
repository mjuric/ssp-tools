"""SSObservation with shutter-motion-corrected times
(docs/design/shutter-timing.md, WP S2): the copy and drop rules, the
all-or-none input columns, the ephemerides at each row's own time, and the
observer state and Sun shifted from the visit time
(ssp.ssobservation.observer_states, ssp.util.solar_elongation_ndarray's dt_s)
against exact evaluation.

The build tests use test_ssobservation_widened's synthetic inputs and stand-ins
(no ASSIST, no network). The ASSIST test is skipped without SSP_ASSIST_*;
the test against astropy's real observer state is skipped unless DE440 is
in the astropy cache (it never downloads)."""

import itertools
import os

import astropy.units as u
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from astropy.coordinates import EarthLocation
from astropy.time import Time

from ssp import ssobservation, util
from ssp.delivery_contract import SHUTTER_INPUT_COLUMNS
from ssp.ssobservation import build_ssobservation, observer_states
from ssp.ssobservation_contract import SHUTTER_INTERNAL, SSObservationDtype

from test_ssobservation_widened import (
    _by_obsid, _fake_ephemerides, _FakeEllipse, _same, make_inputs, read_output,
)

HAVE_ASSIST = bool(os.environ.get("SSP_ASSIST_PLANETS") and os.environ.get("SSP_ASSIST_ASTEROIDS"))

#: An Earth-like circular orbit, inclined like the ecliptic (AU, AU/day);
#: unlike test_ssobservation_widened's stand-in, its velocity is its position's
#: derivative, so shifting along it is meaningful.
_OMEGA = 2 * np.pi / 365.25


def _circular_observatory(obscode, obstime):
    ph = _OMEGA * (obstime.tai.mjd - 60800.0)
    r = np.stack([np.cos(ph), 0.917 * np.sin(ph), 0.398 * np.sin(ph)])
    v = _OMEGA * np.stack([-np.sin(ph), 0.917 * np.cos(ph), 0.398 * np.cos(ph)])
    return r * u.au, v * u.au / u.day


@pytest.fixture
def offline(monkeypatch):
    calls = []

    def spy(provID, ephTimes, mpcorb, ephem, **kw):
        calls.append((provID, ephTimes.tai.mjd.copy(), kw["obs_pos"].copy()))
        return _fake_ephemerides(provID, ephTimes, mpcorb, ephem, **kw)

    monkeypatch.setattr(ssobservation, "compute_ephemerides_one", spy)
    monkeypatch.setattr(ssobservation, "open_ephem", lambda: None)
    monkeypatch.setattr(ssobservation.util, "observatory_barycentric_posvel", _circular_observatory)
    monkeypatch.setattr(ssobservation, "_ellipse", _FakeEllipse())
    return calls


def _build(path, **kw):
    path.mkdir(exist_ok=True)
    dia, _ = make_inputs(path, **kw)
    build_ssobservation(path, path)
    return read_output(path), dia


# --------------------------------------------------------------------------
# The copy and drop rules
# --------------------------------------------------------------------------

def test_copies_corrected_time_and_flags(tmp_path, offline, capsys):
    sss, dia = _build(tmp_path)
    err = capsys.readouterr().err
    s = _by_obsid(sss, dia["obsid"])
    # the corrected time and both flags, as dia_sources.parquet has them
    for c in ("midpointMjdTai", *ssobservation.SHUTTER_FLAGS):
        assert _same(s[c].combine_chunks(), dia[c].combine_chunks()), c
    assert not np.array_equal(dia["midpointMjdTai"].to_numpy(), dia["midpointMjdTaiVisit"].to_numpy())
    # the internal columns are dropped, silently
    for c in SHUTTER_INTERNAL:
        assert c not in sss.column_names
    assert "dropped" not in err
    assert pq.read_table(tmp_path / "ssobservation.parquet").column_names == list(SSObservationDtype.names)


def test_unknown_column_still_warned(tmp_path, offline, capsys):
    make_inputs(tmp_path, extra_column=pa.array(np.arange(14)))
    build_ssobservation(tmp_path, tmp_path)
    assert "dropped: ['extra_column']" in capsys.readouterr().err


def test_interim_inputs_fill_the_flags(tmp_path, offline, capsys):
    sss, dia = _build(tmp_path, shutter=False)
    assert "has no shutter-corrected times" in capsys.readouterr().out
    assert pc.all(sss["midpointMjdTai_flag"]).as_py()
    assert not pc.any(sss["midpointMjdTai_flag_degraded"]).as_py()
    s = _by_obsid(sss, dia["obsid"])
    assert _same(s["midpointMjdTai"].combine_chunks(), dia["midpointMjdTai"].combine_chunks())


@pytest.mark.parametrize("k", [1, 2, 3])
def test_partial_shutter_columns_refused(tmp_path, offline, k):
    dia, _ = make_inputs(tmp_path)
    for drop in itertools.combinations(SHUTTER_INPUT_COLUMNS, k):
        pq.write_table(dia.drop_columns(list(drop)), tmp_path / "dia_sources.parquet")
        with pytest.raises(ValueError, match="shutter-correction columns"):
            build_ssobservation(tmp_path, tmp_path)


def test_shutter_corrected():
    assert ssobservation.shutter_corrected(set(SHUTTER_INPUT_COLUMNS) | {"x"})
    assert not ssobservation.shutter_corrected({"x", "midpointMjdTai"})
    with pytest.raises(ValueError, match=r"lacks \['obstime_basis'\]"):
        ssobservation.shutter_corrected(set(SHUTTER_INPUT_COLUMNS[:-1]))


# --------------------------------------------------------------------------
# Every ephemeris column at the row's own (corrected) time
# --------------------------------------------------------------------------

def test_ephemerides_at_the_corrected_time(tmp_path, offline):
    sss, dia = _build(tmp_path)
    t = dict(zip(dia["obsid"].to_pylist(), dia["midpointMjdTai"].to_numpy()))
    tv = dict(zip(dia["obsid"].to_pylist(), dia["midpointMjdTaiVisit"].to_numpy()))
    assert offline
    got = {}
    for desig, times, obs_pos in offline:
        rows = sss.filter(pc.equal(sss["designation"], desig))
        # the ASSIST times are the rows' corrected midpointMjdTai (the rows
        # are passed in the build's order: compare as sets of times)
        want = sorted(t[o] for o in rows["obsid"].to_pylist())
        np.testing.assert_array_equal(np.sort(times), want)
        # the observer at those times, not at the visits'
        exact, _ = _circular_observatory("X05", Time(times, format="mjd", scale="tai"))
        np.testing.assert_allclose(obs_pos, exact.to_value(u.au), rtol=0, atol=1e-12)   # (0.15 m)
        got[desig] = True
    shifted = [o for o in t if t[o] != tv[o]]
    assert shifted
    # the elongation too: from the corrected time's Sun
    s = _by_obsid(sss, dia["obsid"])
    tt = Time(dia["midpointMjdTai"].to_numpy(), format="mjd", scale="tai")
    want = util.solar_elongation_ndarray(dia["ra"].to_numpy(), dia["dec"].to_numpy(), tt).astype(np.float32)
    np.testing.assert_allclose(s["elongation"].to_numpy(), want, rtol=0, atol=1e-5)


def test_interim_equals_unshifted_correction(tmp_path, offline):
    """Inputs without the correction build bitwise as corrected inputs whose
    times are all the visits' (dt = 0, the flags True / False): the shifted
    observer state and Sun are exact at dt = 0, and the copies agree."""
    a, _ = _build(tmp_path / "a", shutter=False)
    dia, _ = make_inputs(tmp_path / "a", shutter=False)
    n = dia.num_rows
    dia = (dia.append_column("midpointMjdTaiVisit", dia["midpointMjdTai"])
           .append_column("midpointMjdTai_flag", pa.array(np.ones(n, bool)))
           .append_column("midpointMjdTai_flag_degraded", pa.array(np.zeros(n, bool)))
           .append_column("obstime_basis", pa.array(["visit"] * n)))
    (tmp_path / "b").mkdir()
    make_inputs(tmp_path / "b")
    pq.write_table(dia, tmp_path / "b" / "dia_sources.parquet")
    build_ssobservation(tmp_path / "b", tmp_path / "b")
    b = read_output(tmp_path / "b")
    assert a.schema.equals(b.schema)
    for c in a.column_names:
        assert _same(a[c].combine_chunks(), b[c].combine_chunks()), c


# --------------------------------------------------------------------------
# The observer state and the Sun, shifted from the visit time
# --------------------------------------------------------------------------

def _times(n=2000, seed=1):
    rng = np.random.default_rng(seed)
    tv = 60800.0 + np.repeat(rng.uniform(0, 300, n // 20), 20)
    dt = rng.uniform(-0.24, 0.24, n)
    dt[::7] = 0.0
    return tv + dt / 86400, tv, dt


def test_observer_states_shift_vs_exact(monkeypatch):
    monkeypatch.setattr(util, "observatory_barycentric_posvel", _circular_observatory)
    t, tv, dt = _times()
    p_fast, v_fast = observer_states(t, tv)
    p_exact, v_exact = observer_states(t)
    m_per_au = (1 * u.au).to_value(u.m)
    # (the shift's own error is ~1e-11 m: the orbit's curvature over 0.24 s)
    assert np.max(np.abs(p_fast - p_exact)) * m_per_au < 1e-3            # (float64 round-off)
    assert np.max(np.abs(v_fast - v_exact)) < 1e-9          # km/s
    # dt == 0: exactly the visit time's state
    z = dt == 0
    p_v, v_v = observer_states(tv)
    np.testing.assert_array_equal(p_fast[z], p_v[z])
    np.testing.assert_array_equal(v_fast[z], v_v[z])
    # and it is not a no-op
    assert np.max(np.abs(p_fast - p_v)) * m_per_au > 1e3


def test_observer_states_far_rows_exact(monkeypatch):
    monkeypatch.setattr(util, "observatory_barycentric_posvel", _circular_observatory)
    t, tv, dt = _times()
    t[5] = tv[5] + 5.0 / 86400            # beyond MAX_SHIFT_S
    tv[6] = np.nan                        # no visit time
    p_fast, v_fast = observer_states(t, tv)
    p_exact, v_exact = observer_states(t)
    np.testing.assert_array_equal(p_fast[[5, 6]], p_exact[[5, 6]])
    np.testing.assert_array_equal(v_fast[[5, 6]], v_exact[[5, 6]])


def test_observer_states_interim_unchanged(monkeypatch):
    """Without visit times: the old computation, bitwise."""
    monkeypatch.setattr(util, "observatory_barycentric_posvel", _circular_observatory)
    t, _, _ = _times()
    tu, inv = np.unique(t, return_inverse=True)
    r, v = _circular_observatory("X05", Time(tu, format="mjd", scale="tai"))
    p, vel = observer_states(t)
    np.testing.assert_array_equal(p, r.to_value(u.au)[:, inv].T)
    np.testing.assert_array_equal(vel, v.to_value(u.km / u.s)[:, inv].T)


def _real_observatory_or_skip(monkeypatch):
    """The real util.observatory_barycentric_posvel, with X05's location
    given (not fetched from the MPC) and DE440 only from the astropy cache."""
    from astropy.utils.data import conf
    monkeypatch.setattr(conf, "allow_internet", False)
    loc = EarthLocation.from_geodetic(-70.749417 * u.deg, -30.244639 * u.deg, 2663 * u.m)
    monkeypatch.setattr(util, "earthlocation_from_obscode", lambda code: loc)
    try:
        util.observatory_barycentric_posvel("X05", Time([60800.0], format="mjd", scale="tai"))
    except Exception as e:      # (not cached)
        pytest.skip(f"DE440 not available offline: {e}")


def test_observer_states_shift_vs_exact_real(monkeypatch):
    _real_observatory_or_skip(monkeypatch)
    t, tv, dt = _times(400)
    p_fast, v_fast = observer_states(t, tv)
    p_exact, v_exact = observer_states(t)
    m_per_au = (1 * u.au).to_value(u.m)
    dp = np.max(np.linalg.norm(p_fast - p_exact, axis=1)) * m_per_au      # m
    dv = np.max(np.linalg.norm(v_fast - v_exact, axis=1)) * 1e3           # m/s
    # ~mm and ~mm/s at most (float64 round-off of AU-scale positions is
    # ~0.03 mm); a mm at the Moon's distance is 0.5 nano-arcsec
    assert dp < 5e-3 and dv < 5e-3, (dp, dv)


def test_solar_elongation_shift_vs_exact():
    t, tv, dt = _times(400)
    rng = np.random.default_rng(3)
    ra, dec = rng.uniform(0, 360, len(t)), rng.uniform(-80, 80, len(t))
    exact = util.solar_elongation_ndarray(ra, dec, Time(t, format="mjd", scale="tai"))
    fast = util.solar_elongation_ndarray(ra, dec, Time(tv, format="mjd", scale="tai"), dt_s=dt)
    at_visit = util.solar_elongation_ndarray(ra, dec, Time(tv, format="mjd", scale="tai"))
    assert np.max(np.abs(fast - exact)) * 3.6e9 < 1.0              # < 1 uas
    assert np.max(np.abs(at_visit - exact)) * 3.6e6 > 0.1          # > 0.1 mas: the shift matters
    z = dt == 0
    np.testing.assert_array_equal(fast[z], at_visit[z])


# --------------------------------------------------------------------------
# A synthetic object with ASSIST: per-source times move the ephemeris along
# the track by rate x dt
# --------------------------------------------------------------------------

@pytest.mark.skipif(not HAVE_ASSIST, reason="SSP_ASSIST_PLANETS / SSP_ASSIST_ASTEROIDS not set")
def test_along_track_shift_assist(monkeypatch):
    from ssp.ephem_assist import open_ephem
    from ssp.ssobservation import WORK_DTYPE, compute_ssobservation_entry
    monkeypatch.setattr(util, "observatory_barycentric_posvel", _circular_observatory)
    ephem = open_ephem()
    desig = "2026 ZZ1"
    # a near-Earth orbit, a few tenths of an AU from the observer
    mpcorb = pd.DataFrame({"q": [0.95], "e": [0.35], "i": [12.0], "node": [40.0], "argperi": [250.0],
                           "peri_time": [60820.0], "epoch_mjd": [60800.0], "h": [21.0], "g": [0.15]},
                          index=[desig])
    k = 12
    tv = 60800.0 + np.linspace(0, 20, k)
    dt = np.linspace(-0.24, 0.24, k)

    def run(times, ra=None, dec=None):
        de = np.zeros(k, dtype=[(c, "f8") for c in ("midpointMjdTai", "ra", "dec")])
        de["midpointMjdTai"] = times
        sss = np.zeros(k, dtype=WORK_DTYPE)
        sss["designation"] = desig
        assoc = np.zeros(k, dtype=[("dia_index", "i8"), ("obs_pos", "f8", 3), ("obs_vel", "f8", 3)])
        assoc["dia_index"] = np.arange(k)
        assoc["obs_pos"], assoc["obs_vel"] = observer_states(times, tv)
        if ra is not None:
            de["ra"], de["dec"] = ra, dec
        compute_ssobservation_entry(sss, assoc, mpcorb, de, ephem)
        return sss

    at_visit = run(tv)
    # the "observed" positions: the ephemeris at the visit time, offset by 0.2"
    ra = at_visit["ephRa"] + 0.2 / 3600 / np.cos(np.deg2rad(at_visit["ephDec"]))
    dec = at_visit["ephDec"] + 0.1 / 3600
    a = run(tv, ra, dec)
    b = run(tv + dt / 86400, ra, dec)
    shift = a["ephRate"].astype(np.float64) * dt / 86400 * 3600          # arcsec
    assert np.max(np.abs(shift)) > 0.005                                 # a fast object: > 5 mas
    np.testing.assert_allclose(b["ephOffsetAlongTrack"] - a["ephOffsetAlongTrack"], -shift,
                               rtol=1e-3, atol=2e-6)
    np.testing.assert_allclose(b["ephOffsetCrossTrack"] - a["ephOffsetCrossTrack"], 0, atol=2e-6)
