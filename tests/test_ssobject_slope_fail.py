"""{band}_slope_fit_failed: "G12 fit failed in {band} band. G12 contains a
fiducial value used to fit H." Each failure rule, and the fixed-G12
fallback's values."""

import numpy as np
import pytest

from ssp import photfit, ssobject
from ssp.ssobject import (FAIL_BOUND, FAIL_FEW, FAIL_SINGULAR, FAIL_SPAN, compute_ssobject,
                          fit_band)

from test_ssobject_parallel import _tables

KW = dict(magSigmaFloor=0.05, nSigmaClip=10.0)


def _band(n=20, G12=0.5, lo=2.0, hi=25.0, seed=1, steep=0.0, noise=0.01):
    rng = np.random.default_rng(seed)
    phase = np.linspace(lo, hi, n)
    tdist = rng.uniform(0.8, 2.5, n)
    rdist = rng.uniform(1.5, 3.5, n)
    mag = (photfit.HG12_model(np.deg2rad(phase), [15.0, G12]) + steep * phase
           + 5 * np.log10(tdist * rdist) + rng.normal(0, noise, n))
    return mag, np.full(n, noise), phase, tdist, rdist


def _fixed(data, G=0.5, **kw):
    # (clipping as the free fit: only with more than 3 usable points)
    return photfit.fitHG12(*data, fixedG12=G, clipMinObs=3, **(KW | kw))


def _assert_fallback(out, data, G=0.5):
    ref = _fixed(data, G)
    assert out["slope_fit_failed"]
    assert out["G12"] == G and np.isnan(out["G12Err"]) and np.isnan(out["Cov"])
    assert out["H"] == ref.H and out["HErr"] == ref.H_err and out["nObsUsed"] == ref.nobs
    assert np.array_equal(out["Chi2"], ref.chi2dof * (ref.nobs - 1), equal_nan=True)


def test_good_fit_does_not_fail():
    data = _band()
    out = fit_band(*data, **KW)
    ref = photfit.fitHG12(*data, **KW)
    assert out["failures"] == 0 and not out["slope_fit_failed"]
    assert out["G12"] == ref.G12 and 0.3 < out["G12"] < 0.7 and np.isfinite(out["G12Err"])
    assert out["H"] == ref.H and out["nObsUsed"] == 20


@pytest.mark.parametrize("G12,steep", [(0.0, -0.01), (1.0, 0.01)])
def test_bound(G12, steep):
    data = _band(G12=G12, steep=steep)        # beyond what G12 in [0, 1] allows
    assert photfit.fitHG12(*data, **KW).G12 == G12
    out = fit_band(*data, **KW)
    assert out["failures"] == FAIL_BOUND
    _assert_fallback(out, data)


def test_bound_tolerance(monkeypatch):
    data = _band()
    real = photfit.fitHG12

    def near(G):
        def fit(*a, **kw):
            r = real(*a, **kw)
            return r._replace(G12=G) if kw.get("fixedG12") is None else r
        return fit
    monkeypatch.setattr(photfit, "fitHG12", near(1 - 0.5 * ssobject.G12_BOUND_TOL))
    assert fit_band(*data, **KW)["failures"] == FAIL_BOUND
    monkeypatch.setattr(photfit, "fitHG12", near(1 - 2 * ssobject.G12_BOUND_TOL))
    assert fit_band(*data, **KW)["failures"] == 0


def test_singular(monkeypatch):
    data = _band()
    real = photfit.fitHG12
    # the free fit failing as for a singular J^T J (photfit returns _FAILED)
    monkeypatch.setattr(photfit, "fitHG12",
                        lambda *a, **kw: photfit._FAILED if kw.get("fixedG12") is None else real(*a, **kw))
    out = fit_band(*data, **KW)
    assert out["failures"] == FAIL_SINGULAR
    _assert_fallback(out, data)


def test_singular_real():
    # three points at one phase angle: J^T J singular, span too small
    data = _band(n=3, lo=10.0, hi=10.0)
    out = fit_band(*data, **KW)
    assert out["failures"] & FAIL_SPAN
    _assert_fallback(out, data)


@pytest.mark.parametrize("n", [1, 2])
def test_few_observations(n):
    data = _band(n=n, lo=5.0, hi=25.0)
    out = fit_band(*data, **KW)
    assert out["failures"] == FAIL_FEW
    _assert_fallback(out, data)
    assert out["nObsUsed"] == n and np.isfinite(out["H"])
    if n == 1:
        assert out["HErr"] == np.hypot(0.01, 0.05)


def test_few_after_dropping_points():
    mag, sig, phase, tdist, rdist = _band(n=3, lo=5.0, hi=25.0)
    mag[1] = np.nan                         # (a non-positive flux)
    data = (mag, sig, phase, tdist, rdist)
    out = fit_band(*data, **KW)
    assert out["failures"] == FAIL_FEW and out["nObsUsed"] == 2
    _assert_fallback(out, data)


def test_few_after_clipping(monkeypatch):
    mag, sig, phase, tdist, rdist = _band(n=4, lo=5.0, hi=25.0, noise=0.001)
    mag[2] += 3.0                           # a 3 mag outlier, clipped
    data = (mag, sig, phase, tdist, rdist)
    assert photfit.fitHG12(*data, **KW).nobs == 3
    assert fit_band(*data, **KW)["failures"] == 0
    # clipped to 2 points, fitHG12 has no result (as it reports it)
    real = photfit.fitHG12

    def clipped(*a, _details=None, **kw):
        if kw.get("fixedG12") is not None:
            return real(*a, **kw)
        _details.update(keep=np.array([True, False, True, False]))
        return photfit._FAILED
    monkeypatch.setattr(photfit, "fitHG12", clipped)
    out = fit_band(*data, **KW)
    assert out["failures"] == FAIL_FEW
    monkeypatch.undo()
    _assert_fallback(out, data)


def test_fallback_clips_as_the_free_fit():
    # 3 points, one far off: a fixed-G12 fit with its own clipping condition
    # (more than 2 points) would clip; the fallback keeps all 3, as the free
    # fit (which never clips 3 points) would have
    mag, sig, phase, tdist, rdist = _band(n=3, lo=5.0, hi=6.0, noise=0.001)
    mag[1] += 2.0
    data = (mag, sig, phase, tdist, rdist)
    assert photfit.fitHG12(*data, fixedG12=0.5, **KW).nobs < 3
    out = fit_band(*data, **KW)
    assert out["failures"] & FAIL_SPAN and out["nObsUsed"] == 3 and np.isfinite(out["H"])
    _assert_fallback(out, data)
    # with 4 points it clips, as the free fit would
    mag, sig, phase, tdist, rdist = _band(n=4, lo=5.0, hi=6.0, noise=0.001)
    mag[1] += 2.0
    out = fit_band(mag, sig, phase, tdist, rdist, **KW)
    assert out["nObsUsed"] == 3


def test_span():
    data = _band(n=15, lo=10.0, hi=11.5)
    out = fit_band(*data, **KW)
    assert out["failures"] & FAIL_SPAN and not out["failures"] & FAIL_FEW
    _assert_fallback(out, data)
    # configurable; and the span is that of the points used (an outlier
    # far away in phase doesn't count once clipped)
    assert not fit_band(*data, minPhaseSpan=1.0, **KW)["failures"] & FAIL_SPAN
    extra = (data[0][0] + 5, 0.01, 30.0, 1.5, 2.5)
    mag, sig, phase, tdist, rdist = (np.append(a, v) for a, v in zip(data, extra))
    out = fit_band(mag, sig, phase, tdist, rdist, minPhaseSpan=1.0, **KW)
    assert out["nObsUsed"] == 15 and not out["failures"] & FAIL_SPAN


def test_no_usable_point():
    mag, sig, phase, tdist, rdist = _band(n=4)
    mag[:] = np.nan
    out = fit_band(mag, sig, phase, tdist, rdist, **KW)
    assert out["slope_fit_failed"] and out["nObsUsed"] == 0
    assert np.isnan(out["H"]) and np.isnan(out["HErr"]) and np.isnan(out["G12"])


def test_fiducial_and_fixed():
    data = _band(n=2)
    out = fit_band(*data, fiducialG12=0.3, **KW)
    _assert_fallback(out, data, G=0.3)
    # a fixed G12: no slope fit, the rules don't apply
    out = fit_band(*data, fixedG12=0.2, **KW)
    ref = _fixed(data, 0.2)
    assert not out["slope_fit_failed"] and out["failures"] == 0
    assert out["G12"] == 0.2 and out["H"] == ref.H and np.isnan(out["G12Err"])


@pytest.mark.parametrize("case", ["bound", "few", "span", "good"])
def test_fallback_is_order_independent(case):
    data = dict(bound=_band(G12=0.0, steep=0.01), few=_band(n=2), span=_band(n=12, lo=7.0, hi=8.0),
                good=_band())[case]
    ref = fit_band(*data, **KW)
    rng = np.random.default_rng(3)
    for _ in range(5):
        p = rng.permutation(len(data[0]))
        out = fit_band(*(a[p] for a in data), **KW)
        assert np.array(list(out.values()), float).tobytes() == np.array(list(ref.values()), float).tobytes()


def test_ssobject_columns():
    sss, orbits = _tables(n_obj=40, seed=2)
    obj = compute_ssobject(sss, orbits)
    for band in "grizy":
        nobs, failed = obj[f"{band}_nObs"], obj[f"{band}_slope_fit_failed"]
        G, Gerr = obj[f"{band}_G12"], obj[f"{band}_G12Err"]
        used = obj[f"{band}_nObsUsed"]
        # every band with an observation has an H, unless the fixed-G12
        # fallback clipped it away (this fake photometry isn't HG12-like)
        assert np.array_equal(np.isfinite(obj[f"{band}_H"]), used > 0)
        assert (used[nobs > 0] > 0).mean() > 0.8
        assert np.all(failed[(nobs > 0) & (nobs < 3)])
        assert np.all(G[failed & (used > 0)] == np.float32(0.5)) and np.all(np.isnan(Gerr[failed]))
        assert not np.any(failed[nobs == 0])
        ok = ~failed & (nobs > 0)
        assert np.all((G[ok] > 0) & (G[ok] < 1)) and np.all(np.isfinite(Gerr[ok]))
    assert obj["g_slope_fit_failed"].any() and (~obj["g_slope_fit_failed"] & (obj["g_nObs"] > 0)).any()
