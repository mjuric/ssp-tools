"""fitHG12 is a function of the set of observations: any permutation of its
inputs gives a bitwise identical result.

The fit's reductions (weighted means, IRLS sums, costs, J^T J) round
differently in another order. In a well-posed fit that moves G12 within the
bounded search's tolerance (~1e-6); in a degenerate one, whose G12 profile
is flat to rounding, it decides G12 outright, whether G12 ends at a bound
(which switches the H_err formula), and whether J^T J is invertible (the
fit fails or not). The fit therefore takes its inputs in a canonical order.
The regression cases are real fits from the 2026-09-30 fixture that did
all of that.
"""

import numpy as np
import pytest

from ssp import photfit

ROBUST = dict(magSigmaFloor=0.05, nSigmaClip=10.0)


def _mags(flux, flux_err):
    flux = np.asarray(flux, np.float32).astype(float)
    flux_err = np.asarray(flux_err, np.float32).astype(float)
    return 31.4 - 2.5 * np.log10(flux), 1.085736 * (flux_err / flux)


def _case(flux, flux_err, phase, topo, helio):
    mag, err = _mags(flux, flux_err)
    f32 = lambda x: np.asarray(x, np.float32)   # noqa: E731  (as SSObservation has them)
    return mag, err, f32(phase), f32(topo), f32(helio)


# y band of ssObjectId 5635187669466106656: 10 points all at one phase angle
# (28.9724 deg, to 2e-6 deg), so G12 is unconstrained. In dia_sources and
# in SSObservation order the fit gave G12 = 0.99999934 with H_err = 8.2e5, and
# G12 = 1.0 (a bound) with H_err = 0.057.
SINGLE_PHASE = _case(
    [9652.6171875, 10478.6591796875, 10221.1103515625, 13354.4423828125, 14548.0302734375,
     12662.427734375, 10727.3447265625, 17083.279296875, 13184.79296875, 12212.9462890625],
    [1883.2142333984375, 1809.353759765625, 1828.3623046875, 2207.2197265625, 2442.91455078125,
     2006.8287353515625, 1952.6439208984375, 1773.5697021484375, 1968.569580078125, 2218.587646484375],
    [28.972400665283203, 28.97239875793457, 28.972400665283203, 28.972402572631836, 28.972402572631836,
     28.972400665283203, 28.972402572631836, 28.972400665283203, 28.972400665283203, 28.972400665283203],
    [1.8133316040039062, 1.8132818937301636, 1.8133562803268433, 1.8135390281677246, 1.8135268688201904,
     1.8136403560638428, 1.8134527206420898, 1.8133494853973389, 1.8133375644683838, 1.813615322113037],
    [2.08851957321167, 2.088515043258667, 2.0885214805603027, 2.0885372161865234, 2.088536024093628,
     2.088545799255371, 2.0885298252105713, 2.0885210037231445, 2.088520050048828, 2.088543653488159],
)
# SSObservation's order of those rows (by time)
SINGLE_PHASE_SSS = [1, 0, 8, 7, 2, 6, 4, 3, 9, 5]

# g band of ssObjectId 5492494137040128800: 2 points (no degrees of freedom)
# 3e-4 deg apart. J^T J is singular to rounding: inverted in one order
# (H_err = 3e6), and singular, so a failed fit (nObsUsed 0), in the other.
TWO_POINTS = _case(
    [6621.76953125, 6682.1474609375], [170.80953979492188, 161.74453735351562],
    [4.4864821434021, 4.486191272735596], [1.144977331161499, 1.1449769735336304],
    [2.1499478816986084, 2.1499485969543457],
)

# i band of ssObjectId 4914929483702684448: 2 points 2e-6 deg apart; the
# cost is constant over the G12 grid to the last bit or two, so rounding
# picked G12 = 0 in one order and 0.03 in the other.
FLAT = _case(
    [2687.962158203125, 3376.88671875], [449.4222106933594, 497.0403137207031],
    [30.25868797302246, 30.258686065673828], [1.6905497312545776, 1.6905549764633179],
    [2.015220880508423, 2.01522159576416],
)


def _bits(res):
    return np.array(res, dtype=np.float64).tobytes()


def _fit(args, order, fn=photfit.fitHG12, **kw):
    return fn(*(np.asarray(a)[order] for a in args), **(ROBUST | kw))


def _orders(n, seed=0, k=6):
    rng = np.random.default_rng(seed)
    return [np.arange(n), np.arange(n)[::-1]] + [rng.permutation(n) for _ in range(k)]


@pytest.mark.parametrize("case,order", [
    (SINGLE_PHASE, SINGLE_PHASE_SSS), (TWO_POINTS, [1, 0]), (FLAT, [1, 0])])
def test_regression_cases_were_order_sensitive(monkeypatch, case, order):
    """Without the canonical order (the inputs taken as given), these fits
    differ between the dia_sources and SSObservation orders; with it, they
    don't."""
    n = len(case[0])
    monkeypatch.setattr(photfit, "_canonical_order", lambda *a: np.arange(len(a[0])))
    a, b = _fit(case, np.arange(n)), _fit(case, order)
    assert _bits(a) != _bits(b)
    monkeypatch.undo()
    assert _bits(_fit(case, np.arange(n))) == _bits(_fit(case, order))


def test_regression_single_phase_angle_bound_flip(monkeypatch):
    monkeypatch.setattr(photfit, "_canonical_order", lambda *a: np.arange(len(a[0])))
    a, b = _fit(SINGLE_PHASE, np.arange(10)), _fit(SINGLE_PHASE, SINGLE_PHASE_SSS)
    # one interior (with a covariance that is noise), one at the bound
    assert {a.G12 == 1.0, b.G12 == 1.0} == {True, False}
    assert max(a.H_err, b.H_err) > 1e4 * min(a.H_err, b.H_err)


def test_regression_two_points_failure_flip(monkeypatch):
    monkeypatch.setattr(photfit, "_canonical_order", lambda *a: np.arange(len(a[0])))
    a, b = _fit(TWO_POINTS, [0, 1]), _fit(TWO_POINTS, [1, 0])
    assert sorted([a.nobs, b.nobs]) == [0, 2]


def _synthetic(n, seed, single_phase=False):
    rng = np.random.default_rng(seed)
    phase = np.full(n, 12.5) if single_phase else rng.uniform(0.5, 30, n)
    tdist = rng.uniform(0.8, 2.5, n)
    rdist = rng.uniform(1.5, 3.5, n)
    true = photfit.HG12_model(np.deg2rad(phase), [15.0, rng.uniform(0, 1)])
    sig = rng.uniform(0.01, 0.2, n)
    mag = true + 5 * np.log10(tdist * rdist) + rng.normal(0, 1, n) * sig
    if n > 6:
        mag[rng.choice(n, 2, replace=False)] += 3.0     # outliers, for the clipping
        mag[rng.integers(n)] = np.nan                   # dropped by the fit
        k = rng.integers(n - 1)                          # an exact duplicate observation
        mag[k + 1], sig[k + 1], phase[k + 1], tdist[k + 1], rdist[k + 1] = (
            mag[k], sig[k], phase[k], tdist[k], rdist[k])
    return mag, sig, phase, tdist, rdist


@pytest.mark.parametrize("fn", [photfit.fitHG12, photfit._fitHG12_reference])
@pytest.mark.parametrize("kw", [dict(), dict(fixedG12=0.4), dict(nSigmaClip=None), dict(magSigmaFloor=0.0)])
@pytest.mark.parametrize("n,single_phase", [(2, False), (3, False), (5, False), (12, False), (40, False),
                                            (8, True), (2, True)])
def test_permutations_give_identical_fits(fn, kw, n, single_phase):
    for seed in range(4):
        data = _synthetic(n, seed + 10 * n, single_phase)
        ref = None
        for order in _orders(n, seed):
            det = {}
            res = _fit(data, order, fn=fn, _details=det, **kw)
            if ref is None:
                ref, ref_order, ref_det = res, order, det
            assert _bits(res) == _bits(ref), (seed, order)
            if "keep" in det:
                # the clipping mask is in the caller's (kept rows') order
                good = np.isfinite(data[0])
                keep = np.zeros(n, bool)
                keep[np.asarray(order)[good[order]]] = det["keep"]
                ref_keep = np.zeros(n, bool)
                ref_keep[np.asarray(ref_order)[good[ref_order]]] = ref_det["keep"]
                assert np.array_equal(keep, ref_keep)



def test_ties_on_phase_and_mag_are_ordered_by_the_rest():
    """Rows equal in phase angle and magnitude but not in error or
    distance: the canonical order must still be total."""
    for seed in range(20):
        rng = np.random.default_rng(seed)
        k = 4
        phase = np.repeat([3.0, 9.0, 17.0, 26.0], k)
        mag = np.repeat(rng.uniform(19, 21, 4), k)
        sig = rng.uniform(0.01, 0.3, 4 * k)
        tdist = rng.uniform(0.8, 2.5, 4 * k)
        rdist = rng.uniform(1.5, 3.5, 4 * k)
        data = (mag, sig, phase, tdist, rdist)
        ref = _bits(_fit(data, np.arange(4 * k)))
        for order in _orders(4 * k, seed, k=10):
            assert _bits(_fit(data, order)) == ref, seed
