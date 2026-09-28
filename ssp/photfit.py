import numpy as np
from collections import namedtuple
from scipy.interpolate import CubicSpline
from scipy.optimize import leastsq, least_squares, minimize_scalar
import warnings

HG12FitResult = namedtuple(
    "HG12FitResult",
    ["H", "G12", "H_err", "G12_err", "HG_cov", "chi2dof", "nobs"],
)

#Constants

A = [3.332, 1.862]
B = [0.631, 1.218]
C = [0.986, 0.238]

#values taken from sbpy for convenience

alpha_12 = np.deg2rad([7.5, 30., 60, 90, 120, 150])

phi_1_sp = [7.5e-1, 3.3486016e-1, 1.3410560e-1, 5.1104756e-2, 2.1465687e-2, 3.6396989e-3]
phi_1_derivs = [-1.9098593, -9.1328612e-2]

phi_2_sp = [9.25e-1, 6.2884169e-1, 3.1755495e-1, 1.2716367e-1, 2.2373903e-2, 1.6505689e-4]
phi_2_derivs = [-5.7295780e-1, -8.6573138e-8]

alpha_3 = np.deg2rad([0.0, 0.3, 1., 2., 4., 8., 12., 20., 30.])

phi_3_sp = [
    1., 8.3381185e-1, 5.7735424e-1, 4.2144772e-1,
    2.3174230e-1, 1.0348178e-1, 6.1733473e-2,
    1.6107006e-2, 0.
]

phi_3_derivs = [-1.0630097, 0]


phi_1 = CubicSpline(alpha_12, phi_1_sp, bc_type=((1,phi_1_derivs[0]),(1,phi_1_derivs[1])))
phi_2 = CubicSpline(alpha_12, phi_2_sp, bc_type=((1,phi_2_derivs[0]),(1,phi_2_derivs[1])))
phi_3 = CubicSpline(alpha_3, phi_3_sp, bc_type=((1,phi_3_derivs[0]),(1,phi_3_derivs[1])))


def HG_model(phase, params):
    sin_a = np.sin(phase)
    tan_ah = np.tan(phase/2)

    W = np.exp(-90.56 * tan_ah * tan_ah)
    scale_sina = sin_a/(0.119 + 1.341*sin_a - 0.754*sin_a*sin_a)

    phi_1_S = 1 - C[0] * scale_sina
    phi_2_S = 1 - C[1] * scale_sina

    phi_1_L = np.exp(-A[0] * np.power(tan_ah, B[0]))
    phi_2_L = np.exp(-A[1] * np.power(tan_ah, B[1]))

    phi_1 = W * phi_1_S + (1-W) * phi_1_L
    phi_2 = W * phi_2_S + (1-W) * phi_2_L
    return params[0] - 2.5*np.log10((1-params[1])* phi_1 + (params[1]) * phi_2)


def HG1G2_model(phase, params):
    phi_1_ev = phi_1(phase)
    phi_2_ev = phi_2(phase)
    phi_3_ev = phi_3(phase)

    msk = phase < 7.5 * np.pi/180

    phi_1_ev[msk] = 1-6*phase[msk]/np.pi
    phi_2_ev[msk] = 1- 9 * phase[msk]/(5*np.pi)

    phi_3_ev[phase > np.pi/6] = 0


    return params[0] - 2.5 * np.log10(params[1] * phi_1_ev + params[2] * phi_2_ev +
           (1-params[1]-params[2]) * phi_3_ev)

def HG12_model(phase, params):
    if params[1] >= 0.2:
        G1 = +0.9529*params[1] + 0.02162
        G2 = -0.6125*params[1] + 0.5572
    else:
        G1 = +0.7527*params[1] + 0.06164
        G2 = -0.9612*params[1] + 0.6270

    return HG1G2_model(phase, [params[0], G1, G2])

def HG12star_model(phase, params):
    G1 = 0 + params[1] * 0.84293649
    G2 = 0.53513350 - params[1] * 0.53513350

    return HG1G2_model(phase, [params[0], G1, G2])

def _HG1G2_basis(phase):
    """Evaluate the (Phi1, Phi2, Phi3) basis functions of the H,G1,G2
    system, with the same piecewise definitions as ``HG1G2_model``.
    ``phase`` is in radians.
    """
    phi_1_ev = phi_1(phase)
    phi_2_ev = phi_2(phase)
    phi_3_ev = phi_3(phase)

    msk = phase < 7.5 * np.pi/180

    phi_1_ev[msk] = 1-6*phase[msk]/np.pi
    phi_2_ev[msk] = 1- 9 * phase[msk]/(5*np.pi)

    phi_3_ev[phase > np.pi/6] = 0

    return phi_1_ev, phi_2_ev, phi_3_ev

def _HG12_G1G2(G12):
    """Map G12 to (G1, G2) as in ``HG12_model``; also return the
    derivatives dG1/dG12 and dG2/dG12 on the active branch.
    """
    if G12 >= 0.2:
        a1, a2 = +0.9529, -0.6125
        G1 = a1*G12 + 0.02162
        G2 = a2*G12 + 0.5572
    else:
        a1, a2 = +0.7527, -0.9612
        G1 = a1*G12 + 0.06164
        G2 = a2*G12 + 0.6270

    return G1, G2, a1, a2

def _HG12_basis_model(basis, H, G12):
    """HG12 reduced magnitudes from precomputed basis functions (see
    ``_HG1G2_basis``). Returns (mag, F, dF/dG12), where
    mag = H - 2.5 log10(F).
    """
    phi_1_ev, phi_2_ev, phi_3_ev = basis
    G1, G2, a1, a2 = _HG12_G1G2(G12)
    d1 = phi_1_ev - phi_3_ev
    d2 = phi_2_ev - phi_3_ev
    F = phi_3_ev + G1 * d1 + G2 * d2
    return H - 2.5 * np.log10(F), F, a1 * d1 + a2 * d2

def _HG12_residuals_and_jac(basis, mag, magSigma, fixedG12=None):
    """Return (residuals, jac) callables for the HG12 fit of reduced
    magnitudes ``mag``, with residual r = (mag - model) / magSigma.
    The parameters are [H, G12], or [H] if ``fixedG12`` is set.
    """
    def residuals(params):
        G12 = params[1] if fixedG12 is None else fixedG12
        return (mag - _HG12_basis_model(basis, params[0], G12)[0]) / magSigma

    def jac(params):
        # dr/dH = -1/sigma; dr/dG12 = +(2.5/ln 10) (dF/dG12) / (F sigma)
        dH = -1. / magSigma
        if fixedG12 is not None:
            return dH[:, np.newaxis]
        _, F, dF = _HG12_basis_model(basis, params[0], params[1])
        dG = (2.5 / np.log(10)) * dF / (F * magSigma)
        return np.column_stack((dH, dG))

    return residuals, jac

def chi2(params, mag, phase, mag_err, model):
    pred = model(phase, params)
    return (mag - pred)/mag_err

def fit(mag, phase, sigma, model=HG12_model, params=[0.1]):
    phase = np.deg2rad(phase)


    sol = leastsq(chi2, [mag[0]] + params,  (mag, phase, sigma, model), full_output = True)

    return sol

_FAILED = HG12FitResult(*(np.nan,) * 6, nobs=0)

# Coarse grid for the G12 search, and the precision of its refinement.
_G12_GRID = np.linspace(0.0, 1.0, 101)
_G12_XATOL = 1e-6
# Number of the grid's local minima that are refined (the lowest ones).
_G12_NCELLS = 3
# Points checked exactly: the bounds, and the G12 -> (G1, G2) branch point.
_G12_BREAKS = (0.0, 0.2, 1.0)

# IRLS convergence for the profiled robust H (mag), and an iteration cap.
_IRLS_TOL = 1e-9
_IRLS_MAXITER = 500


def _prepare_hg12_inputs(mag, magSigma, phaseAngle, tdist, rdist, magSigmaFloor):
    """Apply the error floor, keep finite magnitudes with positive
    errors, and reduce the magnitudes to 1 AU. Returns (reduced mag,
    magSigma, phase in radians), or None if no observation is left.
    """
    if len(mag) == 0:
        return None

    # ensure these are plain ndarrays
    (mag, magSigma, phaseAngle, tdist, rdist) = map(np.asarray, (mag, magSigma, phaseAngle, tdist, rdist))

    # add systematic error floor in quadrature
    if magSigmaFloor > 0:
        magSigma = np.sqrt(magSigma**2 + magSigmaFloor**2)

    # filter to finite magnitudes and positive errors
    good = (
        np.isfinite(mag) & np.isfinite(magSigma)
        & (magSigma > 0)
    )
    if not good.any():
        return None
    mag = mag[good]
    magSigma = magSigma[good]
    phaseAngle = phaseAngle[good]
    tdist = tdist[good]
    rdist = rdist[good]

    # correct the mag to 1AU distance
    dmag = -5. * np.log10(tdist*rdist)
    return mag + dmag, magSigma, np.deg2rad(phaseAngle)


def _HG12_G1G2_vec(G12):
    """Vectorized ``_HG12_G1G2`` (G1 and G2 only), with the same
    branches and arithmetic.
    """
    hi = G12 >= 0.2
    G1 = np.where(hi, +0.9529*G12 + 0.02162, +0.7527*G12 + 0.06164)
    G2 = np.where(hi, -0.6125*G12 + 0.5572, -0.9612*G12 + 0.6270)
    return G1, G2


class _HG12Profile:
    """The HG12 fit with H profiled out, for a one-parameter search in
    G12.

    For a given G12 the model is linear in H: the residuals are
    r_i = (y_i - H) / sigma_i, with y_i = m_i + 2.5 log10 F_i(G12). The
    best H is then a weighted mean (linear loss) or the IRLS solution
    (soft_l1 loss). The methods return (cost, H) for one G12; the
    ``*_grid`` variants evaluate an array of G12 values at once, as a
    (K, N) array. A cost that isn't finite (F <= 0) is returned as inf.
    """

    def __init__(self, basis, mag, magSigma):
        phi_1_ev, phi_2_ev, phi_3_ev = basis
        self.p3 = phi_3_ev
        self.d1 = phi_1_ev - phi_3_ev
        self.d2 = phi_2_ev - phi_3_ev
        self.mag = mag
        self.w = magSigma**-2.
        self.sw = self.w.sum()
        self.H_last = None  # warm start for the robust H solve

    def y(self, G12):
        G1, G2, _, _ = _HG12_G1G2(G12)
        return self.mag + 2.5 * np.log10(self.p3 + G1 * self.d1 + G2 * self.d2)

    def y_grid(self, G12):
        G1, G2 = _HG12_G1G2_vec(G12[:, np.newaxis])
        return self.mag + 2.5 * np.log10(self.p3 + G1 * self.d1 + G2 * self.d2)

    def linear(self, G12):
        y = self.y(G12)
        H = (self.w @ y) / self.sw
        d = y - H
        cost = self.w @ (d * d)
        return (cost, H) if np.isfinite(cost) else (np.inf, np.nan)

    def linear_grid(self, G12):
        y = self.y_grid(G12)
        H = (y @ self.w) / self.sw
        d = y - H[:, np.newaxis]
        cost = (d * d) @ self.w
        return np.where(np.isfinite(cost), cost, np.inf), H

    def irls(self, y, H):
        """H minimizing sum(soft_l1(r^2)) for each row of y (shape
        (K, N)), starting from H (shape (K,)).

        Iteratively reweighted least squares, with Newton steps: the
        IRLS step alone converges only linearly. The minimum is bracketed
        by the data, and by the sign of the gradient at each iterate; a
        Newton step that leaves the bracket is replaced by the IRLS step
        (a weighted mean, so inside the data), or else by bisection.
        """
        w = self.w
        lo, hi = y.min(axis=1), y.max(axis=1)
        for _ in range(_IRLS_MAXITER):
            d = y - H[:, np.newaxis]
            s = 1. + w * d * d
            wr = w / np.sqrt(s)
            g = (wr * d).sum(axis=1)        # -1/2 d(cost)/dH
            h_irls = wr.sum(axis=1)         # IRLS: the reweighted curvature
            h_newton = (wr / s).sum(axis=1)  # 1/2 d2(cost)/dH2
            lo = np.where(g > 0, H, lo)
            hi = np.where(g < 0, H, hi)
            step = g / h_newton
            out = ~((lo <= H + step) & (H + step <= hi))
            step = np.where(out, g / h_irls, step)
            out &= ~((lo <= H + step) & (H + step <= hi))
            step = np.where(out, 0.5 * (lo + hi) - H, step)
            H = H + step
            if np.max(np.abs(step)) <= _IRLS_TOL:
                break
        return H

    def robust(self, G12):
        """Scalar ``robust_grid``, with the same iteration as ``irls``
        written for one row (it is called ~20 times per search), and
        warm-started from the previous call's H.
        """
        y = self.y(G12)
        w = self.w
        H = (w @ y) / self.sw
        if not np.isfinite(H):
            return np.inf, np.nan
        if self.H_last is not None:
            H = self.H_last
        lo, hi = y.min(), y.max()
        for _ in range(_IRLS_MAXITER):
            d = y - H
            s = 1. + w * d * d
            wr = w / np.sqrt(s)
            g = wr @ d
            if g > 0:
                lo = H
            elif g < 0:
                hi = H
            step = g / (wr @ (1. / s))
            if not lo <= H + step <= hi:
                step = g / wr.sum()
                if not lo <= H + step <= hi:
                    step = 0.5 * (lo + hi) - H
            H += step
            if abs(step) <= _IRLS_TOL:
                break
        self.H_last = H
        d = y - H
        return 2. * (np.sqrt(1. + w * d * d) - 1.).sum(), H

    def robust_grid(self, G12):
        y = self.y_grid(G12)
        cost = np.full(len(y), np.inf)
        H = np.full(len(y), np.nan)
        ok = np.all(np.isfinite(y), axis=1)
        if ok.any():
            y = y[ok]
            Ho = self.irls(y, (y @ self.w) / self.sw)
            d = y - Ho[:, np.newaxis]
            cost[ok] = 2. * (np.sqrt(1. + self.w * d * d) - 1.).sum(axis=1)
            H[ok] = Ho
        return cost, H


def _minimize_G12(cost, cost_grid):
    """Minimize a profiled cost over G12 in [0, 1]: a coarse grid, then
    a bounded scalar refinement of the best grid cells. Returns
    (G12, H, cost), or None if the cost is nowhere finite. A solution
    at a bound is returned as exactly 0.0 or 1.0.
    """
    grid = _G12_GRID
    c, _ = cost_grid(grid)
    if not np.isfinite(c).any():
        return None

    # candidate cells: the grid's local minima, lowest first
    cp = np.concatenate(([np.inf], c, [np.inf]))
    cand = np.flatnonzero((c <= cp[:-2]) & (c <= cp[2:]))
    cand = cand[np.argsort(c[cand], kind="stable")][:_G12_NCELLS]

    best = (np.nan, np.nan, np.inf)
    for k in cand:
        lo, hi = grid[max(k - 1, 0)], grid[min(k + 1, len(grid) - 1)]
        res = minimize_scalar(
            lambda g: cost(g)[0], bounds=(lo, hi), method="bounded",
            options={"xatol": _G12_XATOL},
        )
        # The bounded search never evaluates the ends of its bracket, so
        # check the bounds of [0, 1] explicitly, and the branch point at
        # 0.2: the G12 -> (G1, G2) map is slightly discontinuous there,
        # so the cost jumps, and minima pile up at exactly 0.2.
        for g in [res.x] + [b for b in _G12_BREAKS if lo <= b <= hi]:
            cg, Hg = cost(g)
            if cg < best[2] or (cg == best[2] and g in _G12_BREAKS):
                best = (float(g), float(Hg), float(cg))

    if not np.isfinite(best[2]):
        return None
    return best


def _hg12_result(basis, mag, magSigma, H, G12, fixedG12, chi2_total):
    """Assemble the HG12FitResult of the final (linear-loss) fit at
    (H, G12). Errors come from inv(J^T J) of the fitted parameters; for
    a free G12 at a bound (0 or 1), G12_err and HG_cov are NaN and
    H_err is the fixed-G12 error.
    """
    nobsv = len(mag)
    nparams = 1 if fixedG12 is not None else 2
    x = np.array([H] if fixedG12 is not None else [H, G12])
    _, jac = _HG12_residuals_and_jac(basis, mag, magSigma, fixedG12)
    J = jac(x)
    try:
        cov = np.linalg.inv(J.T @ J)
    except np.linalg.LinAlgError:
        return _FAILED

    if fixedG12 is not None:
        G12, G_err, HG_cov = fixedG12, np.nan, np.nan
        H_err = np.sqrt(cov[0, 0])
    elif G12 == 0.0 or G12 == 1.0:
        G_err, HG_cov = np.nan, np.nan
        H_err = 1. / np.sqrt(np.sum(magSigma**-2.))
    else:
        G_err, HG_cov = np.sqrt(cov[1, 1]), cov[0, 1]
        H_err = np.sqrt(cov[0, 0])

    return HG12FitResult(
        H=H, G12=G12, H_err=H_err, G12_err=G_err,
        HG_cov=HG_cov,
        chi2dof=np.float64(chi2_total) / (nobsv - nparams),
        nobs=nobsv,
    )


def fitHG12(
    mag, magSigma, phaseAngle, tdist, rdist,
    fixedG12=None, magSigmaFloor=0.0, nSigmaClip=None,
    _details=None,
):
    """Fit the HG12 phase curve model (Muinonen et al. 2010).

    Fits absolute magnitude H (and optionally the slope parameter
    G12) to apparent magnitude observations at known phase angles
    and distances. A free G12 is bounded to [0, 1].

    For a given G12 the model is linear in H, so H is profiled out (a
    weighted mean for the least-squares fit, iteratively reweighted
    least squares for the robust one), and G12 is found by a coarse
    grid over [0, 1] followed by a bounded scalar refinement.
    ``_fitHG12_reference`` is the equivalent direct two-parameter
    ``least_squares`` fit, kept for verification.

    Parameters
    ----------
    mag : array_like
        Apparent magnitudes.
    magSigma : array_like
        Magnitude uncertainties (1-sigma).
    phaseAngle : array_like
        Phase angles in degrees.
    tdist : array_like
        Topocentric (observer-target) distances in AU.
    rdist : array_like
        Heliocentric (sun-target) distances in AU.
    fixedG12 : float or None, optional
        If set, fix G12 to this value and only fit H.
        If None (default), both H and G12 (in [0, 1]) are fit.
    magSigmaFloor : float, optional
        Systematic error floor (mag) added in quadrature to
        ``magSigma`` before fitting. Default is 0.0.
    nSigmaClip : float or None, optional
        If set, perform outlier rejection: an initial robust fit
        (soft_l1 loss) followed by sigma clipping at this
        threshold, then a final linear least-squares refit on the
        clipped data. If None (default), no clipping is performed.

    Returns
    -------
    result : `HG12FitResult`
        Named tuple with fields:

        ``H``
            Best-fit absolute magnitude.
        ``G12``
            Best-fit (or fixed) slope parameter.
        ``H_err``
            Uncertainty on H from the covariance matrix. If a free
            G12 ends at a bound (0 or 1), the fixed-G12 error
            ``1/sqrt(sum(1/sigma^2))``.
        ``G12_err``
            Uncertainty on G12 (NaN if ``fixedG12`` is set, or if G12
            is at a bound).
        ``HG_cov``
            H-G12 covariance (NaN if ``fixedG12`` is set, or if G12 is
            at a bound).
        ``chi2dof``
            Reduced chi-squared of the fit.
        ``nobs``
            Number of observations used (after clipping).

        On failure, all float fields are NaN and ``nobs`` is 0.
    """
    prep = _prepare_hg12_inputs(mag, magSigma, phaseAngle, tdist, rdist, magSigmaFloor)
    if prep is None:
        return _FAILED
    mag, magSigma, phase_rad = prep
    nobsv = len(mag)
    nparams = 1 if fixedG12 is not None else 2

    # With fewer observations than parameters J^T J is singular: the
    # unbounded fit failed there through inv(); a bounded one need not.
    if nobsv < nparams:
        return _FAILED

    # The basis functions depend only on the phase angles, so evaluate
    # them once per fit.
    basis = _HG1G2_basis(phase_rad)
    prof = _HG12Profile(basis, mag, magSigma)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        if nSigmaClip is not None and nobsv > nparams + 1:
            # Stage 1: robust fit with soft_l1 loss
            if fixedG12 is not None:
                c, H_r = prof.robust(fixedG12)
                if not np.isfinite(c):
                    return _FAILED
                G_r, H_r = fixedG12, float(H_r)
            else:
                sol = _minimize_G12(prof.robust, prof.robust_grid)
                if sol is None:
                    return _FAILED
                G_r, H_r, _ = sol

            # Sigma clipping on residuals from robust fit
            resid = (prof.y(G_r) - H_r) / magSigma
            keep = np.abs(resid) < nSigmaClip
            if _details is not None:
                _details.update(robust=(H_r, G_r), keep=keep)
            mag = mag[keep]
            magSigma = magSigma[keep]
            phase_rad = phase_rad[keep]
            nobsv = len(mag)

            if nobsv <= nparams:
                return _FAILED

            basis = _HG1G2_basis(phase_rad)
            prof = _HG12Profile(basis, mag, magSigma)

        # Final fit (linear loss for proper chi2/covariance)
        if fixedG12 is not None:
            c, H = prof.linear(fixedG12)
            G, H, chi2_total = fixedG12, float(H), float(c)
        else:
            sol = _minimize_G12(prof.linear, prof.linear_grid)
            if sol is None:
                return _FAILED
            G, H, chi2_total = sol
        if not np.isfinite(chi2_total):
            return _FAILED

        return _hg12_result(basis, mag, magSigma, H, G, fixedG12, chi2_total)


def _fitHG12_reference(
    mag, magSigma, phaseAngle, tdist, rdist,
    fixedG12=None, magSigmaFloor=0.0, nSigmaClip=None,
    _details=None,
):
    """Reference implementation of ``fitHG12``, for tests and
    verification only: direct ``least_squares`` fits of (H, G12), with
    G12 bounded to [0, 1] in both stages. Same parameters and result.
    """
    prep = _prepare_hg12_inputs(mag, magSigma, phaseAngle, tdist, rdist, magSigmaFloor)
    if prep is None:
        return _FAILED
    mag, magSigma, phase_rad = prep
    nobsv = len(mag)

    nparams = 1 if fixedG12 is not None else 2
    if nobsv < nparams:
        return _FAILED
    x0 = np.array(
        [mag[0]] + ([] if fixedG12 is not None else [0.1])
    )
    bounds = (
        (-np.inf, np.inf) if fixedG12 is not None
        else ([-np.inf, 0.], [np.inf, 1.])
    )

    basis = _HG1G2_basis(phase_rad)
    residuals, jac = _HG12_residuals_and_jac(basis, mag, magSigma, fixedG12)

    # fit, suppressing warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        if nSigmaClip is not None and nobsv > nparams + 1:
            # Stage 1: robust fit with soft_l1 loss
            sol_robust = least_squares(
                residuals, x0, jac=jac, bounds=bounds,
                loss='soft_l1', f_scale=1.0,
            )
            if not sol_robust.success:
                return _FAILED

            # Sigma clipping on residuals from robust fit
            resid = residuals(sol_robust.x)
            keep = np.abs(resid) < nSigmaClip
            if _details is not None:
                G_r = fixedG12 if fixedG12 is not None else sol_robust.x[1]
                _details.update(robust=(sol_robust.x[0], G_r), keep=keep)
            mag = mag[keep]
            magSigma = magSigma[keep]
            phase_rad = phase_rad[keep]
            nobsv = len(mag)

            if nobsv <= nparams:
                return _FAILED

            # Redefine residuals (and basis) for clipped data
            basis = _HG1G2_basis(phase_rad)
            residuals, jac = _HG12_residuals_and_jac(basis, mag, magSigma, fixedG12)

            x0 = sol_robust.x

        # Final fit (linear loss for proper chi2/covariance)
        sol = least_squares(residuals, x0, jac=jac, bounds=bounds, loss='linear')

        if not sol.success:
            return _FAILED

        chi2_total = np.sum(sol.fun ** 2)

        # Covariance from Jacobian: cov = inv(J^T J)
        J = sol.jac
        try:
            cov = np.linalg.inv(J.T @ J)
        except np.linalg.LinAlgError:
            return _FAILED

        H = sol.x[0]
        if fixedG12 is not None:
            G = fixedG12
            H_err = np.sqrt(cov[0, 0])
            G_err = np.nan
            HG_cov = np.nan
        elif sol.active_mask[1] != 0:
            # G12 at a bound
            G = sol.x[1]
            H_err = 1. / np.sqrt(np.sum(magSigma**-2.))
            G_err = np.nan
            HG_cov = np.nan
        else:
            G = sol.x[1]
            H_err = np.sqrt(cov[0, 0])
            G_err = np.sqrt(cov[1, 1])
            HG_cov = cov[0, 1]

        return HG12FitResult(
            H=H, G12=G, H_err=H_err, G12_err=G_err,
            HG_cov=HG_cov,
            chi2dof=chi2_total / (nobsv - nparams),
            nobs=nobsv,
        )


####################

def phase_angle_deg(r_obj_sun, r_obs_sun):
    """
    Compute phase angle (Sun–Object–Observer) in degrees.

    Parameters
    ----------
    r_obj_sun : array, shape (3,) or (3, N)
        Object position vector wrt Sun (Sun → object).
    r_obs_sun : array, shape (3,) or (3, N)
        Observer position vector wrt Sun (Sun → observer).

    Returns
    -------
    float or ndarray
        Phase angle(s) in degrees, in [0, 180].
    """
    r_obj_sun = np.asarray(r_obj_sun)
    r_obs_sun = np.asarray(r_obs_sun)

    # Vectors at the object
    v_sun = -r_obj_sun                    # object → Sun
    v_obs = r_obs_sun - r_obj_sun         # object → observer

    # Dot products and norms along axis 0
    dot = np.sum(v_sun * v_obs, axis=0)
    norm_sun = np.linalg.norm(v_sun, axis=0)
    norm_obs = np.linalg.norm(v_obs, axis=0)

    cosang = dot / (norm_sun * norm_obs)
    cosang = np.clip(cosang, -1.0, 1.0)

    return np.degrees(np.arccos(cosang))

def hg_V_mag(H, G, r, delta, phase_deg):
    """
    Compute apparent V magnitude from the IAU H–G system.

    Parameters
    ----------
    H : float or ndarray
        Absolute magnitude (V-band).
    G : float or ndarray
        Slope parameter.
    r : float or ndarray
        Heliocentric distance in AU.
    delta : float or ndarray
        Observer distance (Δ) in AU.
    phase_deg : float or ndarray
        Phase angle in degrees.
    """
    a = np.radians(phase_deg) / 2.0

    # Phase functions
    phi1 = np.exp(-3.33 * np.tan(a)**0.63)
    phi2 = np.exp(-1.87 * np.tan(a)**1.22)

    phi = (1 - G) * phi1 + G * phi2

    return H + 5*np.log10(r * delta) - 2.5*np.log10(phi)
