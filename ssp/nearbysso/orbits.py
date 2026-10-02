"""WP1: read and filter mpc_orbits, with states and covariances at epoch.
See ``_contract.ORBIT_DTYPE``.

The state at epoch is the one ``ssp.ephem_assist.elements_row_to_bary_icrf``
gives (the precise pass's), computed here vectorized over all rows.

The covariance is the 6x6 state block of ``mpc_orb_jsonb.CAR.covariance``.
Its layout, as found in the 2026-09-26 snapshot:

- the keys are ``covIJ``, with ``I`` and ``J`` single digits, ``0 <= I <=
  J <= 9``: the upper triangle of a 10x10 matrix, 55 keys (every row has all
  55, in row-major order, ``cov00, cov01, ..., cov09, cov11, ..., cov99``);
- the indices follow ``CAR.coefficient_names``: ``x, y, z, vx, vy, vz``,
  then any non-gravitational parameters (``yarkovsky``, ``A1``, ``A2``...),
  whose entries are null for purely gravitational orbits;
- the frame is heliocentric ecliptic J2000 and the units AU and AU/day, the
  same as ``CAR.coefficient_values`` (which match our ``COM``-derived
  ecliptic state to ~1e-10 relative).

It is rotated to equatorial ICRF with the obliquity of
``ssp.ephem_assist.ecliptic_to_equatorial``: C_eq = R C R^T, with R the
block diagonal of the 3x3 rotation for position and velocity. The helio ->
bary translation leaves the covariance unchanged.

The non-gravitational parameters (``ng_*``) and ``cov_full`` come from
``ssp.nongrav.nongrav_params`` on the ~640 rows whose CAR coefficient names
go on past ``vz`` (a substring test in the same pass over the JSON); see
``fill_nongrav``.

The JSON is not parsed with a JSON library: the ``CAR`` block (always first,
as PostgreSQL's jsonb orders keys) is cut out with a regular expression and
split with Arrow string kernels, in threads. Rows whose JSON doesn't have
the expected layout fall back to ``json.loads``.
"""

from __future__ import annotations

import json
import time
import warnings
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .. import ephem_assist as ea
from .. import nongrav as _ng
from ._contract import ORBIT_DTYPE

#: The element columns an orbit must have (as get-mpcorb.py requires).
ELEMENTS = ["q", "e", "i", "node", "argperi", "peri_time"]

#: The columns read besides mpc_orb_jsonb.
_COLUMNS = [
    "unpacked_primary_provisional_designation",
    "packed_primary_provisional_designation",
    *ELEMENTS, "epoch_mjd", "h", "g", "normalized_rms",
]

#: Arcs dropped by the filter: get-mpcorb.py keeps the orbits whose
#: ``orbit_fit_statistics.arc_length_total`` string is present and not one
#: of these.
_SHORT_ARCS = ['"0 days"', '"1 days"', '"2 days"']

# The CAR block's covariance and state; jsonb puts "CAR" first.
_CAR_RE = (r'^\{"CAR": \{"covariance": \{(?P<c>[^{}]*)\}, "eigenvalues": [^\]]*\], '
           r'"coefficient_names": \[[^\]]*\], "coefficient_values": \[(?P<v>[^\]]*)\]')
# The arc's JSON value, as text: a quoted string, a number or null.
_ARC_RE = r'"orbit_fit_statistics": \{[^{}]*?"arc_length_total": (?P<a>"[^"]*"|[^,}\s]+)'

_NCOV = 10                                               # the matrix is 10x10
_COV_KEYS = [f"cov{i}{j}" for i in range(_NCOV) for j in range(i, _NCOV)]
_COV_POS = {k: n for n, k in enumerate(_COV_KEYS)}
# Where (i, j) of the 6x6 block, i <= j, is in the 55-entry list.
_IU6 = np.triu_indices(6)
_TRI6 = np.array([_COV_POS[f"cov{i}{j}"] for i, j in zip(*_IU6)])

_R3 = np.array([
    [1.0, 0.0, 0.0],
    [0.0, ea._COS_EPS, -ea._SIN_EPS],
    [0.0, ea._SIN_EPS, ea._COS_EPS],
])
#: Ecliptic -> equatorial rotation of a 6-vector state (x, y, z, vx, vy, vz).
R6 = np.zeros((6, 6))
R6[:3, :3] = _R3
R6[3:, 3:] = _R3


# --------------------------------------------------------------------------
# Elements -> state, vectorized
# --------------------------------------------------------------------------

def _solve_kepler_vec(M, e, tol=1e-14, max_iter=50):
    """``ea.solve_kepler`` for arrays of M *and* e: the same iteration, with
    each element stopped when its own step drops below ``tol`` (as the
    scalar solver stops for its one element)."""
    M = np.mod(M + np.pi, 2 * np.pi) - np.pi
    E = M + 0.85 * e * np.sign(np.sin(M))
    act = np.arange(len(M))
    for _ in range(max_iter):
        Ea, ea_, Ma = E[act], e[act], M[act]
        dE = -(Ea - ea_ * np.sin(Ea) - Ma) / (1.0 - ea_ * np.cos(Ea))
        E[act] = Ea + dE
        act = act[np.abs(dE) >= tol]
        if len(act) == 0:
            break
    return E


def _newton_hyperbolic(H, M, e, tol, max_iter):
    """Newton iterations on e sinh H - H = M from ``H`` (in place), each
    element stopped when its own step is below ``tol`` (relative to
    max(1, |H|)). Returns the indices that failed: non-finite, or still
    taking steps above ``_HYP_FAIL`` (relative) after ``max_iter``. (Steps
    between ``tol`` and that are rounding cycles: converged.)"""
    act = np.arange(len(M))
    dH = np.zeros(0)
    with np.errstate(over="ignore", invalid="ignore"):
        for _ in range(max_iter):
            Ha, ea_, Ma = H[act], e[act], M[act]
            dH = -(ea_ * np.sinh(Ha) - Ha - Ma) / (ea_ * np.cosh(Ha) - 1.0)
            H[act] = Ha + dH
            # (a non-finite step fails the test, and stays active)
            moving = ~(np.abs(dH) < tol * np.maximum(1.0, np.abs(H[act])))
            act, dH = act[moving], dH[moving]
            if len(act) == 0:
                break
        failed = ~(np.abs(dH) < _HYP_FAIL * np.maximum(1.0, np.abs(H[act])))
    return act[failed]


# Newton steps still above this (relative) after max_iter: not converged.
_HYP_FAIL = 1e-10


def _solve_kepler_hyperbolic_vec(M, e, tol=1e-14, max_iter=100):
    """``ea.solve_kepler_hyperbolic`` for arrays of M and e, with a fallback.

    The elements that converge from ``ea.solve_kepler_hyperbolic``'s start,
    arcsinh(M / e), get its result. That start fails for e -> 1 at small
    |M| (e.g. e - 1 = 1e-6, q = 0.005 AU, 80 years from perihelion): there
    e cosh H - 1 ~ e - 1, and the first Newton step overflows. Those
    elements restart from sign(M) ln(2 |M| / e + 1.8) (Danby's start; on
    the convex branch, Newton then converges monotonically), and any that
    still don't converge are NaN. (No orbit of the 2026-10-01 catalog needs
    the fallback.)
    """
    H = np.arcsinh(M / e)
    bad = _newton_hyperbolic(H, M, e, tol, max_iter)
    if len(bad):
        Mb, eb = M[bad], e[bad]
        Hb = np.sign(Mb) * np.log(2.0 * np.abs(Mb) / eb + 1.8)
        still = _newton_hyperbolic(Hb, Mb, eb, tol, max_iter)
        Hb[still] = np.nan
        H[bad] = Hb
    return H


def cometary_to_helio_ecliptic_vec(q, e, inc_rad, Omega_rad, omega_rad, dt_peri_days, mu=ea.GM_SUN):
    """Vectorized ``ea.cometary_to_helio_ecliptic``: arrays of shape (N,) in,
    (X, V) of shape (N, 3) out [AU, AU/day], heliocentric J2000 ecliptic.

    Parabolic orbits (|1 - e| < 1e-10), which the scalar version rejects,
    and rows with NaN inputs give NaN states.
    """
    q, e, inc, Om, om, dt = (np.asarray(x, dtype=np.float64) for x in
                             (q, e, inc_rad, Omega_rad, omega_rad, dt_peri_days))
    n_ = len(q)
    x_pf = np.full(n_, np.nan)
    y_pf = np.full(n_, np.nan)
    vx_pf = np.full(n_, np.nan)
    vy_pf = np.full(n_, np.nan)

    ok = np.isfinite(q) & np.isfinite(e) & np.isfinite(dt) & (np.abs(1.0 - e) >= 1e-10)
    a = q / (1.0 - e)
    with np.errstate(invalid="ignore"):
        n = np.sqrt(mu / np.abs(a) ** 3)
    M = n * dt

    ell = np.flatnonzero(ok & (e < 1.0))
    if len(ell):
        a_, e_, n_e = a[ell], e[ell], n[ell]
        E = _solve_kepler_vec(M[ell], e_)
        cosE, sinE = np.cos(E), np.sin(E)
        Edot = n_e / (1.0 - e_ * cosE)
        b = a_ * np.sqrt(1.0 - e_ * e_)
        x_pf[ell] = a_ * (cosE - e_)
        y_pf[ell] = b * sinE
        vx_pf[ell] = -a_ * sinE * Edot
        vy_pf[ell] = b * cosE * Edot

    hyp = np.flatnonzero(ok & (e > 1.0))
    if len(hyp):
        e_, n_h = e[hyp], n[hyp]
        H = _solve_kepler_hyperbolic_vec(M[hyp], e_)
        coshH, sinhH = np.cosh(H), np.sinh(H)
        Hdot = n_h / (e_ * coshH - 1.0)
        aa = -a[hyp]
        b = aa * np.sqrt(e_ * e_ - 1.0)
        x_pf[hyp] = aa * (e_ - coshH)
        y_pf[hyp] = b * sinhH
        vx_pf[hyp] = -aa * sinhH * Hdot
        vy_pf[hyp] = b * coshH * Hdot

    # The first two columns of ea._perifocal_to_ecliptic (z_pf is 0).
    cosO, sinO = np.cos(Om), np.sin(Om)
    cosw, sinw = np.cos(om), np.sin(om)
    cosi, sini = np.cos(inc), np.sin(inc)
    P = np.stack([cosO * cosw - sinO * sinw * cosi,
                  sinO * cosw + cosO * sinw * cosi,
                  sinw * sini], axis=1)
    Q = np.stack([-cosO * sinw - sinO * cosw * cosi,
                  -sinO * sinw + cosO * cosw * cosi,
                  cosw * sini], axis=1)
    X = P * x_pf[:, None] + Q * y_pf[:, None]
    V = P * vx_pf[:, None] + Q * vy_pf[:, None]
    return X, V


def epoch_to_assist(epoch_tt_mjd):
    """ASSIST time of TT MJD epochs, as ``ea.compute_ephemerides_one`` does
    it (astropy TT -> TDB, minus MJD of J2000), evaluated once per distinct
    epoch."""
    from astropy.time import Time
    from erfa import ErfaWarning

    epoch_tt_mjd = np.asarray(epoch_tt_mjd, dtype=np.float64)
    out = np.full(epoch_tt_mjd.shape, np.nan)
    ok = np.isfinite(epoch_tt_mjd)
    if ok.any():
        uniq, inv = np.unique(epoch_tt_mjd[ok], return_inverse=True)
        with warnings.catch_warnings():
            # "dubious year" for epochs past the leap-second table; TT -> TDB
            # doesn't depend on it
            warnings.simplefilter("ignore", ErfaWarning)
            tdb = Time(uniq, format="mjd", scale="tt").tdb.mjd
        out[ok] = (tdb - ea.MJD_J2000)[inv]
    return out


def sun_states(t_assist, ephem):
    """The Sun's barycentric ICRF state (N, 6) from ASSIST at ASSIST times,
    queried once per distinct time. NaN times give NaN states."""
    t_assist = np.asarray(t_assist, dtype=np.float64)
    out = np.full((len(t_assist), 6), np.nan)
    ok = np.isfinite(t_assist)
    if ok.any():
        uniq, inv = np.unique(t_assist[ok], return_inverse=True)
        s = np.empty((len(uniq), 6))
        for k, t in enumerate(uniq):
            p = ephem.get_particle(ea.ASSIST_SUN, float(t))
            s[k] = (p.x, p.y, p.z, p.vx, p.vy, p.vz)
        out[ok] = s[inv]
    return out


def helio_equatorial_states(rows):
    """Heliocentric equatorial (ICRF-oriented) states (N, 6) from the
    elements of ``rows`` (anything indexable by column name), as
    ``ea.elements_row_to_bary_icrf`` computes them before adding the Sun."""
    X, V = cometary_to_helio_ecliptic_vec(
        rows["q"], rows["e"], np.deg2rad(rows["i"]), np.deg2rad(rows["node"]),
        np.deg2rad(rows["argperi"]), np.asarray(rows["epoch_mjd"]) - np.asarray(rows["peri_time"]),
    )
    # ea.ecliptic_to_equatorial, row-wise
    return np.concatenate([X @ _R3.T, V @ _R3.T], axis=1)


# --------------------------------------------------------------------------
# The CAR block
# --------------------------------------------------------------------------

_EXPECTED = {"arr": pa.array([], type=pa.string())}


def _expected_keys(nrows):
    """The key prefixes of ``nrows`` rows of the fast layout, as one Arrow
    array (built once, and sliced; concurrent rebuilds are harmless)."""
    arr = _EXPECTED["arr"]
    need = nrows * len(_COV_KEYS)
    if len(arr) < need:
        arr = pa.array([f'"{k}": ' for k in _COV_KEYS] * max(nrows, 20_000), type=pa.string())
        _EXPECTED["arr"] = arr
    return arr.slice(0, need)


def _to_float(strings):
    """Arrow strings to float64 numpy; "null" and nulls become NaN."""
    strings = pc.if_else(pc.equal(strings, "null"), pa.scalar(None, pa.string()), strings)
    return pc.fill_null(pc.cast(strings, pa.float64()), np.nan).to_numpy(zero_copy_only=False)


def _parse_car_json(s):
    """The slow path for one JSON string: (cov (6, 6), state (6,), arc)."""
    cov = np.full((6, 6), np.nan)
    state = np.full(6, np.nan)
    arc = None
    try:
        d = json.loads(s)
    except (TypeError, ValueError):
        return cov, state, arc
    a = (d.get("orbit_fit_statistics") or {}).get("arc_length_total")
    arc = None if a is None else json.dumps(a)
    car = d.get("CAR") or {}
    c = car.get("covariance") or {}
    for i, j in zip(*_IU6):
        v = c.get(f"cov{i}{j}")
        if v is not None:
            cov[i, j] = cov[j, i] = float(v)
    vals = car.get("coefficient_values") or []
    if len(vals) >= 6 and all(v is not None for v in vals[:6]):
        state[:] = np.asarray(vals[:6], dtype=np.float64)
    return cov, state, arc


def _parse_car_chunk(arr):
    """Parse one Arrow string array of mpc_orb_jsonb.

    Returns (cov (n, 6, 6) upper triangle filled into both halves, NaN where
    missing; state (n, 6), the CAR coefficient_values, NaN where missing;
    arc, an Arrow string array of the JSON arc_length_total values as text:
    strings still quoted, numbers, "null", or null when absent).
    """
    n = len(arr)
    cov = np.full((n, 6, 6), np.nan)
    state = np.full((n, 6), np.nan)

    arc = pc.struct_field(pc.extract_regex(arr, _ARC_RE), "a")
    m = pc.extract_regex(arr, _CAR_RE)
    c = pc.struct_field(m, "c")
    items = pc.split_pattern(c, ", ")
    fast = pc.fill_null(pc.equal(pc.list_value_length(items), len(_COV_KEYS)), False)
    fast_idx = np.flatnonzero(fast.to_numpy(zero_copy_only=False))
    if len(fast_idx):
        flat = pc.list_flatten(pc.filter(items, fast))
        keys = pc.utf8_slice_codeunits(flat, 0, 9)
        good = pc.all(pc.equal(keys, _expected_keys(len(fast_idx)))).as_py()
        if good:
            v = _to_float(pc.utf8_slice_codeunits(flat, 9)).reshape(len(fast_idx), len(_COV_KEYS))
            tri = v[:, _TRI6]
            blk = np.empty((len(fast_idx), 6, 6))
            blk[:, _IU6[0], _IU6[1]] = tri
            blk[:, _IU6[1], _IU6[0]] = tri
            cov[fast_idx] = blk
        else:
            fast_idx = fast_idx[:0]
    # the state
    vals = pc.split_pattern(pc.struct_field(m, "v"), ", ")
    vfast = pc.fill_null(pc.greater_equal(pc.list_value_length(vals), 6), False)
    vfast_np = vfast.to_numpy(zero_copy_only=False)
    vidx = np.flatnonzero(vfast_np)
    if len(vidx):
        v6 = pc.list_flatten(pc.list_slice(pc.filter(vals, vfast), 0, 6))
        state[vidx] = _to_float(v6).reshape(len(vidx), 6)

    # Rows with JSON but not in the expected layout: parse them properly.
    slow = np.ones(n, dtype=bool)
    slow[fast_idx] = False
    slow &= arr.is_valid().to_numpy(zero_copy_only=False)
    fixed = {}
    for k in np.flatnonzero(slow):
        cov[k], st, a = _parse_car_json(arr[k].as_py())
        if not vfast_np[k]:
            state[k] = st
        if a is not None and not arc[k].is_valid:
            fixed[k] = a
    if fixed:
        arc = pa.array([fixed.get(k, a) for k, a in enumerate(arc.to_pylist())], type=pa.string())
    return cov, state, arc


#: Rows whose JSON contains this have coefficient names after ``vz`` (a
#: non-gravitational fit, ~640 of the 1.5M orbits); only those are parsed by
#: ``ssp.nongrav.nongrav_params``, which makes the final decision.
_NG_MARK = '"vz", "'


def _nongrav_chunk(arr):
    """{row in arr: NonGrav, or the ValueError the parser raised} for the
    rows of an Arrow string array of mpc_orb_jsonb that have a
    non-gravitational fit."""
    cand = pc.fill_null(pc.match_substring(arr, _NG_MARK), False).to_numpy(zero_copy_only=False)
    out = {}
    for k in np.flatnonzero(cand):
        try:
            ng = _ng.nongrav_params(arr[k].as_py())
        except ValueError as e:
            out[int(k)] = e
            continue
        if ng.model:
            out[int(k)] = ng
    return out


def _chunk_job(arr, nongrav):
    res = _parse_car_chunk(arr)
    return (*res, _nongrav_chunk(arr)) if nongrav else res


def _combine(parts, nongrav):
    out = (np.concatenate([p[0] for p in parts]), np.concatenate([p[1] for p in parts]),
           pa.chunked_array([p[2] for p in parts], type=pa.string()))
    if not nongrav:
        return out
    ngd, off = {}, 0
    for p in parts:
        ngd.update({off + k: v for k, v in p[3].items()})
        off += len(p[2])
    return (*out, ngd)


def parse_car(json_strings, nthreads=16, chunk=20_000, nongrav=False):
    """Parse the CAR covariance (6x6 state block, ecliptic), the CAR state
    and the JSON arc_length_total of an array of mpc_orb_jsonb strings, in
    ``nthreads`` threads (Arrow's kernels release the GIL). See
    ``_parse_car_chunk`` for the outputs. With ``nongrav``, a fourth output:
    {row: ssp.nongrav.NonGrav, or the ValueError of an unknown model} for
    the rows with a non-gravitational fit (``_nongrav_chunk``)."""
    arr = json_strings
    if isinstance(arr, pa.Array):
        arrays = [arr]
    elif isinstance(arr, pa.ChunkedArray):
        arrays = arr.chunks          # not combined: > 2 GB overflows string offsets
    else:
        arrays = [pa.array(arr, type=pa.string())]
    # Row order is kept: slices of each chunk, in order.
    pieces = [a.slice(lo, chunk) for a in arrays for lo in range(0, len(a), chunk)]
    if not pieces:
        pieces = [pa.array([], type=pa.string())]
    with ThreadPoolExecutor(max_workers=max(1, nthreads)) as ex:
        parts = list(ex.map(lambda a: _chunk_job(a, nongrav), pieces))
    return _combine(parts, nongrav)


def parse_car_file(path, nthreads=16, chunk=20_000, nongrav=False):
    """``parse_car`` on a Parquet file's mpc_orb_jsonb, streamed: one reader
    thread per row group decompresses batches and hands them to the parsing
    threads, so the ~10 GB of JSON is never in memory at once and reading
    overlaps parsing. Outputs are in file row order."""
    nrg = pq.ParquetFile(path).num_row_groups

    with ThreadPoolExecutor(max_workers=max(1, nthreads)) as ex, \
            ThreadPoolExecutor(max_workers=max(1, min(nrg, nthreads))) as readers:
        def read_rg(rg):
            f = pq.ParquetFile(path)
            return [ex.submit(_chunk_job, b.column(0), nongrav)
                    for b in f.iter_batches(batch_size=chunk, row_groups=[rg],
                                            columns=["mpc_orb_jsonb"], use_threads=False)]
        rg_futs = [readers.submit(read_rg, rg) for rg in range(nrg)]
        parts = [f.result() for rf in rg_futs for f in rf.result()]
    if not parts:
        return parse_car(pa.array([], type=pa.string()), nongrav=nongrav)
    return _combine(parts, nongrav)


def rotate_cov_to_equatorial(cov):
    """C_eq = R C R^T for a stack (N, 6, 6) of ecliptic state covariances,
    symmetrized exactly."""
    c = R6 @ cov @ R6.T
    return 0.5 * (c + np.swapaxes(c, -1, -2))


def cholesky_pivots(a):
    """The Cholesky pivots d_j (L_jj = sqrt(d_j)) of a stack (N, n, n) of
    symmetric matrices, vectorized over N; NaN after the first pivot <= 0.
    All pivots > 0 is positive definiteness (in exact arithmetic), computed
    without LAPACK's per-matrix overhead."""
    n = a.shape[-1]
    L = np.zeros_like(a)
    piv = np.empty(a.shape[:-1])
    with np.errstate(invalid="ignore"):
        for j in range(n):
            d = a[:, j, j] - np.sum(L[:, j, :j] ** 2, axis=1)
            piv[:, j] = d
            d = np.where(d > 0, d, np.nan)
            L[:, j, j] = np.sqrt(d)
            for i in range(j + 1, n):
                L[:, i, j] = (a[:, i, j] - np.sum(L[:, i, :j] * L[:, j, :j], axis=1)) / L[:, j, j]
    return piv


#: Eigenvalues of the correlation matrix in [-PSD_TOL * largest, 0) are
#: rounding noise and are clipped to 0; below that, the matrix is unusable.
#: About 2.7% of the CAR blocks are numerically singular: correlation
#: eigenvalues ~1e-16 (the MPC prints 16 digits), of either sign. Singular
#: but positive semi-definite is fine for C(t) = Phi C0 Phi^T. They are
#: nearly all short arcs with sigma(position) > 1e-3 AU.
PSD_TOL = 1e-9

#: Rows whose correlation Cholesky pivots all exceed this are positive
#: definite (all pivots > 0 proves it; the margin covers rounding, ~1e-15)
#: and skip the eigendecomposition.
_SAFE_PIVOT = 1e-6


def make_psd(cov, tol=PSD_TOL):
    """Check and repair a stack (N, 6, 6) of covariances.

    Works on the correlation matrix (it keeps positions, ~1e-11 AU^2, and
    velocities, ~1e-17 AU^2/d^2, on one scale). Returns (cov, usable,
    clipped):

    - usable: finite, positive diagonal, and no correlation eigenvalue below
      ``-tol`` times the largest;
    - clipped: usable rows that had eigenvalues in [-tol * largest, 0); they
      are set to 0 and the row rebuilt as D V diag(lambda) V^T D, which moves
      the diagonal by ~|clipped eigenvalue| relative;
    - cov: the input, with clipped rows rebuilt and unusable rows NaN.
    """
    cov = np.array(cov, dtype=np.float64, copy=True)
    n = len(cov)
    usable = np.all(np.isfinite(cov), axis=(1, 2))
    d = np.diagonal(cov, axis1=1, axis2=2)
    usable &= np.all(d > 0, axis=1)
    clipped = np.zeros(n, dtype=bool)
    idx = np.flatnonzero(usable)
    if len(idx):
        sd = np.sqrt(d[idx])
        corr = cov[idx] / sd[:, :, None] / sd[:, None, :]
        piv = cholesky_pivots(corr)
        minpiv = np.min(np.where(np.isnan(piv), -np.inf, piv), axis=1)
        chk = np.flatnonzero(~(minpiv > _SAFE_PIVOT))
        if len(chk):
            lam, V = np.linalg.eigh(corr[chk])
            lo = -tol * lam[:, -1]
            bad = np.any(lam < lo[:, None], axis=1)
            clip = ~bad & np.any(lam < 0, axis=1)
            usable[idx[chk[bad]]] = False
            if clip.any():
                lc = np.maximum(lam[clip], 0.0)
                c2 = (V[clip] * lc[:, None, :]) @ np.swapaxes(V[clip], 1, 2)
                s = sd[chk[clip]]
                c2 = c2 * s[:, :, None] * s[:, None, :]
                cov[idx[chk[clip]]] = 0.5 * (c2 + np.swapaxes(c2, 1, 2))
                clipped[idx[chk[clip]]] = True
    cov[~usable] = np.nan
    return cov, usable, clipped


# --------------------------------------------------------------------------
# The filter and the loader
# --------------------------------------------------------------------------

def filter_masks(designation, packed, elements, arc):
    """The get-mpcorb.py filter, as boolean masks that apply in sequence.

    ``elements`` is a dict of the ``ELEMENTS`` arrays (NaN where missing) and
    ``arc`` the JSON ``orbit_fit_statistics.arc_length_total`` values as
    text (None or "null" when absent), a sequence or Arrow array. Returns
    (not_satellite, has_elements, long_arc).

    The arc rule is get-mpcorb.py's SQL, exactly: ``mpc_orb_jsonb->
    'orbit_fit_statistics'->>'arc_length_total' NOT IN ('0 days', '1 days',
    '2 days')``. So:

    - absent or JSON null is SQL NULL, so the row is dropped;
    - the value is compared as text, so multi-opposition arcs ("2007-2021")
      are kept, and so is the number 0 (text '0'), which the MPC writes for
      orbits whose fit statistics are zeroed (``orbit_quality: no_orbit``,
      ~65k rows, all with elements and a covariance).

    The Parquet ``arc_length_total`` column can't be used: it is NULL for a
    third of the orbits, most of them multi-opposition. Here arc values are
    the JSON text, strings still quoted (as ``_parse_car_chunk`` returns
    them).

    Comets are kept (since 2026-10-01, docs/design/nongrav.md): C/, P/,
    D/ and I/ designations, and A/ (asteroids on cometary orbits: inactive,
    but with ordinary orbits, often near-parabolic). Their unpacked primary
    provisional designation is always the provisional one ("P/1991 T1",
    not "145P"), and the element-less placeholders (in the 2026-10-01
    catalog all 18 D/, 2,499 C/, 9 P/ and A/2017 U1) are dropped by
    ``has_elements``; 1,942 comets are kept (1,188 C/, 728 P/, 26 A/).
    Natural satellites, the designations starting "S/" ("S/2004 S 46"),
    are dropped: their elements are heliocentric two-body fits, which
    can't follow the motion about the planet.

    A packed designation starting with "_" is the MPC's extended packed
    format for asteroid provisional designations with a cycle count above
    619 (e.g. ``_FB0088`` = 2015 BE640, ``_PO001I`` = 2025 OF623); no
    comet's or satellite's packed designation starts with "_" (2026-10-01
    catalog). ``packed`` is kept in the signature for callers.
    """
    designation = np.asarray(designation, dtype=str)
    not_satellite = ~np.char.startswith(designation, "S/")
    has_elements = np.ones(len(designation), dtype=bool)
    for k in ELEMENTS:
        has_elements &= np.isfinite(np.asarray(elements[k], dtype=np.float64))
    if not isinstance(arc, (pa.Array, pa.ChunkedArray)):
        arc = pa.array(list(arc), type=pa.string())
    short = pc.or_kleene(pc.is_in(arc, value_set=pa.array(_SHORT_ARCS + ["null"])), pc.is_null(arc))
    long_arc = ~pc.fill_null(short, True).to_numpy(zero_copy_only=False)
    return not_satellite, has_elements, long_arc


def fill_nongrav(out, rows, ngd):
    """Fill ``ng_model``, ``ng_A``, ``ng_fitted`` and ``cov_full`` of
    ``out`` (ORBIT_DTYPE, with state0, cov0 and has_cov already set), whose
    row k came from file row ``rows[k]``; ``ngd`` is ``parse_car``'s
    {file row: NonGrav or ValueError}. See ``_contract`` ("Non-gravitational
    parameters").

    Gravity-only rows get cov0 padded with zeros (NaN where has_cov is
    False), and are otherwise untouched. A non-grav row's cov_full has cov0
    as its state block, the CAR cross terms rotated to equatorial on their
    state side (R6 C_sA), and the A block as fitted; its fitted block (the
    state plus the fitted A's) must pass ``make_psd`` (on the correlation
    matrix, as cov0 does), or has_cov becomes False (and cov0 NaN). A block
    that ``make_psd`` clips to PSD is used as repaired (its state block then
    differs from cov0 by ~PSD_TOL relative). An unknown model (ValueError)
    leaves the row gravity-only. Returns the counts.
    """
    cf = out["cov_full"]             # a view: filled in place
    cf[...] = 0.0
    cf[:, :6, :6] = out["cov0"]
    rows = np.asarray(rows, dtype=np.int64)
    keys = np.array(sorted(ngd), dtype=np.int64)
    inv = np.full(int(max(rows.max(initial=-1), keys.max(initial=-1))) + 1, -1)
    inv[rows] = np.arange(len(rows))
    pos = {int(r): int(inv[r]) for r in keys if inv[r] >= 0}
    counts = dict(comet=0, yarkovsky=0, unparsed=0, cov_missing=0, cov_not_psd=0, cov_clipped=0)
    blocks = {}                      # fitted-block size -> [(k, sel, block)]
    for r, ng in ngd.items():
        k = pos.get(int(r))
        if k is None:
            continue
        if isinstance(ng, Exception):
            counts["unparsed"] += 1
            continue
        counts[ng.model] += 1
        out["ng_model"][k] = ng.model
        out["ng_A"][k] = ng.A
        out["ng_fitted"][k] = ng.fitted
        if not out["has_cov"][k]:
            continue
        if ng.cov is None:
            counts["cov_missing"] += 1
            out["has_cov"][k] = False
            continue
        sel = np.concatenate([np.arange(6), 6 + np.flatnonzero(ng.fitted)])
        C = np.asarray(ng.cov, dtype=np.float64)          # CAR order: state, fitted A's
        b = np.empty((len(sel), len(sel)))
        b[:6, :6] = out["cov0"][k]
        b[:6, 6:] = R6 @ C[:6, 6:]
        b[6:, :6] = b[:6, 6:].T
        b[6:, 6:] = C[6:, 6:]
        blocks.setdefault(len(sel), []).append((k, sel, b))
    for items in blocks.values():
        fixed, usable, clipped = make_psd(np.array([b for _, _, b in items]))
        for (k, sel, _), b, u, c in zip(items, fixed, usable, clipped):
            if not u:
                counts["cov_not_psd"] += 1
                out["has_cov"][k] = False
                continue
            counts["cov_clipped"] += int(c)
            cf[k] = 0.0
            cf[k][np.ix_(sel, sel)] = b
    bad = np.flatnonzero(~out["has_cov"])
    cf[bad] = np.nan
    out["cov0"][bad] = np.nan
    return counts


def _col(table, name):
    col = table[name]
    if pa.types.is_floating(col.type):
        return pc.fill_null(col, np.nan).to_numpy()
    return np.asarray(col.to_pylist(), dtype=object)


def load_orbits(path, with_filter=True, ephem=None, nthreads=16, verbose=True, stats=None):
    """Read mpc_orbits Parquet into an ``ORBIT_DTYPE`` array, sorted by
    designation. See ``_contract``.

    ``ephem`` is an open ASSIST ephemeris for the Sun's state at epoch (opened
    from ``SSP_ASSIST_*`` if None); ``nthreads`` bounds the JSON parsing
    threads. With ``with_filter=False`` every row is kept, and rows whose
    state can't be computed have NaN states. Prints a one-line summary
    unless ``verbose`` is False; a ``stats`` dict, if given, gets its counts.
    """
    t0 = time.perf_counter()
    table = pq.read_table(path, columns=_COLUMNS)
    n_read = table.num_rows

    desig = _col(table, "unpacked_primary_provisional_designation").astype(str)
    packed = _col(table, "packed_primary_provisional_designation").astype(str)
    num = {k: _col(table, k) for k in [*ELEMENTS, "epoch_mjd", "h", "g", "normalized_rms"]}
    # epoch_mjd is required too (it is null exactly where the elements are)
    elements = {k: num[k] for k in ELEMENTS}

    del table
    cov_ecl, _, arc, ngd = parse_car_file(path, nthreads=nthreads, nongrav=True)
    if len(arc) != n_read:
        raise RuntimeError(f"{path}: {len(arc)} JSON rows parsed, {n_read} expected")
    t_parse = time.perf_counter() - t0

    not_sat, has_el, long_arc = filter_masks(desig, packed, elements, arc)
    has_el &= np.isfinite(num["epoch_mjd"])
    if with_filter:
        keep = not_sat & has_el & long_arc
    else:
        keep = np.ones(n_read, dtype=bool)
    counts = dict(
        satellite=int((~not_sat).sum()),
        elements=int((not_sat & ~has_el).sum()),
        arc=int((not_sat & has_el & ~long_arc).sum()),
    )

    idx = np.flatnonzero(keep)
    idx = idx[np.argsort(desig[idx], kind="stable")]
    out = np.zeros(len(idx), dtype=ORBIT_DTYPE)
    out["designation"] = desig[idx]
    out["packed"] = packed[idx]
    for k in [*ELEMENTS, "epoch_mjd", "h", "g", "normalized_rms"]:
        out[k] = num[k][idx]

    # State at epoch
    t2 = time.perf_counter()
    if ephem is None:
        ephem = ea.open_ephem()
    out["epoch"] = epoch_to_assist(out["epoch_mjd"])
    sun = sun_states(out["epoch"], ephem)
    out["state0"] = helio_equatorial_states(out) + sun

    # Covariance
    c = rotate_cov_to_equatorial(cov_ecl[idx])
    missing = ~np.all(np.isfinite(c), axis=(1, 2))
    c, pd_ok, clipped = make_psd(c)
    out["has_cov"] = pd_ok
    out["cov0"] = c
    ngc = fill_nongrav(out, idx, ngd)
    pd_ok = out["has_cov"]
    t_conv = time.perf_counter() - t2

    # 0 is the MPC's placeholder in zeroed ("no_orbit") fit statistics
    rms = out["normalized_rms"]
    n_zero = int(np.sum(rms == 0))
    rms = rms[np.isfinite(rms) & (rms > 0)]
    pct = np.percentile(rms, [5, 50, 95, 99]) if len(rms) else [np.nan] * 4
    if stats is not None:
        stats.update(rows_read=int(n_read), kept=int(len(out)),
                     removed=dict(counts) if with_filter else None,
                     has_cov_false=int((~pd_ok).sum()), cov_missing=int(missing.sum()),
                     cov_not_psd=int((~missing & ~pd_ok).sum()), cov_clipped_to_psd=int(clipped.sum()),
                     nongrav=ngc,
                     normalized_rms=dict(p5=float(pct[0]), p50=float(pct[1]), p95=float(pct[2]),
                                         p99=float(pct[3]), n=int(len(rms)), n_zero=n_zero))
    if verbose:
        removed = (f"removed {counts['satellite']:,} natural satellites, "
                   f"{counts['elements']:,} missing elements, {counts['arc']:,} arcs <= 2 d"
                   if with_filter else "no filter")
        print(f"load_orbits: {n_read:,} rows read, {removed}; {len(out):,} kept; "
              f"has_cov false {int((~pd_ok).sum()):,} ({int(missing.sum()):,} missing, "
              f"{int((~missing & ~pd_ok).sum()):,} not PSD), {int(clipped.sum()):,} clipped to PSD; "
              f"non-grav {ngc['comet']:,} comets and {ngc['yarkovsky']:,} Yarkovsky "
              f"({ngc['unparsed']:,} unknown models kept gravity-only, "
              f"{ngc['cov_missing'] + ngc['cov_not_psd']:,} without a usable full covariance); "
              f"normalized_rms p5/p50/p95/p99 "
              f"{pct[0]:.3f}/{pct[1]:.3f}/{pct[2]:.3f}/{pct[3]:.3f} ({len(rms):,} with it, "
              f"{n_zero:,} zero excluded); "
              f"{time.perf_counter() - t0:.1f} s (read and parse {t_parse:.1f}, "
              f"convert {t_conv:.1f})", flush=True)
    return out
