"""WP1: ssp.nearbysso.orbits.load_orbits.

Runs offline. The Sun's state comes from a fake ephemeris unless
SSP_ASSIST_* is set; the tests on real rows use the mpc_orbits fixture and
are skipped when it's absent.
"""

import json
import os
from types import SimpleNamespace

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from astropy.time import Time

from ssp import ephem_assist as ea
from ssp.nearbysso import orbits as O
from ssp.nearbysso._contract import ORBIT_DTYPE

FIXTURE = "/lscratch/mjuric/sspwt/nearbysso/fixtures/mpc_orbits.2026-09-26.parquet"
NSAMPLE = 30_000


class FakeEphem:
    """get_particle(0, t): a Sun whose state depends on t, so the per-epoch
    mapping is exercised."""

    def get_particle(self, body, t):
        assert body == ea.ASSIST_SUN
        return SimpleNamespace(x=1e-3 * np.sin(t), y=2e-3 * np.cos(t), z=1e-4 * t / 1e4,
                               vx=1e-6 * np.cos(t), vy=-2e-6 * np.sin(t), vz=1e-8)


def _sun(ephem, t):
    p = ephem.get_particle(ea.ASSIST_SUN, t)
    return np.array([p.x, p.y, p.z]), np.array([p.vx, p.vy, p.vz])


def _epoch_like_ephem_assist(epoch_tt_mjd):
    """As ea.compute_ephemerides_one converts one epoch."""
    return Time(float(epoch_tt_mjd), format="mjd", scale="tt").tdb.mjd - ea.MJD_J2000


def _ephem():
    if os.environ.get("SSP_ASSIST_PLANETS") and os.environ.get("SSP_ASSIST_ASTEROIDS"):
        return ea.open_ephem()
    return FakeEphem()


# --------------------------------------------------------------------------
# Synthetic rows
# --------------------------------------------------------------------------

def _cov_json(C):
    """A CAR covariance dict for a 6x6 C, in the MPC 10x10 layout."""
    return {f"cov{i}{j}": (float(C[i, j]) if i < 6 and j < 6 else None)
            for i in range(10) for j in range(i, 10)}


def _orbit_json(C, arc, state=(1.0, 2.0, 0.1, -0.007, 0.008, -0.0006), layout="mpc"):
    car = {"covariance": _cov_json(C), "eigenvalues": [1.0] * 6,
           "coefficient_names": ["x", "y", "z", "vx", "vy", "vz"],
           "coefficient_values": list(state)}
    stats = {"nopp": 1, "arc_length_total": arc}
    if layout == "mpc":
        return json.dumps({"CAR": car, "COM": {}, "orbit_fit_statistics": stats})
    # a different key order and no spaces: must go through the slow path
    return json.dumps({"orbit_fit_statistics": stats, "COM": {}, "CAR": car}, separators=(",", ":"))


def _random_cov(rng, scale=(1e-6, 1e-6, 1e-6, 1e-8, 1e-8, 1e-8)):
    A = rng.normal(size=(6, 6))
    s = np.asarray(scale)
    return (A @ A.T + 6 * np.eye(6)) * s[:, None] * s[None, :]


def _synthetic_table(rng):
    good = _random_cov(rng)
    notpd = good.copy()
    notpd[0, 1] = notpd[1, 0] = 10 * np.sqrt(notpd[0, 0] * notpd[1, 1])
    rows = [
        # designation, packed, json, q (None: missing)
        ("2000 AA", "K00A00A", _orbit_json(good, "2001-2020"), 2.0),        # kept
        ("C/2024 G7", "CK24G070", _orbit_json(good, "2001-2020"), 2.0),     # comet ('/')
        ("2025 OF623", "_PO001I", _orbit_json(good, "2001-2020"), 2.0),     # comet ('_')
        ("2001 BB", "K01B00B", _orbit_json(good, "2001-2020"), None),       # elements missing
        ("2002 CC", "K02C00C", _orbit_json(good, "2 days"), 2.0),           # short arc
        ("2002 CD", "K02C00D", _orbit_json(good, "0 days"), 2.0),           # short arc
        ("2002 CE", "K02C00E", _orbit_json(good, None), 2.0),               # no arc: dropped
        ("1999 ZZ", "J99Z00Z", _orbit_json(good, "3 days"), 2.0),           # kept
        ("1998 YY", "J98Y00Y", _orbit_json(notpd, "3 days"), 2.0),          # kept, not PD
        ("1997 XX", "J97X00X", None, 2.0),                                  # kept, no JSON
        ("1996 WW", "J96W00W", "{}", 2.0),                                  # kept, no CAR
        ("1995 VV", "J95V00V", _orbit_json(good, "10 days", layout="other"), 2.0),  # slow path
        ("1993 TT", "J93T00T", _orbit_json(good, 0), 2.0),                  # kept: arc is the number 0
        ("1994 UU", "J94U00U", _orbit_json(good, "10 days"), 50.0),         # kept, hyperbolic
    ]
    n = len(rows)
    q = [r[3] for r in rows]
    e = [0.2] * n
    e[-1] = 1.3
    cols = {
        "unpacked_primary_provisional_designation": [r[0] for r in rows],
        "packed_primary_provisional_designation": [r[1] for r in rows],
        "mpc_orb_jsonb": [r[2] for r in rows],
        "q": q, "e": e,
        "i": list(rng.uniform(0, 30, n)), "node": list(rng.uniform(0, 360, n)),
        "argperi": list(rng.uniform(0, 360, n)),
        "peri_time": list(60000 + rng.uniform(-500, 500, n)),
        "epoch_mjd": [61000.0, 60800.0] * (n // 2) + [61000.0] * (n % 2),
        "h": [15.0] * n, "g": [0.15] * n,
        "normalized_rms": list(rng.uniform(0.2, 1.2, n)),
        "arc_length_total": [None] * n,     # as the real column mostly is; unused
    }
    return pa.table(cols), good, notpd


def test_filter_masks():
    desig = ["2000 AA", "C/2024 G7", "P/2010 A2", "2025 OF623", "2001 BB", "2002 CC", "2002 CD", "2002 CE",
             "1999 ZZ"]
    packed = ["K00A00A", "CK24G070", "PK10A020", "_PO001I", "K01B00B", "K02C00C", "K02C00D", "K02C00E",
              "J99Z00Z"]
    el = {k: np.ones(len(desig)) for k in O.ELEMENTS}
    el["peri_time"][4] = np.nan
    arc = ['"2001-2020"', '"4 days"', '"4 days"', '"4 days"', '"4 days"', '"2 days"', '"1 days"', None,
           '"3 days"']
    not_comet, has_el, long_arc = O.filter_masks(desig, packed, el, arc)
    np.testing.assert_array_equal(not_comet, [1, 0, 0, 0, 1, 1, 1, 1, 1])
    np.testing.assert_array_equal(has_el, [1, 1, 1, 1, 0, 1, 1, 1, 1])
    np.testing.assert_array_equal(long_arc, [1, 1, 1, 1, 1, 0, 0, 0, 1])
    # JSON null and "0 days" drop; the number 0 (MPC "no_orbit" statistics)
    # is kept, as in get-mpcorb.py's text comparison
    _, _, la = O.filter_masks(["a"] * 4, ["a"] * 4, {k: np.ones(4) for k in O.ELEMENTS},
                              ["null", '"0 days"', "0", '"1 days"'])
    np.testing.assert_array_equal(la, [0, 0, 1, 0])


def test_load_synthetic(tmp_path, capsys):
    rng = np.random.default_rng(42)
    table, good, notpd = _synthetic_table(rng)
    path = tmp_path / "orbits.parquet"
    pq.write_table(table, path)
    ephem = FakeEphem()

    out = O.load_orbits(path, ephem=ephem)
    line = capsys.readouterr().out
    # rows without an arc (no JSON, or none in it) drop, as in get-mpcorb.py
    assert "14 rows read" in line and "removed 2 comets, 1 missing elements, 5 arcs <= 2 d" in line
    assert "6 kept" in line and "has_cov false 1 (0 missing, 1 not PD)" in line

    assert out.dtype == ORBIT_DTYPE
    assert list(out["designation"]) == ["1993 TT", "1994 UU", "1995 VV", "1998 YY", "1999 ZZ", "2000 AA"]
    assert list(out["has_cov"]) == [True, True, True, False, True, True]
    assert list(out["packed"]) == ["J93T00T", "J94U00U", "J95V00V", "J98Y00Y", "J99Z00Z", "K00A00A"]

    Ceq = O.R6 @ good @ O.R6.T
    for r in out[out["has_cov"]]:
        np.testing.assert_allclose(r["cov0"], Ceq, rtol=1e-12, atol=0)
        np.testing.assert_array_equal(r["cov0"], r["cov0"].T)
    assert np.all(np.isnan(out["cov0"][~out["has_cov"]]))

    # states: the same as the per-row function, with the same Sun
    for r in out:
        t0 = _epoch_like_ephem_assist(r["epoch_mjd"])
        assert r["epoch"] == t0
        X, V = ea.elements_row_to_bary_icrf(r, *_sun(ephem, t0))
        np.testing.assert_allclose(r["state0"], np.concatenate([X, V]), rtol=1e-12, atol=1e-15)

    # without the filter, every row, and the incomputable states are NaN
    allrows = O.load_orbits(path, with_filter=False, ephem=ephem, verbose=False)
    assert len(allrows) == 14
    assert np.all(allrows["designation"][:-1] < allrows["designation"][1:])
    bad = allrows[allrows["designation"] == "2001 BB"][0]
    assert np.all(np.isnan(bad["state0"]))
    has = dict(zip(allrows["designation"], allrows["has_cov"]))
    assert not has["1997 XX"] and not has["1996 WW"] and has["2002 CE"]


def test_rotation_preserves_eigenvalues():
    rng = np.random.default_rng(3)
    C = np.stack([_random_cov(rng) for _ in range(20)])
    Ceq = O.rotate_cov_to_equatorial(C)
    np.testing.assert_allclose(np.linalg.eigvalsh(Ceq), np.linalg.eigvalsh(C), rtol=1e-10)
    np.testing.assert_array_equal(Ceq, np.swapaxes(Ceq, 1, 2))
    # R6 rotates the state as ecliptic_to_equatorial does
    v = rng.normal(size=6)
    np.testing.assert_allclose(O.R6 @ v, np.concatenate([ea.ecliptic_to_equatorial(v[:3]),
                                                         ea.ecliptic_to_equatorial(v[3:])]), rtol=1e-15)


def test_cholesky_pivots_match_numpy():
    rng = np.random.default_rng(5)
    A = np.stack([_random_cov(rng, scale=[1.0] * 6) for _ in range(50)])
    A[::3, 0, 1] = A[::3, 1, 0] = 50.0          # not PD
    piv = O.cholesky_pivots(A)
    for a, p in zip(A, piv):
        try:
            L = np.linalg.cholesky(a)
            np.testing.assert_allclose(p, np.diag(L) ** 2, rtol=1e-12)
        except np.linalg.LinAlgError:
            assert not np.all(p > 0)


def test_is_positive_definite():
    rng = np.random.default_rng(6)
    good = np.stack([_random_cov(rng) for _ in range(4)])
    # numerically singular: rank 5, plus rounding-level noise
    B = rng.normal(size=(6, 5))
    sing = B @ B.T * 1e-12
    sing = sing + 1e-16 * np.max(sing) * np.diag(rng.uniform(-1, 1, 6))
    neg = good[0].copy()
    neg[2, 3] = neg[3, 2] = 2 * np.sqrt(neg[2, 2] * neg[3, 3])
    nan = good[1].copy()
    nan[4, 5] = nan[5, 4] = np.nan
    zero = good[2].copy()
    zero[1, :] = zero[:, 1] = 0
    pd = O.is_positive_definite(np.stack([*good, sing, neg, nan, zero]))
    np.testing.assert_array_equal(pd, [1, 1, 1, 1, 0, 0, 0, 0])


def test_vectorized_kepler_matches_scalar():
    rng = np.random.default_rng(7)
    n = 500
    e = np.concatenate([rng.uniform(0, 0.99, n - 50), rng.uniform(1.01, 5, 50)])
    q = rng.uniform(0.1, 40, n)
    inc, Om, om = rng.uniform(0, np.pi, n), rng.uniform(0, 2 * np.pi, n), rng.uniform(0, 2 * np.pi, n)
    dt = rng.uniform(-5000, 5000, n)
    X, V = O.cometary_to_helio_ecliptic_vec(q, e, inc, Om, om, dt)
    for k in range(n):
        x, v = ea.cometary_to_helio_ecliptic(q[k], e[k], inc[k], Om[k], om[k], dt[k])
        np.testing.assert_allclose(X[k], x, rtol=1e-12, atol=1e-13 * np.linalg.norm(x))
        np.testing.assert_allclose(V[k], v, rtol=1e-12, atol=1e-13 * np.linalg.norm(v))


# --------------------------------------------------------------------------
# Real rows
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def sample(tmp_path_factory):
    if not os.path.exists(FIXTURE):
        pytest.skip("mpc_orbits fixture not present")
    batch = next(pq.ParquetFile(FIXTURE).iter_batches(batch_size=NSAMPLE))
    path = tmp_path_factory.mktemp("orbits") / "sample.parquet"
    table = pa.Table.from_batches([batch])
    pq.write_table(table, path)
    ephem = _ephem()
    out = O.load_orbits(path, ephem=ephem)
    return SimpleNamespace(table=table, out=out, ephem=ephem, path=path)


def test_real_ordering_and_filter(sample):
    d = sample.out["designation"]
    assert np.all(d[:-1] < d[1:])                      # sorted, unique
    assert not np.any(np.char.find(d.astype(str), "/") >= 0)
    assert not np.any(np.char.startswith(sample.out["packed"].astype(str), "_"))
    assert 0.9 * NSAMPLE < len(sample.out) < NSAMPLE


def test_real_state_matches_elements_row_to_bary_icrf(sample):
    rng = np.random.default_rng(11)
    rows = sample.out[rng.choice(len(sample.out), 300, replace=False)]
    # plus any hyperbolic ones (the sample's are all comets, so filtered out)
    rows = np.concatenate([rows, sample.out[sample.out["e"] > 1]])
    for r in rows:
        t0 = _epoch_like_ephem_assist(r["epoch_mjd"])
        assert r["epoch"] == t0
        X, V = ea.elements_row_to_bary_icrf(r, *_sun(sample.ephem, t0))
        ref = np.concatenate([X, V])
        np.testing.assert_allclose(r["state0"][:3], X, rtol=1e-12, atol=1e-12 * np.linalg.norm(X))
        np.testing.assert_allclose(r["state0"][3:], V, rtol=1e-12, atol=1e-12 * np.linalg.norm(V))
        assert np.all(np.isfinite(ref))


def test_real_covariance_symmetric_pd(sample):
    o = sample.out[sample.out["has_cov"]]
    assert len(o) > 0.95 * len(sample.out)
    c = o["cov0"]
    np.testing.assert_array_equal(c, np.swapaxes(c, 1, 2))
    for m in c[:: max(1, len(c) // 2000)]:
        np.linalg.cholesky(m)                     # raises if not PD
    assert np.all(np.isnan(sample.out["cov0"][~sample.out["has_cov"]]))


def _parsed(sample):
    return O.parse_car(sample.table["mpc_orb_jsonb"], nthreads=4)


def test_real_fast_parser_matches_json(sample):
    cov, state, arc = _parsed(sample)
    js = sample.table["mpc_orb_jsonb"]
    arc = arc.to_pylist()
    for k in np.random.default_rng(13).choice(len(js), 300, replace=False):
        c2, s2, a2 = O._parse_car_json(js[k].as_py())
        np.testing.assert_array_equal(cov[k], c2)
        np.testing.assert_array_equal(state[k], s2)
        assert arc[k] == a2


def test_real_frame_and_units(sample):
    """CAR is heliocentric ecliptic J2000 in AU, AU/day: its state, rotated,
    is our equatorial heliocentric state; the MPC's CAR "eigenvalues" are the
    signed square roots of our covariance's eigenvalues."""
    cov, state, _ = _parsed(sample)
    t = sample.table
    desig = np.asarray(t["unpacked_primary_provisional_designation"].to_pylist())
    pos = {d: k for k, d in enumerate(desig)}
    o = sample.out[sample.out["has_cov"]]
    idx = np.array([pos[d] for d in o["designation"]])

    helio = O.helio_equatorial_states(o)
    car_eq = state[idx] @ O.R6.T
    rel = np.linalg.norm(car_eq[:, :3] - helio[:, :3], axis=1) / np.linalg.norm(helio[:, :3], axis=1)
    relv = np.linalg.norm(car_eq[:, 3:] - helio[:, 3:], axis=1) / np.linalg.norm(helio[:, 3:], axis=1)
    assert np.percentile(rel, 99) < 1e-8 and np.percentile(relv, 99) < 1e-8
    # and the covariance we store is that one, rotated
    np.testing.assert_allclose(o["cov0"], O.rotate_cov_to_equatorial(cov[idx]), rtol=0, atol=0)

    js = t["mpc_orb_jsonb"]
    rng = np.random.default_rng(17)
    checked = 0
    for k in rng.choice(idx, 200, replace=False):
        ev = np.array(json.loads(js[int(k)].as_py())["CAR"]["eigenvalues"], dtype=float)
        lam = np.linalg.eigvalsh(cov[k])
        if lam[0] < 1e-10 * lam[-1]:      # ill-conditioned: the small ones are rounding noise
            continue
        np.testing.assert_allclose(np.sqrt(lam), ev, rtol=1e-4)    # ev has 6 digits
        checked += 1
    assert checked > 100


def test_real_car_covariance_is_com_covariance_propagated(sample):
    """Units and layout: CAR's covariance mapped to cometary elements with
    our conversion's Jacobian (q AU, e, angles deg, peri_time d) is COM's.

    Only the (q, e, i, node, argperi) block is compared: for ~20% of rows
    (refits at epochs 61000 and 61200, created 2026-04 and later) COM's
    peri_time variance disagrees with CAR by up to ~2x in sigma while the
    rest matches to ~1e-6; we use CAR."""
    cov, _, _ = _parsed(sample)
    js = sample.table["mpc_orb_jsonb"]
    ep = sample.table["epoch_mjd"].to_numpy(zero_copy_only=False)

    def state(p, epoch):
        q, e, i, node, w, tp = p
        X, V = ea.cometary_to_helio_ecliptic(q, e, np.deg2rad(i), np.deg2rad(node), np.deg2rad(w),
                                             epoch - tp)
        return np.concatenate([X, V])

    rng = np.random.default_rng(19)
    good = full = 0
    for k in rng.choice(len(js), 80, replace=False):
        d = json.loads(js[int(k)].as_py()) if js[int(k)].is_valid else {}
        com = d.get("COM")
        if not com or np.isnan(cov[k]).any():
            continue
        p = np.array(com["coefficient_values"][:6], dtype=float)
        Ccom = np.full((6, 6), np.nan)
        for i in range(6):
            for j in range(i, 6):
                Ccom[i, j] = Ccom[j, i] = com["covariance"][f"cov{i}{j}"]
        sig = np.sqrt(np.diag(Ccom))
        J = np.empty((6, 6))
        for j in range(6):
            h = np.zeros(6)
            h[j] = 1e-4 * sig[j]
            J[:, j] = (state(p + h, ep[k]) - state(p - h, ep[k])) / (2 * h[j])
        Ji = np.linalg.inv(J)
        C = (Ji @ cov[k] @ Ji.T)[:5, :5]
        s = sig[:5]
        np.testing.assert_allclose(np.sqrt(np.diag(C)), s, rtol=1e-3)
        np.testing.assert_allclose(C / np.outer(s, s), Ccom[:5, :5] / np.outer(s, s), atol=1e-3)
        good += 1
        full += bool(np.isclose(np.sqrt((Ji @ cov[k] @ Ji.T)[5, 5]), sig[5], rtol=1e-3))
    assert good > 50 and full > 0.5 * good
