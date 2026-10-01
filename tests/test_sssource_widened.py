"""The widened SSSource (docs/design/sssource-widened.md): build_sssource on
small synthetic inputs (with a stand-in for ASSIST and the observatory
state, so no network nor ephemeris files), and the writer's casts and
checks."""

from types import SimpleNamespace

import astropy.units as u
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from ssp import sssource
from ssp.sssource import (
    EPHEMERIS_COLUMNS, MEASUREMENT_COLUMNS, build_sssource, cast_column, sort_indices, sssource_schema,
    sssource_table,
)
from ssp.sssource_contract import (
    ID_SPLIT, MATCH_METHODS, SSSOURCE_DICTIONARY, SSSOURCE_NONNULL, SSSourceDtype,
)

# --------------------------------------------------------------------------
# Synthetic inputs
# --------------------------------------------------------------------------

# (designation, packed, permid, has an orbit)
OBJECTS = {
    "A": ("2025 AA1", "K25A01A", None, True),
    "B": ("2024 BB2", "K24B02B", None, True),
    "C": ("2025 CC3", "K25C03C", None, False),    # #7: no mpc_orbits row
    "D": ("2023 DD4", "K23D04D", "12345", True),  # numbered, no provid in obs_sbn
}

# One row per obs_sbn row: (object or None for an 'I' row, measuredOn,
# processing, obssubid suffix, match, primary)
ROWS = [
    ("A", "difference", "DP2-DS", "", "id", True),
    ("A", "difference", "DP2-DS", "-A", "id", True),     # a trail pair
    ("A", "difference", "DP2-DS", "-B", "id", False),
    ("A", "difference", "AP-DS", "", "id", True),
    ("B", "science", "NV-S", "", "id", True),            # a Source
    ("B", "difference", "DP2-DS", "", "position", True),
    ("B", "difference", "RFL-DS", "-A", "position", True),  # a lone -A: by position
    ("C", "difference", "DP2-DS", "", "id", True),
    ("C", "science", "AP-S", "", "id", True),
    ("D", "difference", "pDP2-DS", "", "id", True),
    ("D", "difference", "pDP2-DS", "", "id", False),     # a repeat submission
    (None, "difference", "DP2-DS", "", "id", True),
    (None, "science", "NV-S", "", "id", True),
    (None, "difference", "AP-DS", "", "id", True),
]


def _measurement(name, n, rng, measured_on):
    """A block-4 column as extract-submitted-sources writes it (the view's
    widened types: double, int64/int32, bool, string)."""
    dt = SSSourceDtype[name]
    if name == "band":
        return pa.array(rng.choice(list("gri"), n))
    if name == "reliabilityVersion":
        return pa.array([None if k % 3 else "v1.0" for k in range(n)], type=pa.string())
    if dt.kind == "b":
        return pa.array(rng.random(n) < 0.5)
    if dt.kind == "i":
        v = rng.integers(0, 1000, n)
        if name == "visit":
            v = v + 2025_0601_00000
        mask = None if name in SSSOURCE_NONNULL else (np.arange(n) % 4 == 1)
        return pa.array(v, type=pa.int32() if name == "detector" else pa.int64(), mask=mask)
    v = rng.normal(100, 30, n)
    if name in ("psfFlux", "psfFluxErr"):
        v = np.abs(v) + 1
    elif name in NAN_COLUMNS:
        v[NAN_ROWS] = np.nan            # (a NaN value, not a NULL: kept as is)
    # nullable columns get some NULLs; the Source-only apertures are NULL
    # on DiaSource rows
    mask = np.arange(n) % 5 == 2
    if name.startswith("ap0") or name.startswith("ap25"):
        mask = measured_on == "difference"
    return pa.array(v, mask=None if name in SSSOURCE_NONNULL else mask)


#: Block-4 float columns with NaN values (at NAN_ROWS; their NULLs are at
#: rows 2, 7, 12) -- a copied NaN stays NaN, a copied NULL stays NULL.
NAN_COLUMNS = ("snr", "apFlux", "trailRa")
NAN_ROWS = [0, 6]


def make_inputs(path, seed=0, match_method=False, **overrides):
    """Write dia_sources, obs_sbn, the identification tables and mpc_orbits
    for ROWS to ``path``; ``overrides`` replace dia_sources columns."""
    rng = np.random.default_rng(seed)
    n = len(ROWS)
    perm = rng.permutation(n)                     # file order != object order
    rows = [ROWS[k] for k in perm]
    obj = [r[0] for r in rows]
    measured_on = np.array([r[1] for r in rows])
    obsid = [f"obs{k:04d}" for k in perm]
    t = 60800.0 + rng.uniform(0, 30, n)
    ids = 1000 + perm

    dia = {
        "diaSourceId": pa.array(ids, type=pa.int64()),
        "parentId": pa.array(np.where(perm % 2, 0, 77), type=pa.int64(), mask=perm % 3 == 0),
    }
    for c in MEASUREMENT_COLUMNS:
        dia[c] = _measurement(c, n, rng, measured_on)
    dia["midpointMjdTai"] = pa.array(t)
    dia["ra"] = pa.array(rng.uniform(0, 360, n))
    dia["dec"] = pa.array(rng.uniform(-30, 30, n))
    dia.update({
        "processing": pa.array([r[2] for r in rows]),
        "measuredOn": pa.array(measured_on),
        "processingTable": pa.array([f"ssp.t_{r[2].lower().replace('-', '_')}" for r in rows]),
        "hpix29": pa.array(rng.integers(0, 10**9, n)), "cx": pa.array(rng.random(n)),
        "cy": pa.array(rng.random(n)), "cz": pa.array(rng.random(n)),
        "obsid": pa.array(obsid),
        "obssubid": pa.array([f"LSST-{r[2]}-{i}{r[3]}" for r, i in zip(rows, ids)]),
        "submission_id": pa.array([f"2026-06-0{k % 9 + 1}T00:00:00.000_000000{k}" for k in range(n)]),
        "trksub": pa.array([f"RL{k:05d}" for k in range(n)]),
        "trkid": pa.array([f"000000IY{k:04d}" for k in range(n)]),
        "primary": pa.array([r[5] for r in rows]),
        "match": pa.array([r[4] for r in rows]),
        "sep_mas": pa.array(rng.uniform(0, 2, n)), "dt_ms": pa.array(rng.uniform(-3, 3, n)),
        "dmag": pa.array(rng.normal(0, 0.1, n)), "band_ok": pa.array(np.ones(n, bool)),
        "n_pass": pa.array(np.ones(n, np.int32)), "ambiguous": pa.array(np.zeros(n, bool)),
    })
    if match_method:
        mm = ["position" if r[4] == "position" else "obssubid_trail" if r[3] else "obssubid" for r in rows]
        dia["matchMethod"] = pa.array(mm)
    dia.update(overrides)
    pq.write_table(pa.table(dia), path / "dia_sources.parquet")

    provid = [None if o is None or OBJECTS[o][2] else OBJECTS[o][0] for o in obj]
    permid = [OBJECTS[o][2] if o else None for o in obj]
    status = ["I" if o is None else "pP"[k % 2] for k, o in enumerate(obj)]
    obs_sbn = pa.table({
        "obsid": pa.array(obsid[::-1] + ["unrelated"]),        # another order, and an extra row
        "status": pa.array(status[::-1] + ["p"]),
        "provid": pa.array(provid[::-1] + ["2020 ZZ9"], type=pa.string()),
        "permid": pa.array(permid[::-1] + [None], type=pa.string()),
    })
    pq.write_table(obs_sbn, path / "obs_sbn.parquet")

    pq.write_table(pa.table({"permid": ["12345"], "unpacked_primary_provisional_designation": ["2023 DD4"]}),
                   path / "numbered_identifications.parquet")
    desig = [v[0] for v in OBJECTS.values()]
    packed = [v[1] for v in OBJECTS.values()]
    pq.write_table(pa.table({
        "unpacked_primary_provisional_designation": desig,
        "unpacked_secondary_provisional_designation": desig,
        "packed_primary_provisional_designation": packed,
    }), path / "current_identifications.parquet")
    orb = [v for v in OBJECTS.values() if v[3]]
    k = len(orb)
    pq.write_table(pa.table({
        "unpacked_primary_provisional_designation": [v[0] for v in orb],
        "packed_primary_provisional_designation": [v[1] for v in orb],
        "a": np.full(k, 2.5), "q": np.linspace(1.9, 2.4, k), "e": np.full(k, 0.1), "i": np.full(k, 5.0),
        "node": np.full(k, 10.0), "argperi": np.full(k, 20.0), "peri_time": np.full(k, 60000.0),
        "mean_anomaly": np.full(k, 30.0), "epoch_mjd": np.full(k, 60800.0), "h": np.full(k, 17.0),
        "g": np.full(k, 0.15),
    }), path / "mpc_orbits.parquet")
    return pq.read_table(path / "dia_sources.parquet"), obs_sbn


def _fake_ephemerides(provID, ephTimes, mpcorb, ephem, row=None, obs_pos=None, obs_vel=None):
    """A deterministic stand-in for compute_ephemerides_one (no ASSIST)."""
    t = ephTimes.tai.mjd - 60800.0
    k = float(row["q"])

    def vec(a):
        return np.array([a + np.sin(t * k), a * 0.5 + np.cos(t), 0.1 * a + t * 1e-3]) + obs_pos

    return SimpleNamespace(
        ra_deg=(k * 37.0 + t * 0.1) % 360, dec_deg=np.clip(k - 3 + 0.01 * t, -89, 89),
        mu_lon=0.1 * k + 0 * t, mu_lat=-0.05 * t, mu_total=np.hypot(0.1 * k, 0.05 * t),
        helio_pos=vec(k), helio_vel=vec(2 * k) + obs_vel, topo_pos=vec(k - 1), topo_vel=vec(k + 1) - obs_vel,
        phase_angle=np.full(len(t), 3.0 * k), H=float(row["h"]), G=float(row["g"]),
    )


def _fake_observatory(obscode, obstime):
    """A stand-in for util.observatory_barycentric_posvel (no network)."""
    ph = 2 * np.pi * (obstime.tai.mjd - 60800.0) / 365.25
    r = np.stack([np.cos(ph), 0.917 * np.sin(ph), 0.398 * np.sin(ph)])
    return r * u.au, -r * 0.0172 * u.au / u.day


#: The object whose orbit has no usable covariance (NULL ellipse).
NO_COV = "2024 BB2"


class _FakeEllipse:
    """A stand-in for ssp.sssource_ellipse: a covariance for every
    requested designation but NO_COV."""

    loaded = None

    def load_orbit_covariances(self, path, designations, ephem, *, verbose=True):
        _FakeEllipse.loaded = sorted(designations)
        return {d: {"designation": d} for d in designations if d != NO_COV}

    def ephemeris_ellipse(self, orbit, t_assist, obs_pos, topo_pos, ephem):
        r = np.linalg.norm(topo_pos, axis=1)
        return 1e-6 * r, 2e-6 * r, 1e-13 * r


@pytest.fixture
def offline(monkeypatch):
    # (inherited by forked workers)
    monkeypatch.setattr(sssource, "compute_ephemerides_one", _fake_ephemerides)
    monkeypatch.setattr(sssource, "open_ephem", lambda: None)
    monkeypatch.setattr(sssource.util, "observatory_barycentric_posvel", _fake_observatory)
    monkeypatch.setattr(sssource, "_ellipse", _FakeEllipse())


def _build(tmp_path, workers=1, **kw):
    dia, obs_sbn = make_inputs(tmp_path, **kw)
    build_sssource(tmp_path, tmp_path, workers=workers)
    return pq.read_table(tmp_path / "sssource.parquet"), dia, obs_sbn


def _same(a, b):
    """Arrays equal, NULLs in the same places, NaN equal to NaN."""
    if a.type != b.type or not a.is_null().equals(b.is_null()):
        return False
    if pa.types.is_floating(a.type):
        return np.array_equal(a.to_numpy(zero_copy_only=False), b.to_numpy(zero_copy_only=False),
                              equal_nan=True)
    return a.equals(b)


def _by_obsid(table, obsid):
    """Rows of ``table`` in the order of ``obsid``."""
    return table.take(pc.index_in(obsid, value_set=table["obsid"]))


# --------------------------------------------------------------------------
# build_sssource
# --------------------------------------------------------------------------

def test_conformance(tmp_path, offline):
    sss, dia, _ = _build(tmp_path)
    schema = pq.read_schema(tmp_path / "sssource.parquet")
    assert schema.names == list(SSSourceDtype.names)
    assert schema.equals(sssource_schema())
    for f in schema:
        assert f.nullable == (f.name not in SSSOURCE_NONNULL), f.name
        assert pa.types.is_dictionary(f.type) == (f.name in SSSOURCE_DICTIONARY
                                                  and SSSourceDtype[f.name].kind == "U"), f.name
        if SSSourceDtype[f.name].kind == "U":
            vt = f.type.value_type if pa.types.is_dictionary(f.type) else f.type
            assert vt == pa.string(), f.name
        else:
            assert f.type == pa.from_numpy_dtype(SSSourceDtype[f.name]), f.name
    for c in SSSOURCE_NONNULL:
        assert sss[c].null_count == 0, c
    md = pq.ParquetFile(tmp_path / "sssource.parquet").metadata
    assert md.row_group(0).column(0).compression == "ZSTD"
    # one row per dia_sources row, obsid unique
    assert sss.num_rows == dia.num_rows == len(ROWS)
    assert sorted(sss["obsid"].to_pylist()) == sorted(dia["obsid"].to_pylist())


def test_sort_order(tmp_path, offline):
    sss, _, _ = _build(tmp_path)
    oid = sss["ssObjectId"].to_pylist()
    t = sss["midpointMjdTai"].to_pylist()
    obsid = sss["obsid"].to_pylist()
    n_null = sum(o is None for o in oid)
    assert n_null and all(o is None for o in oid[-n_null:])
    keys = [(o, tt, ob) for o, tt, ob in zip(oid[:-n_null], t, obsid)]
    assert keys == sorted(keys)
    nul = list(zip(t[-n_null:], obsid[-n_null:]))
    assert nul == sorted(nul)


def test_identification(tmp_path, offline):
    sss, dia, obs_sbn = _build(tmp_path)
    s = _by_obsid(sss, dia["obsid"])
    o = _by_obsid(obs_sbn, dia["obsid"])
    by_row = {ob: r for ob, r in zip(dia["obsid"].to_pylist(), s.to_pylist())}
    status = dict(zip(o["obsid"].to_pylist(), o["status"].to_pylist()))
    rows = {f"obs{k:04d}": ROWS[k] for k in range(len(ROWS))}
    packed = {"A": "K25A01A", "B": "K24B02B", "D": "K23D04D"}
    for ob, r in by_row.items():
        obj = rows[ob][0]
        assert r["status"] == status[ob]
        if obj is None:                                  # 'I' rows: unmatched
            assert r["status"] == "I"
            assert r["ssObjectId"] is None and r["designation"] is None
            assert r["ephRa"] is None
        elif obj == "C":                                 # #7: designated, no orbit
            assert r["ssObjectId"] is None and r["designation"] == "2025 CC3"
        else:
            expect = int(np.frombuffer(packed[obj].rjust(8).encode(), dtype="<u8")[0])
            assert r["ssObjectId"] == expect and r["designation"] == OBJECTS[obj][0]
    # no orbit: NULL orbit-derived columns, but the measured ones are filled
    # (and diaDistanceRank, never computed, is today's 0)
    assert set(s["diaDistanceRank"].to_pylist()) == {0}
    for c in EPHEMERIS_COLUMNS:
        if c == "diaDistanceRank":
            continue
        col = s[c].to_pylist()
        for ob, v in zip(dia["obsid"].to_pylist(), col):
            if rows[ob][0] in (None, "C") and c not in sssource.MEASURED_EPH_COLUMNS:
                assert v is None, (c, ob)
            if c in sssource.MEASURED_EPH_COLUMNS or rows[ob][0] not in (None, "C"):
                if c in sssource.ELLIPSE_COLUMNS and OBJECTS[rows[ob][0]][0] == NO_COV:
                    assert v is None, (c, ob)     # (no usable covariance)
                else:
                    assert v is not None, (c, ob)
    # covariances are loaded for the objects with an orbit only
    assert _FakeEllipse.loaded == sorted(v[0] for v in OBJECTS.values() if v[3])


def test_id_split(tmp_path, offline):
    sss, dia, _ = _build(tmp_path)
    s = _by_obsid(sss, dia["obsid"])
    mo = dia["measuredOn"].to_pylist()
    assert "science" in mo and "difference" in mo
    for k, kind in enumerate(mo):
        idc, parc = ID_SPLIT[kind]
        (oidc, oparc), = [v for kk, v in ID_SPLIT.items() if kk != kind]
        assert s[idc][k].as_py() == dia["diaSourceId"][k].as_py()
        assert s[parc][k].as_py() == dia["parentId"][k].as_py()
        assert s[oidc][k].as_py() is None and s[oparc][k].as_py() is None


@pytest.mark.parametrize("passthrough", [False, True])
def test_match_method(tmp_path, offline, passthrough):
    sss, dia, _ = _build(tmp_path, match_method=passthrough)
    s = _by_obsid(sss, dia["obsid"])
    rows = {f"obs{k:04d}": ROWS[k] for k in range(len(ROWS))}
    expect = ["position" if rows[ob][4] == "position" else "obssubid_trail" if rows[ob][3] else "obssubid"
              for ob in dia["obsid"].to_pylist()]
    assert s["matchMethod"].cast(pa.string()).to_pylist() == expect
    assert set(expect) == set(MATCH_METHODS)


def test_match_method_passthrough_is_checked(tmp_path, offline):
    dia, _ = make_inputs(tmp_path, match_method=True)
    mm = dia["matchMethod"].to_pylist()
    mm[0] = "telepathy"
    make_inputs(tmp_path, match_method=True, matchMethod=pa.array(mm))
    with pytest.raises(ValueError, match="telepathy"):
        build_sssource(tmp_path, tmp_path)


def test_match_method_passthrough_wins(tmp_path, offline):
    # (when present, matchMethod is taken as is, not derived)
    dia, _ = make_inputs(tmp_path, match_method=True)
    mm = ["position"] * dia.num_rows
    make_inputs(tmp_path, match_method=True, matchMethod=pa.array(mm))
    build_sssource(tmp_path, tmp_path)
    assert set(pq.read_table(tmp_path / "sssource.parquet")["matchMethod"].to_pylist()) == {"position"}


def test_copied_columns(tmp_path, offline):
    sss, dia, _ = _build(tmp_path)
    s = _by_obsid(sss, dia["obsid"])
    for c in MEASUREMENT_COLUMNS + sssource.LINK_COLUMNS + sssource.MEASURED_ON_COLUMNS:
        got = s[c].combine_chunks()
        if pa.types.is_dictionary(got.type):
            got = got.cast(pa.string())
        assert _same(got, pc.cast(dia[c], got.type, safe=False).combine_chunks()), c
    # what isn't in SSSourceDtype is dropped
    for c in sssource.DIA_DROPPED:
        assert c not in sss.column_names


def test_copied_nan_stays_nan_and_null_stays_null(tmp_path, offline):
    sss, dia, _ = _build(tmp_path)
    s = _by_obsid(sss, dia["obsid"])
    for c in NAN_COLUMNS:
        src_nan = pc.fill_null(pc.is_nan(dia[c]), False).to_numpy(zero_copy_only=False)
        src_null = dia[c].is_null().to_numpy(zero_copy_only=False)
        assert src_nan.sum() == len(NAN_ROWS) and src_null.any(), c     # (both present)
        got = s[c]
        assert np.array_equal(got.is_null().to_numpy(zero_copy_only=False), src_null), c
        assert np.array_equal(pc.fill_null(pc.is_nan(got), False).to_numpy(zero_copy_only=False), src_nan), c


def test_parallel_identical(tmp_path, offline):
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    a, _, _ = _build(tmp_path / "a", workers=1)
    b, _, _ = _build(tmp_path / "b", workers=3)
    assert a.schema.equals(b.schema)
    for c in a.column_names:            # (Table.equals has NaN != NaN)
        assert _same(a[c].combine_chunks(), b[c].combine_chunks()), c


def test_non_null_failure(tmp_path, offline):
    make_inputs(tmp_path)
    ra = pq.read_table(tmp_path / "dia_sources.parquet")["psfFluxErr"].to_pylist()
    ra[3] = None
    make_inputs(tmp_path, psfFluxErr=pa.array(ra))
    with pytest.raises(ValueError, match="'psfFluxErr' is non-null"):
        build_sssource(tmp_path, tmp_path)


def test_overflow_failure(tmp_path, offline):
    det = np.arange(len(ROWS), dtype=np.int32)
    det[5] = 40_000                                 # detector is a short
    make_inputs(tmp_path, detector=pa.array(det))
    with pytest.raises(ValueError, match="'detector'"):
        build_sssource(tmp_path, tmp_path)


def test_duplicate_obs_sbn_obsid_fails(tmp_path, offline):
    _, obs_sbn = make_inputs(tmp_path)
    pq.write_table(pa.concat_tables([obs_sbn, obs_sbn.slice(3, 1)]), tmp_path / "obs_sbn.parquet")
    with pytest.raises(ValueError, match="obs_sbn.parquet: obsid is not unique"):
        build_sssource(tmp_path, tmp_path)


def test_duplicate_dia_obsid_fails(tmp_path, offline):
    dia, _ = make_inputs(tmp_path)
    pq.write_table(pa.concat_tables([dia, dia.slice(3, 1)]), tmp_path / "dia_sources.parquet")
    with pytest.raises(ValueError, match="dia_sources.parquet: obsid is not unique"):
        build_sssource(tmp_path, tmp_path)


def test_orphan_dia_sources_row_fails(tmp_path, offline):
    dia, obs_sbn = make_inputs(tmp_path)
    orphan = dia["obsid"][0].as_py()
    keep = pc.not_equal(obs_sbn["obsid"], orphan)
    pq.write_table(obs_sbn.filter(keep), tmp_path / "obs_sbn.parquet")
    with pytest.raises(ValueError, match="1 dia_sources.parquet rows have no obs_sbn row"):
        build_sssource(tmp_path, tmp_path)


def test_designated_status_i_fails(tmp_path, offline):
    _, obs_sbn = make_inputs(tmp_path)
    status = obs_sbn["status"].to_pylist()
    k = obs_sbn["provid"].to_pylist().index("2025 AA1")
    status[k] = "I"
    obs_sbn = obs_sbn.set_column(obs_sbn.schema.get_field_index("status"), "status", pa.array(status))
    pq.write_table(obs_sbn, tmp_path / "obs_sbn.parquet")
    with pytest.raises(ValueError, match="status 'I' rows have a provid or permid"):
        build_sssource(tmp_path, tmp_path)


def test_requires_extractor_output(tmp_path, offline):
    dia, _ = make_inputs(tmp_path)
    pq.write_table(dia.drop_columns(["measuredOn"]), tmp_path / "dia_sources.parquet")
    with pytest.raises(ValueError, match="measuredOn"):
        build_sssource(tmp_path, tmp_path)


# --------------------------------------------------------------------------
# The writer's casts and checks
# --------------------------------------------------------------------------

def test_cast_narrowing_overflow():
    assert cast_column("detector", pa.array([1, 32767, -32768], pa.int64())).type == pa.int16()
    with pytest.raises(ValueError, match="'detector'"):
        cast_column("detector", pa.array([1, 32768], pa.int64()))
    with pytest.raises(ValueError, match="'psfNdata'"):
        cast_column("psfNdata", pa.array([2**31], pa.int64()))
    with pytest.raises(ValueError, match="'bboxSize'"):
        cast_column("bboxSize", pa.array([1.5]))      # (float -> int truncation)


def test_cast_float_rounding_and_overflow():
    x = pa.array([1.2345678901234567, None, float("nan"), float("inf")])
    out = cast_column("raErr", x)
    assert out.type == pa.float32()
    assert out.to_pylist()[0] == float(np.float32(1.2345678901234567))
    assert out.null_count == 1 and np.isnan(out[2].as_py()) and out[3].as_py() == float("inf")
    with pytest.raises(ValueError, match="'raErr'.*overflow"):
        cast_column("raErr", pa.array([1e300]))


def test_cast_non_null():
    assert cast_column("ra", pa.array([1.0, 2.0])).null_count == 0
    with pytest.raises(ValueError, match="'ra' is non-null"):
        cast_column("ra", pa.array([1.0, None]))
    with pytest.raises(ValueError, match="'status' is non-null"):
        cast_column("status", pa.array(["p", None]))
    assert cast_column("raErr", pa.array([1.0, None])).null_count == 1


def test_cast_strings():
    out = cast_column("band", pa.array(["r", "g", "r"]))
    assert out.type == pa.dictionary(pa.int32(), pa.string())
    assert out.cast(pa.string()).to_pylist() == ["r", "g", "r"]
    assert cast_column("trksub", pa.array(["RL0001", None])).type == pa.string()
    with pytest.raises(ValueError, match="'band'.*longer"):
        cast_column("band", pa.array(["rr"]))
    # an already dictionary-encoded column passes through
    assert cast_column("band", out).equals(out)


def test_sssource_table_columns():
    with pytest.raises(ValueError, match="missing"):
        sssource_table({"obsid": pa.array(["a"])})


def test_sort_indices_nulls_last():
    oid = pa.array([5, None, 3, 5, None, 3], pa.int64())
    t = pa.array([2.0, 1.0, 9.0, 1.0, 0.5, 9.0])
    obsid = pa.array(["a", "b", "z", "c", "d", "y"])
    assert sort_indices(oid, t, obsid).tolist() == [5, 2, 3, 0, 4, 1]


def test_derive_match_method():
    match = pa.array(["id", "id", "id", "position", "position", "id"])
    obssubid = pa.array(["LSST-DP2-DS-1", "LSST-DP2-DS-2-A", "LSST-DP2-DS-2-B", "x-A", None, "9"])
    assert sssource._derive_match_method(match, obssubid).to_pylist() == [
        "obssubid", "obssubid_trail", "obssubid_trail", "position", "position", "obssubid"]
    # (surrounding whitespace doesn't hide the suffix)
    assert sssource._derive_match_method(pa.array(["id"]), pa.array(["LSST-DP2-DS-2-B "])).to_pylist() == [
        "obssubid_trail"]
    # an unknown match value gives NULL (which the non-null check then rejects)
    assert sssource._derive_match_method(pa.array(["huh"]), pa.array(["1"])).to_pylist() == [None]
