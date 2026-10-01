"""The widened-SSSource validation harness (bench/sssource_validate), on
small synthetic tables. No network, no fixtures."""
import os
import sys

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bench import sssource_validate as V
from ssp.sssource_contract import ELLIPSE_COLUMNS, SSSOURCE_DICTIONARY

COLS = V.schema_columns()
NAMES = [c["name"] for c in COLS]
SPEC = {c["name"]: c for c in COLS}
B = V.blocks(NAMES)

# Six rows: objects 1 (two rows), 2 (two rows), a #7 row (designated, no
# orbit) and an undesignated status-I row; Source and DiaSource rows.
N = 6
ORBIT = np.array([True, True, True, True, False, False])
MEASURED_ON = ["difference", "science", "difference", "difference", "science", "difference"]
PROCESSING = ["AP-DS", "NV-S", "DP2-DS", "AP-DS", "NV-S", "AP-DS"]
IDS = [101, 202, 303, 404, 505, 606]


def _values():
    d = {}
    for c in COLS:
        name, typ = c["name"], c["datatype"]
        if typ in V.FELIS_STRING:
            d[name] = ["x"] * N
        elif typ in ("float", "double"):
            d[name] = [1.5 + k for k in range(N)]
        elif typ == "boolean":
            d[name] = [False] * N
        else:
            d[name] = list(range(1, N + 1))
    d.update(
        obsid=[f"obs{k:03d}" for k in range(N)],
        status=["p", "P", "p", "p", "p", "I"],
        primary=[True] * N,
        matchMethod=["obssubid", "obssubid_trail", "position", "obssubid", "obssubid", "position"],
        ssObjectId=[1, 1, 2, 2, None, None],
        designation=["2020 AB", "2020 AB", "2021 CD", "2021 CD", "2025 MH352", None],
        measuredOn=MEASURED_ON, processing=PROCESSING, processingTable=["ssp.t"] * N,
        midpointMjdTai=[61000.1, 61000.2, 61000.3, 61000.4, 61000.5, 61000.6],
        band=["r"] * N, reliabilityVersion=["v1"] * N,
    )
    for kind, (idc, parent) in V.ID_SPLIT.items():
        d[idc] = [i if m == kind else None for i, m in zip(IDS, MEASURED_ON)]
        d[parent] = [0 if m == kind else None for m in MEASURED_ON]
    for c in B[6]:
        if c.startswith("eph") or c in ("phaseAngle",) or c.startswith(("topo", "helio")):
            d[c] = [v if o else None for v, o in zip(d[c], ORBIT)]
    d["ephRaErr"] = [1e-5 if o else None for o in ORBIT]
    d["ephDecErr"] = [2e-5 if o else None for o in ORBIT]
    d["ephRa_ephDec_Cov"] = [1e-11 if o else None for o in ORBIT]
    return d


def table(d=None, **overrides):
    d = dict(d or _values())
    d.update(overrides)
    fields, arrays = [], []
    for c in COLS:
        if c["name"] not in d:
            continue
        typ = V.felis_arrow_type(c["datatype"], c["name"] in SSSOURCE_DICTIONARY)
        arr = pa.array(d[c["name"]], typ.value_type if pa.types.is_dictionary(typ) else typ)
        if pa.types.is_dictionary(typ):
            arr = arr.dictionary_encode()
        fields.append(pa.field(c["name"], arr.type, nullable=c.get("nullable", True) or arr.null_count > 0))
        arrays.append(arr)
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def write(t, path, **kw):
    kw.setdefault("compression", "zstd")
    pq.write_table(t, path, **kw)
    return str(path)


def failed(rep):
    return set(rep.failed)


@pytest.fixture
def good(tmp_path):
    return write(table(), tmp_path / "sssource.parquet")


def dia_from(t):
    """A dia_sources.parquet as the extractor writes it: view types (wide),
    the view's id as diaSourceId, parentId."""
    cols = {"obsid": t["obsid"]}
    for c in ("trksub", "trkid", "submission_id", "primary", "measuredOn", "processing", "processingTable",
              *B[4]):
        a = V._flat(t[c])
        if pa.types.is_floating(a.type):
            a = a.cast(pa.float64())
        elif pa.types.is_integer(a.type):
            a = a.cast(pa.int64())
        cols[c] = a
    mid, _ = V._measurement_id(t)
    cols["diaSourceId"] = pa.array(mid, pa.int64())
    parent = [p if p is not None else q for p, q in zip(t["parentDiaSourceId"].to_pylist(),
                                                         t["parentSourceId"].to_pylist())]
    cols["parentId"] = pa.array(parent, pa.int64())
    return pa.table(cols)


# --------------------------------------------------------------------------
# the schema and the comparison rules
# --------------------------------------------------------------------------

def test_blocks_match_design():
    assert len(NAMES) == 181
    assert [len(B[k]) for k in (1, 2, 3, 4, 6)] == [7, 2, 7, 126, 39]
    assert B[4][0] == "visit" and B[4][-1] == "glint_trail"
    assert set(ELLIPSE_COLUMNS) <= set(B[6])


def test_arrow_type_ok():
    assert V.arrow_type_ok("char", pa.string())
    assert V.arrow_type_ok("char", pa.large_string())
    assert V.arrow_type_ok("char", pa.dictionary(pa.int32(), pa.string()))
    assert not V.arrow_type_ok("char", pa.dictionary(pa.int32(), pa.int64()))
    assert V.arrow_type_ok("short", pa.int16()) and not V.arrow_type_ok("short", pa.int32())
    assert V.arrow_type_ok("float", pa.float32()) and not V.arrow_type_ok("float", pa.float64())
    assert V.arrow_type_ok("boolean", pa.bool_())
    assert not V.arrow_type_ok("timestamp", pa.timestamp("us"))


def test_compare_float_narrowing():
    src = pa.array([0.1, 1e30, None, np.nan, 2.0], pa.float64())
    ok = pa.array(np.array([0.1, 1e30, np.nan, np.nan, 2.0], np.float32), pa.float32())
    assert not V.compare(ok, src).any()                        # equal after the cast; NULL == NaN
    off = np.array([0.1, 1e30, np.nan, np.nan, 2.0], np.float32)
    off[0] = np.nextafter(off[0], np.float32(1))
    assert list(V.compare(pa.array(off), src)) == [True, False, False, False, False]
    # same type: exact
    assert V.compare(pa.array([0.1]), pa.array([0.1 + 1e-17 * 0 + 2 ** -55])).all()
    # a value where the source is NULL
    assert V.compare(pa.array([1.0], pa.float32()), pa.array([None], pa.float64())).all()


def test_compare_ints_bools_strings():
    assert not V.compare(pa.array([1, None], pa.int16()), pa.array([1, None], pa.int64())).any()
    assert list(V.compare(pa.array([1, 0], pa.int16()), pa.array([1, None], pa.int64()))) == [False, True]
    assert list(V.compare(pa.array([2 ** 16 - 1], pa.int32()), pa.array([-1], pa.int64()))) == [True]
    assert list(V.compare(pa.array([True, False]), pa.array([True, None]))) == [False, True]
    s = pa.array(["a", "b", None]).dictionary_encode()
    assert list(V.compare(s, pa.array(["a", "c", None]))) == [False, True, False]


def test_bitwise_mismatch():
    a = np.array([1.0, np.nan, 3.0])
    b = a.copy()
    b.view(np.uint64)[2] ^= 1
    mism, rd = V.bitwise_mismatch(pa.array(a), pa.array(b))
    assert list(mism) == [False, False, True]
    mism, rd = V.bitwise_mismatch(pa.array([1.0, None]), pa.array([1.0, np.nan]))
    assert not mism.any() and rd == 1
    with pytest.raises(TypeError):
        V.bitwise_mismatch(pa.array([1.0], pa.float32()), pa.array([1.0], pa.float64()))


def test_stratified_sample():
    strata = np.array(["a"] * 100 + ["b"] * 5 + ["c"] * 50)
    rows = V.stratified_sample(strata, 60, np.random.default_rng(0))
    got = {g: int(np.sum(strata[rows] == g)) for g in "abc"}
    assert got == {"a": 28, "b": 5, "c": 27} and len(set(rows)) == 60
    assert len(V.stratified_sample(strata, 1000, np.random.default_rng(0))) == len(strata)


# --------------------------------------------------------------------------
# conformance
# --------------------------------------------------------------------------

def test_conformance_pass(good):
    rep = V.check_conformance(good)
    assert rep.ok, rep.text()


def _conf(tmp_path, t, **kw):
    return failed(V.check_conformance(write(t, tmp_path / "bad.parquet", **kw)))


def test_conformance_order(tmp_path):
    t = table()
    names = t.column_names
    names[3], names[4] = names[4], names[3]
    assert "column names and order" in _conf(tmp_path, t.select(names))
    assert "column names and order" in _conf(tmp_path, t.drop_columns(["glint_trail"]))


def test_conformance_type(tmp_path):
    t = table()
    t = t.set_column(NAMES.index("detector"), "detector", t["detector"].cast(pa.int32()))
    assert "Arrow types match the Felis datatypes" in _conf(tmp_path, t)


def test_conformance_nulls(tmp_path):
    d = _values()
    d["ra"] = [None] + d["ra"][1:]
    assert "no NULLs in nullable: false columns" in _conf(tmp_path, table(d))
    # declared nullable although the YAML says not
    t = table()
    t = t.cast(pa.schema([f.with_nullable(True) for f in t.schema]))
    assert failed(V.check_conformance(write(t, tmp_path / "n.parquet"))) == {
        "Arrow field nullability matches the YAML"}


def test_conformance_obsid_unique(tmp_path):
    d = _values()
    d["obsid"][1] = d["obsid"][0]
    assert "obsid unique" in _conf(tmp_path, table(d))


def test_conformance_char_length(tmp_path):
    d = _values()
    d["status"] = ["pp"] + d["status"][1:]
    assert "char values fit their length" in _conf(tmp_path, table(d))


def test_conformance_sort(tmp_path):
    t = table()
    assert "sort order" in _conf(tmp_path, t.take([1, 0, 2, 3, 4, 5]))     # time within an object
    assert "sort order" in _conf(tmp_path, t.take([4, 0, 1, 2, 3, 5]))     # NULL ssObjectId first
    assert "sort order" in _conf(tmp_path, t.take([2, 3, 0, 1, 4, 5]))     # ssObjectId


def test_conformance_categories(tmp_path):
    d = _values()
    d["matchMethod"] = ["id"] + d["matchMethod"][1:]
    assert "matchMethod values in MATCH_METHODS" in _conf(tmp_path, table(d))
    d = _values()
    d["measuredOn"] = ["direct"] + d["measuredOn"][1:]
    assert "measuredOn values in ID_SPLIT" in _conf(tmp_path, table(d))


def test_conformance_id_split(tmp_path):
    d = _values()
    d["sourceId"][0], d["diaSourceId"][0] = d["diaSourceId"][0], None       # difference row, id in sourceId
    assert "id split by measuredOn" in _conf(tmp_path, table(d))
    d = _values()
    d["parentDiaSourceId"][1] = 7                # science row with a DiaSource parent
    assert "id split by measuredOn" in _conf(tmp_path, table(d))


def test_conformance_ssobjectid(tmp_path):
    d = _values()
    d["ssObjectId"][4] = 3                       # the #7 row (no orbit) has an id
    assert "ssObjectId NULL exactly when no designation or no orbit" in _conf(tmp_path, table(d))
    d = _values()
    d["ssObjectId"][2:4] = [None, None]          # designated, with an orbit, NULL
    f = _conf(tmp_path, table(d))
    assert "ssObjectId NULL exactly when no designation or no orbit" in f
    d = _values()
    d["ssObjectId"][5] = 9
    d["designation"][5] = ""                     # blank designation counts as none
    f = _conf(tmp_path, table(d))
    assert {"ssObjectId NULL exactly when no designation or no orbit",
            "status 'I' rows have NULL ssObjectId"} <= f
    d = _values()
    d["ssObjectId"][2:4] = [1, 1]                # two designations, one id
    assert "ssObjectId <-> designation one-to-one" in _conf(tmp_path, table(d))


def test_conformance_eph_together(tmp_path):
    d = _values()
    d["ephVmag"][4] = 17.0                       # an eph* value where ephRa is NULL
    assert "eph* all NULL where ephRa is NULL" in _conf(tmp_path, table(d))


def test_conformance_ellipse(tmp_path):
    d = _values()
    d["ephRaErr"][4] = 1e-5
    assert "ellipse NULL where ephRa is NULL" in _conf(tmp_path, table(d))
    d = _values()
    d["ephRa_ephDec_Cov"][0] = 1e-9              # |cov| > raErr * decErr
    assert "ellipse sane (NULL together, errors > 0, |cov| <= raErr*decErr)" in _conf(tmp_path, table(d))
    d = _values()
    d["ephDecErr"][0] = None                     # partially NULL
    assert "ellipse sane (NULL together, errors > 0, |cov| <= raErr*decErr)" in _conf(tmp_path, table(d))


def test_conformance_primary(tmp_path):
    d = _values()
    d["primary"][0] = False
    assert "one primary row per measurement (processing, id)" in _conf(tmp_path, table(d))
    d = _values()
    d["diaSourceId"][2] = d["diaSourceId"][0]
    d["processing"][2] = d["processing"][0]      # two primary rows of one measurement
    assert "one primary row per measurement (processing, id)" in _conf(tmp_path, table(d))


def test_conformance_parquet_layout(tmp_path):
    assert "zstd compression" in _conf(tmp_path, table(), compression="snappy")
    assert "SSSOURCE_DICTIONARY columns dictionary-encoded" in _conf(tmp_path, table(), use_dictionary=False)


# --------------------------------------------------------------------------
# copied (dia_sources) and clickhouse (a fake fetch)
# --------------------------------------------------------------------------

def test_copied_pass(tmp_path, good):
    dia = write(dia_from(table()), tmp_path / "dia.parquet")
    rep = V.check_copied(good, dia)
    assert rep.ok, rep.text()


def test_copied_fail(tmp_path, good):
    t = dia_from(table())
    i = t.column_names.index("psfFlux")
    bad = t.set_column(i, "psfFlux", pa.array([1.5 + 1e-6] + t["psfFlux"].to_pylist()[1:]))
    assert "block 4 equal (exact, float64->float32 after the cast)" in failed(
        V.check_copied(good, write(bad, tmp_path / "d1.parquet")))
    i = t.column_names.index("diaSourceId")
    bad = t.set_column(i, "diaSourceId", pa.array([999] + t["diaSourceId"].to_pylist()[1:]))
    assert "id split values (diaSourceId/parentId -> the measuredOn pair)" in failed(
        V.check_copied(good, write(bad, tmp_path / "d2.parquet")))
    i = t.column_names.index("processingTable")
    bad = t.set_column(i, "processingTable", pa.array(["other"] * N))
    assert "block 3 (measuredOn, processing, processingTable) equal" in failed(
        V.check_copied(good, write(bad, tmp_path / "d3.parquet")))
    assert "same obsid set as dia_sources" in failed(
        V.check_copied(good, write(t.slice(1), tmp_path / "d4.parquet")))
    assert "block 4 equal (exact, float64->float32 after the cast): source has every column" in failed(
        V.check_copied(good, write(t.drop_columns(["glint_trail"]), tmp_path / "d5.parquet")))


def _view(t):
    v = dia_from(t).rename_columns(["id" if c == "diaSourceId" else c for c in dia_from(t).column_names])
    return v.append_column("hpix29", pa.array(np.arange(N), pa.int64()))


def _fake_fetch(view, seen=None):
    def fetch(keys):
        if seen is not None:
            seen.append(keys)
        proc = V.to_np(view["processing"])[0]
        ids = V.to_np(view["id"])[0]
        keep = np.zeros(len(view), dtype=bool)
        for p, k in keys.items():
            keep |= (proc == p) & np.isin(ids, k)
        return view.filter(pa.array(keep))
    return fetch


def test_clickhouse_pass(good):
    seen = []
    rep = V.check_clickhouse(good, n=100, fetch=_fake_fetch(_view(table()), seen))
    assert rep.ok, rep.text()
    assert sorted(seen[0]) == sorted(set(PROCESSING))


def test_clickhouse_fail(good):
    v = _view(table())
    i = v.column_names.index("ra")
    bad = v.set_column(i, "ra", pa.array([0.0] + v["ra"].to_pylist()[1:]))
    assert "block 4 equal (exact, float64->float32 after the cast)" in failed(
        V.check_clickhouse(good, n=100, fetch=_fake_fetch(bad)))
    assert "every sampled (processing, id) found in the view" in failed(
        V.check_clickhouse(good, n=100, fetch=_fake_fetch(v.slice(1))))
    dup = pa.concat_tables([v, v.slice(0, 1)])
    assert "(processing, id) unique in the view" in failed(
        V.check_clickhouse(good, n=100, fetch=_fake_fetch(dup)))
    i = v.column_names.index("parentId")
    bad = v.set_column(i, "parentId", pa.array([5] + v["parentId"].to_pylist()[1:]))
    assert "id split values (view id/parentId -> the measuredOn pair)" in failed(
        V.check_clickhouse(good, n=100, fetch=_fake_fetch(bad)))


def test_clickhouse_limits(good):
    with pytest.raises(SystemExit):
        V.check_clickhouse(good, n=V.CH_MAX_ROWS + 1, fetch=lambda k: None)
    with pytest.raises(ValueError):
        V.ch_fetch({"AP-DS": [1]}, workers=V.CH_MAX_WORKERS + 1)


# --------------------------------------------------------------------------
# regression
# --------------------------------------------------------------------------

def ref_from(t):
    """Today's SSSource layout: ssObjectId 0 when unmatched, '' designation,
    NaN (not NULL) ephemerides, no ellipse."""
    cols = {"diaSourceId": pa.array(V._measurement_id(t)[0], pa.int64()),
            "ssObjectId": pc.fill_null(t["ssObjectId"], 0),
            "designation": pc.fill_null(t["designation"], "")}
    for c in V.regression_columns(t.column_names):
        a = V._flat(t[c])
        cols[c] = pc.fill_null(a, np.nan) if pa.types.is_floating(a.type) else a
    for c in ("processing", "submission_id", "trksub", "trkid", "obsid", "primary"):
        cols[c] = V._flat(t[c])
    return pa.table(cols)


def test_regression_pass(tmp_path, good):
    ref = ref_from(table())
    ref = ref.take([5, 3, 1, 0, 2, 4])           # row order doesn't matter
    rep = V.check_regression(good, write(ref, tmp_path / "ref.parquet"))
    assert rep.ok, rep.text()
    assert any("NULL vs NaN" in line for line in rep.lines)


def test_regression_fail(tmp_path, good):
    ref = ref_from(table())
    a = ref["ephRa"].to_numpy().copy()
    a.view(np.uint64)[0] ^= 1
    bad = ref.set_column(ref.column_names.index("ephRa"), "ephRa", pa.array(a))
    assert "ephemeris/geometry columns bitwise equal" in failed(
        V.check_regression(good, write(bad, tmp_path / "r1.parquet")))
    assert "same row set (obsid)" in failed(
        V.check_regression(good, write(ref.slice(1), tmp_path / "r2.parquet")))
    bad = ref.set_column(ref.column_names.index("designation"), "designation",
                         pa.array(["2020 AB", "2020 AB", "2021 CD", "2021 CE", "2025 MH352", ""]))
    assert "designation equal ('' == NULL)" in failed(
        V.check_regression(good, write(bad, tmp_path / "r3.parquet")))
    bad = ref.set_column(ref.column_names.index("ssObjectId"), "ssObjectId", pa.array([1, 1, 2, 3, 0, 0]))
    assert "ssObjectId: reference's where non-zero with an orbit, else NULL" in failed(
        V.check_regression(good, write(bad, tmp_path / "r4.parquet")))
    i = ref.column_names.index("topoRange")
    bad = ref.set_column(i, "topoRange", ref["topoRange"].cast(pa.float64()))
    assert "ephemeris/geometry column types unchanged" in failed(
        V.check_regression(good, write(bad, tmp_path / "r5.parquet")))
    bad = ref.append_column("ephFoo", pa.array([1.0] * N))
    assert "every reference ephemeris/geometry column present" in failed(
        V.check_regression(good, write(bad, tmp_path / "r6.parquet")))


def test_regression_ellipse_not_compared(tmp_path, good):
    ref = ref_from(table()).append_column("ephRaErr", pa.array([9.0] * N, pa.float32()))
    assert V.check_regression(good, write(ref, tmp_path / "r.parquet")).ok


# --------------------------------------------------------------------------
# ellipse
# --------------------------------------------------------------------------

def _orbits(path, des, a=(2.0, 2.5, 3.0), jsonb=("x", "y", "z")):
    t = pa.table({V.ORBIT_KEY: list(des), "a": list(a), "e": [0.1] * len(des), "mpc_orb_jsonb": list(jsonb)})
    return write(t, path)


def _nss(t, scale=1.0, rows=(0, 3)):
    """NearbySSO rows for the AP-DS DiaSource rows ``rows`` of ``t``."""
    rows = list(rows)
    return pa.table({
        "diaSourceId": pa.array([t["diaSourceId"][i].as_py() for i in rows], pa.int64()),
        "designation": [t["designation"][i].as_py() for i in rows],
        "ephRaErr": pa.array([t["ephRaErr"][i].as_py() * scale for i in rows], pa.float32()),
        "ephDecErr": pa.array([t["ephDecErr"][i].as_py() for i in rows], pa.float32()),
        "ephRa_ephDec_Cov": pa.array([t["ephRa_ephDec_Cov"][i].as_py() for i in rows], pa.float32()),
    })


def test_ellipse_pass(tmp_path, good):
    t = table()
    oa = _orbits(tmp_path / "a.parquet", ["2020 AB", "2021 CD", "2022 EF"])
    ob = _orbits(tmp_path / "b.parquet", ["2020 AB", "2021 CD", "2022 EF"])
    rep = V.check_ellipse(good, write(_nss(t, 1.001), tmp_path / "n.parquet"), oa, ob)
    assert rep.ok, rep.text()


def test_ellipse_fail(tmp_path, good):
    t = table()
    oa = _orbits(tmp_path / "a.parquet", ["2020 AB", "2021 CD", "2022 EF"])
    ob = _orbits(tmp_path / "b.parquet", ["2020 AB", "2021 CD", "2022 EF"])
    rep = V.check_ellipse(good, write(_nss(t, 1.1), tmp_path / "n.parquet"), oa, ob)
    assert "ellipses agree (rel. err <= 0.01, |d rho| <= 0.01)" in failed(rep)
    # changed orbits are not compared: then nothing is left
    ob2 = _orbits(tmp_path / "b2.parquet", ["2020 AB", "2021 CD", "2022 EF"], jsonb=("x2", "y2", "z"))
    rep = V.check_ellipse(good, write(_nss(t, 1.1), tmp_path / "n.parquet"), oa, ob2)
    assert "rows to compare (identical orbits)" in failed(rep)
    # only one orbit changed: the other object's bad ellipse is still caught
    ob3 = _orbits(tmp_path / "b3.parquet", ["2020 AB", "2021 CD", "2022 EF"], a=(2.0, 2.6, 3.0))
    rep = V.check_ellipse(good, write(_nss(t, 1.1), tmp_path / "n.parquet"), oa, ob3)
    assert "ellipses agree (rel. err <= 0.01, |d rho| <= 0.01)" in failed(rep)
    # NearbySSO has an ellipse where SSSource has none
    d = _values()
    d["ephRaErr"][0] = d["ephDecErr"][0] = d["ephRa_ephDec_Cov"][0] = None
    ss = write(table(d), tmp_path / "ss.parquet")
    rep = V.check_ellipse(ss, write(_nss(t), tmp_path / "n.parquet"), oa, ob)
    assert "ellipse present in SSSource wherever NearbySSO has one" in failed(rep)


def test_identical_orbits(tmp_path):
    oa = _orbits(tmp_path / "a.parquet", ["A", "B", "C"], a=(1.0, 2.0, np.nan))
    ob = _orbits(tmp_path / "b.parquet", ["A", "B", "D"], a=(1.0, 2.1, np.nan))
    assert V.identical_orbits(oa, ob, ["A", "B", "C", "D", "E"]) == {"A": True, "B": False}


# --------------------------------------------------------------------------
# counts
# --------------------------------------------------------------------------

def _obs_sbn(t, extra=0):
    obsid = t["obsid"].to_pylist() + [f"zz{k}" for k in range(extra)]
    status = [str(s) for s in V.to_np(t["status"])[0]] + ["p"] * extra
    return pa.table({"obsid": obsid + ["other"], "stn": ["X05"] * len(obsid) + ["I41"],
                     "status": status + ["p"]})


def test_counts_pass(tmp_path, good):
    t = table()
    ob = write(_obs_sbn(t, extra=2), tmp_path / "obs.parquet")
    dia = write(dia_from(t), tmp_path / "dia.parquet")
    rep = V.check_counts(good, ob, dia)
    assert rep.ok, rep.text()
    text = rep.text()
    assert "#7 rows (designated, no SSObject): 1 rows of 1 objects: 2025 MH352 (1)" in text
    assert "I rows: 1" in text and "X05 rows it did not resolve: 2" in text
    assert V.check_counts(good, ob).ok           # without dia_sources


def test_counts_fail(tmp_path, good):
    t = table()
    obs = _obs_sbn(t)
    st = obs["status"].to_pylist()
    st[0] = "P"
    bad = obs.set_column(2, "status", pa.array(st))
    assert "status agrees with obs_sbn" in failed(V.check_counts(good, write(bad, tmp_path / "o1.parquet")))
    bad = obs.filter(pa.array([o != "obs002" for o in obs["obsid"].to_pylist()]))
    assert "every SSSource row is an obs_sbn X05 row" in failed(
        V.check_counts(good, write(bad, tmp_path / "o2.parquet")))
    ob = write(obs, tmp_path / "o3.parquet")
    dia = write(dia_from(t).slice(1), tmp_path / "dia.parquet")
    assert "rows == the X05 obs_sbn rows dia_sources resolved" in failed(V.check_counts(good, ob, dia))
    d = _values()
    d["designation"][5] = "2030 ZZ"
    bad_ss = write(table(d), tmp_path / "ss.parquet")
    assert "status 'I' rows undesignated and without ssObjectId" in failed(V.check_counts(bad_ss, ob))


# --------------------------------------------------------------------------
# the CLI
# --------------------------------------------------------------------------

def test_cli_exit_codes(tmp_path, good):
    out = tmp_path / "rep.txt"
    assert V.main(["conformance", good, "--out", str(out)]) == 0
    assert "RESULT: PASS" in out.read_text()
    bad = write(table().take([1, 0, 2, 3, 4, 5]), tmp_path / "bad.parquet")
    assert V.main(["conformance", bad]) == 1


def test_apply_faults_known_names():
    with pytest.raises(ValueError):
        V.apply_faults(table(), ["nope"])
    t = V.apply_faults(table(), ["status"])
    assert t["status"].to_pylist()[0] == "P"
