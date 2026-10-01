"""The widened-SSSource contract agrees with its schema (sso_base.yaml)."""
from pathlib import Path

import yaml

from ssp import sssource_contract as C

SCHEMA = Path(__file__).parent / "data/sdm_schemas/sso_base.yaml"


def _table(name):
    return [t for t in yaml.safe_load(SCHEMA.read_text())["tables"] if t["name"] == name][0]


def test_sssource_columns_match_schema():
    cols = _table("SSSource")["columns"]
    assert list(C.SSSourceDtype.names) == [c["name"] for c in cols]
    assert len(cols) == 181
    assert {c["name"] for c in cols if c.get("nullable") is False} == C.SSSOURCE_NONNULL
    assert _table("SSSource")["primaryKey"] == "#SSSource.obsid"


def test_named_columns_exist():
    names = set(C.SSSourceDtype.names)
    assert set(C.SSSOURCE_DICTIONARY) <= names
    assert set(C.SSSOURCE_SORT) <= names
    assert set(C.ELLIPSE_COLUMNS) <= names
    assert {c for pair in C.ID_SPLIT.values() for c in pair} <= names
    assert not set(C.VIEW_DROPPED) & names
    assert len(C.MATCH_METHODS) == len(set(C.MATCH_METHODS)) == 3
    # the ellipse sits next to the position, as raErr/decErr do
    n = list(C.SSSourceDtype.names)
    assert n[n.index("ephRa"):n.index("ephRa") + 5] == ["ephRa", "ephRaErr", "ephDec", "ephDecErr",
                                                         "ephRa_ephDec_Cov"]


def test_nearbysso_columns_match_schema():
    assert list(C.NearbySSODtype.names) == [c["name"] for c in _table("NearbySSO")["columns"]]
