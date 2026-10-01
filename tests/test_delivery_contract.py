"""The SSO delivery contract agrees with the vendored schemas."""
from ssp import delivery_contract as C
from ssp import sssource_contract as S


def test_delivery_schema_tables():
    sch = C.delivery_schema()
    assert set(sch) == set(C.DELIVERY_TABLES)
    assert [c["name"] for c in sch["SSSource"]] == list(S.SSSourceDtype.names)
    assert [c["name"] for c in sch["NearbySSO"]] == list(S.NearbySSODtype.names)
    names = {t: [c["name"] for c in cols] for t, cols in sch.items()}
    assert "designation" in names["mpc_orbits"]
    assert "identifier_ids" not in names["current_identifications"]
    assert not {"numbered_publication_references", "named_publication_references"} & set(
        names["numbered_identifications"])
    pub = [c for c in sch["current_identifications"] if c["name"] == "published"][0]
    assert pub["datatype"] == "int"


def test_inputs_and_steps():
    assert set(C.REQUIRED_INPUT_COLUMNS) == set(C.INPUT_FILES)
    assert set(C.MPC_SNAPSHOT) <= set(C.INPUT_FILES)
    assert C.BUILD_STEPS[-1] == "check"
    assert C.UPLOAD_MESSAGE_FIELDS == ("bucket", "object_prefix", "uploaded_tables")
