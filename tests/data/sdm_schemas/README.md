`sso_base.yaml`: a copy of `lsst/sdm_schemas`' `python/lsst/sdm/schemas/sso_base.yaml` from branch `tickets/DM-55375` (commit a8615ae, RFC-1188: the widened SSSource and NearbySSO). The widened SSSource's schema-conformance tests read it, and `ssp/schema_ppdb.py` was generated from it. Refresh both when the schema changes:

    cp .../sdm_schemas/python/lsst/sdm/schemas/sso_base.yaml tests/data/sdm_schemas/
    ssp-generate-dtypes tests/data/sdm_schemas/sso_base.yaml SSSource NearbySSO > ssp/schema_ppdb.py
