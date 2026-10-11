Copies of `lsst/sdm_schemas`' `python/lsst/sdm/schemas/sso_base.yaml` and `ppdb.yaml` from branch `tickets/DM-55375` (commit fd56595; PR lsst/sdm_schemas#549): RFC-1188's SSObservation and NearbySSO, and the PPDB's Solar System tables, which `ppdb.yaml` takes from `sso_base.yaml` by `columnRefs`.

- The SSObservation tests and the delivery checks read them.
- `ssp/schema_ppdb.py` was generated from `sso_base.yaml`.
- Refresh all three when the schema changes:

      cp .../sdm_schemas/python/lsst/sdm/schemas/{sso_base,ppdb}.yaml tests/data/sdm_schemas/
      ssp-generate-dtypes tests/data/sdm_schemas/sso_base.yaml SSObservation NearbySSO > ssp/schema_ppdb.py
