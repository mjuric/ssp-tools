import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from ssp.export import arrow_type_for_oid


def test_oid_mapping_basic():
    # Known OIDs
    assert str(arrow_type_for_oid(16)) == str(pa.bool_())
    assert str(arrow_type_for_oid(23)) == str(pa.int32())
    assert str(arrow_type_for_oid(701)) == str(pa.float64())
    assert str(arrow_type_for_oid(1114)) == str(pa.timestamp('us'))
    assert str(arrow_type_for_oid(1184)) == str(pa.timestamp('us', tz='UTC'))
    # Unknown OID should default to string
    assert str(arrow_type_for_oid(999999)) == str(pa.string())


essential_cols = {
    'i32': pa.array(list(range(12)), type=pa.int32()),
    'txt': pa.array([f"s{i}" for i in range(12)], type=pa.string()),
}


def test_row_group_count_tmpfile():
    table = pa.table(essential_cols)
    with tempfile.TemporaryDirectory() as tmpdir:
        out_path = os.path.join(tmpdir, "test.parquet")
        # Write with small row groups to force multiple groups
        pq.write_table(
            table,
            out_path,
            compression="zstd",
            use_dictionary=False,
            row_group_size=5,
        )
        pf = pq.ParquetFile(out_path)
        assert pf.metadata.num_row_groups == 3  # 12 rows with groups of 5 => 3 groups
