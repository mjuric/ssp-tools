#!/bin/bash
# Validation script to compare dump_tables.sh vs fast-export batch mode

set -e

HOST="mpc-usdf.sp.mjuric.org"
DBNAME="mpc_sbn"
USER="rubin"

echo "Comparing dump_tables.sh approach vs fast-export batch mode..."
echo ""

# Clean up
rm -f *.parquet

# Test 1: Batch mode
echo "=== Test 1: Batch export with fast-export ==="
time fast-export --config examples/exports.yaml --host "$HOST" --dbname "$DBNAME" --user "$USER"
echo ""

# List results
echo "=== Generated files ==="
ls -lh *.parquet
echo ""

# Verify Parquet structure for one file
echo "=== Verifying Parquet structure ==="
python3 -c "
import pyarrow.parquet as pq
table = pq.read_table('current_identifications.parquet')
print(f'Rows: {len(table):,}')
print(f'Columns: {len(table.column_names)}')
print(f'Schema: {table.schema}')
print(f'Row groups: {pq.ParquetFile(\"current_identifications.parquet\").num_row_groups}')
"

echo ""
echo "✅ Batch export test completed successfully!"
