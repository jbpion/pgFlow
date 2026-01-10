#!/bin/bash
set -eo pipefail

# pgFlow Artifact Build Script
# Concatenates all SQL files in dependency order into a single deployable script.
# Does NOT deploy to any database - just creates the artifact.

VERSION="${1:-dev}"
OUTPUT_DIR="${2:-dist}"

echo "=== pgFlow Artifact Build ==="
echo "Version: $VERSION"
echo "Output:  $OUTPUT_DIR"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")
OUTPUT_FILE="$OUTPUT_DIR/pgflow-$VERSION.sql"

# Define deployment order
SOURCE_FILES=(
    "src/db/state/schema/schema.flow.sql"
    "src/db/state/tables/table.flow.pipeline.sql"
    "src/db/state/tables/table.flow.pipeline_step.sql"
    "src/db/state/types/type.flow.measure.sql"
    "src/db/state/types/type.flow.expr.sql"
    "src/db/state/functions/function.flow.__ensure_session_steps.sql"
    "src/db/state/functions/function.flow.__assert_pipeline_started.sql"
    "src/db/state/functions/function.flow.__ensure_source_exists.sql"
    "src/db/state/functions/function.flow.__ensure_step_exists.sql"
    "src/db/state/functions/function.flow.__next_step_order.sql"
    "src/db/state/functions/function.flow.read_db_object.sql"
    "src/db/state/functions/function.flow.read_flow.sql"
    "src/db/state/functions/function.flow.step.sql"
    "src/db/state/functions/function.flow.select.sql"
    "src/db/state/functions/function.flow.where.sql"
    "src/db/state/functions/function.flow.lookup.sql"
    "src/db/state/functions/function.flow.sum.sql"
    "src/db/state/functions/function.flow.count.sql"
    "src/db/state/functions/function.flow.avg.sql"
    "src/db/state/functions/function.flow.min.sql"
    "src/db/state/functions/function.flow.max.sql"
    "src/db/state/functions/function.flow.group_by.sql"
    "src/db/state/functions/function.flow.having.sql"
    "src/db/state/functions/function.flow.aggregate.sql"
    "src/db/state/functions/function.flow.write.sql"
    "src/db/state/functions/function.flow.__extract_column_names.sql"
    "src/db/state/functions/function.flow.__compile_write.sql"
    "src/db/state/functions/function.flow.__compile_session.sql"
    "src/db/state/functions/function.flow.compile.sql"
    "src/db/state/functions/function.flow.show_steps.sql"
    "src/db/state/functions/function.flow.inspect_step.sql"
    "src/db/state/functions/function.flow.show_pipeline.sql"
    "src/db/state/functions/function.flow.register_pipeline.sql"
    "src/db/state/functions/function.flow.export_pipeline.sql"
    "src/db/state/functions/function.flow.list_pipelines.sql"
    "src/db/state/functions/function.flow.run.sql"
    "src/db/state/functions/function.flow.get_pipeline_variables.sql"
    "src/db/state/functions/function.flow.help.sql"
)

echo "Building artifact: $OUTPUT_FILE"

# Create header
cat > "$OUTPUT_FILE" <<EOF
-- ============================================================================
-- pgFlow: PostgreSQL-Native Pipeline Framework
-- Version: $VERSION
-- Built: $TIMESTAMP
-- ============================================================================
-- 
-- Installation:
--   psql -h <host> -U <user> -d <database> -f pgflow-$VERSION.sql
--
-- Documentation:
--   https://github.com/jbpion/pgFlow
--
-- ============================================================================

BEGIN;

EOF

FILE_COUNT=0
MISSING_COUNT=0

for file in "${SOURCE_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo "  [SKIP] $file (not found)"
        MISSING_COUNT=$((MISSING_COUNT + 1))
        continue
    fi
    
    echo "  [ADD]  $file"
    
    # Add file separator comment
    cat >> "$OUTPUT_FILE" <<EOF

-- ============================================================================
-- Source: $file
-- ============================================================================

EOF
    
    # Append file content
    cat "$file" >> "$OUTPUT_FILE"
    
    FILE_COUNT=$((FILE_COUNT + 1))
done

# Add footer
cat >> "$OUTPUT_FILE" <<'EOF'

-- ============================================================================
-- Installation Complete
-- ============================================================================

COMMIT;

-- Verify installation
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'flow') THEN
        RAISE EXCEPTION 'pgFlow installation failed: schema "flow" not found';
    END IF;
    
    RAISE NOTICE 'pgFlow installation successful';
END;
$$;
EOF

echo ""
echo "=== Build Summary ==="
echo "Files included: $FILE_COUNT"
echo "Missing files:  $MISSING_COUNT"

if [ $MISSING_COUNT -gt 0 ]; then
    echo ""
    echo "Warning: Some files were not found"
fi

echo ""
echo "Artifact created: $OUTPUT_FILE"
echo "Size: $(stat -f%z "$OUTPUT_FILE" 2>/dev/null || stat -c%s "$OUTPUT_FILE") bytes"
echo ""
echo "Deploy with:"
echo "  psql -h <host> -U <user> -d <database> -f $OUTPUT_FILE"

exit 0
