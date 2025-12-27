#!/bin/bash
set -eo pipefail

# pgFlow Runtime-Only Artifact Build Script
# Concatenates SQL files for production runtime (excludes pipeline builder DSL functions)

VERSION="${1:-dev}"
OUTPUT_DIR="${2:-dist}"

echo "=== pgFlow Runtime Artifact Build ==="
echo "Version: $VERSION"
echo "Output:  $OUTPUT_DIR"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")
OUTPUT_FILE="$OUTPUT_DIR/pgflow-$VERSION-runtime.sql"

# Define deployment order - RUNTIME ONLY
# Excludes: read_db_object, read_flow, step, select, where, lookup, aggregate, write, register_pipeline
SOURCE_FILES=(
    "src/db/state/schema/schema.flow.sql"
    "src/db/state/tables/table.flow.pipeline.sql"
    "src/db/state/tables/table.flow.pipeline_step.sql"
    "src/db/state/functions/function.flow.__ensure_session_steps.sql"
    "src/db/state/functions/function.flow.__assert_pipeline_started.sql"
    "src/db/state/functions/function.flow.__ensure_source_exists.sql"
    "src/db/state/functions/function.flow.__ensure_step_exists.sql"
    "src/db/state/functions/function.flow.__next_step_order.sql"
    "src/db/state/functions/function.flow.__extract_column_names.sql"
    "src/db/state/functions/function.flow.__compile_write.sql"
    "src/db/state/functions/function.flow.__compile_session.sql"
    "src/db/state/functions/function.flow.compile.sql"
    "src/db/state/functions/function.flow.show_steps.sql"
    "src/db/state/functions/function.flow.inspect_step.sql"
    "src/db/state/functions/function.flow.show_pipeline.sql"
    "src/db/state/functions/function.flow.list_pipelines.sql"
    "src/db/state/functions/function.flow.run.sql"
    "src/db/state/functions/function.flow.get_pipeline_variables.sql"
    "src/db/state/functions/function.flow.help.sql"
)

echo "Building runtime artifact: $OUTPUT_FILE"

# Create header
cat > "$OUTPUT_FILE" <<EOF
-- ============================================================================
-- pgFlow: PostgreSQL-Native Pipeline Framework (RUNTIME ONLY)
-- Version: $VERSION
-- Built: $TIMESTAMP
-- ============================================================================
-- 
-- This is the RUNTIME-ONLY version.
-- Includes: pipeline execution, introspection, and helper functions
-- Excludes: pipeline builder DSL (read, select, where, aggregate, write, etc.)
--
-- Installation:
--   psql -h <host> -U <user> -d <database> -f pgflow-$VERSION-runtime.sql
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
cat >> "$OUTPUT_FILE" <<EOF

-- ============================================================================
-- Runtime artifact complete
-- Files: $FILE_COUNT
-- ============================================================================

COMMIT;

-- Verify installation
SELECT 'pgFlow runtime installed. Version: $VERSION' as status;
SELECT flow.help() as available_functions;
EOF

echo ""
echo "✓ Runtime artifact built successfully"
echo "  Files included: $FILE_COUNT"
[ $MISSING_COUNT -gt 0 ] && echo "  Files missing: $MISSING_COUNT"
echo "  Output: $OUTPUT_FILE"
echo ""
echo "Deploy with:"
echo "  ./deploy.sh -v $VERSION --runtime"
