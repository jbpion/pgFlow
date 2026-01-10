-- ============================================================================
-- Example: Using flow.export_pipeline_code()
-- ============================================================================
-- This demonstrates how to export, modify, and recreate a pipeline.

-- Step 1: Create an original pipeline
SELECT flow.read_db_object('raw.orders');
SELECT flow.where('order_date >= ''2025-01-01''');
SELECT flow.where('status = ''completed''');
SELECT flow.select('order_id', 'customer_id', 'total_amount', 'order_date');
SELECT flow.register_pipeline('original_pipeline', 'Original sales pipeline');

-- Step 2: Export the pipeline code
SELECT flow.export_pipeline_code('original_pipeline');

-- Output will look like:
/*
-- ============================================================================
-- Pipeline Code Export: original_pipeline
-- Description: Original sales pipeline
-- Version: 1.0.0
-- Exported: 2026-01-10 14:30:00
-- ============================================================================

-- Step 1: read raw.orders (read_db_object)
SELECT flow.read_db_object('raw.orders');

-- Step 2: where order_date >= '2025-01-01' (where)
SELECT flow.where('order_date >= ''2025-01-01''');

-- Step 3: where status = 'completed' (where)
SELECT flow.where('status = ''completed''');

-- Step 4: select columns (select)
SELECT flow.select('order_id', 'customer_id', 'total_amount', 'order_date');

-- ============================================================================
-- Verify Compilation
-- ============================================================================
SELECT flow.compile();

-- ============================================================================
-- Register Pipeline
-- ============================================================================
SELECT flow.register_pipeline(
    'original_pipeline_copy',
    'Copy of original_pipeline',
    'replace',
    '1.0.0'
);
*/

-- Step 3: Save to file and modify
\o /tmp/modified_pipeline.sql
SELECT flow.export_pipeline_code('original_pipeline');
\o

-- Step 4: Edit /tmp/modified_pipeline.sql, for example:
--   - Change pipeline name to 'monthly_sales'
--   - Modify date filter to use variable: 'order_date >= ''{{start_date}}'''
--   - Add another select column

-- Step 5: Execute the modified file
\i /tmp/modified_pipeline.sql

-- Step 6: Run the new pipeline
SELECT * FROM flow.run_pipeline(
    'monthly_sales',
    jsonb_build_object('start_date', '2025-01-01')
);

-- ============================================================================
-- Export Options
-- ============================================================================

-- Export without register call (just the steps)
SELECT flow.export_pipeline_code('original_pipeline', false);

-- This is useful when you want to:
-- 1. Execute the steps in current session to test
-- 2. Add your own register call with custom parameters
-- 3. Chain multiple pipeline definitions together

-- ============================================================================
-- Use Cases
-- ============================================================================

-- 1. Create variations of existing pipelines
--    - Export base pipeline
--    - Modify filters or transformations
--    - Register as new pipeline

-- 2. Pipeline templates
--    - Create a template pipeline
--    - Export code
--    - Distribute to team
--    - Everyone modifies for their use case

-- 3. Version control
--    - Export pipeline code to Git
--    - Track changes over time
--    - Review changes in PRs

-- 4. Documentation
--    - Generate readable pipeline definitions
--    - Include in technical documentation
--    - Share with stakeholders

-- 5. Migration between environments
--    - Export from dev
--    - Modify connection details
--    - Import to production
