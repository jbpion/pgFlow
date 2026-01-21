-- ============================================================================
-- pgTap Unit Tests for flow.delete() Function
-- ============================================================================
-- Tests the delete function with various scenarios
-- Run: psql -d your_database -f tests/test_unit_delete.sql
-- ============================================================================

-- Create extension if not exists
CREATE EXTENSION IF NOT EXISTS pgtap;

BEGIN;

-- Initialize flow session
SELECT flow.__ensure_session_steps();

-- Setup test data
CREATE TEMP TABLE test_delete_target (
    id int PRIMARY KEY,
    name text,
    category text,
    created_date date,
    status text
);

INSERT INTO test_delete_target VALUES 
    (1, 'Record 1', 'A', '2024-01-01', 'active'),
    (2, 'Record 2', 'A', '2024-06-01', 'active'),
    (3, 'Record 3', 'B', '2024-12-01', 'active'),
    (4, 'Record 4', 'B', '2025-01-01', 'inactive'),
    (5, 'Record 5', 'C', '2025-01-15', 'active');

-- Plan tests
SELECT plan(16);

-- ============================================================================
-- Test 1: Basic delete function exists
-- ============================================================================
SELECT has_function(
    'flow',
    'delete',
    ARRAY['text', 'text', 'text', 'boolean', 'text'],
    'flow.delete function exists with correct signature'
);

-- ============================================================================
-- Test 2: Delete with WHERE clause - timing before
-- ============================================================================
DELETE FROM __session_steps;
DELETE FROM flow.pipeline_step;
DELETE FROM flow.pipeline;

SELECT flow.read_db_object('pg_temp.test_delete_target');
SELECT flow.delete('pg_temp.test_delete_target', 'created_date < ''2024-07-01''');
SELECT flow.write('pg_temp.result_table', 'insert', auto_create => true);

SELECT lives_ok(
    $$SELECT flow.compile()$$,
    'Delete with WHERE clause compiles successfully'
);

-- Check that DELETE statement is in compiled SQL
SELECT matches(
    flow.compile(),
    'DELETE FROM pg_temp\.test_delete_target WHERE created_date < ''2024-07-01''',
    'Compiled SQL contains DELETE with WHERE clause'
);

-- ============================================================================
-- Test 3: Delete with execution_order = after
-- ============================================================================
DELETE FROM __session_steps;
DELETE FROM flow.pipeline_step;
DELETE FROM flow.pipeline;

SELECT flow.read_db_object('pg_temp.test_delete_target');
SELECT flow.write('pg_temp.result_table2', 'insert', auto_create => true);
SELECT flow.delete('pg_temp.test_delete_target', 'status = ''inactive''', execution_order => 'after');

SELECT lives_ok(
    $$SELECT flow.compile()$$,
    'Delete with execution_order=after compiles successfully'
);

-- Verify DELETE comes after INSERT in the compiled SQL
SELECT ok(
    position('DELETE FROM pg_temp.test_delete_target' in flow.compile()) > 
    position('INSERT' in flow.compile()),
    'DELETE with execution_order=after appears after INSERT in compiled SQL'
);

-- ============================================================================
-- Test 4: Delete with truncate mode
-- ============================================================================
DELETE FROM __session_steps;
DELETE FROM flow.pipeline_step;
DELETE FROM flow.pipeline;

SELECT flow.read_db_object('pg_temp.test_delete_target');
SELECT flow.delete('pg_temp.test_delete_target', truncate_mode => true);
SELECT flow.write('pg_temp.result_table3', 'insert', auto_create => true);

SELECT lives_ok(
    $$SELECT flow.compile()$$,
    'Delete with truncate_mode compiles successfully'
);

-- Check for TRUNCATE statement
SELECT matches(
    flow.compile(),
    'TRUNCATE TABLE pg_temp\.test_delete_target',
    'Compiled SQL contains TRUNCATE statement'
);

-- ============================================================================
-- Test 5: Delete without WHERE (delete all)
-- ============================================================================
DELETE FROM __session_steps;
DELETE FROM flow.pipeline_step;
DELETE FROM flow.pipeline;

SELECT flow.read_db_object('pg_temp.test_delete_target');
SELECT flow.delete('pg_temp.test_delete_target');
SELECT flow.write('pg_temp.result_table4', 'insert', auto_create => true);

SELECT lives_ok(
    $$SELECT flow.compile()$$,
    'Delete without WHERE clause compiles successfully'
);

-- Check for DELETE without WHERE
SELECT matches(
    flow.compile(),
    'DELETE FROM pg_temp\.test_delete_target',
    'Compiled SQL contains DELETE statement'
);

SELECT doesnt_match(
    flow.compile(),
    'DELETE FROM pg_temp\.test_delete_target WHERE',
    'DELETE statement has no WHERE clause when not specified'
);

-- ============================================================================
-- Test 6: Multiple delete operations
-- ============================================================================
DELETE FROM __session_steps;
DELETE FROM flow.pipeline_step;
DELETE FROM flow.pipeline;

SELECT flow.read_db_object('pg_temp.test_delete_target');
SELECT flow.delete('pg_temp.test_delete_target', 'category = ''A''', step_name => 'Delete A');
SELECT flow.delete('pg_temp.test_delete_target', 'category = ''B''', step_name => 'Delete B');
SELECT flow.write('pg_temp.result_table5', 'insert', auto_create => true);

SELECT lives_ok(
    $$SELECT flow.compile()$$,
    'Multiple delete operations compile successfully'
);

-- Check that both DELETE statements are present
SELECT matches(
    flow.compile(),
    'DELETE FROM pg_temp\.test_delete_target WHERE category = ''A''',
    'First DELETE statement present in compiled SQL'
);

SELECT matches(
    flow.compile(),
    'DELETE FROM pg_temp\.test_delete_target WHERE category = ''B''',
    'Second DELETE statement present in compiled SQL'
);

-- ============================================================================
-- Test 7: Custom step name
-- ============================================================================
DELETE FROM __session_steps;
DELETE FROM flow.pipeline;

SELECT flow.read_db_object('pg_temp.test_delete_target');
SELECT flow.delete('pg_temp.test_delete_target', 'status = ''inactive''', step_name => 'Clean inactive records');

-- Check that the step was added with custom name
SELECT is(
    (SELECT step_name FROM __session_steps WHERE step_type = 'delete'),
    'Clean inactive records',
    'Custom step name is stored correctly'
);

-- ============================================================================
-- Test 8: Invalid execution_order parameter
-- ============================================================================
DELETE FROM __session_steps;
DELETE FROM flow.pipeline;

SELECT flow.read_db_object('pg_temp.test_delete_target');

SELECT throws_ok(
    $$SELECT flow.delete('pg_temp.test_delete_target', 'id = 1', execution_order => 'invalid')$$,
    'Invalid execution_order: invalid. Must be before or after.',
    'Invalid execution_order parameter raises exception'
);

-- ============================================================================
-- Test 9: Delete pipeline with variables
-- ============================================================================
DELETE FROM __session_steps;
DELETE FROM flow.pipeline_step;
DELETE FROM flow.pipeline;

-- Create a pipeline with delete using variables
SELECT flow.read_db_object('pg_temp.test_delete_target');
SELECT flow.delete(
    'pg_temp.test_delete_target',
    'category = {{target_category}} AND created_date < {{cutoff_date}}::date',
    step_name => 'Delete by category and date'
);
SELECT flow.write('pg_temp.result_table6', 'insert', auto_create => true);
SELECT flow.register_pipeline(
    'delete_with_vars',
    'Delete records with variable filters'
);

-- Run the pipeline with variables
SELECT lives_ok(
    $$SELECT * FROM flow.run_pipeline(
        'delete_with_vars',
        jsonb_build_object(
            'target_category', 'A',
            'cutoff_date', '2024-07-01'
        )
    )$$,
    'Delete pipeline with variables executes successfully'
);

-- ============================================================================
-- Test 10: Delete job with variables
-- ============================================================================
DELETE FROM __session_steps;
DELETE FROM flow.pipeline_step;
DELETE FROM flow.pipeline;

-- Create two pipelines with delete operations using variables
SELECT flow.read_db_object('pg_temp.test_delete_target');
SELECT flow.delete(
    'pg_temp.test_delete_target',
    'category = {{category}}',
    step_name => 'Delete by category'
);
SELECT flow.write('pg_temp.result_table7', 'insert', auto_create => true);
SELECT flow.register_pipeline(
    'delete_category_pipeline',
    'Delete records by category'
);

SELECT flow.read_db_object('pg_temp.test_delete_target');
SELECT flow.delete(
    'pg_temp.test_delete_target',
    'status = {{status}}',
    step_name => 'Delete by status'
);
SELECT flow.write('pg_temp.result_table8', 'insert', auto_create => true);
SELECT flow.register_pipeline(
    'delete_status_pipeline',
    'Delete records by status'
);

-- Create job and add pipelines
SELECT flow.create_job('cleanup_job', p_description => 'Job to clean up records');
SELECT flow.add_pipeline_to_job('cleanup_job', 'delete_category_pipeline');
SELECT flow.add_pipeline_to_job('cleanup_job', 'delete_status_pipeline');

-- Execute job with variables (both pipelines receive these variables)
SELECT lives_ok(
    $$SELECT * FROM flow.run_job(
        'cleanup_job',
        jsonb_build_object(
            'category', 'B',
            'status', 'inactive'
        )
    )$$,
    'Delete job with variables executes successfully'
);

-- Finish tests
SELECT * FROM finish();

ROLLBACK;
