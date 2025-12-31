-- ============================================================================
-- pgTap Unit Tests for Core Flow Functions
-- ============================================================================
-- Tests individual flow functions (read_db_object, select, where, etc.)
-- Run: psql -d your_database -f tests/test_unit_core.sql
-- ============================================================================

-- Create extension if not exists
CREATE EXTENSION IF NOT EXISTS pgtap;

-- Create test helpers schema
CREATE SCHEMA IF NOT EXISTS test_helpers;

-- Helper: Setup test tables
CREATE OR REPLACE FUNCTION test_helpers.setup_test_tables()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS pg_temp.test_orders CASCADE;
    DROP TABLE IF EXISTS pg_temp.test_customers CASCADE;
    DROP TABLE IF EXISTS pg_temp.test_products CASCADE;
    
    CREATE TEMP TABLE test_orders (
        order_id int,
        customer_id int,
        product_id int,
        amount numeric,
        quantity int,
        order_date date,
        status text
    );

    CREATE TEMP TABLE test_customers (
        customer_id int,
        customer_name text,
        email text,
        account_status text
    );

    CREATE TEMP TABLE test_products (
        product_id int,
        product_name text,
        category text,
        price numeric
    );
    
    INSERT INTO test_orders VALUES 
        (1, 100, 1, 150.00, 2, '2025-01-15', 'completed'),
        (2, 100, 2, 75.00, 1, '2025-01-16', 'completed'),
        (3, 101, 1, 200.00, 1, '2025-01-17', 'pending'),
        (4, 102, 3, 50.00, 3, '2025-01-18', 'completed'),
        (5, 999, 1, 25.00, 1, '2025-01-19', 'cancelled');
    
    INSERT INTO test_customers VALUES 
        (100, 'Alice Smith', 'alice@example.com', 'active'),
        (101, 'Bob Jones', 'bob@example.com', 'active'),
        (102, 'Carol White', 'carol@example.com', 'inactive');
    
    INSERT INTO test_products VALUES 
        (1, 'Widget A', 'widgets', 75.00),
        (2, 'Widget B', 'widgets', 75.00),
        (3, 'Gadget X', 'gadgets', 16.67);
END;
$$;

-- Helper: Reset flow session
CREATE OR REPLACE FUNCTION test_helpers.reset_flow_session()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM flow.__ensure_session_steps();
    TRUNCATE TABLE __session_steps;
END;
$$;

BEGIN;

-- Plan the number of tests
SELECT plan(29);

-- Setup test environment
SELECT test_helpers.setup_test_tables();

-- ============================================================================
-- TEST GROUP 1: flow.read_db_object()
-- ============================================================================

-- Test 1.1: Return value format
SELECT test_helpers.reset_flow_session();
SELECT matches(
    flow.read_db_object('pg_temp.test_orders'),
    'Step 1: .* \(read .* as t0\)',
    'read_db_object returns correctly formatted message'
);

-- Test 1.2: Step is recorded in __session_steps
SELECT is(
    (SELECT COUNT(*)::int FROM __session_steps WHERE step_type = 'read'),
    1,
    'read_db_object creates one step in __session_steps'
);

-- Test 1.3: Step spec contains object name
SELECT is(
    (SELECT step_spec->>'object' FROM __session_steps WHERE step_order = 1),
    'pg_temp.test_orders',
    'Step spec contains correct object name'
);

-- Test 1.4: Step spec contains alias
SELECT is(
    (SELECT step_spec->>'alias' FROM __session_steps WHERE step_order = 1),
    't0',
    'Step spec contains correct alias (t0)'
);

-- Test 1.5: Program call is stored
SELECT matches(
    (SELECT program_call FROM __session_steps WHERE step_order = 1),
    'flow\.read_db_object\(',
    'Program call is stored correctly'
);

-- ============================================================================
-- TEST GROUP 2: flow.select() - Simple columns
-- ============================================================================

-- Test 2.1: Select returns success message
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('pg_temp.test_orders');
SELECT matches(
    flow.select('order_id', 'customer_id', 'amount'),
    'Step .+: select .+ columns',
    'select returns formatted message'
);

-- Test 2.2: Column mapping created correctly
SELECT is(
    (SELECT step_spec->'column_mapping'->>'order_id' 
     FROM __session_steps WHERE step_type = 'select'),
    'order_id',
    'Simple column mapping: order_id -> order_id'
);

SELECT is(
    (SELECT step_spec->'column_mapping'->>'customer_id' 
     FROM __session_steps WHERE step_type = 'select'),
    'customer_id',
    'Simple column mapping: customer_id -> customer_id'
);

-- ============================================================================
-- TEST GROUP 3: flow.select() - Expressions with aliases
-- ============================================================================

-- Test 3.1: Expression with alias mapping
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('pg_temp.test_orders');
SELECT flow.select(
    'order_id',
    'amount * 1.1:adjusted_amount',
    'UPPER(status):status_upper'
);

SELECT is(
    (SELECT step_spec->'column_mapping'->>'adjusted_amount' 
     FROM __session_steps WHERE step_type = 'select'),
    'amount * 1.1',
    'Expression mapping: adjusted_amount -> amount * 1.1'
);

SELECT is(
    (SELECT step_spec->'column_mapping'->>'status_upper' 
     FROM __session_steps WHERE step_type = 'select'),
    'UPPER(status)',
    'Function expression mapping: status_upper -> UPPER(status)'
);

-- ============================================================================
-- TEST GROUP 4: flow.select() - CASE statements
-- ============================================================================

-- Test 4.1: CASE expression stored correctly
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('pg_temp.test_orders');
SELECT flow.select(
    'order_id',
    'CASE WHEN amount > 100 THEN ''high'' ELSE ''low'' END:amount_tier'
);

SELECT matches(
    (SELECT step_spec->'column_mapping'->>'amount_tier' 
     FROM __session_steps WHERE step_type = 'select'),
    'CASE WHEN',
    'CASE expression stored in column mapping'
);

-- ============================================================================
-- TEST GROUP 5: flow.where() - Single condition
-- ============================================================================

-- Test 5.1: Where returns success message
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('pg_temp.test_orders');
SELECT matches(
    flow.where('status = ''completed'''),
    'Step .+: where .+',
    'where returns formatted message'
);

-- Test 5.2: Condition stored correctly
SELECT is(
    (SELECT step_spec->>'condition' 
     FROM __session_steps WHERE step_type = 'where'),
    'status = ''completed''',
    'Where condition stored correctly'
);

-- Test 5.3: Default operator is AND
SELECT is(
    (SELECT step_spec->>'operator' 
     FROM __session_steps WHERE step_type = 'where'),
    'AND',
    'Default where operator is AND'
);

-- ============================================================================
-- TEST GROUP 6: flow.where() - Grouped conditions
-- ============================================================================

-- Test 6.1: Group name stored
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('pg_temp.test_orders');
SELECT flow.where('amount > 100', operator => 'AND', group_name => 'price_filter');

SELECT is(
    (SELECT step_spec->>'group' 
     FROM __session_steps WHERE step_type = 'where'),
    'price_filter',
    'Where group name stored correctly'
);

-- ============================================================================
-- TEST GROUP 7: flow.lookup() - Basic join
-- ============================================================================

-- Test 7.1: Lookup returns success message
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('pg_temp.test_orders');
SELECT matches(
    flow.lookup(
        'pg_temp.test_customers',
        't0.customer_id = cust.customer_id',
        ARRAY['customer_name', 'email'],
        'cust',
        'allow',
        'error'
    ),
    'Step .+: lookup .+',
    'lookup returns formatted message'
);

-- Test 7.2: Lookup object stored
SELECT is(
    (SELECT step_spec->>'lookup_object' 
     FROM __session_steps WHERE step_type = 'lookup'),
    'pg_temp.test_customers',
    'Lookup object name stored correctly'
);

-- Test 7.3: Lookup alias stored
SELECT is(
    (SELECT step_spec->>'lookup_alias' 
     FROM __session_steps WHERE step_type = 'lookup'),
    'cust',
    'Lookup alias stored correctly'
);

-- Test 7.4: ON condition stored
SELECT is(
    (SELECT step_spec->>'on' 
     FROM __session_steps WHERE step_type = 'lookup'),
    't0.customer_id = cust.customer_id',
    'Lookup ON condition stored correctly'
);

-- Test 7.5: on_miss parameter stored
SELECT is(
    (SELECT step_spec->>'on_miss' 
     FROM __session_steps WHERE step_type = 'lookup'),
    'allow',
    'Lookup on_miss parameter stored correctly'
);

-- Test 7.6: on_duplicate parameter stored
SELECT is(
    (SELECT step_spec->>'on_duplicate' 
     FROM __session_steps WHERE step_type = 'lookup'),
    'error',
    'Lookup on_duplicate parameter stored correctly'
);

-- ============================================================================
-- TEST GROUP 8: flow.aggregate() - Group by
-- ============================================================================

-- Test 8.1: Aggregate returns success message
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('pg_temp.test_orders');
SELECT matches(
    flow.aggregate(
        ARRAY['customer_id', 'status'],
        'COUNT(*):order_count',
        'SUM(amount):total_amount'
    ),
    'Step .+: aggregate',
    'aggregate returns formatted message'
);

-- Test 8.2: Group by columns stored
SELECT is(
    (SELECT jsonb_array_length(step_spec->'group_by_columns') 
     FROM __session_steps WHERE step_type = 'aggregate'),
    2,
    'Group by has correct number of columns'
);

-- Test 8.3: Aggregation mapping stored
SELECT is(
    (SELECT step_spec->'aggregation_mapping'->>'order_count' 
     FROM __session_steps WHERE step_type = 'aggregate'),
    'COUNT(*)',
    'Aggregation mapping: order_count -> COUNT(*)'
);

SELECT is(
    (SELECT step_spec->'aggregation_mapping'->>'total_amount' 
     FROM __session_steps WHERE step_type = 'aggregate'),
    'SUM(amount)',
    'Aggregation mapping: total_amount -> SUM(amount)'
);

-- ============================================================================
-- TEST GROUP 9: flow.write() - Create mode
-- ============================================================================

-- Test 9.1: Write returns success message
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('pg_temp.test_orders');
SELECT flow.select('order_id', 'amount');
SELECT matches(
    flow.write('src', 'pg_temp.test_output', 'create'),
    'Step .+: write to .+',
    'write returns formatted message'
);

-- Test 9.2: Target table stored
SELECT is(
    (SELECT step_spec->>'target' 
     FROM __session_steps WHERE step_type = 'write'),
    'pg_temp.test_output',
    'Write target table stored correctly'
);

-- Test 9.3: Write mode stored
SELECT is(
    (SELECT step_spec->>'mode' 
     FROM __session_steps WHERE step_type = 'write'),
    'create',
    'Write mode stored correctly'
);

-- Finish tests
SELECT * FROM finish();
ROLLBACK;
