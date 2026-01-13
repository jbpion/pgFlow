-- ============================================================================
-- pgTap Execution Tests for pgFlow
-- ============================================================================
-- Tests that execute compiled SQL and verify the results match expectations
-- Run: psql -d your_database -f tests/test_unit_execution.sql
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

-- Helper: Execute and count rows
CREATE OR REPLACE FUNCTION test_helpers.execute_and_count(
    compiled_sql text
) RETURNS int
LANGUAGE plpgsql
AS $$
DECLARE
    row_count int;
BEGIN
    EXECUTE format('WITH query AS (%s) SELECT COUNT(*) FROM query', compiled_sql)
    INTO row_count;
    RETURN row_count;
END;
$$;

BEGIN;

-- Plan the number of tests
SELECT plan(18);

-- Setup test environment with sample data
SELECT test_helpers.setup_test_tables();

-- ============================================================================
-- TEST 1: Simple read execution - row count
-- ============================================================================
SELECT test_helpers.reset_flow_session();
PERFORM flow.read_db_object('pg_temp.test_orders');

SELECT is(
    test_helpers.execute_and_count(flow.compile()),
    5,
    'Simple read returns 5 rows from test_orders'
);

-- ============================================================================
-- TEST 2: Select specific columns - verify column count
-- ============================================================================
SELECT test_helpers.reset_flow_session();
PERFORM flow.read_db_object('pg_temp.test_orders');
PERFORM flow.select('order_id', 'amount');

-- Execute and verify
DECLARE 
    v_compiled text;
    v_col_count int;
BEGIN
    v_compiled := flow.compile();
    
    -- Count columns in result
    EXECUTE format('
        SELECT COUNT(*) 
        FROM information_schema.columns 
        WHERE table_name IN (
            SELECT tablename 
            FROM pg_temp.pg_tables 
            WHERE tablename LIKE ''test_result_%%''
        ) LIMIT 1
    ') INTO v_col_count;
    
    -- Create temp table to hold results
    EXECUTE format('CREATE TEMP TABLE test_result_select AS %s', v_compiled);
    
    SELECT is(
        (SELECT COUNT(*)::int FROM information_schema.columns 
         WHERE table_name = 'test_result_select'),
        2,
        'Select returns exactly 2 columns (order_id, amount)'
    );
    
    DROP TABLE test_result_select;
END;

-- ============================================================================
-- TEST 3: WHERE filter - verify filtered results
-- ============================================================================
SELECT test_helpers.reset_flow_session();
PERFORM flow.read_db_object('pg_temp.test_orders');
PERFORM flow.where('status = ''completed''');

SELECT is(
    test_helpers.execute_and_count(flow.compile()),
    3,
    'WHERE status=completed returns 3 rows'
);

-- ============================================================================
-- TEST 4: WHERE with multiple conditions
-- ============================================================================
SELECT test_helpers.reset_flow_session();
PERFORM flow.read_db_object('pg_temp.test_orders');
PERFORM flow.where('status = ''completed''');
PERFORM flow.where('amount > 50');

SELECT is(
    test_helpers.execute_and_count(flow.compile()),
    2,
    'WHERE status=completed AND amount>50 returns 2 rows'
);

-- ============================================================================
-- TEST 5: Expression calculation
-- ============================================================================
SELECT test_helpers.reset_flow_session();
PERFORM flow.read_db_object('pg_temp.test_orders');
PERFORM flow.select('order_id', 'amount', 'amount * 1.1:adjusted_amount');
PERFORM flow.where('order_id = 1');

DECLARE
    v_compiled text;
    v_amount numeric;
    v_adjusted numeric;
BEGIN
    v_compiled := flow.compile();
    
    EXECUTE format('CREATE TEMP TABLE test_expr AS %s', v_compiled);
    
    SELECT amount, adjusted_amount 
    INTO v_amount, v_adjusted 
    FROM test_expr;
    
    SELECT is(
        v_adjusted,
        v_amount * 1.1,
        'Expression amount * 1.1 calculated correctly'
    );
    
    DROP TABLE test_expr;
END;

-- ============================================================================
-- TEST 6: CASE statement execution
-- ============================================================================
SELECT test_helpers.reset_flow_session();
PERFORM flow.read_db_object('pg_temp.test_orders');
PERFORM flow.select(
    'order_id',
    'amount',
    'CASE WHEN amount > 100 THEN ''high'' ELSE ''low'' END:amount_tier'
);

DECLARE
    v_compiled text;
    v_tier text;
BEGIN
    v_compiled := flow.compile();
    
    EXECUTE format('CREATE TEMP TABLE test_case AS %s', v_compiled);
    
    -- Check order_id=1 (amount=150, should be 'high')
    SELECT amount_tier INTO v_tier FROM test_case WHERE order_id = 1;
    
    SELECT is(
        v_tier,
        'high',
        'CASE statement: amount 150 classified as high'
    );
    
    -- Check order_id=4 (amount=50, should be 'low')
    SELECT amount_tier INTO v_tier FROM test_case WHERE order_id = 4;
    
    SELECT is(
        v_tier,
        'low',
        'CASE statement: amount 50 classified as low'
    );
    
    DROP TABLE test_case;
END;

-- ============================================================================
-- TEST 7: Lookup (JOIN) execution - matching rows
-- ============================================================================
SELECT test_helpers.reset_flow_session();
PERFORM flow.read_db_object('pg_temp.test_orders');
PERFORM flow.lookup(
    'pg_temp.test_customers',
    't0.customer_id = cust.customer_id',
    ARRAY['customer_name'],
    'cust',
    'allow',
    'error'
);
PERFORM flow.where('t0.order_id = 1');

DECLARE
    v_compiled text;
    v_name text;
BEGIN
    v_compiled := flow.compile();
    
    EXECUTE format('CREATE TEMP TABLE test_lookup AS %s', v_compiled);
    
    SELECT customer_name INTO v_name FROM test_lookup;
    
    SELECT is(
        v_name,
        'Alice Smith',
        'Lookup joins customer name correctly'
    );
    
    DROP TABLE test_lookup;
END;

-- ============================================================================
-- TEST 8: Lookup - NULL for unmatched rows (on_miss='allow')
-- ============================================================================
SELECT test_helpers.reset_flow_session();
PERFORM flow.read_db_object('pg_temp.test_orders');
PERFORM flow.lookup(
    'pg_temp.test_customers',
    't0.customer_id = cust.customer_id',
    ARRAY['customer_name'],
    'cust',
    'allow',
    'error'
);
PERFORM flow.where('t0.order_id = 5');

DECLARE
    v_compiled text;
    v_name text;
BEGIN
    v_compiled := flow.compile();
    
    EXECUTE format('CREATE TEMP TABLE test_lookup_null AS %s', v_compiled);
    
    SELECT customer_name INTO v_name FROM test_lookup_null;
    
    SELECT is(
        v_name,
        NULL,
        'Lookup returns NULL for unmatched row (customer_id=999)'
    );
    
    DROP TABLE test_lookup_null;
END;

-- ============================================================================
-- TEST 9: Multiple lookups (chained joins)
-- ============================================================================
SELECT test_helpers.reset_flow_session();
PERFORM flow.read_db_object('pg_temp.test_orders');
PERFORM flow.lookup(
    'pg_temp.test_customers',
    't0.customer_id = cust.customer_id',
    ARRAY['customer_name'],
    'cust',
    'allow',
    'error'
);
PERFORM flow.lookup(
    'pg_temp.test_products',
    't0.product_id = prod.product_id',
    ARRAY['product_name'],
    'prod',
    'allow',
    'error'
);
PERFORM flow.where('t0.order_id = 1');

DECLARE
    v_compiled text;
    v_cust_name text;
    v_prod_name text;
BEGIN
    v_compiled := flow.compile();
    
    EXECUTE format('CREATE TEMP TABLE test_multi_lookup AS %s', v_compiled);
    
    SELECT customer_name, product_name 
    INTO v_cust_name, v_prod_name 
    FROM test_multi_lookup;
    
    SELECT is(
        v_cust_name,
        'Alice Smith',
        'Multiple lookups: customer name joined'
    );
    
    SELECT is(
        v_prod_name,
        'Widget A',
        'Multiple lookups: product name joined'
    );
    
    DROP TABLE test_multi_lookup;
END;

-- ============================================================================
-- TEST 10: Aggregate (GROUP BY) execution
-- ============================================================================
SELECT test_helpers.reset_flow_session();
PERFORM flow.read_db_object('pg_temp.test_orders');
PERFORM flow.where('status = ''completed''');
PERFORM flow.aggregate(
    ARRAY['customer_id'],
    'COUNT(*):order_count',
    'SUM(amount):total_amount'
);

DECLARE
    v_compiled text;
    v_count int;
    v_total numeric;
BEGIN
    v_compiled := flow.compile();
    
    EXECUTE format('CREATE TEMP TABLE test_agg AS %s', v_compiled);
    
    -- Check customer 100 (2 completed orders, total 225)
    SELECT order_count, total_amount 
    INTO v_count, v_total 
    FROM test_agg 
    WHERE customer_id = 100;
    
    SELECT is(
        v_count,
        2,
        'Aggregate: customer 100 has 2 completed orders'
    );
    
    SELECT is(
        v_total,
        225.00,
        'Aggregate: customer 100 total is 225.00'
    );
    
    DROP TABLE test_agg;
END;

-- ============================================================================
-- TEST 11: Write CREATE mode - table created with data
-- ============================================================================
SELECT test_helpers.reset_flow_session();

DROP TABLE IF EXISTS pg_temp.test_write_output;

PERFORM flow.read_db_object('pg_temp.test_orders');
PERFORM flow.select('order_id', 'amount');
PERFORM flow.where('order_id <= 3');
PERFORM flow.write('src', 'pg_temp.test_write_output', 'create');

DECLARE
    v_compiled text;
BEGIN
    v_compiled := flow.compile();
    
    -- Execute the CREATE TABLE AS statement
    EXECUTE v_compiled;
    
    -- Verify table was created
    SELECT ok(
        EXISTS(
            SELECT 1 FROM pg_tables 
            WHERE schemaname LIKE 'pg_temp%' 
            AND tablename = 'test_write_output'
        ),
        'Write CREATE: table created'
    );
    
    -- Verify row count
    SELECT is(
        (SELECT COUNT(*)::int FROM pg_temp.test_write_output),
        3,
        'Write CREATE: 3 rows inserted'
    );
END;

-- ============================================================================
-- TEST 12: Write INSERT mode - data appended
-- ============================================================================
SELECT test_helpers.reset_flow_session();

-- Create target table
DROP TABLE IF EXISTS pg_temp.test_write_insert;
CREATE TEMP TABLE test_write_insert (order_id int, amount numeric);

PERFORM flow.read_db_object('pg_temp.test_orders');
PERFORM flow.select('order_id', 'amount');
PERFORM flow.where('order_id <= 2');
PERFORM flow.write('src', 'pg_temp.test_write_insert', 'insert');

DECLARE
    v_compiled text;
BEGIN
    v_compiled := flow.compile();
    
    -- Execute the INSERT statement
    EXECUTE v_compiled;
    
    -- Verify row count
    SELECT is(
        (SELECT COUNT(*)::int FROM pg_temp.test_write_insert),
        2,
        'Write INSERT: 2 rows inserted'
    );
END;

-- ============================================================================
-- TEST 13: Complex pipeline execution
-- ============================================================================
SELECT test_helpers.reset_flow_session();
PERFORM flow.read_db_object('pg_temp.test_orders');
PERFORM flow.where('status = ''completed''');
PERFORM flow.lookup(
    'pg_temp.test_customers',
    't0.customer_id = cust.customer_id',
    ARRAY['customer_name'],
    'cust',
    'allow',
    'error'
);
PERFORM flow.aggregate(
    ARRAY['customer_name'],
    'COUNT(*):order_count',
    'SUM(amount):total_spent'
);

DECLARE
    v_compiled text;
    v_count int;
    v_total numeric;
BEGIN
    v_compiled := flow.compile();
    
    EXECUTE format('CREATE TEMP TABLE test_complex AS %s', v_compiled);
    
    -- Check Alice Smith's aggregates
    SELECT order_count, total_spent 
    INTO v_count, v_total 
    FROM test_complex 
    WHERE customer_name = 'Alice Smith';
    
    SELECT is(
        v_count,
        2,
        'Complex pipeline: Alice has 2 orders'
    );
    
    SELECT is(
        v_total,
        225.00,
        'Complex pipeline: Alice total is 225.00'
    );
    
    DROP TABLE test_complex;
END;

-- Finish tests
SELECT * FROM finish();
ROLLBACK;
