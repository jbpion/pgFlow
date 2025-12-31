-- ============================================================================
-- pgTap Unit Tests for flow.compile() Function
-- ============================================================================
-- Tests the compile function by setting up known pipeline steps
-- and verifying the generated SQL matches expected output.
-- Run: psql -d your_database -f tests/test_unit_compile.sql
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

-- Helper: Check if SQL contains pattern
CREATE OR REPLACE FUNCTION test_helpers.sql_contains(
    compiled_sql text,
    pattern text
) RETURNS boolean
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN compiled_sql ILIKE '%' || pattern || '%';
END;
$$;

-- Helper: Check if SQL does NOT contain pattern
CREATE OR REPLACE FUNCTION test_helpers.sql_not_contains(
    compiled_sql text,
    pattern text
) RETURNS boolean
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN compiled_sql NOT ILIKE '%' || pattern || '%';
END;
$$;

BEGIN;

-- Plan the number of tests
SELECT plan(36);

-- Setup test environment
SELECT test_helpers.setup_test_tables();

-- ============================================================================
-- TEST 1: Simple read -> compile (should expand columns)
-- ============================================================================
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('pg_temp.test_orders');

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 't0.order_id AS order_id'),
    'Compiled SQL contains t0.order_id AS order_id'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 't0.customer_id AS customer_id'),
    'Compiled SQL contains t0.customer_id AS customer_id'
);

SELECT ok(
    test_helpers.sql_not_contains(flow.compile(), 't0.*'),
    'Compiled SQL does not contain wildcard t0.*'
);

-- ============================================================================
-- TEST 2: Read -> Select (simple columns)
-- ============================================================================
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('pg_temp.test_orders');
SELECT flow.select('order_id', 'customer_id', 'amount');

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 't0.order_id AS order_id'),
    'Select: t0.order_id AS order_id'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 't0.customer_id AS customer_id'),
    'Select: t0.customer_id AS customer_id'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 't0.amount AS amount'),
    'Select: t0.amount AS amount'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'FROM pg_temp.test_orders t0'),
    'Select: FROM clause correct'
);

-- ============================================================================
-- TEST 3: Read -> Select with expressions
-- ============================================================================
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('pg_temp.test_orders');
SELECT flow.select(
    'order_id',
    'amount * 1.1:adjusted_amount',
    'quantity * 2:double_qty'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 't0.amount * 1.1 AS adjusted_amount'),
    'Expression: amount * 1.1 compiled with alias'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 't0.quantity * 2 AS double_qty'),
    'Expression: quantity * 2 compiled with alias'
);

-- ============================================================================
-- TEST 4: Read -> Select with CASE (should NOT prefix with t0)
-- ============================================================================
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('pg_temp.test_orders');
SELECT flow.select(
    'order_id',
    'CASE WHEN amount > 100 THEN ''high'' ELSE ''low'' END:amount_tier'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'CASE WHEN amount > 100'),
    'CASE statement present in compiled SQL'
);

SELECT ok(
    test_helpers.sql_not_contains(flow.compile(), 't0.CASE'),
    'CASE statement NOT prefixed with t0.'
);

-- ============================================================================
-- TEST 5: Read -> Select -> Where (single condition)
-- ============================================================================
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('pg_temp.test_orders');
SELECT flow.select('order_id', 'amount', 'status');
SELECT flow.where('status = ''completed''');

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'WHERE'),
    'WHERE clause generated'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'status = ''completed'''),
    'WHERE condition present'
);

-- ============================================================================
-- TEST 6: Read -> Where (multiple conditions, same group)
-- ============================================================================
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('pg_temp.test_orders');
SELECT flow.where('amount > 50');
SELECT flow.where('status = ''completed''');

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'amount > 50'),
    'Multiple WHERE: first condition'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'AND'),
    'Multiple WHERE: AND operator'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'status = ''completed'''),
    'Multiple WHERE: second condition'
);

-- ============================================================================
-- TEST 7: Read -> Lookup (LEFT JOIN)
-- ============================================================================
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('pg_temp.test_orders');
SELECT flow.lookup(
    'pg_temp.test_customers',
    't0.customer_id = cust.customer_id',
    ARRAY['customer_name', 'email'],
    'cust',
    'allow',
    'error'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'LEFT JOIN pg_temp.test_customers cust'),
    'Lookup: LEFT JOIN generated'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'ON t0.customer_id = cust.customer_id'),
    'Lookup: ON condition correct'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'cust.customer_name AS customer_name'),
    'Lookup: customer_name column aliased'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'cust.email AS email'),
    'Lookup: email column aliased'
);

-- ============================================================================
-- TEST 8: Read -> Lookup with on_duplicate='first' (LATERAL)
-- ============================================================================
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('pg_temp.test_orders');
SELECT flow.lookup(
    'pg_temp.test_customers',
    't0.customer_id = cust.customer_id',
    ARRAY['customer_name'],
    'cust',
    'allow',
    'first'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'LEFT JOIN LATERAL'),
    'LATERAL: LEFT JOIN LATERAL generated'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'LIMIT 1'),
    'LATERAL: LIMIT 1 present'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), ') cust ON true'),
    'LATERAL: ON true clause'
);

-- ============================================================================
-- TEST 9: Read -> Multiple Lookups (chained joins)
-- ============================================================================
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('pg_temp.test_orders');
SELECT flow.lookup(
    'pg_temp.test_customers',
    't0.customer_id = cust.customer_id',
    ARRAY['customer_name'],
    'cust',
    'allow',
    'error'
);
SELECT flow.lookup(
    'pg_temp.test_products',
    't0.product_id = prod.product_id',
    ARRAY['product_name', 'category'],
    'prod',
    'allow',
    'error'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'LEFT JOIN pg_temp.test_customers cust'),
    'Multiple lookups: first join'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'LEFT JOIN pg_temp.test_products prod'),
    'Multiple lookups: second join'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'cust.customer_name'),
    'Multiple lookups: first lookup column'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'prod.product_name'),
    'Multiple lookups: second lookup column'
);

-- ============================================================================
-- TEST 10: Read -> Aggregate (GROUP BY)
-- ============================================================================
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('pg_temp.test_orders');
SELECT flow.aggregate(
    ARRAY['customer_id', 'status'],
    'COUNT(*):order_count',
    'SUM(amount):total_amount'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'GROUP BY customer_id, status'),
    'Aggregate: GROUP BY clause'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'COUNT(*) AS order_count'),
    'Aggregate: COUNT(*) aggregation'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'SUM(amount) AS total_amount'),
    'Aggregate: SUM(amount) aggregation'
);

-- ============================================================================
-- TEST 11: Read -> Select -> Write (CREATE mode)
-- ============================================================================
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('pg_temp.test_orders');
SELECT flow.select('order_id', 'amount');
SELECT flow.write('src', 'pg_temp.test_output', 'create');

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'CREATE TABLE pg_temp.test_output AS'),
    'Write CREATE: CREATE TABLE AS generated'
);

-- ============================================================================
-- TEST 12: Read -> Select -> Write (INSERT mode)
-- ============================================================================
SELECT test_helpers.reset_flow_session();

-- Create target table first
DROP TABLE IF EXISTS pg_temp.test_target;
CREATE TEMP TABLE test_target (order_id int, amount numeric);

SELECT flow.read_db_object('pg_temp.test_orders');
SELECT flow.select('order_id', 'amount');
SELECT flow.write('src', 'pg_temp.test_target', 'insert');

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'INSERT INTO pg_temp.test_target'),
    'Write INSERT: INSERT INTO generated'
);

-- ============================================================================
-- TEST 13: Function calls should not be aliased
-- ============================================================================
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('pg_temp.test_orders');
SELECT flow.select(
    'order_id',
    'UPPER(status):status_upper',
    'COALESCE(amount, 0):safe_amount'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'UPPER(status) AS status_upper'),
    'Function call UPPER() not prefixed'
);

SELECT ok(
    test_helpers.sql_not_contains(flow.compile(), 't0.UPPER'),
    'Function call UPPER() not prefixed with t0.'
);

SELECT ok(
    test_helpers.sql_contains(flow.compile(), 'COALESCE(amount, 0) AS safe_amount'),
    'Function call COALESCE() not prefixed'
);

SELECT ok(
    test_helpers.sql_not_contains(flow.compile(), 't0.COALESCE'),
    'Function call COALESCE() not prefixed with t0.'
);

-- Finish tests
SELECT * FROM finish();
ROLLBACK;
