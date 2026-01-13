-- ============================================================================
-- Test Helper Functions for pgFlow Unit Tests
-- ============================================================================
-- Common utilities for testing Flow pipelines and compiled SQL
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS test_helpers;

-- ============================================================================
-- Helper: Check if compiled SQL contains a pattern (case-insensitive)
-- ============================================================================
CREATE OR REPLACE FUNCTION test_helpers.sql_contains(
    compiled_sql text,
    pattern text,
    description text DEFAULT NULL
) RETURNS boolean
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN compiled_sql ILIKE '%' || pattern || '%';
END;
$$;

COMMENT ON FUNCTION test_helpers.sql_contains IS 
'Check if compiled SQL contains a pattern (case-insensitive)';

-- ============================================================================
-- Helper: Check if compiled SQL does NOT contain a pattern
-- ============================================================================
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

-- ============================================================================
-- Helper: Normalize SQL for comparison (remove extra whitespace, case)
-- ============================================================================
CREATE OR REPLACE FUNCTION test_helpers.normalize_sql(
    sql_text text
) RETURNS text
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN regexp_replace(
        regexp_replace(
            lower(trim(sql_text)),
            '\s+', ' ', 'g'
        ),
        '\(\s+', '(', 'g'
    );
END;
$$;

-- ============================================================================
-- Helper: Setup standard test tables
-- ============================================================================
CREATE OR REPLACE FUNCTION test_helpers.setup_test_tables()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- Drop existing test tables
    DROP TABLE IF EXISTS pg_temp.test_orders CASCADE;
    DROP TABLE IF EXISTS pg_temp.test_customers CASCADE;
    DROP TABLE IF EXISTS pg_temp.test_products CASCADE;
    
    -- Create test_orders table
    CREATE TEMP TABLE test_orders (
        order_id int,
        customer_id int,
        product_id int,
        amount numeric,
        quantity int,
        order_date date,
        status text
    );

    -- Create test_customers table
    CREATE TEMP TABLE test_customers (
        customer_id int,
        customer_name text,
        email text,
        account_status text
    );

    -- Create test_products table
    CREATE TEMP TABLE test_products (
        product_id int,
        product_name text,
        category text,
        price numeric
    );
    
    -- Insert sample data
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

COMMENT ON FUNCTION test_helpers.setup_test_tables IS 
'Create and populate standard test tables for Flow tests';

-- ============================================================================
-- Helper: Check step spec in __session_steps
-- ============================================================================
CREATE OR REPLACE FUNCTION test_helpers.check_step_spec(
    step_order_val int,
    step_type_val text,
    expected_spec jsonb
) RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    actual_spec jsonb;
BEGIN
    SELECT step_spec INTO actual_spec
    FROM __session_steps
    WHERE step_order = step_order_val 
      AND step_type = step_type_val;
    
    IF actual_spec IS NULL THEN
        RETURN false;
    END IF;
    
    -- Check if expected keys exist and match
    RETURN actual_spec @> expected_spec;
END;
$$;

COMMENT ON FUNCTION test_helpers.check_step_spec IS 
'Verify step_spec contains expected JSON structure';

-- ============================================================================
-- Helper: Execute compiled SQL and return row count
-- ============================================================================
CREATE OR REPLACE FUNCTION test_helpers.execute_and_count(
    compiled_sql text
) RETURNS int
LANGUAGE plpgsql
AS $$
DECLARE
    row_count int;
BEGIN
    -- Wrap in CTE and count
    EXECUTE format('WITH query AS (%s) SELECT COUNT(*) FROM query', compiled_sql)
    INTO row_count;
    
    RETURN row_count;
END;
$$;

COMMENT ON FUNCTION test_helpers.execute_and_count IS 
'Execute compiled SQL and return the number of rows';

-- ============================================================================
-- Helper: Execute compiled SQL and return first column of first row
-- ============================================================================
CREATE OR REPLACE FUNCTION test_helpers.execute_and_get_value(
    compiled_sql text
) RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    result_value text;
BEGIN
    EXECUTE format('WITH query AS (%s) SELECT * FROM query LIMIT 1', compiled_sql)
    INTO result_value;
    
    RETURN result_value;
END;
$$;

-- ============================================================================
-- Helper: Reset flow session
-- ============================================================================
CREATE OR REPLACE FUNCTION test_helpers.reset_flow_session()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM flow.__ensure_session_steps();
    TRUNCATE TABLE __session_steps;
END;
$$;

COMMENT ON FUNCTION test_helpers.reset_flow_session IS 
'Clear all steps from the current flow session';

-- ============================================================================
-- Helper: Assert compiled SQL contains pattern (pgTap-style)
-- ============================================================================
CREATE OR REPLACE FUNCTION test_helpers.compiled_contains(
    compiled_sql text,
    pattern text,
    description text
) RETURNS text
LANGUAGE plpgsql
AS $$
BEGIN
    IF compiled_sql ILIKE '%' || pattern || '%' THEN
        RETURN ok(true, description);
    ELSE
        RETURN ok(false, description) || E'\n' || 
               diag('  Expected pattern: ' || pattern) || E'\n' ||
               diag('  Compiled SQL: ' || compiled_sql);
    END IF;
END;
$$;

-- ============================================================================
-- Helper: Assert compiled SQL does NOT contain pattern
-- ============================================================================
CREATE OR REPLACE FUNCTION test_helpers.compiled_not_contains(
    compiled_sql text,
    pattern text,
    description text
) RETURNS text
LANGUAGE plpgsql
AS $$
BEGIN
    IF compiled_sql NOT ILIKE '%' || pattern || '%' THEN
        RETURN ok(true, description);
    ELSE
        RETURN ok(false, description) || E'\n' || 
               diag('  Should not contain: ' || pattern) || E'\n' ||
               diag('  Compiled SQL: ' || compiled_sql);
    END IF;
END;
$$;
