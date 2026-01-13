-- ============================================================================
-- pgTap Integration Tests for Examples Documentation
-- ============================================================================
-- Tests pipelines from docs/examples.md by building them, registering,
-- and verifying the compiled SQL stored in flow.pipeline table.
-- Run: psql -d your_database -f tests/test_integration_examples.sql
-- ============================================================================

-- Create extension if not exists
CREATE EXTENSION IF NOT EXISTS pgtap;

-- Create test helpers schema if not exists
CREATE SCHEMA IF NOT EXISTS test_helpers;

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

-- Helper: Setup test tables matching sample-data.md
CREATE OR REPLACE FUNCTION test_helpers.setup_integration_test_tables()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- Create schemas
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS staging;
    
    -- Create test tables (lightweight versions)
    DROP TABLE IF EXISTS raw.orders CASCADE;
    DROP TABLE IF EXISTS raw.customers CASCADE;
    DROP TABLE IF EXISTS raw.products CASCADE;
    DROP TABLE IF EXISTS raw.transactions CASCADE;
    DROP TABLE IF EXISTS raw.order_items CASCADE;
    DROP TABLE IF EXISTS raw.events CASCADE;
    DROP TABLE IF EXISTS raw.user_activity CASCADE;
    DROP TABLE IF EXISTS raw.shipments CASCADE;
    DROP TABLE IF EXISTS raw.warehouses CASCADE;
    DROP TABLE IF EXISTS staging.product_counts CASCADE;
    
    CREATE TABLE raw.orders (
        order_id INT PRIMARY KEY,
        customer_id INT,
        order_date DATE,
        status TEXT,
        total_amount NUMERIC(10,2),
        region TEXT
    );
    
    CREATE TABLE raw.customers (
        customer_id INT PRIMARY KEY,
        customer_name TEXT,
        email TEXT,
        region TEXT,
        total_purchases NUMERIC(10,2),
        last_purchase_date DATE
    );
    
    CREATE TABLE raw.products (
        product_id INT PRIMARY KEY,
        product_name TEXT,
        category TEXT,
        unit_price NUMERIC(10,2)
    );
    
    CREATE TABLE raw.transactions (
        transaction_id INT PRIMARY KEY,
        transaction_date DATE,
        customer_id INT,
        product_id INT,
        quantity INT,
        unit_price NUMERIC(10,2),
        amount NUMERIC(10,2)
    );
    
    CREATE TABLE raw.order_items (
        order_item_id INT PRIMARY KEY,
        order_id INT,
        product_id INT,
        quantity INT,
        unit_price NUMERIC(10,2),
        region TEXT,
        product_category TEXT
    );
    
    CREATE TABLE raw.events (
        event_id INT PRIMARY KEY,
        event_type TEXT,
        user_id INT,
        event_timestamp TIMESTAMP,
        event_date DATE
    );
    
    CREATE TABLE raw.user_activity (
        activity_id INT PRIMARY KEY,
        user_id INT,
        activity_type TEXT,
        activity_date DATE,
        activity_timestamp TIMESTAMP
    );
    
    CREATE TABLE raw.shipments (
        shipment_id INT PRIMARY KEY,
        order_id INT,
        warehouse_id INT,
        ship_date DATE,
        carrier TEXT
    );
    
    CREATE TABLE raw.warehouses (
        warehouse_id INT PRIMARY KEY,
        warehouse_name TEXT,
        region TEXT,
        is_active BOOLEAN
    );
    
    CREATE TABLE staging.product_counts (
        product_id INT,
        warehouse_id INT,
        quantity_on_hand INT
    );
    
    -- Insert minimal test data
    INSERT INTO raw.orders VALUES (1, 100, '2025-01-15', 'completed', 150.00, 'US');
    INSERT INTO raw.customers VALUES (100, 'Test Customer', 'test@example.com', 'US', 5000.00, '2025-01-01');
    INSERT INTO raw.products VALUES (1, 'Test Product', 'Electronics', 100.00);
    INSERT INTO raw.warehouses VALUES (1, 'Main Warehouse', 'US', true);
END;
$$;

-- Helper: Get compiled SQL from registered pipeline
CREATE OR REPLACE FUNCTION test_helpers.get_pipeline_sql(p_pipeline_name text)
RETURNS text
LANGUAGE sql
AS $$
    SELECT compiled_sql FROM flow.pipeline WHERE pipeline_name = p_pipeline_name;
$$;

-- Helper: Clean up test pipelines
CREATE OR REPLACE FUNCTION test_helpers.cleanup_test_pipelines()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM flow.pipeline_step WHERE pipeline_id IN (
        SELECT pipeline_id FROM flow.pipeline WHERE pipeline_name LIKE 'test_%'
    );
    DELETE FROM flow.pipeline WHERE pipeline_name LIKE 'test_%';
END;
$$;

BEGIN;

-- Plan the number of tests
SELECT plan(30);

-- Setup test environment
SELECT test_helpers.setup_integration_test_tables();
SELECT test_helpers.cleanup_test_pipelines();

-- ============================================================================
-- TEST GROUP 1: Simple SELECT with Filter
-- Example from: Basic Pipelines > Simple SELECT with Filter
-- ============================================================================
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('raw.orders');
SELECT flow.where('order_date >= ''2025-01-01''');
SELECT flow.where('status = ''completed''');
SELECT flow.select('order_id', 'customer_id', 'total_amount', 'order_date');
SELECT flow.register_pipeline('test_completed_orders_ytd', 'Orders completed in 2025');

SELECT ok(
    test_helpers.get_pipeline_sql('test_completed_orders_ytd') IS NOT NULL,
    'Simple SELECT with Filter: Pipeline registered'
);

SELECT ok(
    test_helpers.get_pipeline_sql('test_completed_orders_ytd') ILIKE '%FROM raw.orders%',
    'Simple SELECT with Filter: Contains FROM raw.orders'
);

SELECT ok(
    test_helpers.get_pipeline_sql('test_completed_orders_ytd') ILIKE '%WHERE%order_date >= ''2025-01-01''%status = ''completed''%',
    'Simple SELECT with Filter: Contains both WHERE conditions'
);

SELECT ok(
    test_helpers.get_pipeline_sql('test_completed_orders_ytd') ILIKE '%t0.order_id AS order_id%',
    'Simple SELECT with Filter: Contains order_id column'
);

-- ============================================================================
-- TEST GROUP 2: Multiple Filters with OR Logic
-- Example from: Basic Pipelines > Multiple Filters with OR Logic
-- ============================================================================
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('raw.orders');
SELECT flow.where('status = ''completed''', 'OR', 'status_group');
SELECT flow.where('status = ''shipped''', 'OR', 'status_group');
SELECT flow.where('order_date >= ''2025-01-01''');
SELECT flow.where('total_amount >= 1000', 'OR', 'amount_group');
SELECT flow.where('total_amount <= 100', 'OR', 'amount_group');
SELECT flow.select('order_id', 'customer_id', 'status', 'total_amount', 'order_date');
SELECT flow.register_pipeline('test_or_filters', 'Orders with OR logic');

SELECT ok(
    test_helpers.get_pipeline_sql('test_or_filters') ILIKE '%status = ''completed'' OR status = ''shipped''%',
    'OR Logic: Contains OR condition for status'
);

SELECT ok(
    test_helpers.get_pipeline_sql('test_or_filters') ILIKE '%total_amount >= 1000 OR total_amount <= 100%',
    'OR Logic: Contains OR condition for amount'
);

SELECT ok(
    test_helpers.get_pipeline_sql('test_or_filters') ILIKE '%order_date >= ''2025-01-01''%',
    'OR Logic: Contains ungrouped AND condition'
);

-- ============================================================================
-- TEST GROUP 3: Calculated Columns
-- Example from: Basic Pipelines > Calculated Columns
-- ============================================================================
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('raw.transactions');
SELECT flow.select(
    'transaction_id',
    'product_id',
    'quantity',
    'unit_price',
    'quantity * unit_price:line_total',
    'ROUND(quantity * unit_price * 0.08, 2):tax_amount',
    'ROUND(quantity * unit_price * 1.08, 2):total_with_tax'
);
SELECT flow.where('transaction_date = CURRENT_DATE');
SELECT flow.register_pipeline('test_todays_sales_with_tax', 'Today sales with tax');

SELECT ok(
    test_helpers.get_pipeline_sql('test_todays_sales_with_tax') ILIKE '%quantity * unit_price AS line_total%',
    'Calculated Columns: Contains line_total calculation'
);

SELECT ok(
    test_helpers.get_pipeline_sql('test_todays_sales_with_tax') ILIKE '%ROUND(quantity * unit_price * 0.08, 2) AS tax_amount%',
    'Calculated Columns: Contains tax_amount calculation'
);

SELECT ok(
    test_helpers.get_pipeline_sql('test_todays_sales_with_tax') ILIKE '%ROUND(quantity * unit_price * 1.08, 2) AS total_with_tax%',
    'Calculated Columns: Contains total_with_tax calculation'
);

-- ============================================================================
-- TEST GROUP 4: Conditional Logic (CASE expressions)
-- Example from: Basic Pipelines > Conditional Logic
-- ============================================================================
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('raw.customers');
SELECT flow.select(
    'customer_id',
    'customer_name',
    'total_purchases',
    flow.step('Customer tier'),
    'CASE 
        WHEN total_purchases >= 10000 THEN ''platinum''
        WHEN total_purchases >= 5000 THEN ''gold''
        WHEN total_purchases >= 1000 THEN ''silver''
        ELSE ''bronze''
    END:customer_tier'
);
SELECT flow.register_pipeline('test_customer_segmentation', 'Customer segmentation');

SELECT ok(
    test_helpers.get_pipeline_sql('test_customer_segmentation') ILIKE '%CASE%WHEN total_purchases >= 10000 THEN ''platinum''%',
    'Conditional Logic: Contains CASE expression for customer_tier'
);

SELECT ok(
    test_helpers.get_pipeline_sql('test_customer_segmentation') ILIKE '%AS customer_tier%',
    'Conditional Logic: Contains customer_tier alias'
);

-- ============================================================================
-- TEST GROUP 5: Basic GROUP BY Aggregation
-- Example from: Aggregations > Basic GROUP BY
-- ============================================================================
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('raw.orders');
SELECT flow.where('order_date >= ''2025-01-01''');
SELECT flow.aggregate(
    'orders',
    ARRAY['region'],
    flow.count('*', 'order_count'),
    flow.sum('total_amount', 'total_sales'),
    flow.avg('total_amount', 'avg_order_value')
);
SELECT flow.register_pipeline('test_regional_summary', 'Regional sales summary');

SELECT ok(
    test_helpers.get_pipeline_sql('test_regional_summary') ILIKE '%GROUP BY%region%',
    'Basic Aggregation: Contains GROUP BY region'
);

SELECT ok(
    test_helpers.get_pipeline_sql('test_regional_summary') ILIKE '%COUNT(*) AS order_count%',
    'Basic Aggregation: Contains COUNT(*) aggregate'
);

SELECT ok(
    test_helpers.get_pipeline_sql('test_regional_summary') ILIKE '%SUM(total_amount) AS total_sales%',
    'Basic Aggregation: Contains SUM(total_amount) aggregate'
);

SELECT ok(
    test_helpers.get_pipeline_sql('test_regional_summary') ILIKE '%AVG(total_amount) AS avg_order_value%',
    'Basic Aggregation: Contains AVG(total_amount) aggregate'
);

-- ============================================================================
-- TEST GROUP 6: Multi-Level Aggregation with Lookup
-- Example from: Aggregations > Multi-Level Aggregation
-- ============================================================================
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('raw.order_items');
SELECT flow.lookup('raw.products', 't0.product_id = t1.product_id', ARRAY['category'], 't1');
SELECT flow.aggregate(
    'order_items',
    ARRAY['t0.region', 't1.category'],
    flow.sum('t0.quantity', 'total_quantity'),
    flow.sum('t0.quantity * t0.unit_price', 'total_sales'),
    flow.count('*', 'order_count')
);
SELECT flow.register_pipeline('test_regional_category_summary', 'Regional category summary');

SELECT ok(
    test_helpers.get_pipeline_sql('test_regional_category_summary') ILIKE '%LEFT JOIN raw.products%',
    'Multi-Level Aggregation: Contains LEFT JOIN to products'
);

SELECT ok(
    test_helpers.get_pipeline_sql('test_regional_category_summary') ILIKE '%GROUP BY%region%category%',
    'Multi-Level Aggregation: Contains GROUP BY region and category'
);

SELECT ok(
    test_helpers.get_pipeline_sql('test_regional_category_summary') ILIKE '%SUM(t0.quantity) AS total_quantity%',
    'Multi-Level Aggregation: Contains quantity sum'
);

-- ============================================================================
-- TEST GROUP 7: Simple Lookup
-- Example from: Joins and Lookups > Simple Lookup
-- ============================================================================
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('raw.orders');
SELECT flow.lookup('raw.customers', 't0.customer_id = t1.customer_id', ARRAY['customer_name', 'email'], 't1');
SELECT flow.select(
    't0.order_id',
    't0.order_date',
    't1.customer_name',
    't1.email',
    't0.total_amount'
);
SELECT flow.where('t0.order_date >= ''2025-01-01''');
SELECT flow.register_pipeline('test_orders_with_customers', 'Orders with customer info');

SELECT ok(
    test_helpers.get_pipeline_sql('test_orders_with_customers') ILIKE '%LEFT JOIN raw.customers%t0.customer_id = t1.customer_id%',
    'Simple Lookup: Contains LEFT JOIN with join condition'
);

SELECT ok(
    test_helpers.get_pipeline_sql('test_orders_with_customers') ILIKE '%t1.customer_name%',
    'Simple Lookup: Contains customer_name from lookup'
);

SELECT ok(
    test_helpers.get_pipeline_sql('test_orders_with_customers') ILIKE '%t1.email%',
    'Simple Lookup: Contains email from lookup'
);

-- ============================================================================
-- TEST GROUP 8: Multiple Lookups
-- Example from: Joins and Lookups > Multiple Lookups
-- ============================================================================
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('raw.order_items');
SELECT flow.lookup('raw.orders', 't0.order_id = t1.order_id', ARRAY['order_id', 'order_date', 'status', 'customer_id'], 't1');
SELECT flow.lookup('raw.customers', 't1.customer_id = t2.customer_id', ARRAY['customer_name'], 't2');
SELECT flow.lookup('raw.products', 't0.product_id = t3.product_id', ARRAY['product_name', 'category'], 't3');
SELECT flow.select(
    't0.order_item_id',
    't1.order_id',
    't1.order_date',
    't2.customer_name',
    't3.product_name',
    't3.category',
    't0.quantity',
    't0.unit_price',
    't0.quantity * t0.unit_price:line_total'
);
SELECT flow.where('t1.status = ''completed''');
SELECT flow.register_pipeline('test_order_details_enriched', 'Order details with lookups');

SELECT ok(
    test_helpers.get_pipeline_sql('test_order_details_enriched') ILIKE '%LEFT JOIN raw.orders%',
    'Multiple Lookups: Contains first lookup to orders'
);

SELECT ok(
    test_helpers.get_pipeline_sql('test_order_details_enriched') ILIKE '%LEFT JOIN raw.customers%',
    'Multiple Lookups: Contains second lookup to customers'
);

SELECT ok(
    test_helpers.get_pipeline_sql('test_order_details_enriched') ILIKE '%LEFT JOIN raw.products%',
    'Multiple Lookups: Contains third lookup to products'
);

-- ============================================================================
-- TEST GROUP 9: Lookup with Complex Join Condition
-- Example from: Joins and Lookups > Lookup with Complex Join Condition
-- ============================================================================
SELECT test_helpers.reset_flow_session();
SELECT flow.read_db_object('raw.shipments');
SELECT flow.lookup(
    'raw.warehouses', 
    't0.warehouse_id = t1.warehouse_id AND t1.is_active = true',
    ARRAY['warehouse_name', 'region'],
    't1'
);
SELECT flow.select(
    't0.shipment_id',
    't0.ship_date',
    't1.warehouse_name',
    't1.region',
    't0.carrier'
);
SELECT flow.register_pipeline('test_active_warehouse_shipments', 'Shipments from active warehouses');

SELECT ok(
    test_helpers.get_pipeline_sql('test_active_warehouse_shipments') ILIKE '%t0.warehouse_id = t1.warehouse_id AND t1.is_active = true%',
    'Complex Join: Contains complex join condition with AND'
);

SELECT ok(
    test_helpers.get_pipeline_sql('test_active_warehouse_shipments') ILIKE '%t1.warehouse_name%',
    'Complex Join: Contains warehouse_name from lookup'
);

-- Finish tests
SELECT * FROM finish();
ROLLBACK;
