-- ============================================================================
-- Example: Transform denormalized order data into JSON with aggregated lines
-- ============================================================================
-- Scenario: We receive order data in a flat structure where each row is a 
-- line item. We need to transform this into a JSON document where each order
-- has an array of line items.
--
-- Raw data structure (denormalized):
--   - Each row contains both order header info (order_id, order_date, customer)
--   - And line item details (line_num, product, quantity, unit_price)
--
-- Desired output (1 row per order):
--   {
--     "order_id": 1001,
--     "order_date": "2025-12-25",
--     "customer": "ACME CORP",  -- uppercased
--     "items": [
--       {"line": 1, "product": "Widget A", "qty": 5, "price": 10.00},
--       {"line": 2, "product": "Widget B", "qty": 3, "price": 15.00}
--     ]
--   }
-- ============================================================================

-- Setup: Create raw table and insert sample data
DROP TABLE IF EXISTS raw.order_lines CASCADE;
CREATE TABLE raw.order_lines (
    order_id int,
    order_date date,
    customer text,
    line_num int,
    product text,
    quantity int,
    unit_price numeric(10,2)
);

-- Insert 1 order with 2 line items
INSERT INTO raw.order_lines VALUES
    (1001, '2025-12-25', 'Acme Corp', 1, 'Widget A', 5, 10.00),
    (1001, '2025-12-25', 'Acme Corp', 2, 'Widget B', 3, 15.00);

-- Verify raw data
SELECT * FROM raw.order_lines;

-- ============================================================================
-- Build the transformation pipeline using pgFlow
-- ============================================================================

-- Step 1: Read from raw table
SELECT flow.read_db_object('raw.order_lines', 'Read raw order lines');

-- Step 2: Select and transform columns with explicit source:target mapping
-- - Simple VARIADIC syntax - no ARRAY[] wrapper needed
-- - Optional ':target_name' suffix for explicit aliasing
-- - Use flow.step() anywhere to provide description
SELECT flow.select(
    flow.step('Transform and prepare line items'),
    'order_id',
    'order_date',
    'UPPER(customer):customer_name',
    'jsonb_build_object(
        ''line'', line_num,
        ''product'', product,
        ''qty'', quantity,
        ''price'', unit_price
    ):line_item'
);

-- Step 3: Aggregate lines into JSON array grouped by order
SELECT flow.aggregate(
    ARRAY['order_id', 'order_date', 'customer_name'],
    flow.step('Aggregate line items into JSON array'),
    'jsonb_agg(line_item ORDER BY (line_item->>''line'')::int):items'
);

-- View the pipeline
SELECT flow.show_pipeline();

-- View pipeline steps
SELECT * FROM __session_steps;

-- Compile and see the final SQL
SELECT flow.compile();

-- Step 4: Write to target table with upsert (creates table in development)
-- Using upsert mode to handle incremental updates based on order_id
SELECT flow.write(
    'stage.orders_json',
    mode => 'upsert',
    unique_keys => ARRAY['order_id'],
    auto_create => true
);

-- Final compile with write step
SELECT flow.compile();

-- Execute the pipeline
-- (In production, you would run: SELECT flow.execute();)


-- ============================================================================
-- Alternative write modes
-- ============================================================================

-- Simple insert (append only):
-- SELECT flow.write('stage.orders_json');

-- Full refresh (truncate and insert):
-- SELECT flow.write('stage.orders_json', truncate_before => true);

-- Upsert without delete (insert new, update existing):
-- SELECT flow.write('stage.orders_json',
--                   mode => 'upsert',
--                   unique_keys => ARRAY['order_id']);

-- Full sync (upsert + delete orphaned records):
-- SELECT flow.write('stage.orders_json',
--                   mode => 'upsert_delete',
--                   unique_keys => ARRAY['order_id']);


-- ============================================================================
-- Manual verification (optional)
-- ============================================================================
-- To verify the expected output, you can run the compiled SQL manually:
    order_id,
    order_date,
    customer_name,
    jsonb_agg(line_item ORDER BY line_item->>'line') as items
FROM (
-- To verify the expected output, you can run the compiled SQL manually:
/*
WITH transformed_lines AS (
    SELECT
        order_id,
        order_date,
        UPPER(customer) as customer_name,
        jsonb_build_object(
            'line', line_num,
            'product', product,
            'qty', quantity,
            'price', unit_price
        ) as line_item
    FROM raw.order_lines t0
)
SELECT
    order_id,
    order_date,
    customer_name,
    jsonb_agg(line_item ORDER BY (line_item->>'line')::int) as items
FROM transformed_lines
GROUP BY order_id, order_date, customer_name;
*/

-- The result should be:
-- order_id | order_date | customer_name | items
-- ---------+------------+---------------+-------------------------------------------------------
-- 1001     | 2025-12-25 | ACME CORP     | [{"line": 1, "product": "Widget A", "qty": 5, ...}, ...]


-- ============================================================================
-- Notes:
-- ============================================================================
-- This example demonstrates:
-- 1. Reading denormalized data with read_db_object()
-- 2. Transforming columns with select() using VARIADIC syntax and flow.step()
-- 3. Building JSON structures with jsonb_build_object()
-- 4. Aggregating rows with aggregate() and GROUP BY
-- 5. Creating complex JSON aggregations with jsonb_agg()
-- 6. Writing results with various modes (insert, upsert, upsert_delete)
-- 7. Auto-creating target tables for development
--
-- The pipeline produces one row per order with all line items aggregated
-- into a JSON array, and writes the result to stage.orders_json.
