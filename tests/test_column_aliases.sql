-- Test script for column alias tracking feature
-- This demonstrates that aliased columns can be referenced in subsequent steps

\echo 'Testing Column Alias Tracking Feature'
\echo '======================================'
\echo ''

-- Setup test data
CREATE SCHEMA IF NOT EXISTS raw;
DROP TABLE IF EXISTS raw.orders CASCADE;
CREATE TABLE raw.orders (
    order_id INTEGER,
    customer_id INTEGER,
    order_date DATE,
    status TEXT,
    total_amount NUMERIC(10,2),
    region TEXT
);

INSERT INTO raw.orders VALUES
    (1, 100, '2025-01-01', 'completed', 100.00, 'North'),
    (2, 101, '2025-01-02', 'pending', 150.00, 'South'),
    (3, 102, '2025-01-03', 'completed', 200.00, 'East'),
    (4, 103, '2025-01-04', 'cancelled', 75.00, 'West'),
    (5, 104, '2025-01-05', 'completed', 125.00, 'North');

\echo 'Test 1: Basic column alias creation and reuse'
\echo '----------------------------------------------'

-- Start pipeline
SELECT flow.read_db_object('raw.orders');

-- Create alias for transformed column
SELECT flow.select('UPPER(status):status_cleaned');

-- Use the alias in a WHERE clause
SELECT flow.where('status_cleaned = ''COMPLETED''');

-- Reference the alias in another SELECT
SELECT flow.select('order_id:id', 'status_cleaned:final_status', 'total_amount:amount');

-- Compile and show the SQL
\echo 'Generated SQL:'
SELECT flow.compile();

\echo ''
\echo 'Test 2: Chained aliases (alias an alias)'
\echo '-----------------------------------------'

-- Reset session
TRUNCATE TABLE __session_steps;

-- Start new pipeline
SELECT flow.read_db_object('raw.orders');

-- Create first alias
SELECT flow.select('total_amount:amount');

-- Create second alias based on first
SELECT flow.select('amount * 1.1:amount_with_tax');

-- Use the second alias
SELECT flow.select('order_id:id', 'amount_with_tax:final_amount');

-- Compile and show the SQL
\echo 'Generated SQL:'
SELECT flow.compile();

\echo ''
\echo 'Test 3: Multiple transformations with aliases'
\echo '---------------------------------------------'

-- Reset session
TRUNCATE TABLE __session_steps;

-- Start new pipeline
SELECT flow.read_db_object('raw.orders');

-- Create multiple aliases
SELECT flow.select(
    'UPPER(status):status_cleaned',
    'total_amount:amount',
    'LOWER(region):region_lower'
);

-- Use all aliases in subsequent steps
SELECT flow.where('status_cleaned = ''COMPLETED''');
SELECT flow.select('order_id:id', 'status_cleaned:status', 'amount * 1.1:amount_with_tax', 'region_lower:region');

-- Compile and show the SQL
\echo 'Generated SQL:'
SELECT flow.compile();

\echo ''
\echo 'All tests completed!'
\echo 'If no errors occurred, the column alias tracking feature is working correctly.'
