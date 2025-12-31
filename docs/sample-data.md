# Sample Data Setup

This guide creates a sample e-commerce dataset for testing and learning pgFlow. The schema is based on common PostgreSQL example databases and matches all examples in the documentation.

## Quick Setup

Run this entire script to set up the sample database:

```sql
-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS dim;

-- ============================================================================
-- RAW LAYER: Source data tables
-- ============================================================================

-- Customers
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id SERIAL PRIMARY KEY,
    customer_name TEXT NOT NULL,
    email TEXT,
    phone TEXT,
    region TEXT,
    signup_date DATE DEFAULT CURRENT_DATE,
    total_purchases NUMERIC(10,2) DEFAULT 0,
    last_purchase_date DATE
);

-- Products
CREATE TABLE IF NOT EXISTS raw.products (
    product_id SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT,
    unit_price NUMERIC(10,2),
    cost NUMERIC(10,2),
    description TEXT
);

-- Orders
CREATE TABLE IF NOT EXISTS raw.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES raw.customers(customer_id),
    order_date DATE NOT NULL,
    status TEXT,
    total_amount NUMERIC(10,2),
    region TEXT
);

-- Order Lines (detailed line items)
DROP TABLE IF EXISTS raw.order_lines CASCADE;
CREATE TABLE raw.order_lines (
    order_line_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES raw.orders(order_id),
    product_id INTEGER REFERENCES raw.products(product_id),
    customer TEXT,  -- denormalized for examples
    line_num INTEGER,
    product TEXT,
    quantity INTEGER,
    unit_price NUMERIC(10,2)
);

-- Order Items (for aggregation examples)
CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price NUMERIC(10,2),
    region TEXT,
    product_category TEXT
);

-- Transactions
CREATE TABLE IF NOT EXISTS raw.transactions (
    transaction_id SERIAL PRIMARY KEY,
    transaction_date DATE,
    customer_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price NUMERIC(10,2),
    amount NUMERIC(10,2)
);

-- User Activity (for event tracking examples)
CREATE TABLE IF NOT EXISTS raw.events (
    event_id SERIAL PRIMARY KEY,
    event_type TEXT,
    user_id INTEGER,
    event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    event_date DATE DEFAULT CURRENT_DATE
);

-- User Activity (for today's activity example)
CREATE TABLE IF NOT EXISTS raw.user_activity (
    activity_id SERIAL PRIMARY KEY,
    user_id INTEGER,
    activity_type TEXT,
    activity_date DATE,
    activity_timestamp TIMESTAMP
);

-- Shipments
CREATE TABLE IF NOT EXISTS raw.shipments (
    shipment_id SERIAL PRIMARY KEY,
    order_id INTEGER,
    warehouse_id INTEGER,
    ship_date DATE,
    carrier TEXT,
    tracking_number TEXT
);

-- Warehouses
CREATE TABLE IF NOT EXISTS raw.warehouses (
    warehouse_id SERIAL PRIMARY KEY,
    warehouse_name TEXT,
    region TEXT,
    is_active BOOLEAN DEFAULT true
);

-- ============================================================================
-- STAGING LAYER: Intermediate processing tables
-- ============================================================================

CREATE TABLE IF NOT EXISTS staging.new_customers (
    customer_id INTEGER,
    name TEXT,
    email TEXT,
    signup_date DATE
);

CREATE TABLE IF NOT EXISTS staging.product_counts (
    product_id INTEGER,
    warehouse_id INTEGER,
    quantity_on_hand INTEGER
);

CREATE TABLE IF NOT EXISTS staging.orders_json (
    order_id INTEGER PRIMARY KEY,
    order_date DATE,
    customer_name TEXT,
    items JSONB
);

-- ============================================================================
-- DIMENSION TABLES: For lookup examples
-- ============================================================================

CREATE TABLE IF NOT EXISTS dim.products (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    unit_price NUMERIC(10,2)
);

-- ============================================================================
-- SAMPLE DATA
-- ============================================================================

-- Customers
TRUNCATE raw.customers RESTART IDENTITY CASCADE;
INSERT INTO raw.customers (customer_name, email, phone, region, signup_date, total_purchases, last_purchase_date) VALUES
('John Smith', 'john.smith@email.com', '555-0101', 'US', '2024-01-15', 15420.50, '2025-12-15'),
('Jane Doe', 'jane.doe@email.com', '555-0102', 'US', '2024-02-20', 8750.00, '2025-12-10'),
('Bob Johnson', 'bob.j@email.com', '555-0103', 'CA', '2024-03-10', 12300.00, '2025-11-20'),
('Alice Williams', 'alice.w@email.com', '555-0104', 'US', '2024-04-05', 6200.00, '2025-12-01'),
('Charlie Brown', 'charlie.b@email.com', '555-0105', 'EU', '2024-05-12', 3400.00, '2025-10-15'),
('Diana Prince', 'diana.p@email.com', '555-0106', 'EU', '2024-06-18', 9800.00, '2025-12-20'),
('Eve Davis', 'eve.d@email.com', '555-0107', 'APAC', '2024-07-22', 11200.00, '2025-12-18'),
('Frank Miller', 'frank.m@email.com', '555-0108', 'US', '2024-08-30', 4500.00, '2025-09-10'),
('Grace Lee', 'grace.l@email.com', '555-0109', 'APAC', '2024-09-14', 7800.00, '2025-12-12'),
('Henry Wilson', 'henry.w@email.com', '555-0110', 'CA', '2024-10-08', 13500.00, '2025-12-22');

-- Products
TRUNCATE raw.products RESTART IDENTITY CASCADE;
INSERT INTO raw.products (product_name, category, unit_price, cost, description) VALUES
('Laptop Pro 15', 'Electronics', 1299.99, 800.00, '15-inch professional laptop'),
('Wireless Mouse', 'Electronics', 29.99, 12.00, 'Ergonomic wireless mouse'),
('USB-C Cable', 'Electronics', 19.99, 5.00, 'USB-C charging cable'),
('Office Chair', 'Furniture', 299.99, 150.00, 'Ergonomic office chair'),
('Standing Desk', 'Furniture', 599.99, 300.00, 'Adjustable standing desk'),
('Monitor 27"', 'Electronics', 399.99, 200.00, '27-inch 4K monitor'),
('Keyboard Mechanical', 'Electronics', 149.99, 60.00, 'Mechanical keyboard RGB'),
('Desk Lamp', 'Furniture', 49.99, 20.00, 'LED desk lamp'),
('Notebook Set', 'Office Supplies', 24.99, 8.00, 'Set of 5 notebooks'),
('Pen Pack', 'Office Supplies', 12.99, 3.00, 'Pack of 12 pens');

-- Orders
TRUNCATE raw.orders RESTART IDENTITY CASCADE;
INSERT INTO raw.orders (customer_id, order_date, status, total_amount, region) VALUES
(1, '2025-01-15', 'completed', 1329.98, 'US'),
(1, '2025-02-20', 'completed', 399.99, 'US'),
(2, '2025-01-18', 'completed', 649.98, 'US'),
(3, '2025-01-25', 'completed', 899.97, 'CA'),
(3, '2025-03-10', 'completed', 1299.99, 'CA'),
(4, '2025-02-14', 'completed', 349.98, 'US'),
(5, '2025-02-28', 'cancelled', 599.99, 'EU'),
(6, '2025-03-15', 'completed', 1749.97, 'EU'),
(7, '2025-03-22', 'completed', 1329.98, 'APAC'),
(8, '2025-04-10', 'pending', 299.99, 'US'),
(9, '2025-11-15', 'completed', 449.98, 'APAC'),
(10, '2025-12-01', 'completed', 1899.97, 'CA'),
(1, '2025-12-15', 'completed', 1299.99, 'US'),
(6, '2025-12-20', 'shipped', 599.99, 'EU'),
(7, '2025-12-18', 'completed', 899.98, 'APAC');

-- Order Lines (for JSON aggregation example)
TRUNCATE raw.order_lines RESTART IDENTITY CASCADE;
INSERT INTO raw.order_lines (order_id, product_id, customer, line_num, product, quantity, unit_price) VALUES
(1, 1, 'John Smith', 1, 'Laptop Pro 15', 1, 1299.99),
(1, 2, 'John Smith', 2, 'Wireless Mouse', 1, 29.99),
(2, 6, 'John Smith', 1, 'Monitor 27"', 1, 399.99),
(3, 4, 'Jane Doe', 1, 'Office Chair', 1, 299.99),
(3, 5, 'Jane Doe', 2, 'Standing Desk', 1, 599.99),
(4, 1, 'Bob Johnson', 1, 'Laptop Pro 15', 1, 1299.99),
(5, 6, 'Alice Williams', 1, 'Monitor 27"', 1, 399.99),
(6, 1, 'Diana Prince', 1, 'Laptop Pro 15', 1, 1299.99),
(6, 7, 'Diana Prince', 2, 'Keyboard Mechanical', 1, 149.99);

-- Order Items (for aggregation examples)
TRUNCATE raw.order_items RESTART IDENTITY CASCADE;
INSERT INTO raw.order_items (order_id, product_id, quantity, unit_price, region, product_category)
SELECT 
    o.order_id,
    (1 + (random() * 9)::int),
    (1 + (random() * 3)::int),
    (random() * 1000 + 50)::numeric(10,2),
    o.region,
    CASE (random() * 2)::int 
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Furniture'
        ELSE 'Office Supplies'
    END
FROM raw.orders o
CROSS JOIN generate_series(1, 2);

-- Transactions
TRUNCATE raw.transactions RESTART IDENTITY CASCADE;
INSERT INTO raw.transactions (transaction_date, customer_id, product_id, quantity, unit_price, amount)
SELECT 
    CURRENT_DATE - (random() * 365)::int,
    1 + (random() * 10)::int,
    1 + (random() * 10)::int,
    1 + (random() * 3)::int,
    (random() * 1000 + 20)::numeric(10,2),
    ((1 + (random() * 3)::int) * (random() * 1000 + 20))::numeric(10,2)
FROM generate_series(1, 100);

-- Today's transactions
INSERT INTO raw.transactions (transaction_date, customer_id, product_id, quantity, unit_price, amount)
SELECT 
    CURRENT_DATE,
    1 + (random() * 10)::int,
    1 + (random() * 10)::int,
    1 + (random() * 3)::int,
    (random() * 1000 + 20)::numeric(10,2),
    ((1 + (random() * 3)::int) * (random() * 1000 + 20))::numeric(10,2)
FROM generate_series(1, 20);

-- Events
TRUNCATE raw.events RESTART IDENTITY;
INSERT INTO raw.events (event_type, user_id, event_timestamp, event_date)
SELECT 
    CASE (random() * 4)::int 
        WHEN 0 THEN 'login'
        WHEN 1 THEN 'view_product'
        WHEN 2 THEN 'add_to_cart'
        WHEN 3 THEN 'purchase'
        ELSE 'logout'
    END,
    1 + (random() * 10)::int,
    CURRENT_DATE - (random() * 30)::int * interval '1 day' + (random() * 86400)::int * interval '1 second',
    CURRENT_DATE - (random() * 30)::int
FROM generate_series(1, 500);

-- Today's events
INSERT INTO raw.events (event_type, user_id, event_timestamp, event_date)
SELECT 
    CASE (random() * 4)::int 
        WHEN 0 THEN 'login'
        WHEN 1 THEN 'view_product'
        WHEN 2 THEN 'add_to_cart'
        WHEN 3 THEN 'purchase'
        ELSE 'logout'
    END,
    1 + (random() * 10)::int,
    CURRENT_TIMESTAMP - (random() * 86400)::int * interval '1 second',
    CURRENT_DATE
FROM generate_series(1, 100);

-- User Activity
TRUNCATE raw.user_activity RESTART IDENTITY;
INSERT INTO raw.user_activity (user_id, activity_type, activity_date, activity_timestamp)
SELECT 
    1 + (random() * 10)::int,
    CASE (random() * 3)::int 
        WHEN 0 THEN 'page_view'
        WHEN 1 THEN 'button_click'
        WHEN 2 THEN 'form_submit'
        ELSE 'search'
    END,
    CURRENT_DATE - (random() * 7)::int,
    CURRENT_DATE - (random() * 7)::int * interval '1 day' + (random() * 86400)::int * interval '1 second'
FROM generate_series(1, 200);

-- Today's activities
INSERT INTO raw.user_activity (user_id, activity_type, activity_date, activity_timestamp)
SELECT 
    1 + (random() * 10)::int,
    CASE (random() * 3)::int 
        WHEN 0 THEN 'page_view'
        WHEN 1 THEN 'button_click'
        WHEN 2 THEN 'form_submit'
        ELSE 'search'
    END,
    CURRENT_DATE,
    CURRENT_TIMESTAMP - (random() * 86400)::int * interval '1 second'
FROM generate_series(1, 50);

-- Warehouses
TRUNCATE raw.warehouses RESTART IDENTITY CASCADE;
INSERT INTO raw.warehouses (warehouse_name, region, is_active) VALUES
('North Warehouse', 'US', true),
('South Warehouse', 'US', true),
('East Warehouse', 'EU', true),
('West Warehouse', 'APAC', true),
('Central Warehouse', 'CA', false);

-- Shipments
TRUNCATE raw.shipments RESTART IDENTITY;
INSERT INTO raw.shipments (order_id, warehouse_id, ship_date, carrier, tracking_number) VALUES
(1, 1, '2025-01-16', 'FedEx', 'FX123456789'),
(2, 1, '2025-02-21', 'UPS', 'UP987654321'),
(3, 2, '2025-01-19', 'USPS', 'US456789123'),
(4, 2, '2025-01-26', 'FedEx', 'FX789123456'),
(5, 1, '2025-03-11', 'DHL', 'DH321654987'),
(6, 3, '2025-02-15', 'FedEx', 'FX654987321'),
(8, 3, '2025-03-16', 'UPS', 'UP111222333'),
(9, 4, '2025-03-23', 'DHL', 'DH444555666'),
(11, 4, '2025-11-16', 'FedEx', 'FX777888999'),
(12, 2, '2025-12-02', 'UPS', 'UP000111222');

-- Dimension Tables
TRUNCATE dim.products CASCADE;
INSERT INTO dim.products (product_id, product_name, category, unit_price)
SELECT product_id, product_name, category, unit_price FROM raw.products;

-- Staging Tables
TRUNCATE staging.new_customers;
INSERT INTO staging.new_customers (customer_id, name, email, signup_date) VALUES
(11, 'New Customer 1', 'new1@email.com', CURRENT_DATE),
(12, 'New Customer 2', 'new2@email.com', CURRENT_DATE),
(13, 'New Customer 3', 'new3@email.com', CURRENT_DATE);

TRUNCATE staging.product_counts;
INSERT INTO staging.product_counts (product_id, warehouse_id, quantity_on_hand)
SELECT 
    1 + (random() * 10)::int,
    1 + (random() * 4)::int,
    (random() * 100)::int + 10
FROM generate_series(1, 30);

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

SELECT 'Customers' as table_name, count(*) as row_count FROM raw.customers
UNION ALL
SELECT 'Products', count(*) FROM raw.products
UNION ALL
SELECT 'Orders', count(*) FROM raw.orders
UNION ALL
SELECT 'Order Lines', count(*) FROM raw.order_lines
UNION ALL
SELECT 'Order Items', count(*) FROM raw.order_items
UNION ALL
SELECT 'Transactions', count(*) FROM raw.transactions
UNION ALL
SELECT 'Events', count(*) FROM raw.events
UNION ALL
SELECT 'User Activity', count(*) FROM raw.user_activity
UNION ALL
SELECT 'Warehouses', count(*) FROM raw.warehouses
UNION ALL
SELECT 'Shipments', count(*) FROM raw.shipments
ORDER BY table_name;

-- Sample queries to verify data
SELECT 'Sample Orders' as info;
SELECT order_id, customer_id, order_date, status, total_amount, region 
FROM raw.orders 
LIMIT 5;

SELECT 'Sample Customers' as info;
SELECT customer_id, customer_name, region, total_purchases 
FROM raw.customers 
LIMIT 5;

SELECT 'Sample Products' as info;
SELECT product_id, product_name, category, unit_price 
FROM raw.products 
LIMIT 5;

-- Ready message
SELECT '✓ Sample data setup complete!' as status;
SELECT 'You can now run the examples from the documentation.' as next_steps;
```

## Data Model

The sample database uses a typical e-commerce schema:

### Raw Layer
- **customers**: Customer master data with region, purchase history
- **products**: Product catalog with categories and pricing
- **orders**: Order headers with status and totals
- **order_lines**: Detailed line items (for JSON aggregation examples)
- **order_items**: Denormalized items (for aggregation examples)
- **transactions**: Transaction log for time-series examples
- **events**: User event tracking
- **user_activity**: Daily user activity log
- **warehouses**: Warehouse locations
- **shipments**: Shipment tracking information

### Staging Layer
- **new_customers**: Incoming customer data
- **product_counts**: Inventory counts
- **orders_json**: Orders with JSON-formatted line items

### Dimension Layer
- **dim.products**: Product dimension table for lookup examples

### Analytics Layer
Empty - populated by pipeline outputs

## Quick Reset

To reset all sample data:

```sql
-- Drop all schemas
DROP SCHEMA IF EXISTS raw CASCADE;
DROP SCHEMA IF EXISTS staging CASCADE;
DROP SCHEMA IF EXISTS analytics CASCADE;
DROP SCHEMA IF EXISTS dim CASCADE;

-- Then re-run the setup script above
```

## Usage

After setting up the sample data, you can run any example from the [examples documentation](./examples.md):

```sql
-- Example: Basic pipeline
SELECT flow.read_db_object('raw.orders');
SELECT flow.where('order_date >= ''2025-01-01''');
SELECT flow.where('status = ''completed''');
SELECT flow.select('order_id', 'customer_id', 'total_amount', 'order_date');
SELECT flow.compile();
```

## Notes

- Data includes realistic e-commerce transactions
- Dates are relative to current date for time-based examples
- Random data ensures variety in aggregation results
- Multiple schemas demonstrate real-world data organization
- Based on common PostgreSQL tutorial schemas
