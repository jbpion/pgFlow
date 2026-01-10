# pgFlow Examples

Practical examples demonstrating common pgFlow patterns and use cases.

## Table of Contents

- [Basic Pipelines](#basic-pipelines)
- [Aggregations](#aggregations)
- [Joins and Lookups](#joins-and-lookups)
- [Write Operations](#write-operations)
- [Variables and Templating](#variables-and-templating)
- [Production Workflows](#production-workflows)

---

## Basic Pipelines

### Simple SELECT with Filter

```sql
-- Read orders, filter, and project columns
SELECT flow.read_db_object('raw.orders');
SELECT flow.where('order_date >= ''2025-01-01''');
SELECT flow.where('status = ''completed''');
SELECT flow.select('order_id', 'customer_id', 'total_amount', 'order_date');

-- Compile to see generated SQL
SELECT flow.compile();

-- Register for reuse
SELECT flow.register_pipeline('completed_orders_ytd', 'Orders completed in 2025');
```

**Generated SQL:**
```sql
SELECT t0.customer_id AS customer_id, t0.order_date AS order_date, t0.order_id AS order_id, t0.total_amount AS total_amount
FROM raw.orders t0
WHERE (order_date >= '2025-01-01' AND status = 'completed')
```

### Calculated Columns

```sql
-- Add computed fields
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

SELECT flow.register_pipeline('todays_sales_with_tax');
```

**Generated SQL:**
```sql
SELECT t0.quantity * unit_price AS line_total, t0.product_id AS product_id, t0.quantity AS quantity, ROUND(quantity * unit_price * 0.08, 2) AS tax_amount, ROUND(quantity * unit_price * 1.08, 2) AS total_with_tax, t0.transaction_id AS transaction_id, t0.unit_price AS unit_price
FROM raw.transactions t0
WHERE transaction_date = CURRENT_DATE
```

### Conditional Logic

```sql
-- CASE expressions in SELECT
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
    END:customer_tier',
    'CASE 
        WHEN last_purchase_date > CURRENT_DATE - INTERVAL ''30 days'' THEN true 
        ELSE false 
    END:is_active'
);

SELECT flow.register_pipeline('customer_segmentation');
```

**Generated SQL:**
```sql
SELECT t0.customer_id AS customer_id, t0.customer_name AS customer_name,
CASE         
WHEN total_purchases >= 10000 THEN 'platinum'        
WHEN total_purchases >= 5000 THEN 'gold'        
WHEN total_purchases >= 1000 THEN 'silver'        
ELSE 'bronze'    
END AS customer_tier, 
CASE         
WHEN last_purchase_date > CURRENT_DATE - INTERVAL '30 days' THEN true         
ELSE false     
END AS is_active,
 t0.total_purchases AS total_purchases
FROM raw.customers t0
```

---

## Aggregations

### Basic GROUP BY

```sql
-- Sales by region
SELECT flow.read_db_object('raw.orders');
SELECT flow.where('order_date >= ''2025-01-01''');
SELECT flow.aggregate(
    ARRAY['region'],
    'COUNT(*):order_count',
    'SUM(total_amount):total_sales',
    'AVG(total_amount):avg_order_value'
);
SELECT flow.write('sales.regional_summary', 'insert', truncate_before => true);

SELECT flow.register_pipeline('daily_regional_rollup');
```

**Generated SQL:**
```sql
TRUNCATE TABLE sales.regional_summary; 
INSERT INTO sales.regional_summary 
SELECT region, AVG(total_amount) AS avg_order_value, COUNT(*) AS order_count, SUM(total_amount) AS total_sales
FROM (
    SELECT t0.order_id AS order_id, t0.customer_id AS customer_id, t0.order_date AS order_date, t0.status AS status, t0.total_amount AS total_amount, t0.region AS region
FROM raw.orders t0) subquery
WHERE order_date >= '2025-01-01'
GROUP BY region
```

### Multi-Level Aggregation

```sql
-- Sales by region and product category
SELECT flow.read_db_object('raw.order_items');
SELECT flow.lookup('raw.products', 't0.product_id = t1.product_id'
, ARRAY['category'], 't1');

SELECT flow.aggregate(
    'orders',
    flow.group_by('product_id'),
        flow.sum('quantity', 'total_quantity'),
        flow.count('*', 'order_count'),
    flow.having('total_quantity > 1000')
);

SELECT flow.write('analytics.regional_category_summary', 'upsert', 
    ARRAY['region', 'category'], auto_create => true);
```

**Generated SQL:**
```sql
SELECT product_id AS product_id,
COUNT(*) AS order_count,
SUM(quantity) AS total_quantity
FROM (
    SELECT t0.order_item_id AS order_item_id,
    t0.order_id AS order_id,
    t0.product_id AS product_id,
    t0.quantity AS quantity,
    t0.unit_price AS unit_price,
    t0.region AS region,
    t0.product_category AS product_category,
    t1.category AS category
    FROM raw.order_items t0
    LEFT JOIN raw.products t1 ON t0.product_id = t1.product_id
) subquery
GROUP BY subquery.product_id
HAVING SUM(quantity) > 1000
```

### Time-Based Aggregation

```sql
-- Monthly sales trend
SELECT flow.read_db_object('sales.transactions');
SELECT flow.aggregate(
    ARRAY['DATE_TRUNC(''month'', transaction_date)'],
    'SUM(amount):monthly_total',
    'COUNT(*):transaction_count',
    'AVG(amount):avg_transaction',
    'MIN(amount):min_transaction',
    'MAX(amount):max_transaction'
);
SELECT flow.select(
    'DATE_TRUNC:month',
    'monthly_total',
    'transaction_count',
    'avg_transaction',
    'min_transaction',
    'max_transaction'
);
SELECT flow.write('analytics.monthly_sales', 'upsert', ARRAY['month']);

SELECT flow.register_pipeline('monthly_sales_rollup');
```

---

## Joins and Lookups

### Simple Lookup

```sql
-- Enrich orders with customer info
SELECT flow.read_db_object('orders');
SELECT flow.lookup('customers', 't1', 't0.customer_id = t1.customer_id');
SELECT flow.select(
    't0.order_id',
    't0.order_date',
    't1.customer_name',
    't1.email',
    't0.total_amount'
);
SELECT flow.where('t0.order_date >= ''2025-01-01''');

SELECT flow.register_pipeline('orders_with_customers');
```

### Multiple Lookups

```sql
-- Orders with customer and product details
SELECT flow.read_db_object('order_items');
SELECT flow.lookup('orders', 't1', 't0.order_id = t1.order_id');
SELECT flow.lookup('customers', 't2', 't1.customer_id = t2.customer_id');
SELECT flow.lookup('products', 't3', 't0.product_id = t3.product_id');
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

SELECT flow.register_pipeline('order_details_enriched');
```

### Lookup with Complex Join Condition

```sql
-- Join with multiple conditions
SELECT flow.read_db_object('shipments');
SELECT flow.lookup(
    'warehouses', 
    't1', 
    't0.warehouse_id = t1.warehouse_id AND t1.is_active = true'
);
SELECT flow.select(
    't0.shipment_id',
    't0.ship_date',
    't1.warehouse_name',
    't1.region',
    't0.carrier'
);

SELECT flow.register_pipeline('active_warehouse_shipments');
```

---

## Write Operations

### Insert (Append)

```sql
-- Simple insert - append new records
SELECT flow.read_db_object('staging.new_customers');
SELECT flow.select('customer_id', 'name', 'email', 'signup_date');
SELECT flow.write('customers', 'insert');

SELECT flow.register_pipeline('load_new_customers');
```

### Insert with Truncate

```sql
-- Replace entire table contents
SELECT flow.read_db_object('raw.daily_snapshot');
SELECT flow.where('snapshot_date = CURRENT_DATE');
SELECT flow.select('*');
SELECT flow.write('reports.current_snapshot', 'insert', truncate_before => true);

SELECT flow.register_pipeline('refresh_snapshot');
```

### Update Existing Rows

```sql
-- Update customer totals
SELECT flow.read_db_object('orders');
SELECT flow.where('status = ''completed''');
SELECT flow.aggregate(
    ARRAY['customer_id'],
    'SUM(total_amount):lifetime_value',
    'COUNT(*):order_count',
    'MAX(order_date):last_order_date'
);
SELECT flow.write(
    'customers_summary',
    'update',
    ARRAY['customer_id']
);

SELECT flow.register_pipeline('update_customer_totals');
```

### Upsert (Insert or Update)

```sql
-- Sync product inventory
SELECT flow.read_db_object('staging.product_counts');
SELECT flow.select(
    'product_id',
    'warehouse_id',
    'quantity_on_hand',
    'CURRENT_TIMESTAMP:last_updated'
);
SELECT flow.write(
    'inventory',
    'upsert',
    ARRAY['product_id', 'warehouse_id'],
    auto_create => true
);

SELECT flow.register_pipeline('sync_inventory');
```

### Full Sync (Upsert + Delete)

```sql
-- Maintain active subscriptions (add new, update existing, remove cancelled)
SELECT flow.read_db_object('source.active_subscriptions');
SELECT flow.select(
    'subscription_id',
    'customer_id',
    'plan_type',
    'start_date',
    'next_billing_date'
);
SELECT flow.write(
    'subscriptions',
    'upsert_delete',
    ARRAY['subscription_id']
);

SELECT flow.register_pipeline('sync_active_subscriptions');
```

### Auto-Create Table

```sql
-- Create target table if it doesn't exist
SELECT flow.read_db_object('raw.events');
SELECT flow.where('event_date = CURRENT_DATE');
SELECT flow.aggregate(
    ARRAY['event_type', 'user_id'],
    'COUNT(*):event_count',
    'MIN(event_timestamp):first_event',
    'MAX(event_timestamp):last_event'
);
SELECT flow.write(
    'analytics.daily_user_events',
    'insert',
    auto_create => true
);

SELECT flow.register_pipeline('daily_event_summary');
```

---

## Variables and Templating

### Date Range Filter

```sql
-- Pipeline with date parameters
SELECT flow.read_db_object('transactions');
SELECT flow.where('transaction_date >= ''{{start_date}}''');
SELECT flow.where('transaction_date <= ''{{end_date}}''');
SELECT flow.select('*');

SELECT flow.register_pipeline(
    'transactions_by_date_range',
    'Extract transactions within a date range',
    jsonb_build_object(
        'start_date', 'Start date in YYYY-MM-DD format',
        'end_date', 'End date in YYYY-MM-DD format'
    )
);

-- Execute with variables
SELECT * FROM flow.run(
    'transactions_by_date_range',
    jsonb_build_object('start_date', '2025-01-01', 'end_date', '2025-01-31')
);
```

### Multiple Variables

```sql
-- Filtered report with multiple parameters
SELECT flow.read_db_object('sales.orders');
SELECT flow.where('region = ''{{region}}''');
SELECT flow.where('order_date >= ''{{start_date}}''');
SELECT flow.where('total_amount >= {{min_amount}}');
SELECT flow.aggregate(
    ARRAY['product_category'],
    'SUM(total_amount):category_total',
    'COUNT(*):order_count'
);

SELECT flow.register_pipeline(
    'sales_by_category',
    'Sales by category for region and date range',
    jsonb_build_object(
        'region', 'Region code (US, EU, APAC)',
        'start_date', 'Start date (YYYY-MM-DD)',
        'min_amount', 'Minimum order amount (numeric)'
    )
);

-- Run with specific values
SELECT * FROM flow.run(
    'sales_by_category',
    jsonb_build_object(
        'region', 'US',
        'start_date', '2025-01-01',
        'min_amount', '1000'
    )
);
```

### Using Built-in Variables

```sql
-- Today's activity
SELECT flow.read_db_object('user_activity');
SELECT flow.where('activity_date = ''{{today}}''');
SELECT flow.select('user_id', 'activity_type', 'activity_timestamp');

SELECT flow.register_pipeline('todays_activity');

-- Run without passing variables (uses current date)
SELECT * FROM flow.run('todays_activity');
```

---

## Production Workflows

### Development: Build and Test

```sql
-- 1. Build pipeline interactively
SELECT flow.read_db_object('raw.orders');
SELECT flow.lookup('customers', 't1', 't0.customer_id = t1.id');
SELECT flow.where('t0.status = ''completed''');
SELECT flow.aggregate(
    ARRAY['t1.region', 't0.product_category'],
    'SUM(t0.amount):total_sales',
    'COUNT(*):order_count'
);
SELECT flow.write('analytics.sales_summary', 'upsert', 
    ARRAY['region', 'product_category']);

-- 2. Test compilation
SELECT flow.compile();

-- 3. Register
SELECT flow.register_pipeline(
    'daily_sales_summary',
    'Daily rollup of sales by region and category',
    version => '1.0.0'
);

-- 4. Test execution
SELECT * FROM flow.run('daily_sales_summary');

-- 5. Export for production
SELECT flow.export_pipeline('daily_sales_summary', '1.0.0');
```

### Production: Deploy

```bash
# Build runtime artifact
./build-runtime.sh 1.0.0

# Deploy runtime to production
psql $PROD_CONNECTION -f dist/pgflow-1.0.0-runtime.sql

# Export pipeline in dev
psql $DEV_CONNECTION -o migrations/daily_sales_summary_v1_0_0.sql \
    -c "SELECT flow.export_pipeline('daily_sales_summary', '1.0.0')"

# Deploy pipeline to production
psql $PROD_CONNECTION -f migrations/daily_sales_summary_v1_0_0.sql

# Run pipeline in production
psql $PROD_CONNECTION -c "SELECT * FROM flow.run('daily_sales_summary')"
```

### Scheduled Execution

```sql
-- Create wrapper function for scheduled job
CREATE OR REPLACE FUNCTION run_daily_sales_summary()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM flow.run('daily_sales_summary');
    
    RAISE NOTICE 'Daily sales summary completed at %', now();
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Daily sales summary failed: %', SQLERRM;
END;
$$;

-- Schedule with pg_cron (if available)
SELECT cron.schedule(
    'daily-sales-summary',
    '0 2 * * *',  -- 2 AM daily
    'SELECT run_daily_sales_summary()'
);
```

### Error Handling

```sql
-- Wrapper with error handling and logging
CREATE TABLE IF NOT EXISTS pipeline_execution_log (
    log_id SERIAL PRIMARY KEY,
    pipeline_name TEXT,
    execution_time TIMESTAMP DEFAULT now(),
    status TEXT,
    error_message TEXT,
    rows_affected BIGINT
);

CREATE OR REPLACE FUNCTION run_pipeline_with_logging(p_pipeline_name TEXT)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_row_count BIGINT;
    v_error_msg TEXT;
BEGIN
    -- Execute pipeline
    PERFORM flow.run(p_pipeline_name);
    
    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    
    -- Log success
    INSERT INTO pipeline_execution_log (pipeline_name, status, rows_affected)
    VALUES (p_pipeline_name, 'SUCCESS', v_row_count);
    
EXCEPTION
    WHEN OTHERS THEN
        v_error_msg := SQLERRM;
        
        -- Log failure
        INSERT INTO pipeline_execution_log (pipeline_name, status, error_message)
        VALUES (p_pipeline_name, 'FAILED', v_error_msg);
        
        RAISE;
END;
$$;

-- Use in scheduled jobs
SELECT run_pipeline_with_logging('daily_sales_summary');
```

---

## See Also

- [Function Reference](./function-reference.md)
- [Getting Started Guide](./getting-started.md)
- [Architecture Overview](./architecture.md)
