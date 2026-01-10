# (pg)Flow

![Flow Logo](docs/images/flow_logo.png)

**Flow** is a PostgreSQL-native, developer-friendly framework for building **explicit, inspectable data pipelines** directly inside the database.

It provides a small set of composable, user-facing functions (`read`, `select`, `lookup`, etc.) that record intent, compile deterministic SQL, and execute it in a controlled way—without hiding what the database is doing.

Flow is designed for **ease of implementation of data transformations, especially in an environment of high requirements churn**.

---

## Why Flow?

Modern data systems often suffer from one or more of the following:

- Transformations embedded in layers of views  
- ETL frameworks that obscure the final SQL  
- Difficult-to-review pipelines with no intermediate state  

Flow addresses this by:

- Treating a pipeline as a **first-class object**
- Recording each step explicitly
- Compiling pipelines into plain SQL
- Making intermediate state visible
- Running entirely inside PostgreSQL

No external runtime. No hidden execution engine. Just SQL you can inspect.

---

## Core Concepts

### 1. Pipelines are Explicit

A pipeline starts with a **read** and builds step-by-step:

```sql
SELECT flow.read_db_object('public.trades');
SELECT flow.select('trade_date', 'price * quantity AS notional');
```

Each step is recorded, validated, and compiled later.

* * * * *

### 2. Session-Scoped State

Flow stores pipeline steps in a **session-level temporary table** managed internally.

-   No user-created temp tables

-   No schema pollution

-   One active pipeline per session

-   Clear lifecycle

Calling `read` resets the pipeline intentionally (with a warning if one already exists).

* * * * *

### 3. Separation of Concerns

Flow is internally divided into:

| Layer | Responsibility |
| --- | --- |
| **User-facing steps** | Declare intent (`read`, `select`, `lookup`) |
| **Session management** | Track pipeline steps |
| **Compiler** | Convert steps → SQL |
| **Registry** | Store compiled artifacts |
| **Runner** | Execute compiled SQL |

Each layer is small, testable, and replaceable.

* * * * *

### 4. Compile, Don't Guess

Flow does **not** execute immediately.

Instead:

1.  User declares steps

2.  Steps are validated

3.  Compiler builds SQL

4.  SQL is registered

5.  Runner executes explicitly

This makes it possible to:

-   Inspect compiled SQL

-   Optimize later

-   Add alternative execution strategies

* * * * *

Minimal Example
---------------
```sql
-- Extract soruce data
SELECT flow.read_db_object('staging.orders');

-- Transform
--Use the ":" to alias columns.
SELECT flow.select(
  'order_id:order_key',
  'customer_id:customer_key',
  'amount * tax_rate:total_amount'
);

-- Write the result to a target table.
SELECT flow.write(
    'stage.orders',
    mode => 'upsert',
    unique_keys => ARRAY['order_id'],
    auto_create => false 
);

-- View the current session SQL.
SELECT flow.compile();

--Register the pipeline.
SELECT flow.register_pipepline('orders_example', 'An example pipeline showing order processing including a calculated column');'

--Execute the pipeline.
SELECT flow.run_pipeline('orders_example');`
```
At any point, you can inspect session state or compiled output.

* * * * *

Design Principles
-----------------

### PostgreSQL First

Flow uses native Postgres features:

-   Temporary tables

-   JSONB

-   PL/pgSQL

-   SQL generation

No DSL outside SQL. No YAML. No Python runtime.

* * * * *

### Minimal Surface Area

Only a few primitives exist initially:

-   `read`

-   `select`

-   `lookup`

-   `compile`

-   `run`

Power comes from composition, not feature count.

* * * * *

### Inspectability Over Convenience

You should always be able to answer:

-   What SQL will run?

-   Where did this column come from?

-   What step introduced this transformation?

Flow optimizes for **understanding first**, performance second.

* * * * *

### Installation
Got to the [releases ](https://github.com/jbpion/pgFlow/releases/) and follow the installation instructions.

### Upgrading
pgFlow is idempotent — re-running the installer is safe.
