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
SELECT flow.read_db_object('raw.trades');
SELECT flow.select('trade_date', 'price * quantity AS notional');
```

Each step is recorded, and compiled later.

* * * * *

### 2. Session-Scoped State

Flow stores pipeline steps in a **session-level temporary table** managed internally.

-   No user-created temp tables

-   No schema pollution

-   One active pipeline per session

-   Clear lifecycle

Calling `read` again resets the pipeline intentionally (with a warning if one already exists). This allows you to rework your flow until it emits the desired SQL and is written as readable, maintainable code.

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

Small Example
---------------
```sql
--Read
SELECT flow.read_db_object('raw.orders');

--Clean - Create status_cleaned alias
SELECT flow.select('UPPER(status):status_cleaned');

--Filter - Use the status_cleaned alias from previous step
SELECT flow.where('status_cleaned = ''COMPLETED''');

--Map
SELECT flow.select('order_id:order_id',
                   'customer_id:client_id',
                   'order_date:date_of_order',
                   'status_cleaned:order_status',
                   'total_amount:amount_total',
                   'region:sales_region');

--Compute - Use amount_total alias and include all columns for final output
SELECT flow.select('amount_total * 1.1:amount_with_tax');

--Write
SELECT flow.write('order_report', 'insert', ARRAY['order_id'], auto_create => true);

--Review the generated code [optional]
SELECT flow.compile();

--Record pipeline
SELECT flow.register_pipeline('order_reporting', 'Get the current order report with tax included.');

--Create the job
SELECT flow.create_job('generate_order_reporting');

--Add the pipeline to the job.
SELECT flow.add_pipeline_to_job('generate_order_reporting', 'order_reporting');

--Run the job.
SELECT flow.run_job('generate_order_reporting');
```
At any point, you can inspect session state or compiled output.

You can find more examples [here](docs/examples.md).
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

### Installation
Got to the [releases ](https://github.com/jbpion/pgFlow/releases/) and follow the installation instructions.

### Upgrading
pgFlow is idempotent — re-running the installer is safe.

### Security
Jobs and pipelines accept user variables that execute dynamically. The variables are wrapped in the built-in quote_literal() function to prevent SQL injection.

### Use Case
The imagined use cases are for running in an environment where you want to do rapid prototyping of ETL. This can also be useful where your database environment is isolated from standard ETL tools.
