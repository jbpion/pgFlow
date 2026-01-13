-- Composite type for aggregate measure AST node
drop type if exists flow.measure cascade;

create type flow.measure as (
    op       text,   -- aggregate operation: 'sum', 'count', 'avg', 'min', 'max'
    "column" text,   -- column expression to aggregate
    alias    text    -- result alias (AS clause)
);

comment on type flow.measure is
'AST node representing an aggregate measure (e.g., SUM, COUNT, AVG).
Used in flow.aggregate() to define aggregation operations.

Fields:
  op     - Aggregate operation name: sum, count, avg, min, max
  column - Column or expression to aggregate
  alias  - Result column alias

Example:
  (''sum'', ''quantity'', ''total_quantity'')::flow.measure
';
