-- Composite type for expression AST node
-- Used in HAVING clauses and other filter contexts
drop type if exists flow.expr cascade;

create type flow.expr as (
    expression text   -- SQL expression (e.g., 'total_quantity > 100')
);

comment on type flow.expr is
'AST node representing a SQL expression.
Used in HAVING clauses and filter contexts.

Fields:
  expression - SQL expression text

Example:
  (''total_quantity > 100'')::flow.expr
';
