create or replace function flow.sum(column_expr text, as_name text)
returns flow.measure
language sql
immutable
as $$
    select row('sum', column_expr, as_name)::flow.measure;
$$;

comment on function flow.sum(text, text) is
'@category Core: Aggregation

Create a SUM aggregate measure AST node.

Parameters:
  column_expr - Column or expression to sum
  as_name     - Result column alias

Returns:
  flow.measure AST node with op=''sum''

Example:
  SELECT flow.sum(''quantity'', ''total_quantity'');
  -- Returns: (''sum'', ''quantity'', ''total_quantity'')::flow.measure
';
