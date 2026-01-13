create or replace function flow.count(column_expr text, as_name text)
returns flow.measure
language sql
immutable
as $$
    select row('count', column_expr, as_name)::flow.measure;
$$;

comment on function flow.count(text, text) is
'@category Core: Aggregation

Create a COUNT aggregate measure AST node.

Parameters:
  column_expr - Column or expression to count (use ''*'' for COUNT(*))
  as_name     - Result column alias

Returns:
  flow.measure AST node with op=''count''

Example:
  SELECT flow.count(''DISTINCT customer_id'', ''customer_count'');
  -- Returns: (''count'', ''DISTINCT customer_id'', ''customer_count'')::flow.measure
';
