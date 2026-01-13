create or replace function flow.min(column_expr text, as_name text)
returns flow.measure
language sql
immutable
as $$
    select row('min', column_expr, as_name)::flow.measure;
$$;

comment on function flow.min(text, text) is
'@category Core: Aggregation

Create a MIN aggregate measure AST node.

Parameters:
  column_expr - Column or expression to find minimum
  as_name     - Result column alias

Returns:
  flow.measure AST node with op=''min''

Example:
  SELECT flow.min(''price'', ''min_price'');
  -- Returns: (''min'', ''price'', ''min_price'')::flow.measure
';
