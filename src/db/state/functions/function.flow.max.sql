create or replace function flow.max(column_expr text, as_name text)
returns flow.measure
language sql
immutable
as $$
    select row('max', column_expr, as_name)::flow.measure;
$$;

comment on function flow.max(text, text) is
'@category Core: Aggregation

Create a MAX aggregate measure AST node.

Parameters:
  column_expr - Column or expression to find maximum
  as_name     - Result column alias

Returns:
  flow.measure AST node with op=''max''

Example:
  SELECT flow.max(''price'', ''max_price'');
  -- Returns: (''max'', ''price'', ''max_price'')::flow.measure
';
