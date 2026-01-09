create or replace function flow.avg(column_expr text, as_name text)
returns flow.measure
language sql
immutable
as $$
    select row('avg', column_expr, as_name)::flow.measure;
$$;

comment on function flow.avg(text, text) is
'@category Core: Aggregation

Create an AVG aggregate measure AST node.

Parameters:
  column_expr - Column or expression to average
  as_name     - Result column alias

Returns:
  flow.measure AST node with op=''avg''

Example:
  SELECT flow.avg(''price'', ''average_price'');
  -- Returns: (''avg'', ''price'', ''average_price'')::flow.measure
';
