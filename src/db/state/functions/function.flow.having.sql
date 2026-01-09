create or replace function flow.having(expression text)
returns flow.measure
language sql
immutable
as $$
    -- Use 'having' as a special op marker to distinguish from actual measures
    select row('having', expression, null)::flow.measure;
$$;

comment on function flow.having(text) is
'@category Core: Aggregation

Create a HAVING clause expression for use with flow.aggregate().

This function creates a pseudo-measure that represents a HAVING condition.
It can be mixed with actual measures in the VARIADIC parameter list.

Parameters:
  expression - SQL expression for HAVING clause (e.g., ''total_amount > 1000'')

Returns:
  flow.measure node with op=''having'' (used internally to distinguish from measures)

Example:
  SELECT flow.aggregate(
      ''orders'',
      ARRAY[''customer_id''],
      flow.sum(''amount'', ''total_amount''),
      flow.count(''*'', ''order_count''),
      flow.having(''total_amount > 1000'')
  );
';
