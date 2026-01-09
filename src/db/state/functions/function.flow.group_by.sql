create or replace function flow.group_by(VARIADIC columns text[])
returns text[]
language sql
immutable
as $$
    select columns;
$$;

comment on function flow.group_by(VARIADIC text[]) is
'@category Core: Aggregation

Create a group by column list for use with flow.aggregate().

This is a helper function that simply returns its arguments as an array,
providing clearer syntax than writing ARRAY[...] explicitly.

Parameters:
  columns - VARIADIC list of column names or expressions to group by

Returns:
  text[] array of column names

Examples:
  -- Single column
  flow.group_by(''customer_id'')
  
  -- Multiple columns
  flow.group_by(''region'', ''category'')
  
  -- With aggregate
  SELECT flow.aggregate(
      ''orders'',
      flow.group_by(''customer_id'', ''region''),
      flow.sum(''amount'', ''total_amount'')
  );
';
