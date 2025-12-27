create or replace function flow.step(
    step_name text
)
returns text
language sql
immutable
as $body$
    select chr(2) || step_name;
$body$;

comment on function flow.step(text)
is $comment$@category Core: Utilities

Helper function to tag a step description when using VARIADIC functions.

Returns a specially-tagged string that flow.select() and flow.aggregate() 
recognize as a step name rather than a column expression.

Parameters:
  step_name - Descriptive name for the step

Examples:
  -- Use with flow.select
  SELECT flow.select(
      flow.step('Transform user columns'),
      'id:user_id',
      'email:email_address'
  );
  
  -- Use with flow.aggregate
  SELECT flow.aggregate(
      ARRAY['customer_id'],
      flow.step('Sum totals by customer'),
      'SUM(amount):total_amount',
      'COUNT(*):order_count'
  );
  
  -- Can be placed anywhere in the VARIADIC parameters
  SELECT flow.select(
      'id',
      'email',
      flow.step('Select user info')
  );
$comment$;
