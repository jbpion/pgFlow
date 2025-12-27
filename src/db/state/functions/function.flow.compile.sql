create or replace function flow.compile()
returns text
language plpgsql
as $$
begin
    -- return flow.__compile_session(true, false);
    return flow.__compile_session();
end;
$$;

comment on function flow.compile()
is $comment$@category Compilation

Compile the current session pipeline into executable SQL.

This function converts the pipeline steps stored in __session_steps into a single executable SQL SELECT statement. Use this to preview the generated SQL before running or registering a pipeline.

Returns: Text containing the compiled SQL statement

Example:
  SELECT flow.read_db_object('public.customers');
  SELECT flow.where('created_at > current_date - interval ''30 days''');
  SELECT flow.select(ARRAY['id', 'email', 'created_at']);
  
  -- See the generated SQL
  SELECT flow.compile();
$comment$;
