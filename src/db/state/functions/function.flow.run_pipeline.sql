create or replace function flow.run_pipeline(
    pipeline_name text,
    variables jsonb default '{}'::jsonb
)
returns table(
    result_row jsonb
)
language plpgsql
as $body$
declare
    v_sql text;
    v_key text;
    v_value text;
    v_current_date text;
begin
    -- Load the pre-compiled SQL from the pipeline table
    select compiled_sql into v_sql
    from flow.pipeline
    where flow.pipeline.pipeline_name = run_pipeline.pipeline_name;

    if v_sql is null then
        raise exception 'Pipeline "%" not found or has no compiled SQL. Re-register the pipeline.', pipeline_name;
    end if;

    -- Apply variable substitution
    -- Built-in tokens
    v_current_date := current_date::text;
    v_sql := replace(v_sql, '{{current_date}}', quote_literal(v_current_date));
    v_sql := replace(v_sql, '{{today}}', quote_literal(v_current_date));
    v_sql := replace(v_sql, '{{now}}', quote_literal(now()::text));

    -- User-provided variables
    for v_key, v_value in
        select * from jsonb_each_text(variables)
    loop
        v_sql := replace(v_sql, '{{' || v_key || '}}', quote_literal(v_value));
    end loop;

    -- Execute and return results
    raise notice 'Executing pipeline: %', pipeline_name;
    raise notice 'SQL: %', v_sql;

    -- Check if SQL contains CREATE TABLE (DDL statement that doesn't return rows)
    if upper(v_sql) ~ 'CREATE\s+TABLE' then
        -- Execute DDL+DML statements without expecting return values
        execute v_sql;
        -- Return empty result set
        return;
    else
        -- Execute query that returns rows, wrap each row as jsonb
        return query execute 'SELECT to_jsonb(subq.*) FROM (' || v_sql || ') subq';
    end if;
end;
$body$;

comment on function flow.run_pipeline(text, jsonb)
is $comment$@category Pipeline: Execution

Execute a registered pipeline with optional variable substitution.

This function loads a saved pipeline, compiles it into SQL, replaces {{variable}} placeholders, and executes it.

Built-in Variables:
- {{current_date}}, {{today}} - Current date
- {{now}} - Current timestamp

Parameters:
  pipeline_name - Name of the registered pipeline to execute
  variables     - JSONB object with variable name/value pairs (default: {})

Examples:
  -- Run without variables
  SELECT * FROM flow.run_pipeline('daily_report');
  
  -- Run with custom variables
  SELECT * FROM flow.run_pipeline(
      'sales_by_region',
      jsonb_build_object('region', 'US', 'min_amount', '1000')
  );
  
  -- Pipeline using variables
  SELECT flow.read_db_object('public.orders');
  SELECT flow.where('order_date >= ''{{start_date}}'' AND region = ''{{region}}''');
  SELECT flow.register_pipeline('orders_filtered');
  
  -- Then execute
  SELECT * FROM flow.run_pipeline('orders_filtered', 
      jsonb_build_object('start_date', '2025-01-01', 'region', 'US'));
$comment$;
