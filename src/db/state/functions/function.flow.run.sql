create or replace function flow.run(
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
    v_compiled_sql text;
    v_key text;
    v_value text;
    v_current_date text;
begin
    -- Get the compiled SQL for the pipeline
    -- For now, recompile from steps. Later we can cache compiled SQL in pipeline table.
    select string_agg(
        format(
            'SELECT * FROM (%s) t%s',
            step_spec::text,
            step_order
        ),
        E'\nUNION ALL\n'
        order by s.step_order
    )
    into v_compiled_sql
    from flow.pipeline p
    join flow.pipeline_step s on s.pipeline_id = p.pipeline_id
    where p.pipeline_name = run.pipeline_name;

    if v_compiled_sql is null then
        raise exception 'Pipeline "%" not found', pipeline_name;
    end if;

    -- For now, recompile the pipeline from registered steps
    -- Load steps into temp table
    perform flow.__ensure_session_steps();
    truncate table __session_steps;

    insert into __session_steps (
        step_order,
        step_type,
        step_name,
        program_call,
        step_spec
    )
    select
        s.step_order,
        s.step_type,
        s.step_name,
        s.program_call,
        s.step_spec
    from flow.pipeline p
    join flow.pipeline_step s on s.pipeline_id = p.pipeline_id
    where p.pipeline_name = run.pipeline_name
    order by s.step_order;

    -- Compile the SQL
    v_sql := flow.compile();

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

    return query execute v_sql;
end;
$body$;

comment on function flow.run(text, jsonb)
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
  SELECT * FROM flow.run('daily_report');
  
  -- Run with custom variables
  SELECT * FROM flow.run(
      'sales_by_region',
      jsonb_build_object('region', 'US', 'min_amount', '1000')
  );
  
  -- Pipeline using variables
  SELECT flow.read_db_object('public.orders');
  SELECT flow.where('order_date >= ''{{start_date}}'' AND region = ''{{region}}''');
  SELECT flow.register_pipeline('orders_filtered');
  
  -- Then execute
  SELECT * FROM flow.run('orders_filtered', 
      jsonb_build_object('start_date', '2025-01-01', 'region', 'US'));
$comment$;
