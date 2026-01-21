create or replace function flow.delete(
    target_table     text,
    where_clause     text default null,
    execution_order  text default 'before',
    truncate_mode    boolean default false,
    step_name        text default null
)
returns text
language plpgsql
as $body$
declare
    v_step_num int;
    v_step_name text;
    v_warning text := '';
begin
    perform flow.__ensure_session_steps();
    perform flow.__assert_pipeline_started();

    -- validate execution_order
    if execution_order not in ('before', 'after') then
        raise exception
            'Invalid execution_order: %. Must be before or after.',
            execution_order;
    end if;

    -- validate truncate with where clause
    if truncate_mode and where_clause is not null then
        raise warning 'truncate_mode is true, where_clause will be ignored. TRUNCATE removes all rows regardless of conditions.';
        v_warning := ' (WHERE clause ignored)';
    end if;

    v_step_num := flow.__next_step_order();
    
    -- Build step name
    if step_name is not null then
        v_step_name := step_name;
    elsif truncate_mode then
        v_step_name := 'truncate ' || target_table;
    elsif where_clause is not null then
        v_step_name := 'delete from ' || target_table || ' where ' || left(where_clause, 30);
    else
        v_step_name := 'delete all from ' || target_table;
    end if;

    insert into __session_steps (
        step_order,
        step_type,
        step_name,
        program_call,
        step_spec
    )
    values (
        v_step_num,
        'delete',
        v_step_name,
        'flow.delete('
            || quote_literal(target_table)
            || case when where_clause is not null then ', where_clause => ' || quote_literal(where_clause) else '' end
            || case when execution_order != 'before' then ', execution_order => ' || quote_literal(execution_order) else '' end
            || case when truncate_mode then ', truncate_mode => true' else '' end
            || case when step_name is not null then ', step_name => ' || quote_literal(step_name) else '' end
            || ')',
        jsonb_build_object(
            'target_table', target_table,
            'where_clause', where_clause,
            'execution_order', execution_order,
            'truncate_mode', truncate_mode
        )
    );

    return format(
        'Step %s: %s (%s write%s)',
        v_step_num,
        v_step_name,
        execution_order,
        v_warning
    );
end;
$body$;

comment on function flow.delete(text, text, text, boolean, text)
is $comment$@category Core: Transformations

Delete or cleanup data from a target table before or after the main write operation.

This function allows you to remove data from a table as part of your pipeline.
By default, it runs BEFORE the write step, but can be configured to run AFTER.
It supports conditional deletion via WHERE clauses with variable substitution,
or can truncate the entire table for maximum performance.

Parameters:
  target_table    - Schema-qualified target table name
  where_clause    - Optional WHERE condition (supports variables). Default: null (delete all rows)
  execution_order - When to execute: 'before' or 'after' the write. Default: 'before'
  truncate_mode   - If true, use TRUNCATE instead of DELETE. Default: false
  step_name       - Optional custom step name for display

Modes:
- DELETE with WHERE: Removes rows matching the condition
- DELETE without WHERE: Removes all rows (slower than truncate)
- TRUNCATE: Fast removal of all rows (WHERE clause ignored with warning)

Execution Order:
- The step's position in the pipeline determines its execution order relative to write()
- If called BEFORE write(), it defaults to execution_order='before' (executed before write)
- If called AFTER write(), it defaults to execution_order='after' (executed after write)
- The 'execution_order' parameter can override this default behavior

Variables:
- WHERE clauses can reference pipeline variables using ${variable_name} syntax
- Variables are resolved during pipeline compilation

Examples:
  -- Delete old records before writing new ones
  SELECT flow.delete('staging.orders', 'order_date < CURRENT_DATE - 30');
  SELECT flow.write('staging.orders', 'insert');
  
  -- Delete processed records after writing
  SELECT flow.write('processed.orders', 'insert');
  SELECT flow.delete('staging.orders', 'status = ''PROCESSED''', execution_order => 'after');
  
  -- Truncate for full refresh
  SELECT flow.delete('staging.orders', truncate_mode => true);
  SELECT flow.write('staging.orders', 'insert');
  
  -- Delete with variable
  SELECT flow.delete('orders', 'region = ${target_region}');
  
  -- Custom step name
  SELECT flow.delete(
      'temp.processing_log',
      'created_at < NOW() - INTERVAL ''7 days''',
      step_name => 'Clean old logs'
  );

Performance Notes:
- TRUNCATE is much faster than DELETE but cannot have WHERE clauses
- TRUNCATE cannot be rolled back in some database configurations
- DELETE with WHERE can use indexes for better performance
$comment$;
