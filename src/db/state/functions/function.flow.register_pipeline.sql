create or replace function flow.register_pipeline(
    pipeline_name text,
    description text default null,
    mode text default 'append',
    version text default '1.0.0'
) returns bigint
language plpgsql as $$
declare
    pid bigint;
    v_compiled_sql text;
    v_existing_pid bigint;
    v_variables jsonb;
begin
    -- Validate mode parameter
    if mode not in ('append', 'replace') then
        raise exception 'Invalid mode "%". Must be "append" or "replace".', mode;
    end if;

    -- Compile the pipeline to get the final SQL
    v_compiled_sql := flow.__compile_session();

    -- Extract variables from the compiled SQL
    select jsonb_object_agg(var, null)
    into v_variables
    from (
        select distinct (regexp_matches(v_compiled_sql, '\{\{([^}]+)\}\}', 'g'))[1] as var
    ) vars;

    -- Check if pipeline already exists
    select pipeline_id into v_existing_pid
    from flow.pipeline
    where flow.pipeline.pipeline_name = register_pipeline.pipeline_name;

    if v_existing_pid is not null then
        if mode = 'append' then
            raise exception 'Pipeline "%" already exists. Use mode => ''replace'' to overwrite.', pipeline_name;
        elsif mode = 'replace' then
            -- Delete existing pipeline and its steps (cascade should handle steps)
            delete from flow.pipeline_step where pipeline_id = v_existing_pid;
            delete from flow.pipeline where pipeline_id = v_existing_pid;
            raise notice 'Replaced existing pipeline "%"', pipeline_name;
        end if;
    end if;

    -- Insert new pipeline with compiled SQL
    insert into flow.pipeline (pipeline_name, description, compiled_sql, variables, version, created_at, updated_at)
    values (pipeline_name, description, v_compiled_sql, v_variables, version, now(), now())
    returning pipeline_id into pid;

    -- Insert pipeline steps
    insert into flow.pipeline_step (
        pipeline_id,
        step_order,
        step_type,
        step_name,
        program_call,
        step_spec
    )
    select
        pid,
        step_order,
        step_type,
        step_name,
        program_call,
        step_spec
    from __session_steps
    order by step_order;

    return pid;
end;
$$;

comment on function flow.register_pipeline(text, text, text, text)
is $comment$@category Pipeline: Management

Save the current session pipeline for later execution.

This function persists the pipeline steps from __session_steps into the flow.pipeline and flow.pipeline_step tables, allowing the pipeline to be executed later via flow.run().

Parameters:
  pipeline_name - Unique name for the pipeline
  description   - Optional description
  mode          - Registration mode: 'append' (default) or 'replace'
                  * 'append': Create new pipeline. Fails if name already exists.
                  * 'replace': Replace existing pipeline with same name, or create if not exists.
  version       - Pipeline version string (default: '1.0.0')

Returns: pipeline_id (bigint)

Examples:
  -- Build pipeline
  SELECT flow.read_db_object('public.orders');
  SELECT flow.where('status = ''completed''');
  SELECT flow.select('order_id', 'customer_id', 'total');
  
  -- Save it (append mode - will error if exists)
  SELECT flow.register_pipeline('completed_orders', 'Daily completed orders report');
  
  -- Save with version
  SELECT flow.register_pipeline('completed_orders', 'Daily report', 'append', '1.0.0');
  
  -- Replace existing pipeline with new version
  SELECT flow.register_pipeline('completed_orders', 'Updated report', 'replace', '1.1.0');
  
  -- Later, execute it
  SELECT * FROM flow.run('completed_orders');
$comment$;
