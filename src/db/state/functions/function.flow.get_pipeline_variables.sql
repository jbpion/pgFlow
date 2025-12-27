create or replace function flow.get_pipeline_variables(
    pipeline_name text
)
returns table(
    variable_name text,
    occurrences bigint,
    found_in_steps text[]
)
language plpgsql
as $body$
declare
    v_pipeline_id bigint;
begin
    -- Get pipeline ID
    select p.pipeline_id into v_pipeline_id
    from flow.pipeline p
    where p.pipeline_name = get_pipeline_variables.pipeline_name;

    if v_pipeline_id is null then
        raise exception 'Pipeline "%" not found', pipeline_name;
    end if;

    -- Extract all variables from step_spec jsonb
    -- Variables are in format {{variable_name}}
    return query
    with all_text as (
        select
            s.step_order,
            s.step_name,
            s.step_spec::text as spec_text
        from flow.pipeline_step s
        where s.pipeline_id = v_pipeline_id
    ),
    extracted_vars as (
        select
            step_order,
            step_name,
            regexp_matches(spec_text, '\{\{([^}]+)\}\}', 'g') as var_match
        from all_text
    ),
    vars_with_steps as (
        select
            var_match[1] as var_name,
            step_order,
            step_name
        from extracted_vars
    )
    select
        var_name as variable_name,
        count(*)::bigint as occurrences,
        array_agg(distinct format('Step %s: %s', step_order, coalesce(step_name, '(unnamed)'))) as found_in_steps
    from vars_with_steps
    group by var_name
    order by var_name;
end;
$body$;

comment on function flow.get_pipeline_variables(text)
is $comment$@category Pipeline: Inspection

Extract all {{variable}} placeholders from a registered pipeline.

Use this to discover what variables a pipeline expects before running it with flow.run().

Parameters:
  pipeline_name - Name of registered pipeline to inspect

Returns:
- variable_name: Variable name (without {{ }})
- occurrences: Number of times the variable appears
- found_in_steps: Array of step descriptions where variable is used

Built-in variables (automatically available):
- {{current_date}}, {{today}} - Current date
- {{now}} - Current timestamp

Example:
  -- Pipeline with variables
  SELECT flow.read_db_object('public.orders');
  SELECT flow.where('order_date >= ''{{start_date}}'' AND region = ''{{region}}''');
  SELECT flow.register_pipeline('orders_filtered');
  
  -- Discover variables
  SELECT * FROM flow.get_pipeline_variables('orders_filtered');
  -- Returns: start_date, region
$comment$;
