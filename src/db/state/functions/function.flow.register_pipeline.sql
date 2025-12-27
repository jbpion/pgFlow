create or replace function flow.register_pipeline(
    pipeline_name text,
    description text default null
) returns bigint
language plpgsql as $$
declare
    pid bigint;
begin
    insert into flow.pipeline (pipeline_name, description)
    values (pipeline_name, description)
    returning pipeline_id into pid;

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

comment on function flow.register_pipeline(text, text)
is $comment$@category Pipeline: Management

Save the current session pipeline for later execution.

This function persists the pipeline steps from __session_steps into the flow.pipeline and flow.pipeline_step tables, allowing the pipeline to be executed later via flow.run().

Parameters:
  pipeline_name - Unique name for the pipeline
  description   - Optional description

Returns: pipeline_id (bigint)

Example:
  -- Build pipeline
  SELECT flow.read_db_object('public.orders');
  SELECT flow.where('status = ''completed''');
  SELECT flow.select(ARRAY['order_id', 'customer_id', 'total']);
  
  -- Save it
  SELECT flow.register_pipeline('completed_orders', 'Daily completed orders report');
  
  -- Later, execute it
  SELECT * FROM flow.run('completed_orders');
$comment$;
