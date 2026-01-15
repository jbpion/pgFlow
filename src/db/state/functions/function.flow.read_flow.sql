create or replace function flow.read_flow(
    pipeline_name text,
    step_name     text default null
) returns void
language plpgsql as $$
declare
    next_step int;
begin
    perform flow.__ensure_session_steps();

    select coalesce(max(step_order), 0) + 1
      into next_step
      from __session_steps;

    insert into __session_steps
    values (
        next_step,
        'read_flow',
        step_name,
        format('flow.read_flow(%L, %L)', pipeline_name, step_name),
        jsonb_build_object(
            'pipeline_name', pipeline_name
        )
    );
end;
$$;

comment on function flow.read_flow(text, text)
is $comment$@category Core: Pipeline Initialization

Read from a registered pipeline as a data source. This allows chaining pipelines together, using the output of one pipeline as input to another.

Parameters:
  pipeline_name - Name of the registered pipeline to read from
  step_name     - Optional name for this step (default: auto-generated)
$comment$;
