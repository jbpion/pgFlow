create or replace function flow.read_db_object(
    object_name text,
    step_name   text default null
) returns text
language plpgsql
as $body$
declare
    v_existing_steps int;
    v_step_name text := coalesce(step_name, 'read ' || object_name);
begin
    perform flow.__ensure_session_steps();

    select count(*) into v_existing_steps
    from __session_steps;

    if v_existing_steps > 0 then
        truncate table __session_steps;
        raise warning
            'Existing flow pipeline was reset by read_db_object(%).',
            object_name;
    end if;

    insert into __session_steps (
        step_order,
        step_type,
        step_name,
        program_call,
        step_spec
    )
    values (
        1,
        'read',
        v_step_name,
        format(
            'flow.read_db_object(%L, %L)',
            object_name,
            step_name
        ),
        jsonb_build_object(
            'object', object_name,
            'alias', 't0'
        )
    );

    return format(
        'Step 1: %s (read %s as t0)',
        v_step_name,
        object_name
    );
end;
$body$;

comment on function flow.read_db_object(text, text)
is $comment$@category Core: Pipeline Initialization

Initialize a new pipeline by reading from a database table or view.

This function creates the first step in your transformation pipeline. The source object is assigned the alias 't0' for use in subsequent transformation steps.

Parameters:
  object_name - Schema-qualified table or view name (e.g., 'public.users')
  step_name   - Optional descriptive name for this step (default: 'read <object_name>')

Examples:
  -- Start a simple pipeline
  SELECT flow.read_db_object('public.customers');
  
  -- With custom step name
  SELECT flow.read_db_object('sales.orders', 'load orders');
  
  -- Continue with transformations
  SELECT flow.read_db_object('public.users');
  SELECT flow.select(ARRAY['id', 'email', 'created_at']);
  SELECT flow.where('created_at > current_date - interval ''30 days''');
$comment$;
