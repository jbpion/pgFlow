create or replace function flow.inspect_step(step_num int)
returns jsonb
language plpgsql
as $body$
declare
    v_result jsonb;
begin
    perform flow.__ensure_session_steps();

    select jsonb_build_object(
        'step_order', s.step_order,
        'step_type', s.step_type,
        'step_name', s.step_name,
        'program_call', s.program_call,
        'step_spec', s.step_spec
    )
    into v_result
    from __session_steps s
    where s.step_order = step_num;

    if v_result is null then
        raise exception 'Step % not found in current pipeline', step_num;
    end if;

    return v_result;
end;
$body$;

comment on function flow.inspect_step(int)
is $comment$@category Inspection: Session Pipeline

Get detailed JSONB view of a specific step in the session pipeline.

Parameters:
  step_num - Step order number to inspect

Returns: JSONB object containing:
- step_order, step_type, step_name
- program_call (original function call)
- step_spec (step-specific payload)

Example:
  SELECT flow.read_db_object('public.orders');
  SELECT flow.where('status = ''completed''');
  
  -- Inspect the second step
  SELECT flow.inspect_step(2);
$comment$;
