create or replace function flow.show_steps()
returns table(
    step_order int,
    step_type text,
    step_name text,
    program_call text,
    step_spec jsonb
)
language plpgsql
as $body$
begin
    perform flow.__ensure_session_steps();

    return query
    select
        s.step_order,
        s.step_type,
        s.step_name,
        s.program_call,
        s.step_spec
    from __session_steps s
    order by s.step_order;
end;
$body$;

comment on function flow.show_steps()
is $comment$@category Inspection: Session Pipeline

Display all steps in the current session pipeline as a table.

Use this to debug and inspect the pipeline AST before compilation.

Returns:
- step_order: Sequence number
- step_type: Step type (read, select, where, lookup, write)
- step_name: User-friendly description
- program_call: Original function call
- step_spec: JSONB payload with step-specific details

Example:
  SELECT flow.read_db_object('public.orders');
  SELECT flow.where('status = ''completed''');
  
  -- View pipeline structure
  SELECT * FROM flow.show_steps();
$comment$;
