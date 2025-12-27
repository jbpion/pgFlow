create or replace function flow.__ensure_session_steps()
returns void
language plpgsql as $$
begin
   set local client_min_messages = warning;
    create temporary table if not exists __session_steps (
        step_order   int,
        step_type    text,
        step_name    text,
        program_call text,
        step_spec    jsonb
    ) on commit preserve rows;
    set local client_min_messages = notice;

end;
$$;
