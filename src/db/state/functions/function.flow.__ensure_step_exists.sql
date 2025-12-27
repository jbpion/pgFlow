create or replace function flow.__ensure_step_exists(step_name text)
returns void
language plpgsql as $$
declare
    cnt int;
begin
    select count(*)
      into cnt
      from __session_steps s
     where s.step_name = $1; --step_name (use placeholder to solve ambiguous column error.)

    if cnt = 0 then
        raise exception
            'No flow state found for step "%". You must call a read_* function first.',
            step_name;
    end if;
end;
$$;
