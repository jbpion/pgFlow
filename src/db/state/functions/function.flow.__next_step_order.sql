create or replace function flow.__next_step_order()
returns int
language plpgsql as $$
declare
    next_step int;
begin
    perform flow.__ensure_session_steps();

    select coalesce(max(step_order), 0) + 1
      into next_step
      from __session_steps;

    return next_step;
end;
$$;
