create or replace function flow.__ensure_source_exists(source_step text)
returns void
language plpgsql as $$
declare
    cnt int;
begin
    select count(*) into cnt
    from __session_steps
    where step_name = source_step;

    if cnt = 0 then
        raise exception 'No prior step found with name "%". You must call a read_* function first.', source_step;
    end if;
end;
$$;
