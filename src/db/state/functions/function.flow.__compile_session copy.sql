create or replace function flow.__compile_session()
returns table(step_order int, compiled_sql text)
language plpgsql as $$
declare
    r record;
    sql text := '';
    prev_sql text := '';
begin
    for r in
        select * from __session_steps order by step_order
    loop
        if r.step_type = 'read_db_object' then
            sql := format('select * from %s', (r.step_spec->>'object_name'));

        elsif r.step_type = 'read_flow' then
            sql := format('select * from flow.run(%L)', (r.step_spec->>'pipeline_name'));

        elsif r.step_type = 'select' then
            sql := format(
                'select %s from (%s) src%s',
                (
                    select string_agg(
                        format('%s as %I', x->>'expr', x->>'as'),
                        ', '
                    )
                    from jsonb_array_elements(r.step_spec->'select_list') x
                ),
                prev_sql,
                case
                    when r.step_spec->>'where' is not null
                    then ' where ' || (r.step_spec->>'where')
                    else ''
                end
            );
        else
            raise exception 'Unknown step_type: %', r.step_type;
        end if;

        prev_sql := sql;

        return query select r.step_order, sql;
    end loop;
end;
$$;
