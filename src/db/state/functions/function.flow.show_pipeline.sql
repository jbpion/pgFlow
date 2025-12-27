create or replace function flow.show_pipeline()
returns text
language plpgsql
as $body$
declare
    v_output text := '';
    v_step record;
    v_compiled text;
begin
    perform flow.__ensure_session_steps();

    v_output := v_output || E'=== PIPELINE AST ===\n\n';

    for v_step in
        select * from __session_steps order by step_order
    loop
        v_output := v_output || format(
            'Step %s: %s (%s)' || E'\n',
            v_step.step_order,
            v_step.step_type,
            v_step.step_name
        );
        v_output := v_output || format(
            '  Call: %s' || E'\n',
            v_step.program_call
        );
        v_output := v_output || format(
            '  Spec: %s' || E'\n\n',
            v_step.step_spec::text
        );
    end loop;

    v_output := v_output || E'=== COMPILED SQL ===\n\n';

    begin
        v_compiled := flow.compile();
        v_output := v_output || v_compiled || E'\n';
    exception
        when others then
            v_output := v_output || 'ERROR: ' || SQLERRM || E'\n';
    end;

    return v_output;
end;
$body$;

comment on function flow.show_pipeline()
is $comment$@category Inspection: Session Pipeline

Display a human-readable formatted view of the session pipeline and compiled SQL.

Returns a formatted text report showing:
1. Pipeline AST: All steps with order, type, name, call, and spec
2. Compiled SQL: The final executable query

Example:
  SELECT flow.read_db_object('public.orders');
  SELECT flow.where('status = ''completed''');
  SELECT flow.select(ARRAY['order_id', 'total']);
  
  -- View complete pipeline
  SELECT flow.show_pipeline();
$comment$;
