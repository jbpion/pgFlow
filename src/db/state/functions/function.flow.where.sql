create or replace function flow.where(
    where_clause text,
    operator     text default 'AND',
    group_name   text default null,
    step_name    text default null
)
returns text
language plpgsql
as $body$
declare
    v_step_num int;
    v_step_name text := coalesce(step_name, 'where ' || left(where_clause, 30));
begin
    -- Validate operator
    if operator not in ('AND', 'OR') then
        raise exception
            'Invalid operator: %. Must be AND or OR.',
            operator;
    end if;

    perform flow.__ensure_session_steps();
    perform flow.__assert_pipeline_started();
    
    v_step_num := flow.__next_step_order();

    insert into __session_steps (
        step_order,
        step_type,
        step_name,
        program_call,
        step_spec
    )
    values (
        v_step_num,
        'where',
        v_step_name,
        case
            when group_name is not null then
                format(
                    'flow.where(%L, %L, %L)',
                    where_clause,
                    operator,
                    group_name
                )
            else
                format(
                    'flow.where(%L, %L)',
                    where_clause,
                    operator
                )
        end,
        jsonb_build_object(
            'condition', where_clause,
            'operator', operator,
            'group', group_name
        )
    );

    return format(
        'Step %s: %s',
        v_step_num,
        v_step_name
    );
end;
$body$;

comment on function flow.where(text, text, text, text)
is $comment$@category Core: Transformations

Add a filter predicate to the pipeline with AND/OR logic and optional grouping.

Multiple where() calls are combined in order. Use group_name to create OR clauses within AND logic:
  (group1_condition1 OR group1_condition2) AND ungrouped_condition

Parameters:
  where_clause - SQL condition (e.g., 't0.status = ''active''')
  operator     - 'AND' or 'OR' (default: 'AND')
  group_name   - Optional group name for OR logic within AND groups
  step_name    - Optional descriptive name (default: 'where <clause>')

Examples:
  -- Simple filter
  SELECT flow.read_db_object('public.orders');
  SELECT flow.where('t0.amount > 100');
  
  -- Grouped OR conditions: (status='active' OR status='pending') AND amount > 100
  SELECT flow.read_db_object('public.orders');
  SELECT flow.where('t0.status = ''active''', 'OR', 'status_group');
  SELECT flow.where('t0.status = ''pending''', 'OR', 'status_group');
  SELECT flow.where('t0.amount > 100', 'AND');
$comment$;
