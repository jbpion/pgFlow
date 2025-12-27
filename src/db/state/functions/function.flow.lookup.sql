create or replace function flow.lookup(
    lookup_object  text,
    on_clause      text,
    columns        text[],
    lookup_alias   text default null,
    on_miss        text default 'allow',
    on_duplicate   text default 'error',
    step_name      text default null
) returns text
language plpgsql
as $body$
declare
    v_step int;
    v_alias text := coalesce(lookup_alias, 'l' || floor(random()*1000)::int);
    v_step_name text := coalesce(step_name, 'lookup ' || lookup_object);
begin
    perform flow.__ensure_session_steps();
    perform flow.__assert_pipeline_started();

    if columns is null or array_length(columns, 1) = 0 then
        raise exception 'lookup requires at least one column';
    end if;

    -- validate on_miss
    if on_miss not in ('allow', 'error', 'warn') then
        raise exception 'Invalid on_miss value: %. Must be allow, error, or warn.', on_miss;
    end if;

    -- validate on_duplicate
    if on_duplicate not in ('error', 'first', 'warn') then
        raise exception 'Invalid on_duplicate value: %. Must be error, first, or warn.', on_duplicate;
    end if;

    v_step := flow.__next_step_order();

    insert into __session_steps (
        step_order,
        step_type,
        step_name,
        program_call,
        step_spec
    )
    values (
        v_step,
        'lookup',
        v_step_name,
        format(
            'flow.lookup(%L, %L, ARRAY[%s], %L, %L, %L)',
            lookup_object,
            on_clause,
            array_to_string(
                array(
                    select quote_literal(c)
                    from unnest(columns) c
                ),
                ','
            ),
            v_alias,
            on_miss,
            on_duplicate
        ),
        jsonb_build_object(
            'lookup_object', lookup_object,
            'lookup_alias', v_alias,
            'on', on_clause,
            'columns', columns,
            'on_miss', on_miss,
            'on_duplicate', on_duplicate
        )
    );

    return format(
        'Step %s: %s (alias: %s)',
        v_step,
        v_step_name,
        v_alias
    );
end;
$body$;

comment on function flow.lookup(text, text, text[], text, text, text, text)
is $comment$@category Core: Transformations

Enrich pipeline data with columns from another table via a join with cardinality control.

This function performs a LEFT JOIN and provides control over missing matches and duplicates:
- on_miss: 'allow' (default), 'error', or 'warn' - how to handle rows with no match
- on_duplicate: 'error' (default), 'first', or 'warn' - how to handle multiple matches

Parameters:
  lookup_object - Schema-qualified table/view name to join
  on_clause     - Join condition (e.g., 't0.customer_id = l1.id')
  columns       - Array of column expressions to add (e.g., ARRAY['l1.name', 'l1.email'])
  lookup_alias  - Alias for lookup table (default: auto-generated 'lN')
  on_miss       - Missing match behavior: 'allow', 'error', 'warn' (default: 'allow')
  on_duplicate  - Duplicate match behavior: 'error', 'first', 'warn' (default: 'error')
  step_name     - Optional descriptive name (default: 'lookup <object>')

Examples:
  -- Simple lookup (must have exactly 1 match)
  SELECT flow.read_db_object('public.orders');
  SELECT flow.lookup('public.customers', 't0.customer_id = l1.id', 
                     ARRAY['l1.name as customer_name'], 'l1');
  
  -- Allow missing matches, take first on duplicate
  SELECT flow.read_db_object('public.orders');
  SELECT flow.lookup('public.products', 't0.product_id = l2.id',
                     ARRAY['l2.category', 'l2.price'], 'l2', 'allow', 'first');
$comment$;
