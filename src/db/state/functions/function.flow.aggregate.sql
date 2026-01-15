create or replace function flow.aggregate(
    source        text,
    group_by      text[] default null,
    VARIADIC measures_and_having flow.measure[] default array[]::flow.measure[]
)
returns text
language plpgsql
as $body$
declare
    v_step_num int;
    v_step_name text;
    v_measure_mapping jsonb;
    v_having_exprs text[];
    v_measure flow.measure;
    v_aliases text[];
    v_alias text;
    v_measures flow.measure[];
    v_having flow.expr[];
begin
    -- Separate measures from having expressions
    -- HAVING expressions can be cast as flow.measure with op='having'
    v_measures := array(
        select m from unnest(measures_and_having) m 
        where (m).op != 'having'
    );
    
    v_having := array(
        select row((m).column)::flow.expr 
        from unnest(measures_and_having) m 
        where (m).op = 'having'
    );

    -- Validate: measures must not be empty
    if v_measures is null or array_length(v_measures, 1) is null or array_length(v_measures, 1) = 0 then
        raise exception 'measures must not be empty';
    end if;

    -- Validate: each measure alias must be unique
    v_aliases := array(select (m).alias from unnest(v_measures) m);
    if array_length(v_aliases, 1) != (select count(distinct a) from unnest(v_aliases) a) then
        raise exception 'measure aliases must be unique';
    end if;

    -- Validate: registered aggregate operations
    foreach v_measure in array v_measures
    loop
        if (v_measure).op not in ('sum', 'count', 'avg', 'min', 'max') then
            raise exception 'unknown aggregate operation: %', (v_measure).op;
        end if;
    end loop;

    -- Build measure mapping: alias -> aggregate expression
    -- Format: { "alias": "AGG(column)" }
    v_measure_mapping := (
        select jsonb_object_agg(
            (m).alias,
            upper((m).op) || '(' || (m).column || ')'
        )
        from unnest(v_measures) m
    );

    -- Extract HAVING expressions and replace aliases with aggregate definitions
    if v_having is not null and array_length(v_having, 1) > 0 then
        -- For each HAVING expression, replace measure aliases with their aggregate expressions
        v_having_exprs := (
            select array_agg(resolved_expr)
            from (
                select (
                    -- Start with the original expression
                    select coalesce(
                        (
                            -- Try to replace each measure alias with its aggregate expression
                            select regexp_replace(
                                (e).expression,
                                '\m' || (m).alias || '\M',
                                upper((m).op) || '(' || (m).column || ')',
                                'gi'
                            )
                            from unnest(v_measures) m
                            where (e).expression ~* ('\m' || (m).alias || '\M')
                            order by length((m).alias) desc  -- Replace longest aliases first
                            limit 1
                        ),
                        (e).expression  -- If no replacement found, use original
                    )
                ) as resolved_expr
                from unnest(v_having) e
            ) resolved
        );
    end if;

    -- Generate step name
    v_step_name := 'aggregate ' || array_length(v_measures, 1)::text || ' measure(s)';
    if group_by is not null and array_length(group_by, 1) > 0 then
        v_step_name := v_step_name || ' grouped by ' || array_length(group_by, 1)::text || ' column(s)';
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
        'aggregate',
        v_step_name,
        format(
            'flow.aggregate(%L, %L, VARIADIC %L)',
            source,
            group_by,
            v_measures
        ),
        jsonb_build_object(
            'source', source,
            'group_by', coalesce(to_jsonb(group_by), '[]'::jsonb),
            'measures', v_measure_mapping,
            'having', coalesce(to_jsonb(v_having_exprs), '[]'::jsonb)
        )
    );

    return format(
        'Step %s: %s',
        v_step_num,
        v_step_name
    );
end;
$body$;

comment on function flow.aggregate(text, text[], VARIADIC flow.measure[])
is $comment$@category Core: Aggregation

Add a GROUP BY aggregation step to the pipeline using structured measure AST nodes.

This function follows an AST-first design. Use measure constructors (flow.sum, flow.count, etc.) 
to build structured aggregation specifications. The VARIADIC parameter allows passing measures
directly without wrapping in ARRAY[].

Parameters:
  source               - Source relation identifier
  group_by             - Array of columns to group by (use flow.group_by() or NULL for ungrouped)
  measures_and_having  - VARIADIC list of flow.measure nodes (measures + optional flow.having())

Helper Functions:
  - flow.group_by(col, ...)    - Create group by list (cleaner than ARRAY[])
  - flow.sum(column, alias)    - Sum aggregation
  - flow.count(column, alias)  - Count aggregation
  - flow.avg(column, alias)    - Average aggregation
  - flow.min(column, alias)    - Minimum aggregation
  - flow.max(column, alias)    - Maximum aggregation
  - flow.having(expression)    - HAVING clause condition

Examples:
  -- Group orders by customer (clean syntax!)
  SELECT flow.aggregate(
      'orders',
      flow.group_by('customer_id'),
      flow.sum('amount', 'total_amount'),
      flow.count('*', 'order_count')
  );

  -- Multiple group columns
  SELECT flow.aggregate(
      'sales',
      flow.group_by('region', 'product'),
      flow.sum('quantity', 'total_quantity'),
      flow.avg('price', 'avg_price'),
      flow.having('total_quantity > 100')
  );

  -- Ungrouped aggregation (totals) - use NULL
  SELECT flow.aggregate(
      'transactions',
      NULL,
      flow.sum('amount', 'grand_total'),
      flow.count('*', 'transaction_count')
  );

  -- You can still use ARRAY[] if preferred
  SELECT flow.aggregate(
      'orders',
      ARRAY['customer_id', 'region'],
      flow.sum('amount', 'total'),
      flow.having('total > 500')
  );
$comment$;
