create or replace function flow.show_lineage(
    pipeline_name text default null
)
returns table(
    output_column text,
    source_table text,
    source_expression text,
    transformation_type text
)
language plpgsql
as $body$
declare
    v_pipeline_id bigint;
    v_base_table text;
    v_base_alias text := 't0';
    v_lookup_map jsonb := '{}'::jsonb;
    v_alias_counter int := 0;
begin
    -- If pipeline_name provided, query from pipeline table
    -- Otherwise, use current session
    if pipeline_name is not null then
        select p.pipeline_id into v_pipeline_id
        from flow.pipeline p
        where p.pipeline_name = show_lineage.pipeline_name;
        
        if v_pipeline_id is null then
            raise exception 'Pipeline "%" not found', pipeline_name;
        end if;
        
        -- Build lineage from registered pipeline steps
        return query
        with pipeline_steps as (
            select ps.*
            from flow.pipeline_step ps
            where ps.pipeline_id = v_pipeline_id
            order by ps.step_order
        ),
        base_source as (
            -- Get the primary source table (first read_db_object)
            select 
                (step_spec->>'object')::text as table_name,
                't0' as alias
            from pipeline_steps
            where step_type = 'read' OR step_type = 'read_db_object'
            order by step_order
            limit 1
        ),
        lookup_sources as (
            -- Get all lookup/join tables
            select 
                step_order,
                (step_spec->>'lookup_object')::text as table_name,
                (step_spec->>'lookup_alias')::text as alias
            from pipeline_steps
            where step_type = 'lookup'
            order by step_order
        ),
        select_columns as (
            -- Get all selected columns from the last select step
            select 
                ps.step_order,
                cols.col_key as column_expr
            from (
                select step_order, step_spec
                from pipeline_steps
                where step_type = 'select'
                order by step_order desc
                limit 1
            ) ps,
            jsonb_each_text(ps.step_spec->'column_mapping') as cols(col_key, col_val)
        ),
        aggregate_measures as (
            -- Get aggregated measures
            select 
                step_order,
                m->>'alias' as column_name,
                m->>'op' as agg_op,
                m->>'column' as source_column
            from pipeline_steps,
                 jsonb_array_elements(step_spec->'measures') m
            where step_type = 'aggregate'
        ),
        aggregate_groups as (
            -- Get group by columns
            select 
                step_order,
                jsonb_array_elements_text(step_spec->'group_by') as column_expr
            from pipeline_steps
            where step_type = 'aggregate'
        )
        select 
            -- Output column name
            case 
                when sc.column_expr ~ ':' then 
                    split_part(sc.column_expr, ':', 2)
                when sc.column_expr ~ '\sAS\s' then
                    regexp_replace(sc.column_expr, '.*\sAS\s+(\w+).*', '\1', 'i')
                when sc.column_expr ~ '\.' then
                    split_part(sc.column_expr, '.', 2)
                else sc.column_expr
            end as output_column,
            
            -- Source table
            case
                when sc.column_expr ~ '^t\d+\.' then
                    coalesce(
                        (select ls.table_name 
                         from lookup_sources ls 
                         where split_part(sc.column_expr, '.', 1) = ls.alias
                         limit 1),
                        (select bs.table_name from base_source bs limit 1)
                    )
                else (select bs.table_name from base_source bs limit 1)
            end as source_table,
            
            -- Source expression
            case
                when sc.column_expr ~ ':' then
                    split_part(sc.column_expr, ':', 1)
                else sc.column_expr
            end as source_expression,
            
            -- Transformation type
            case
                when sc.column_expr ~ ':' then 'calculated'
                when sc.column_expr ~ 'CASE' then 'conditional'
                when sc.column_expr ~ '\(' then 'function'
                else 'direct'
            end as transformation_type
        from select_columns sc
        
        union all
        
        -- Add aggregate measures
        select
            am.column_name as output_column,
            (select bs.table_name from base_source bs limit 1) as source_table,
            upper(am.agg_op) || '(' || am.source_column || ')' as source_expression,
            'aggregate' as transformation_type
        from aggregate_measures am
        
        union all
        
        -- Add group by columns
        select
            case 
                when ag.column_expr ~ '\.' then
                    split_part(ag.column_expr, '.', 2)
                when ag.column_expr ~ '\(' then
                    regexp_replace(ag.column_expr, '.*\(.*\)', 'expr', 'i')
                else ag.column_expr
            end as output_column,
            case
                when ag.column_expr ~ '^t\d+\.' then
                    coalesce(
                        (select ls.table_name 
                         from lookup_sources ls 
                         where split_part(ag.column_expr, '.', 1) = ls.alias
                         limit 1),
                        (select bs.table_name from base_source bs limit 1)
                    )
                else (select bs.table_name from base_source bs limit 1)
            end as source_table,
            ag.column_expr as source_expression,
            'group_by' as transformation_type
        from aggregate_groups ag
        
        order by 1;
        
    else
        -- Use current session
        perform flow.__ensure_session_steps();
        
        return query
        with session_steps as (
            select *
            from __session_steps
            order by step_order
        ),
        base_source as (
            select 
                (step_spec->>'object_name')::text as table_name,
                't0' as alias
            from session_steps
            where step_type = 'read' OR step_type = 'read_db_object'
            order by step_order
            limit 1
        ),
        lookup_sources as (
            select 
                step_order,
                (step_spec->>'lookup_object')::text as table_name,
                (step_spec->>'lookup_alias')::text as alias
            from session_steps
            where step_type = 'lookup'
            order by step_order
        ),
        select_columns as (
            -- Get all selected columns from the last select step
            select 
                ss.step_order,
                cols.col_key as column_expr
            from (
                select step_order, step_spec
                from session_steps
                where step_type = 'select'
                order by step_order desc
                limit 1
            ) ss,
            jsonb_each_text(ss.step_spec->'column_mapping') as cols(col_key, col_val)
        ),
        aggregate_measures as (
            select 
                step_order,
                m->>'alias' as column_name,
                m->>'op' as agg_op,
                m->>'column' as source_column
            from session_steps,
                 jsonb_array_elements(step_spec->'measures') m
            where step_type = 'aggregate'
        ),
        aggregate_groups as (
            select 
                step_order,
                jsonb_array_elements_text(step_spec->'group_by') as column_expr
            from session_steps
            where step_type = 'aggregate'
        )
        select 
            case 
                when sc.column_expr ~ ':' then 
                    split_part(sc.column_expr, ':', 2)
                when sc.column_expr ~ '\sAS\s' then
                    regexp_replace(sc.column_expr, '.*\sAS\s+(\w+).*', '\1', 'i')
                when sc.column_expr ~ '\.' then
                    split_part(sc.column_expr, '.', 2)
                else sc.column_expr
            end as output_column,
            
            case
                when sc.column_expr ~ '^t\d+\.' then
                    coalesce(
                        (select ls.table_name 
                         from lookup_sources ls 
                         where split_part(sc.column_expr, '.', 1) = ls.alias
                         limit 1),
                        (select bs.table_name from base_source bs limit 1)
                    )
                else (select bs.table_name from base_source bs limit 1)
            end as source_table,
            
            case
                when sc.column_expr ~ ':' then
                    split_part(sc.column_expr, ':', 1)
                else sc.column_expr
            end as source_expression,
            
            case
                when sc.column_expr ~ ':' then 'calculated'
                when sc.column_expr ~ 'CASE' then 'conditional'
                when sc.column_expr ~ '\(' then 'function'
                else 'direct'
            end as transformation_type
        from select_columns sc
        
        union all
        
        select
            am.column_name as output_column,
            (select bs.table_name from base_source bs limit 1) as source_table,
            upper(am.agg_op) || '(' || am.source_column || ')' as source_expression,
            'aggregate' as transformation_type
        from aggregate_measures am
        
        union all
        
        select
            case 
                when ag.column_expr ~ '\.' then
                    split_part(ag.column_expr, '.', 2)
                when ag.column_expr ~ '\(' then
                    regexp_replace(ag.column_expr, '.*\(.*\)', 'expr', 'i')
                else ag.column_expr
            end as output_column,
            case
                when ag.column_expr ~ '^t\d+\.' then
                    coalesce(
                        (select ls.table_name 
                         from lookup_sources ls 
                         where split_part(ag.column_expr, '.', 1) = ls.alias
                         limit 1),
                        (select bs.table_name from base_source bs limit 1)
                    )
                else (select bs.table_name from base_source bs limit 1)
            end as source_table,
            ag.column_expr as source_expression,
            'group_by' as transformation_type
        from aggregate_groups ag
        
        order by 1;
    end if;
end;
$body$;

comment on function flow.show_lineage(text)
is $comment$@category Inspection: Lineage

Show column lineage for a pipeline - tracking where each output column comes from.

Displays:
- output_column: Name of the column in the output
- source_table: Original source table
- source_expression: Source column or expression
- transformation_type: Type of transformation (direct, calculated, aggregate, etc.)

Parameters:
  pipeline_name - Name of registered pipeline (default: null = current session)

Transformation Types:
- 'direct': Simple column passthrough (e.g., 'customer_id')
- 'calculated': Computed expression with alias (e.g., 'quantity * price:total')
- 'conditional': CASE expression
- 'function': Function call
- 'aggregate': Aggregation function (SUM, COUNT, AVG, etc.)
- 'group_by': Column used in GROUP BY

Examples:
  -- Show lineage for current session pipeline
  SELECT flow.read_db_object('raw.orders');
  SELECT flow.lookup('raw.customers', 't0.customer_id = t1.customer_id', 
                     ARRAY['customer_name'], 't1');
  SELECT flow.select('t0.order_id', 't1.customer_name', 
                     't0.quantity * t0.price:total');
  
  SELECT * FROM flow.show_lineage();
  
  -- Show lineage for registered pipeline
  SELECT * FROM flow.show_lineage('daily_sales_report');
  
  -- Use in documentation or analysis
  SELECT 
      output_column,
      source_table || '.' || source_expression as full_path,
      transformation_type
  FROM flow.show_lineage('customer_summary')
  ORDER BY output_column;
$comment$;
