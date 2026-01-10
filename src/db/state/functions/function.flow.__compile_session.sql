create or replace function flow.__compile_session()
returns text
language plpgsql
as $body$
declare
    v_sql              text;
    v_from             text;
    v_select_cols      text[] := array[]::text[];
    v_joins            text := '';
    v_where            text := '';
    v_group_by         text := '';
    v_step             record;
    v_grouped_where    jsonb := '{}'::jsonb;
    v_obj              text;
    v_alias            text;
    v_obj_schema       text;
    v_obj_name         text;
    v_write_target     text;
    v_write_mode       text;
    v_write_unique_keys text[];
    v_write_auto_create boolean;
    v_write_truncate   boolean;
begin
    perform flow.__ensure_session_steps();
    perform flow.__assert_pipeline_started();

    for v_step in
        select *
        from __session_steps
        order by step_order
    loop
        case v_step.step_type

            -- =====================
            -- READ
            -- =====================
            when 'read' then
                    v_obj := v_step.step_spec->>'object';
                    v_alias := v_step.step_spec->>'alias';  -- should be 't0'

                    v_from := format('%s %s', v_obj, v_alias);

                    -- try to expand explicit column list from information_schema
                    v_obj_schema := split_part(v_obj, '.', 1);
                    v_obj_name := split_part(v_obj, '.', 2);
                    if v_obj_name = '' then
                        -- object not schema-qualified, assume public
                        v_obj_name := v_obj_schema;
                        v_obj_schema := 'public';
                    end if;

                -- handle temp tables: pg_temp maps to the session's actual temp schema
                if v_obj_schema = 'pg_temp' then
                    select array_agg(format('%s.%s AS %s', v_alias, column_name, column_name) order by ordinal_position)
                    into v_select_cols
                    from information_schema.columns c
                    where c.table_schema ~ '^pg_temp_'
                      and c.table_name = v_obj_name;
                else
                    select array_agg(format('%s.%s AS %s', v_alias, column_name, column_name) order by ordinal_position)
                    into v_select_cols
                    from information_schema.columns c
                    where c.table_schema = v_obj_schema
                      and c.table_name = v_obj_name;
                end if;

            -- =====================
            -- SELECT
            -- =====================
            when 'select' then
                -- Replace current select columns using explicit target->source mapping
                -- Auto-qualify unqualified source expressions with t0 alias
                v_select_cols := (
                    select array_agg(
                        case
                            -- If source already has table reference (contains '.'), function call, or is qualified
                            when value ~ '\.' or value ~ '\(' or value ~* '^[lt]\d+\.' then 
                                format('%s AS %s', value, key)
                            -- If starts with CASE (case-insensitive), don't prefix
                            when value ~* '^\s*CASE\s' then
                                format('%s AS %s', value, key)
                            -- Otherwise, prefix with t0 alias
                            else 
                                format('t0.%s AS %s', value, key)
                        end
                        order by key  -- deterministic ordering
                    )
                    from jsonb_each_text(v_step.step_spec->'column_mapping')
                );

            -- =====================
            -- LOOKUP
            -- =====================
            when 'lookup' then
                declare
                    v_lookup_alias text := v_step.step_spec->>'lookup_alias';
                    v_on_duplicate text := v_step.step_spec->>'on_duplicate';
                begin
                    -- Use LATERAL subquery with LIMIT 1 for on_duplicate='first'
                    -- Simple LEFT JOIN for on_duplicate='error' (runtime check needed)
                    if v_on_duplicate = 'first' then
                        v_joins := v_joins || E'\nLEFT JOIN LATERAL (\n' ||
                            format(
                                '  SELECT * FROM %s WHERE %s LIMIT 1',
                                v_step.step_spec->>'lookup_object',
                                v_step.step_spec->>'on'
                            ) ||
                            format(E'\n) %s ON true', v_lookup_alias);
                    else
                        v_joins := v_joins || E'\nLEFT JOIN ' ||
                            format(
                                '%s %s ON %s',
                                v_step.step_spec->>'lookup_object',
                                v_lookup_alias,
                                v_step.step_spec->>'on'
                            );
                    end if;

                    -- append lookup columns
                    v_select_cols := v_select_cols ||
                        (
                            select array_agg(
                                case
                                    -- If column already has an alias prefix (contains '.'), use as-is but extract column name for alias
                                    when col like '%.%' then format('%s AS %s', col, substring(col from '[^.]+$'))
                                    -- Otherwise, prepend the lookup alias
                                    else format('%s.%s AS %s', v_lookup_alias, col, col)
                                end
                            )
                            from jsonb_array_elements_text(v_step.step_spec->'columns') col
                        );
                end;

            -- =====================
            -- WHERE
            -- =====================
            when 'where' then
                declare
                    g text := coalesce(v_step.step_spec->>'group', '__default__');
                    existing jsonb;
                begin
                    existing := v_grouped_where->g;

                    if existing is null then
                        v_grouped_where :=
                            jsonb_set(
                                v_grouped_where,
                                array[g],
                                jsonb_build_array(v_step.step_spec->>'condition')
                            );
                    else
                        v_grouped_where :=
                            jsonb_set(
                                v_grouped_where,
                                array[g],
                                existing || jsonb_build_array(v_step.step_spec->>'condition')
                            );
                    end if;
                end;

            -- =====================            -- AGGREGATE
            -- =====================
            when 'aggregate' then
                declare
                    v_group_by_cols text[];
                    v_group_by_select_exprs text[];
                    v_agg_cols text[];
                    v_group_clause text;
                    v_having_clause text;
                    v_having_exprs text[];
                begin
                    -- Build GROUP BY clause from group_by array (may be empty for ungrouped aggregation)
                    v_group_by_cols := (
                        select array_agg(col)
                        from jsonb_array_elements_text(v_step.step_spec->'group_by') col
                    );

                    -- Before replacing select columns, wrap current query in subquery
                    -- Build the pre-aggregate query
                    v_sql := 
                        'SELECT ' || array_to_string(v_select_cols, E',\n') || E'\n' ||
                        'FROM ' || v_from ||
                        (case when v_joins != '' then v_joins else E'\n' end) ||
                        (case when v_where != '' then v_where || E'\n' else E'\n' end);
                    
                    -- Wrap in subquery
                    v_from := '(' || E'\n' || v_sql || E'\n' || ') subquery';
                    v_joins := '';
                    v_where := '';
                    
                    -- Extract column names from group by expressions (e.g., t0.region -> region)
                    -- After subquery wrap, we reference these columns with subquery. prefix
                    if v_group_by_cols is not null and array_length(v_group_by_cols, 1) > 0 then
                        v_group_by_select_exprs := (
                            select array_agg(
                                'subquery.' ||
                                case
                                    when col like '%.%' then substring(col from '[^.]+$')
                                    else col
                                end
                            )
                            from unnest(v_group_by_cols) col
                        );
                    else
                        v_group_by_select_exprs := ARRAY[]::text[];
                    end if;
                    
                    -- Build aggregation columns from measures mapping
                    -- measures contains: { "alias": "AGG(column)" }
                    -- Replace table aliases (t0., t1., etc.) with subquery. prefix
                    v_agg_cols := (
                        select array_agg(
                            format('%s AS %s', 
                                -- Replace table aliases with 'subquery.' (e.g., t0.quantity -> subquery.quantity)
                                regexp_replace(value, '\b[a-z][a-z0-9_]*\.', 'subquery.', 'g'),
                                key
                            )
                            order by key
                        )
                        from jsonb_each_text(v_step.step_spec->'measures')
                    );
                    
                    -- Build SELECT columns: group by columns + measures
                    -- For SELECT, use just the column name without subquery. prefix for cleaner output
                    if array_length(v_group_by_select_exprs, 1) > 0 then
                        v_select_cols := (
                            select array_agg(
                                substring(col from 'subquery\.(.*)') || ' AS ' || substring(col from 'subquery\.(.*)')
                            )
                            from unnest(v_group_by_select_exprs) col
                        ) || v_agg_cols;
                    else
                        v_select_cols := v_agg_cols;
                    end if;

                    -- Build GROUP BY clause (only if we have group columns)
                    -- Use full subquery.column references for GROUP BY
                    if array_length(v_group_by_select_exprs, 1) > 0 then
                        v_group_clause := 'GROUP BY ' || array_to_string(v_group_by_select_exprs, ', ');
                    else
                        v_group_clause := '';
                    end if;
                    
                    -- Build HAVING clause if present (appends to GROUP BY)
                    -- HAVING already has subquery. prefixes from the aggregate function
                    if jsonb_array_length(v_step.step_spec->'having') > 0 then
                        v_having_exprs := (
                            select array_agg(
                                -- Replace table aliases with subquery. prefix
                                regexp_replace(expr, '\b[a-z][a-z0-9_]*\.', 'subquery.', 'g')
                            )
                            from jsonb_array_elements_text(v_step.step_spec->'having') expr
                        );
                        v_having_clause := 'HAVING ' || array_to_string(v_having_exprs, ' AND ');
                        -- Append HAVING after GROUP BY
                        if v_group_clause != '' then
                            v_group_by := v_group_clause || E'\n' || v_having_clause;
                        else
                            -- HAVING without GROUP BY (allowed in SQL)
                            v_group_by := v_having_clause;
                        end if;
                    else
                        v_group_by := v_group_clause;
                    end if;
                end;

            -- =====================            -- WRITE
            -- =====================
            when 'write' then
                v_write_target := v_step.step_spec->>'target_table';
                v_write_mode := v_step.step_spec->>'mode';
                v_write_unique_keys := 
                    case 
                        when jsonb_typeof(v_step.step_spec->'unique_keys') = 'array' then
                            (select array_agg(value::text)
                             from jsonb_array_elements_text(v_step.step_spec->'unique_keys'))
                        else null
                    end;
                v_write_auto_create := coalesce((v_step.step_spec->>'auto_create')::boolean, false);
                v_write_truncate := coalesce((v_step.step_spec->>'truncate_before')::boolean, false);

            else
                raise exception 'Unknown step type: %', v_step.step_type;
        end case;
    end loop;

    -- =====================
    -- BUILD WHERE CLAUSE
    -- =====================
    if v_grouped_where != '{}'::jsonb then
        v_where := 'WHERE ' ||
        (
            select string_agg(
                case
                    when jsonb_array_length(value) = 1
                        then value->>0
                    else
                        '(' ||
                        (
                            select string_agg(value_elem, ' AND ')
                            from jsonb_array_elements_text(value) value_elem
                        ) ||
                        ')'
                end,
                ' OR '
            )
            from jsonb_each(v_grouped_where)
        );
    end if;

    -- =====================
    -- FINAL SQL
    -- =====================
    -- If no columns selected, default to *
    if v_select_cols is null or array_length(v_select_cols, 1) is null then
        v_select_cols := array['*'];
    end if;
    
    v_sql :=
        'SELECT ' || array_to_string(v_select_cols, E',\n') || E'\n' ||
        'FROM ' || v_from ||
        (case when v_joins != '' then v_joins else E'\n' end) ||
        (case when v_where != '' then v_where || E'\n' else E'\n' end) ||
        (case when v_group_by != '' then v_group_by else '' end);

    -- =====================
    -- WRAP WITH WRITE IF PRESENT
    -- =====================
    if v_write_target is not null then
        v_sql := flow.__compile_write(
            v_sql,
            v_select_cols,
            v_write_target,
            v_write_mode,
            v_write_unique_keys,
            v_write_auto_create,
            v_write_truncate
        );
    end if;

    return v_sql;
end;
$body$;
