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
    v_column_aliases   jsonb := '{}'::jsonb;  -- Tracks column alias -> source expression mapping
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
                
                -- Initialize column aliases for base table columns
                -- Each column maps to table_alias.column_name
                if v_obj_schema = 'pg_temp' then
                    select jsonb_object_agg(column_name, format('%s.%s', v_alias, column_name))
                    into v_column_aliases
                    from information_schema.columns c
                    where c.table_schema ~ '^pg_temp_'
                      and c.table_name = v_obj_name;
                else
                    select jsonb_object_agg(column_name, format('%s.%s', v_alias, column_name))
                    into v_column_aliases
                    from information_schema.columns c
                    where c.table_schema = v_obj_schema
                      and c.table_name = v_obj_name;
                end if;

            -- =====================
            -- SELECT
            -- =====================
            when 'select' then
                declare
                    v_resolved_source text;
                    v_target_col text;
                    v_source_col text;
                    v_alias_name text;
                    v_alias_expr text;
                    v_temp_cols text[];
                begin
                    -- Build select columns by resolving aliases
                    v_temp_cols := ARRAY[]::text[];
                    
                    for v_target_col, v_source_col in
                        select key, value
                        from jsonb_each_text(v_step.step_spec->'column_mapping')
                        order by key
                    loop
                        -- Resolve the source column expression
                        if v_column_aliases ? v_source_col then
                            -- Direct alias match
                            v_resolved_source := v_column_aliases->>v_source_col;
                        elsif v_source_col ~* '^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*' or v_source_col ~* '^[lt]\d+\.' then
                            -- Already qualified (matches patterns like: t0.column, table.column, l1.field)
                            -- But NOT decimal numbers like 1.1 or 3.14
                            v_resolved_source := v_source_col;
                        elsif v_source_col ~ '\(' then
                            -- Function call - resolve any aliases within it
                            v_resolved_source := v_source_col;
                            for v_alias_name, v_alias_expr in
                                select key, value from jsonb_each_text(v_column_aliases)
                            loop
                                -- Only replace if NOT preceded by a dot (not already qualified)
                                v_resolved_source := regexp_replace(
                                    v_resolved_source,
                                    '(?<!\.)\y' || v_alias_name || '\y',
                                    v_alias_expr,
                                    'g'
                                );
                            end loop;
                        elsif v_source_col ~* '^\s*CASE\s' then
                            -- CASE expression - resolve any aliases within it
                            v_resolved_source := v_source_col;
                            for v_alias_name, v_alias_expr in
                                select key, value from jsonb_each_text(v_column_aliases)
                            loop
                                -- Only replace if NOT preceded by a dot (not already qualified)
                                v_resolved_source := regexp_replace(
                                    v_resolved_source,
                                    '(?<!\.)\y' || v_alias_name || '\y',
                                    v_alias_expr,
                                    'g'
                                );
                            end loop;
                        else
                            -- Could be a simple column, expression with aliases, or arithmetic
                            v_resolved_source := v_source_col;
                            
                            -- Try to replace any known aliases in the expression
                            -- Use negative lookbehind to avoid matching already-qualified columns
                            for v_alias_name, v_alias_expr in
                                select key, value from jsonb_each_text(v_column_aliases)
                            loop
                                -- Only replace if NOT preceded by a dot (not already qualified)
                                v_resolved_source := regexp_replace(
                                    v_resolved_source,
                                    '(?<!\.)(\y' || v_alias_name || '\y)',
                                    v_alias_expr,
                                    'g'
                                );
                            end loop;
                            
                            -- If nothing was replaced and it's a simple identifier, qualify it
                            if v_resolved_source = v_source_col and v_source_col ~ '^\w+$' then
                                v_resolved_source := format('t0.%s', v_source_col);
                            end if;
                        end if;
                        
                        -- Add to column list
                        v_temp_cols := v_temp_cols || format('%s AS %s', v_resolved_source, v_target_col);
                        
                        -- Store the alias mapping for this target column
                        v_column_aliases := jsonb_set(
                            v_column_aliases,
                            array[v_target_col],
                            to_jsonb(v_resolved_source)
                        );
                    end loop;
                    
                    -- Remove any existing columns with aliases that are being redefined in v_temp_cols
                    -- Then append the new columns to avoid duplicates
                    declare
                        v_new_aliases text[];
                        v_existing_alias text;
                    begin
                        -- Extract aliases from new columns (format is "expression AS alias")
                        v_new_aliases := (
                            select array_agg(substring(col from ' AS (.+)$'))
                            from unnest(v_temp_cols) col
                        );
                        
                        -- Filter out columns from v_select_cols whose aliases are being redefined
                        v_select_cols := (
                            select array_agg(col)
                            from unnest(v_select_cols) col
                            where substring(col from ' AS (.+)$') != ALL(v_new_aliases)
                        );
                        
                        -- Handle case where v_select_cols becomes NULL if all columns were filtered out
                        if v_select_cols is null then
                            v_select_cols := ARRAY[]::text[];
                        end if;
                        
                        -- Now append the new columns
                        v_select_cols := v_select_cols || v_temp_cols;
                    end;
                end;

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
        declare
            v_condition_text text;
            v_resolved_condition text;
            v_alias_key text;
            v_col_name text;
        begin
            v_where := 'WHERE ' ||
            (
                select string_agg(
                    case
                        when jsonb_array_length(value) = 1
                            then 
                                -- Resolve aliases in single condition
                                (
                                    select 
                                        case
                                            -- Extract the column name (first word before operator)
                                            when v_column_aliases ? (regexp_match(value->>0, '^\s*(\w+)\s*[=<>!]'))[1] then
                                                -- Replace only the column name, not the entire condition
                                                regexp_replace(
                                                    value->>0,
                                                    '^\s*(\w+)(\s*[=<>!].*)$',
                                                    v_column_aliases->>(regexp_match(value->>0, '^\s*(\w+)\s*[=<>!]'))[1] || '\2'
                                                )
                                            else
                                                value->>0
                                        end
                                )
                        else
                            '(' ||
                            (
                                select string_agg(
                                    case
                                        -- Extract the column name (first word before operator)
                                        when v_column_aliases ? (regexp_match(value_elem, '^\s*(\w+)\s*[=<>!]'))[1] then
                                            -- Replace only the column name
                                            regexp_replace(
                                                value_elem,
                                                '^\s*(\w+)(\s*[=<>!].*)$',
                                                v_column_aliases->>(regexp_match(value_elem, '^\s*(\w+)\s*[=<>!]'))[1] || '\2'
                                            )
                                        else
                                            value_elem
                                    end,
                                    ' AND '
                                )
                                from jsonb_array_elements_text(value) value_elem
                            ) ||
                            ')'
                    end,
                    ' OR '
                )
                from jsonb_each(v_grouped_where)
            );
        end;
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
