create or replace function flow.__compile_write(
    p_select_sql text,
    p_select_cols text[],
    p_target_table text,
    p_mode text,
    p_unique_keys text[],
    p_auto_create boolean,
    p_truncate_before boolean
)
returns text
language plpgsql
as $body$
declare
    v_sql text;
    v_column_names text[];
    v_non_key_cols text[];
    v_create_table_sql text;
    v_group_by_pos int;
begin
    v_sql := p_select_sql;
    
    -- Extract column names from select expressions
    v_column_names := flow.__extract_column_names(p_select_cols);
    
    -- For update/upsert modes, get non-key columns
    if p_mode in ('update', 'upsert', 'upsert_delete') then
        v_non_key_cols := (
            select array_agg(col)
            from unnest(v_column_names) col
            where col != all(p_unique_keys)
        );
    end if;
    
    -- Helper: create table with WHERE false (inject before GROUP BY if present)
    if p_auto_create then
        v_group_by_pos := position('GROUP BY' in upper(v_sql));
        if v_group_by_pos > 0 then
            -- Insert WHERE false before GROUP BY
            v_create_table_sql := 'CREATE TABLE IF NOT EXISTS ' || p_target_table || ' AS ' || E'\n' ||
                                  substring(v_sql from 1 for v_group_by_pos - 1) ||
                                  ' WHERE false ' || E'\n' ||
                                  substring(v_sql from v_group_by_pos);
        else
            -- No GROUP BY, append WHERE false at end
            v_create_table_sql := 'CREATE TABLE IF NOT EXISTS ' || p_target_table || ' AS ' || E'\n' || 
                                  v_sql || ' WHERE false';
        end if;
        v_create_table_sql := v_create_table_sql || '; ' || E'\n';
    end if;
    
    case p_mode
        when 'insert' then
            if p_auto_create then
                v_sql := v_create_table_sql ||
                         'INSERT INTO ' || p_target_table || ' ' || E'\n' || v_sql;
            elsif p_truncate_before then
                v_sql := 'TRUNCATE TABLE ' || p_target_table || '; ' || E'\n' ||
                         'INSERT INTO ' || p_target_table || ' ' || E'\n' || v_sql;
            else
                v_sql := 'INSERT INTO ' || p_target_table || ' ' || E'\n' || v_sql;
            end if;
            
        when 'update' then
            if p_auto_create then
                v_sql := v_create_table_sql ||
                         'UPDATE ' || p_target_table || ' dst SET ' || E'\n' ||
                         '  ' || array_to_string(
                             (select array_agg(col || ' = src.' || col)
                              from unnest(v_non_key_cols) col),
                             ', ' || E'\n  '
                         ) || E'\n' ||
                         'FROM (' || E'\n' || v_sql || E'\n' || ') src' || E'\n' ||
                         'WHERE ' || array_to_string(
                             (select array_agg('dst.' || key || ' = src.' || key)
                              from unnest(p_unique_keys) key),
                             ' AND '
                         );
            else
                v_sql := 'UPDATE ' || p_target_table || ' dst SET ' || E'\n' ||
                         '  ' || array_to_string(
                             (select array_agg(col || ' = src.' || col)
                              from unnest(v_non_key_cols) col),
                             ', ' || E'\n  '
                         ) || E'\n' ||
                         'FROM (' || E'\n' || v_sql || E'\n' || ') src' || E'\n' ||
                         'WHERE ' || array_to_string(
                             (select array_agg('dst.' || key || ' = src.' || key)
                              from unnest(p_unique_keys) key),
                             ' AND '
                         );
            end if;
            
        when 'upsert' then
            if p_auto_create then
                v_sql := v_create_table_sql ||
                         'INSERT INTO ' || p_target_table || ' ' || E'\n' || v_sql || E'\n' ||
                         'ON CONFLICT (' || array_to_string(p_unique_keys, ', ') || ') DO UPDATE SET ' || E'\n' ||
                         '  ' || array_to_string(
                             (select array_agg(col || ' = EXCLUDED.' || col)
                              from unnest(v_non_key_cols) col),
                             ', ' || E'\n  '
                         );
            else
                v_sql := 'INSERT INTO ' || p_target_table || ' ' || E'\n' || v_sql || E'\n' ||
                         'ON CONFLICT (' || array_to_string(p_unique_keys, ', ') || ') DO UPDATE SET ' || E'\n' ||
                         '  ' || array_to_string(
                             (select array_agg(col || ' = EXCLUDED.' || col)
                              from unnest(v_non_key_cols) col),
                             ', ' || E'\n  '
                         );
            end if;
            
        when 'upsert_delete' then
            if p_auto_create then
                v_sql := v_create_table_sql ||
                         'WITH src_data AS (' || E'\n' || v_sql || E'\n' || ')' || E'\n' ||
                         'INSERT INTO ' || p_target_table || ' ' || E'\n' ||
                         'SELECT * FROM src_data ' || E'\n' ||
                         'ON CONFLICT (' || array_to_string(p_unique_keys, ', ') || ') DO UPDATE SET ' || E'\n' ||
                         '  ' || array_to_string(
                             (select array_agg(col || ' = EXCLUDED.' || col)
                              from unnest(v_non_key_cols) col),
                             ', ' || E'\n  '
                         ) || '; ' || E'\n' ||
                         'DELETE FROM ' || p_target_table || ' dst ' || E'\n' ||
                         'WHERE NOT EXISTS (' || E'\n' ||
                         '  SELECT 1 FROM src_data src WHERE ' ||
                         array_to_string(
                             (select array_agg('dst.' || key || ' = src.' || key)
                              from unnest(p_unique_keys) key),
                             ' AND '
                         ) || E'\n' ||
                         ')';
            else
                v_sql := 'WITH src_data AS (' || E'\n' || v_sql || E'\n' || ')' || E'\n' ||
                         'INSERT INTO ' || p_target_table || ' ' || E'\n' ||
                         'SELECT * FROM src_data ' || E'\n' ||
                         'ON CONFLICT (' || array_to_string(p_unique_keys, ', ') || ') DO UPDATE SET ' || E'\n' ||
                         '  ' || array_to_string(
                             (select array_agg(col || ' = EXCLUDED.' || col)
                              from unnest(v_non_key_cols) col),
                             ', ' || E'\n  '
                         ) || '; ' || E'\n' ||
                         'DELETE FROM ' || p_target_table || ' dst ' || E'\n' ||
                         'WHERE NOT EXISTS (' || E'\n' ||
                         '  SELECT 1 FROM src_data src WHERE ' ||
                         array_to_string(
                             (select array_agg('dst.' || key || ' = src.' || key)
                              from unnest(p_unique_keys) key),
                             ' AND '
                         ) || E'\n' ||
                         ')';
            end if;
    end case;
    
    return v_sql;
end;
$body$;

comment on function flow.__compile_write(text, text[], text, text, text[], boolean, boolean)
is 'Helper function to wrap a SELECT query with INSERT/UPDATE/UPSERT logic based on write mode. Handles auto-create, truncate, and various write patterns.';
