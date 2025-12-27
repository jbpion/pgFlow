create or replace function flow.select(
    VARIADIC columns text[]
)
returns text
language plpgsql
as $body$
declare
    v_step_num int;
    v_col_count int;
    v_step_name text;
    v_column_mapping jsonb;
    v_actual_columns text[];
    v_col text;
begin
    -- Extract step_name if any column is tagged with chr(2) prefix
    v_step_name := null;
    v_actual_columns := ARRAY[]::text[];
    
    foreach v_col in array columns
    loop
        if v_col like chr(2) || '%' then
            v_step_name := substring(v_col from 2);  -- Remove chr(2) prefix
        else
            v_actual_columns := v_actual_columns || v_col;
        end if;
    end loop;
    
    v_col_count := array_length(v_actual_columns, 1);
    
    if v_col_count is null or v_col_count = 0 then
        raise exception 'Column list cannot be empty';
    end if;

    -- Parse column array into mapping
    -- Format: 'source_expr' or 'source_expr:target_name'
    -- Note: Handle :: (cast operator) vs : (our delimiter)
    v_column_mapping := (
        select jsonb_object_agg(
            case 
                when col ~ '[^:]:$|[^:]:([^:]|$)' then  -- has single colon (not ::)
                    substring(col from '.*[^:]:([^:]+)$')  -- extract after last single colon
                else 
                    col  -- no delimiter: use whole string as target
            end,
            case
                when col ~ '[^:]:$|[^:]:([^:]|$)' then  -- has single colon
                    substring(col from '^(.*):[^:]+$')     -- extract before last colon
                else
                    col  -- source is whole string
            end
        )
        from unnest(v_actual_columns) col
        where col is not null and trim(col) != ''
    );

    v_step_name := coalesce(v_step_name, 'select ' || v_col_count::text || ' columns');

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
        'select',
        v_step_name,
        'flow.select('
            || case when v_step_name != 'select ' || v_col_count::text || ' columns' 
                    then 'flow.step(' || quote_literal(v_step_name) || '), ' 
                    else '' 
               end
            || array_to_string(
                   array(
                       select quote_literal(c)
                       from unnest(v_actual_columns) c
                   ),
                   ', '
               )
            || ')',
        jsonb_build_object(
            'column_mapping', v_column_mapping
        )
    );

    return format(
        'Step %s: %s',
        v_step_num,
        v_step_name
    );
end;
$body$;

comment on function flow.select(VARIADIC text[])
is $comment$@category Core: Transformations

Add a projection step with explicit target-to-source column mapping.

Use simple VARIADIC syntax with optional colon-separated mapping:
- 'column_name' - maps to itself (column_name AS column_name)
- 'source_expr:target_name' - explicit mapping (source_expr AS target_name)
- flow.step('description') - anywhere in the list to provide a step name

Unqualified source columns are auto-qualified with t0.

Parameters:
  columns - Variable number of column expressions with optional :target_name suffix
            Can include flow.step('name') anywhere to provide step description

Examples:
  -- Simple column selection (maps to same names)
  SELECT flow.read_db_object('public.users');
  SELECT flow.select('id', 'email', 'created_at');
  
  -- With transformations and aliases
  SELECT flow.read_db_object('public.users');
  SELECT flow.select(
      'id:user_id',
      'email:email_address',
      'first_name || '' '' || last_name:full_name'
  );
  
  -- With flow.step() for description (can be anywhere)
  SELECT flow.read_db_object('public.users');
  SELECT flow.select(
      flow.step('Transform user columns'),
      'id:user_id',
      'email:email_address'
  );
  
  -- flow.step() at the end
  SELECT flow.select(
      'id:user_id',
      'email:email_address',
      flow.step('Transform user columns')
  );
$comment$;
