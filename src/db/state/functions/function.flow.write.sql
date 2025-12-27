create or replace function flow.write(
    target_table     text,
    mode             text default 'insert',
    unique_keys      text[] default null,
    auto_create      boolean default false,
    truncate_before  boolean default false
)
returns text
language plpgsql
as $body$
declare
    v_step_num int;
    v_step_name text;
begin
    perform flow.__ensure_session_steps();
    perform flow.__assert_pipeline_started();

    -- validate mode
    if mode not in ('insert', 'update', 'upsert', 'upsert_delete') then
        raise exception
            'Invalid mode: %. Must be insert, update, upsert, or upsert_delete.',
            mode;
    end if;

    -- validate that unique_keys is provided for update/upsert modes
    if mode in ('update', 'upsert', 'upsert_delete') and 
       (unique_keys is null or array_length(unique_keys, 1) = 0) then
        raise exception
            'unique_keys must be specified for mode: %', mode;
    end if;

    -- validate that truncate_before is only used with insert mode
    if truncate_before and mode != 'insert' then
        raise exception
            'truncate_before can only be used with mode: insert';
    end if;

    v_step_num := flow.__next_step_order();
    v_step_name := 'write to ' || target_table;

    insert into __session_steps (
        step_order,
        step_type,
        step_name,
        program_call,
        step_spec
    )
    values (
        v_step_num,
        'write',
        v_step_name,
        'flow.write('
            || quote_literal(target_table)
            || case when mode != 'insert' then ', mode => ' || quote_literal(mode) else '' end
            || case when unique_keys is not null then 
                   ', unique_keys => ARRAY[' || 
                   array_to_string(
                       array(select quote_literal(k) from unnest(unique_keys) k),
                       ', '
                   ) || ']'
               else '' end
            || case when auto_create then ', auto_create => true' else '' end
            || case when truncate_before then ', truncate_before => true' else '' end
            || ')',
        jsonb_build_object(
            'target_table', target_table,
            'mode', mode,
            'unique_keys', unique_keys,
            'auto_create', auto_create,
            'truncate_before', truncate_before
        )
    );

    return format(
        'Step %s: %s (mode: %s%s%s)',
        v_step_num,
        v_step_name,
        mode,
        case when truncate_before then ', truncate' else '' end,
        case when auto_create then ', auto-create' else '' end
    );
end;
$body$;

comment on function flow.write(text, text, text[], boolean, boolean)
is $comment$@category Core: Transformations

Terminal step that materializes the pipeline into a target table.

This function must be the last step in a pipeline. It wraps the compiled 
SELECT in an INSERT, UPDATE, or UPSERT statement.

Modes:
- 'insert': INSERT INTO - appends to existing table (default)
- 'update': UPDATE - updates existing rows based on unique_keys
- 'upsert': INSERT ... ON CONFLICT DO UPDATE - insert new, update existing
- 'upsert_delete': Like upsert, but also deletes rows not in source

Parameters:
  target_table    - Schema-qualified target table name
  mode            - Write mode: 'insert', 'update', 'upsert', or 'upsert_delete' 
                    (default: 'insert')
  unique_keys     - Column(s) that uniquely identify rows (required for 
                    update/upsert modes)
  auto_create     - If true, create target table if it doesn't exist
                    WARNING: Development use only! (default: false)
  truncate_before - If true, truncate table before insert (only valid with 
                    'insert' mode, default: false)

Examples:
  -- Simple insert (append to existing table)
  SELECT flow.read_db_object('public.raw_orders');
  SELECT flow.where('status = ''completed''');
  SELECT flow.write('public.completed_orders');
  
  -- Truncate and insert (full refresh)
  SELECT flow.read_db_object('public.daily_snapshot');
  SELECT flow.write('public.current_snapshot', 
                    truncate_before => true);
  
  -- Upsert based on order_id
  SELECT flow.read_db_object('public.order_updates');
  SELECT flow.write('public.orders',
                    mode => 'upsert',
                    unique_keys => ARRAY['order_id']);
  
  -- Upsert with delete (synchronize target with source)
  SELECT flow.read_db_object('public.current_products');
  SELECT flow.write('public.product_catalog',
                    mode => 'upsert_delete',
                    unique_keys => ARRAY['product_id']);
  
  -- Auto-create table for development
  SELECT flow.read_db_object('public.raw_data');
  SELECT flow.select(flow.step('Transform'), 'id', 'name', 'value');
  SELECT flow.write('scratch.test_output',
                    auto_create => true);
$comment$;
