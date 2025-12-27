create or replace function flow.aggregate(
    group_by_columns text[],
    VARIADIC aggregations text[]
)
returns text
language plpgsql
as $body$
declare
    v_step_num int;
    v_step_name text;
    v_agg_mapping jsonb;
    v_actual_aggs text[];
    v_agg text;
begin
    if array_length(group_by_columns, 1) is null or array_length(group_by_columns, 1) = 0 then
        raise exception 'GROUP BY columns cannot be empty';
    end if;

    -- Extract step_name if any aggregation is tagged with chr(2) prefix
    v_step_name := null;
    v_actual_aggs := ARRAY[]::text[];
    
    foreach v_agg in array aggregations
    loop
        if v_agg like chr(2) || '%' then
            v_step_name := substring(v_agg from 2);  -- Remove chr(2) prefix
        else
            v_actual_aggs := v_actual_aggs || v_agg;
        end if;
    end loop;

    if array_length(v_actual_aggs, 1) is null or array_length(v_actual_aggs, 1) = 0 then
        raise exception 'Aggregation list cannot be empty';
    end if;

    -- Parse aggregation array into mapping
    -- Format: 'agg_expr' or 'agg_expr:target_name'
    -- Note: Handle :: (cast operator) vs : (our delimiter)
    v_agg_mapping := (
        select jsonb_object_agg(
            case 
                when agg ~ '[^:]:$|[^:]:([^:]|$)' then  -- has single colon (not ::)
                    substring(agg from '.*[^:]:([^:]+)$')  -- extract after last single colon
                else 
                    agg  -- no delimiter: use whole string as target
            end,
            case
                when agg ~ '[^:]:$|[^:]:([^:]|$)' then  -- has single colon
                    substring(agg from '^(.*):[^:]+$')     -- extract before last colon
                else
                    agg  -- source is whole string
            end
        )
        from unnest(v_actual_aggs) agg
        where agg is not null and trim(agg) != ''
    );

    v_step_name := coalesce(
        v_step_name, 
        'aggregate with ' || array_length(group_by_columns, 1)::text || ' group columns'
    );

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
        'flow.aggregate(ARRAY['
            || array_to_string(
                   array(
                       select quote_literal(c)
                       from unnest(group_by_columns) c
                   ),
                   ','
               )
            || '], '
            || case when v_step_name != 'aggregate with ' || array_length(group_by_columns, 1)::text || ' group columns'
                    then 'flow.step(' || quote_literal(v_step_name) || '), '
                    else ''
               end
            || array_to_string(
                   array(
                       select quote_literal(a)
                       from unnest(v_actual_aggs) a
                   ),
                   ', '
               )
            || ')',
        jsonb_build_object(
            'group_by', to_jsonb(group_by_columns),
            'aggregations', v_agg_mapping
        )
    );

    return format(
        'Step %s: %s',
        v_step_num,
        v_step_name
    );
end;
$body$;

comment on function flow.aggregate(text[], VARIADIC text[])
is $comment$@category Core: Transformations

Add a GROUP BY aggregation step to the pipeline.

Groups rows by specified columns and applies aggregation functions. Aggregations use VARIADIC syntax with optional colon-separated mapping.

Parameters:
  group_by_columns - Array of columns to group by (use ARRAY[] syntax)
  aggregations     - Variable number of aggregation expressions with optional :target_name suffix
                     Can include flow.step('name') anywhere to provide step description

Aggregation Syntax:
- 'agg_function' - uses function call as column name
- 'agg_function:target_name' - explicit alias
- flow.step('description') - anywhere in the list to provide a step name

Examples:
  -- Group orders by customer and sum totals
  SELECT flow.read_db_object('public.orders');
  SELECT flow.aggregate(
      ARRAY['customer_id'],
      'SUM(amount):total_amount',
      'COUNT(*):order_count'
  );
  
  -- JSON aggregation with flow.step() for description
  SELECT flow.read_db_object('raw.order_lines');
  SELECT flow.select(
      'order_id',
      'order_date',
      'UPPER(customer):customer_name',
      'jsonb_build_object(''line'', line_num, ''product'', product):line_item'
  );
  SELECT flow.aggregate(
      ARRAY['order_id', 'order_date', 'customer_name'],
      flow.step('Aggregate line items into JSON array'),
      'jsonb_agg(line_item ORDER BY (line_item->>''line'')::int):items'
  );
  
  -- flow.step() can be placed anywhere
  SELECT flow.aggregate(
      ARRAY['customer_id'],
      'SUM(amount):total_amount',
      flow.step('Customer totals'),
      'COUNT(*):order_count'
  );
$comment$;
