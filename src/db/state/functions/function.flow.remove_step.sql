create or replace function flow.remove_step(
    p_step_order int default null,
    p_step_name text default null
)
returns void
language plpgsql
as $body$
declare
    v_deleted_count int;
    v_order_to_delete int;
begin
    -- Ensure session steps table exists
    perform flow.__ensure_session_steps();

    -- Validate input: must provide either step_order or step_name
    if p_step_order is null and p_step_name is null then
        raise exception 'Must provide either step_order or step_name parameter';
    end if;

    -- If both provided, step_order takes precedence
    if p_step_order is not null then
        v_order_to_delete := p_step_order;
    else
        -- Find step_order by step_name
        select step_order
          into v_order_to_delete
          from __session_steps
         where step_name = p_step_name
         limit 1;

        if v_order_to_delete is null then
            raise exception 'Step with name "%" not found in current pipeline', p_step_name;
        end if;
    end if;

    -- Delete the step
    delete from __session_steps
     where step_order = v_order_to_delete;

    get diagnostics v_deleted_count = row_count;

    if v_deleted_count = 0 then
        raise exception 'Step with order % not found in current pipeline', v_order_to_delete;
    end if;

    -- Reorder remaining steps to maintain sequential order
    with reordered as (
        select step_order,
               row_number() over (order by step_order) as new_order
          from __session_steps
    )
    update __session_steps s
       set step_order = r.new_order
      from reordered r
     where s.step_order = r.step_order
       and r.step_order != r.new_order;

    raise notice 'Step % removed. Remaining steps renumbered.', v_order_to_delete;
end;
$body$;

comment on function flow.remove_step(int, text)
is $comment$@category Pipeline Building: Step Management

Remove a step from the current session pipeline by step order or step name.

After removing a step, all remaining steps are automatically renumbered to 
maintain sequential order (1, 2, 3, ...).

Parameters:
  p_step_order - The order number of the step to remove (optional)
  p_step_name - The name of the step to remove (optional)
  
At least one parameter must be provided. If both are provided, step_order 
takes precedence.

Examples:
  -- Remove step by order number
  SELECT flow.remove_step(p_step_order => 2);
  
  -- Remove step by name
  SELECT flow.remove_step(p_step_name => 'Transform user columns');
  
  -- View remaining steps
  SELECT * FROM flow.show_steps();

Workflow:
  -- Build a pipeline
  SELECT flow.read_db_object('raw.orders');
  SELECT flow.select('order_id', 'customer_name');
  SELECT flow.where('status = ''active''');
  SELECT flow.write('stage.orders');
  
  -- Oops, don't need the where clause
  SELECT flow.remove_step(3);  -- Remove step 3
  
  -- Verify the change
  SELECT * FROM flow.show_steps();
  -- Steps are now: 1 (read), 2 (select), 3 (write - was 4, renumbered)
$comment$;
