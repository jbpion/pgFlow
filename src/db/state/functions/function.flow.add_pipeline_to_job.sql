create or replace function flow.add_pipeline_to_job(
    p_job_name text,
    p_pipeline_name text,
    p_step_order int default null
)
returns void
language plpgsql
as $body$
declare
    v_job_id bigint;
    v_pipeline_id bigint;
    v_next_order int;
begin
    -- Get job_id
    select job_id into v_job_id
    from flow.job
    where job_name = p_job_name;
    
    if v_job_id is null then
        raise exception 'Job "%" not found. Create it first with flow.create_job()', p_job_name;
    end if;
    
    -- Get pipeline_id
    select pipeline_id into v_pipeline_id
    from flow.pipeline
    where pipeline_name = p_pipeline_name;
    
    if v_pipeline_id is null then
        raise exception 'Pipeline "%" not found', p_pipeline_name;
    end if;
    
    -- Determine step order
    if p_step_order is null then
        select coalesce(max(step_order), 0) + 1 into v_next_order
        from flow.job_step
        where job_id = v_job_id;
        p_step_order := v_next_order;
    end if;
    
    -- Insert or update
    insert into flow.job_step (job_id, pipeline_id, step_order)
    values (v_job_id, v_pipeline_id, p_step_order)
    on conflict (job_id, pipeline_id) do update
        set step_order = excluded.step_order,
            enabled = true;
    
    -- Update job timestamp
    update flow.job
    set updated_at = now()
    where job_id = v_job_id;
end;
$body$;

comment on function flow.add_pipeline_to_job(text, text, int)
is $comment$@category Jobs: Management

Add a pipeline to a job at a specific position in the execution order.

If the pipeline already exists in the job, updates its step order.

Parameters:
  p_job_name      - Name of the job
  p_pipeline_name - Name of the pipeline to add
  p_step_order    - Execution order (default: append to end)

Examples:
  -- Create job
  SELECT flow.create_job('daily_etl', 'Daily ETL process');
  
  -- Add pipelines in order
  SELECT flow.add_pipeline_to_job('daily_etl', 'extract_orders');
  SELECT flow.add_pipeline_to_job('daily_etl', 'transform_orders');
  SELECT flow.add_pipeline_to_job('daily_etl', 'load_orders');
  
  -- Insert at specific position
  SELECT flow.add_pipeline_to_job('daily_etl', 'validate_data', 2);
$comment$;
