create or replace function flow.create_job(
    p_job_name text,
    p_pipeline_name text default null,
    p_description text default null,
    p_step_order int default null
)
returns bigint
language plpgsql
as $body$
declare
    v_job_id bigint;
    v_pipeline_id bigint;
    v_next_order int;
begin
    -- Create or update job
    insert into flow.job (job_name, description)
    values (p_job_name, p_description)
    on conflict (job_name) do update
        set description = coalesce(excluded.description, flow.job.description),
            updated_at = now()
    returning job_id into v_job_id;
    
    -- Optionally add pipeline if provided
    if p_pipeline_name is not null then
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
        
        -- Add pipeline to job
        insert into flow.job_step (job_id, pipeline_id, step_order)
        values (v_job_id, v_pipeline_id, p_step_order)
        on conflict (job_id, pipeline_id) do update
            set step_order = excluded.step_order,
                enabled = true;
    end if;
    
    return v_job_id;
end;
$body$;

comment on function flow.create_job(text, text, text, int)
is $comment$@category Jobs: Management

Create a job or add a pipeline to an existing job.

This function can create an empty job or create/update a job with a pipeline.
If the job already exists, it updates the description (if provided) and/or adds the pipeline.

Parameters:
  p_job_name      - Name of the job (will be created if doesn't exist)
  p_pipeline_name - Optional name of pipeline to add to the job
  p_description   - Optional description of the job
  p_step_order    - Execution order (default: append to end)

Returns:
  job_id - The ID of the job

Examples:
  -- Create an empty job
  SELECT flow.create_job('daily_etl');
  
  -- Create job with description
  SELECT flow.create_job('daily_etl', null, 'Daily ETL process');
  
  -- Create job and add first pipeline
  SELECT flow.create_job('daily_etl', 'extract_orders', 'Daily ETL process');
  
  -- Add more pipelines to existing job
  SELECT flow.create_job('daily_etl', 'transform_orders');
  SELECT flow.create_job('daily_etl', 'load_orders');
  
  -- Insert at specific position
  SELECT flow.create_job('daily_etl', 'validate_data', null, 2);
$comment$;
