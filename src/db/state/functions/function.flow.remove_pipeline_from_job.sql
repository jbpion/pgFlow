create or replace function flow.remove_pipeline_from_job(
    p_job_name text,
    p_pipeline_name text
)
returns void
language plpgsql
as $body$
declare
    v_job_id bigint;
    v_pipeline_id bigint;
begin
    -- Get job_id
    select job_id into v_job_id
    from flow.job
    where job_name = p_job_name;
    
    if v_job_id is null then
        raise exception 'Job "%" not found', p_job_name;
    end if;
    
    -- Get pipeline_id
    select pipeline_id into v_pipeline_id
    from flow.pipeline
    where pipeline_name = p_pipeline_name;
    
    if v_pipeline_id is null then
        raise exception 'Pipeline "%" not found', p_pipeline_name;
    end if;
    
    -- Delete job step
    delete from flow.job_step
    where job_id = v_job_id
      and pipeline_id = v_pipeline_id;
    
    -- Update job timestamp
    update flow.job
    set updated_at = now()
    where job_id = v_job_id;
end;
$body$;

comment on function flow.remove_pipeline_from_job(text, text)
is $comment$@category Jobs: Management

Remove a pipeline from a job.

Parameters:
  p_job_name      - Name of the job
  p_pipeline_name - Name of the pipeline to remove

Examples:
  -- Remove a pipeline from a job
  SELECT flow.remove_pipeline_from_job('daily_etl', 'obsolete_step');
$comment$;
