create or replace function flow.create_job(
    p_job_name text,
    p_description text default null
)
returns bigint
language plpgsql
as $body$
declare
    v_job_id bigint;
begin
    insert into flow.job (job_name, description)
    values (p_job_name, p_description)
    on conflict (job_name) do update
        set description = excluded.description,
            updated_at = now()
    returning job_id into v_job_id;
    
    return v_job_id;
end;
$body$;

comment on function flow.create_job(text, text)
is $comment$@category Jobs: Management

Create a new job or update an existing job's description.

A job is a collection of pipelines that run in sequence.

Parameters:
  p_job_name    - Unique name for the job
  p_description - Optional description of what the job does

Returns:
  job_id - The ID of the created or updated job

Examples:
  -- Create a new job
  SELECT flow.create_job('daily_etl', 'Daily ETL process for sales data');
  
  -- Update job description
  SELECT flow.create_job('daily_etl', 'Updated: Daily ETL with new sources');
$comment$;
