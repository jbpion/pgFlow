create or replace function flow.list_job_steps(
    p_job_name text
)
returns table(
    step_order int,
    pipeline_name text,
    pipeline_description text,
    enabled boolean
)
language sql
as $body$
    select 
        js.step_order,
        p.pipeline_name,
        p.description as pipeline_description,
        js.enabled
    from flow.job j
    join flow.job_step js on j.job_id = js.job_id
    join flow.pipeline p on js.pipeline_id = p.pipeline_id
    where j.job_name = p_job_name
    order by js.step_order;
$body$;

comment on function flow.list_job_steps(text)
is $comment$@category Jobs: Inspection

List all pipelines in a job in execution order.

Parameters:
  p_job_name - Name of the job

Returns:
- step_order: Execution order
- pipeline_name: Name of the pipeline
- pipeline_description: Pipeline description
- enabled: Whether the step is enabled

Examples:
  -- View all steps in a job
  SELECT * FROM flow.list_job_steps('daily_etl');
  
  -- Count steps in a job
  SELECT count(*) FROM flow.list_job_steps('daily_etl');
$comment$;
