create or replace function flow.list_jobs()
returns table(
    job_name text,
    description text,
    step_count bigint,
    created_at timestamptz,
    updated_at timestamptz
)
language sql
as $body$
    select 
        j.job_name,
        j.description,
        count(js.job_step_id) as step_count,
        j.created_at,
        j.updated_at
    from flow.job j
    left join flow.job_step js on j.job_id = js.job_id
    group by j.job_id, j.job_name, j.description, j.created_at, j.updated_at
    order by j.job_name;
$body$;

comment on function flow.list_jobs()
is $comment$@category Jobs: Inspection

List all jobs with their step counts.

Returns:
- job_name: Name of the job
- description: Job description
- step_count: Number of pipelines in the job
- created_at: When job was created
- updated_at: When job was last modified

Examples:
  -- List all jobs
  SELECT * FROM flow.list_jobs();
  
  -- Find jobs with specific patterns
  SELECT * FROM flow.list_jobs()
  WHERE job_name LIKE 'daily%';
$comment$;
