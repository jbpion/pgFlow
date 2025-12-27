create or replace function flow.list_pipelines()
returns table(
    pipeline_id bigint,
    pipeline_name text,
    description text,
    step_count bigint,
    created_at timestamptz
)
language sql
as $$
    select
        p.pipeline_id,
        p.pipeline_name,
        p.description,
        count(s.step_id) as step_count,
        p.created_at
    from flow.pipeline p
    left join flow.pipeline_step s on s.pipeline_id = p.pipeline_id
    group by p.pipeline_id, p.pipeline_name, p.description, p.created_at
    order by p.created_at desc;
$$;

comment on function flow.list_pipelines()
is $comment$@category Pipeline: Management

List all registered pipelines with metadata.

Returns a table showing all saved pipelines with their step counts and creation timestamps.

Returns:
- pipeline_id: Unique identifier
- pipeline_name: User-assigned name
- description: Optional description
- step_count: Number of steps in pipeline
- created_at: Registration timestamp

Example:
  SELECT * FROM flow.list_pipelines();
$comment$;
