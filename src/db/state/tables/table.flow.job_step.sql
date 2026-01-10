create table if not exists flow.job_step (
    job_step_id      bigserial primary key,
    job_id           bigint not null references flow.job(job_id) on delete cascade,
    pipeline_id      bigint not null references flow.pipeline(pipeline_id) on delete cascade,
    step_order       int not null,
    enabled          boolean default true,
    created_at       timestamptz default now(),
    
    unique (job_id, step_order),
    unique (job_id, pipeline_id)
);

create index if not exists idx_job_step_job_id on flow.job_step(job_id);
create index if not exists idx_job_step_pipeline_id on flow.job_step(pipeline_id);

comment on table flow.job_step is 'Defines which pipelines run in a job and in what order';
comment on column flow.job_step.job_step_id is 'Unique identifier for the job step';
comment on column flow.job_step.job_id is 'Reference to the parent job';
comment on column flow.job_step.pipeline_id is 'Reference to the pipeline to execute';
comment on column flow.job_step.step_order is 'Execution order within the job';
comment on column flow.job_step.enabled is 'Whether this step should run (allows temporary disabling)';
