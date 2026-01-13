create table if not exists flow.job (
    job_id           bigserial primary key,
    job_name         text unique not null,
    description      text,
    created_at       timestamptz default now(),
    updated_at       timestamptz default now()
);

comment on table flow.job is 'Stores job definitions - collections of pipelines to run in sequence';
comment on column flow.job.job_id is 'Unique identifier for the job';
comment on column flow.job.job_name is 'Unique name for the job';
comment on column flow.job.description is 'Optional description of what the job does';
