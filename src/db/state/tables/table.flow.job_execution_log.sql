create table if not exists flow.job_execution_log (
    execution_id     bigserial primary key,
    job_id           bigint not null references flow.job(job_id) on delete cascade,
    pipeline_id      bigint references flow.pipeline(pipeline_id) on delete set null,
    pipeline_name    text not null,
    step_order       int not null,
    start_time       timestamptz not null,
    end_time         timestamptz,
    status           text not null check (status in ('running', 'success', 'failed')),
    rows_affected    bigint,
    error_message    text,
    
    created_at       timestamptz default now()
);

create index if not exists idx_job_execution_log_job_id on flow.job_execution_log(job_id);
create index if not exists idx_job_execution_log_start_time on flow.job_execution_log(start_time);
create index if not exists idx_job_execution_log_status on flow.job_execution_log(status);

comment on table flow.job_execution_log is 'Logs each pipeline execution within a job run';
comment on column flow.job_execution_log.execution_id is 'Unique identifier for this execution log entry';
comment on column flow.job_execution_log.job_id is 'Reference to the job';
comment on column flow.job_execution_log.pipeline_id is 'Reference to the pipeline (nullable if pipeline deleted)';
comment on column flow.job_execution_log.pipeline_name is 'Pipeline name at time of execution';
comment on column flow.job_execution_log.step_order is 'Order in which this pipeline ran';
comment on column flow.job_execution_log.start_time is 'When the pipeline started executing';
comment on column flow.job_execution_log.end_time is 'When the pipeline finished executing';
comment on column flow.job_execution_log.status is 'Execution status: running, success, failed';
comment on column flow.job_execution_log.rows_affected is 'Number of rows affected by the pipeline';
comment on column flow.job_execution_log.error_message is 'Error message if execution failed';
