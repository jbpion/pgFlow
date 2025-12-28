create table if not exists flow.pipeline (
    pipeline_id      bigserial primary key,
    pipeline_name    text unique not null,
    description      text,
    compiled_sql     text,           -- final compiled SQL for the entire pipeline
    variables        jsonb,          -- extracted variables from pipeline (for documentation)
    version          text,           -- pipeline version for tracking changes
    created_at       timestamptz default now(),
    updated_at       timestamptz default now()
);
