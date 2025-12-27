create table if not exists flow.pipeline (
    pipeline_id      bigserial primary key,
    pipeline_name    text unique not null,
    description      text,
    created_at       timestamptz default now()
);
