create table if not exists flow.pipeline_step (
    step_id          bigserial primary key,
    pipeline_id      bigint not null references flow.pipeline,
    step_order       int not null,

    step_type        text not null,  -- read_db_object | select
    step_name        text,           -- optional symbolic name

    program_call     text not null,  -- e.g. flow.select('s1', ...)
    step_spec        jsonb not null,  -- canonical AST node

    created_at       timestamptz default now(),

    unique (pipeline_id, step_order)
);
