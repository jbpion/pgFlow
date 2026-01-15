create or replace function flow.export_pipeline(
    p_pipeline_name text,
    p_version text default '1.0.0'
)
returns text
language plpgsql
as $body$
declare
    v_pipeline record;
    v_step record;
    v_sql text;
    v_steps_sql text := '';
    v_step_count int := 0;
begin
    -- Get pipeline metadata
    select * into v_pipeline
    from flow.pipeline
    where pipeline_name = p_pipeline_name;
    
    if not found then
        raise exception 'Pipeline not found: %', p_pipeline_name;
    end if;
    
    -- Build header
    v_sql := format($template$-- ============================================================================
-- pgFlow Pipeline Export
-- Pipeline: %s
-- Version: %s
-- Exported: %s
-- Description: %s
-- Variables: %s
-- ============================================================================
-- This script safely upserts the pipeline definition.
-- Safe to run multiple times (idempotent).
-- ============================================================================

BEGIN;

-- Upsert pipeline metadata
INSERT INTO flow.pipeline (
    pipeline_name,
    description,
    compiled_sql,
    variables,
    version,
    created_at,
    updated_at
)
VALUES (
    %L,
    %L,
    %L,
    %L::jsonb,
    %L,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
)
ON CONFLICT (pipeline_name) 
DO UPDATE SET
    description = EXCLUDED.description,
    compiled_sql = EXCLUDED.compiled_sql,
    variables = EXCLUDED.variables,
    version = EXCLUDED.version,
    updated_at = CURRENT_TIMESTAMP;

-- Delete existing steps (will be replaced)
DELETE FROM flow.pipeline_step
WHERE pipeline_id = (SELECT pipeline_id FROM flow.pipeline WHERE pipeline_name = %L);

$template$,
        p_pipeline_name,
        coalesce(v_pipeline.version, p_version),
        to_char(current_timestamp, 'YYYY-MM-DD HH24:MI:SS'),
        coalesce(v_pipeline.description, ''),
        coalesce(v_pipeline.variables::text, 'null'),
        p_pipeline_name,
        v_pipeline.description,
        v_pipeline.compiled_sql,
        coalesce(v_pipeline.variables::text, 'null'),
        coalesce(v_pipeline.version, p_version),
        p_pipeline_name
    );
    
    -- Build steps
    for v_step in
        select *
        from flow.pipeline_step
        where pipeline_id = v_pipeline.pipeline_id
        order by step_order
    loop
        v_step_count := v_step_count + 1;
        
        v_steps_sql := v_steps_sql || format($template$
-- Step %s: %s (%s)
INSERT INTO flow.pipeline_step (
    pipeline_id,
    step_order,
    step_type,
    step_name,
    program_call,
    step_spec
)
VALUES (
    (SELECT pipeline_id FROM flow.pipeline WHERE pipeline_name = %L),
    %s,
    %L,
    %L,
    %L,
    %L::jsonb
);

$template$,
            v_step.step_order,
            coalesce(v_step.step_name, 'unnamed'),
            v_step.step_type,
            p_pipeline_name,
            v_step.step_order,
            v_step.step_type,
            v_step.step_name,
            v_step.program_call,
            v_step.step_spec::text
        );
    end loop;
    
    v_sql := v_sql || v_steps_sql;
    
    -- Add footer
    v_sql := v_sql || format($template$
-- ============================================================================
-- Pipeline export complete
-- Steps: %s
-- ============================================================================

COMMIT;

-- Verify deployment
SELECT 
    'Pipeline deployed: ' || pipeline_name || ' v' || coalesce(version, '(no version)') || ' (' || 
    (SELECT count(*) FROM flow.pipeline_step WHERE pipeline_id = p.pipeline_id) || ' steps)' as status
FROM flow.pipeline p
WHERE pipeline_name = %L;

$template$,
        v_step_count,
        p_pipeline_name
    );
    
    -- Print the exported SQL to the console
    raise notice '%', v_sql;
    
    return v_sql;
end;
$body$;

comment on function flow.export_pipeline(text, text) is 
'@category Pipeline: Export

Export a registered pipeline as deployable SQL script. Returns SQL that safely upserts the pipeline definition with version tracking. Safe to include in migration scripts. Also prints the SQL to the console via RAISE NOTICE.';
