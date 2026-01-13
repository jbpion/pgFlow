create or replace function flow.export_pipeline_code(
    p_pipeline_name text,
    p_include_register boolean default true
)
returns text
language plpgsql
as $body$
declare
    v_pipeline record;
    v_step record;
    v_output text := '';
    v_step_count int := 0;
begin
    -- Get pipeline metadata
    select * into v_pipeline
    from flow.pipeline
    where pipeline_name = p_pipeline_name;
    
    if not found then
        raise exception 'Pipeline "%" not found', p_pipeline_name;
    end if;
    
    -- Build header comment
    v_output := format($template$-- ============================================================================
-- Pipeline Code Export: %s
-- Description: %s
-- Version: %s
-- Exported: %s
-- ============================================================================
-- This code recreates the pipeline from scratch using flow functions.
-- Copy and modify as needed, then execute to create a new pipeline.
-- ============================================================================

$template$,
        p_pipeline_name,
        coalesce(v_pipeline.description, 'No description'),
        coalesce(v_pipeline.version, '1.0.0'),
        to_char(current_timestamp, 'YYYY-MM-DD HH24:MI:SS')
    );
    
    -- Add variable documentation if present
    if v_pipeline.variables is not null and v_pipeline.variables::text != 'null' and v_pipeline.variables::text != '{}' then
        v_output := v_output || format($template$-- Variables used in this pipeline:
-- %s
--
$template$,
            jsonb_pretty(v_pipeline.variables)
        );
    end if;
    
    -- Add each step's program call
    for v_step in
        select 
            step_order,
            step_type,
            step_name,
            program_call
        from flow.pipeline_step
        where pipeline_id = v_pipeline.pipeline_id
        order by step_order
    loop
        v_step_count := v_step_count + 1;
        
        -- Add step comment
        v_output := v_output || format($template$-- Step %s: %s (%s)
SELECT %s;

$template$,
            v_step.step_order,
            coalesce(v_step.step_name, 'unnamed'),
            v_step.step_type,
            v_step.program_call
        );
    end loop;
    
    -- Add compilation check
    v_output := v_output || format($template$-- ============================================================================
-- Verify Compilation
-- ============================================================================
SELECT flow.compile();

$template$);
    
    -- Optionally add register call
    if p_include_register then
        v_output := v_output || format($template$-- ============================================================================
-- Register Pipeline
-- ============================================================================
-- Modify the pipeline name and description as needed
SELECT flow.register_pipeline(
    %L,  -- pipeline_name
    %L,  -- description
    'replace',       -- mode: 'replace', 'append', 'error'
    %L               -- version
);

$template$,
            p_pipeline_name || '_copy',
            coalesce(v_pipeline.description, 'Copy of ' || p_pipeline_name),
            coalesce(v_pipeline.version, '1.0.0')
        );
    end if;
    
    -- Add footer with helpful notes
    v_output := v_output || format($template$-- ============================================================================
-- Execution
-- ============================================================================
-- After registering, execute with:
-- SELECT * FROM flow.run_pipeline('%s');
--
-- Or with variables:
-- SELECT * FROM flow.run_pipeline(
--     '%s',
--     jsonb_build_object('var_name', 'var_value')
-- );

$template$,
        p_pipeline_name || '_copy',
        p_pipeline_name || '_copy'
    );
    
    -- Print the output to console
    raise notice '%', v_output;
    
    return v_output;
end;
$body$;

comment on function flow.export_pipeline_code(text, boolean)
is $comment$@category Pipeline: Export

Export a pipeline as executable flow function calls that can be modified and re-run.

This is useful for:
- Creating a starting point for a new similar pipeline
- Modifying an existing pipeline's logic
- Understanding how a pipeline was built
- Version control and code review of pipeline definitions

Unlike flow.export_pipeline() which exports DDL/DML for deployment,
this exports the original flow function calls used to build the pipeline.

Parameters:
  p_pipeline_name     - Name of the pipeline to export
  p_include_register  - Include flow.register_pipeline() call (default: true)

Returns:
  Formatted SQL code with SELECT flow.function() calls

Examples:
  -- Export pipeline code
  SELECT flow.export_pipeline_code('daily_sales_summary');
  
  -- Export without register call (just the pipeline steps)
  SELECT flow.export_pipeline_code('daily_sales_summary', false);
  
  -- Copy output to a new file, modify, and execute
  \o /tmp/my_pipeline.sql
  SELECT flow.export_pipeline_code('daily_sales_summary');
  \o
  
  -- Edit /tmp/my_pipeline.sql, then:
  \i /tmp/my_pipeline.sql

Workflow:
  1. Export existing pipeline code
  2. Copy to new file
  3. Modify pipeline name, description, and logic as needed
  4. Execute to create new pipeline
  5. Test with flow.compile() and flow.run_pipeline()
$comment$;
