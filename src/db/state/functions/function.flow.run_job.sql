create or replace function flow.run_job(
    p_job_name text,
    p_variables jsonb default '{}'::jsonb,
    p_stop_on_error boolean default true
)
returns table(
    step_order int,
    pipeline_name text,
    status text,
    start_time timestamptz,
    end_time timestamptz,
    duration interval,
    rows_affected bigint,
    error_message text
)
language plpgsql
as $body$
declare
    v_job_id bigint;
    v_step record;
    v_execution_id bigint;
    v_start_time timestamptz;
    v_end_time timestamptz;
    v_rows_affected bigint;
    v_error_message text;
    v_status text;
begin
    -- Get job_id
    select job_id into v_job_id
    from flow.job
    where job_name = p_job_name;
    
    if v_job_id is null then
        raise exception 'Job "%" not found', p_job_name;
    end if;
    
    raise notice '=================================================================';
    raise notice 'Starting job: %', p_job_name;
    raise notice '=================================================================';
    
    -- Loop through job steps in order
    for v_step in
        select 
            js.step_order,
            p.pipeline_id,
            p.pipeline_name,
            p.compiled_sql
        from flow.job_step js
        join flow.pipeline p on js.pipeline_id = p.pipeline_id
        where js.job_id = v_job_id
          and js.enabled = true
        order by js.step_order
    loop
        v_start_time := clock_timestamp();
        v_rows_affected := null;
        v_error_message := null;
        v_status := 'running';
        
        raise notice '';
        raise notice '-----------------------------------------------------------------';
        raise notice 'Step %: % ', v_step.step_order, v_step.pipeline_name;
        raise notice 'Started: %', v_start_time;
        raise notice '-----------------------------------------------------------------';
        
        -- Insert log entry with running status
        insert into flow.job_execution_log (
            job_id, pipeline_id, pipeline_name, step_order,
            start_time, status
        )
        values (
            v_job_id, v_step.pipeline_id, v_step.pipeline_name, v_step.step_order,
            v_start_time, 'running'
        )
        returning execution_id into v_execution_id;
        
        -- Execute pipeline
        begin
            -- Use flow.run_pipeline to execute the pipeline
            execute format('SELECT * FROM flow.run_pipeline(%L, %L)', v_step.pipeline_name, p_variables);
            
            get diagnostics v_rows_affected = row_count;
            v_status := 'success';
            v_end_time := clock_timestamp();
            
            raise notice 'Completed: %', v_end_time;
            raise notice 'Duration: %', v_end_time - v_start_time;
            if v_rows_affected is not null then
                raise notice 'Rows affected: %', v_rows_affected;
            end if;
            
        exception
            when others then
                v_status := 'failed';
                v_end_time := clock_timestamp();
                v_error_message := SQLERRM;
                
                raise notice 'FAILED: %', v_end_time;
                raise notice 'Duration: %', v_end_time - v_start_time;
                raise notice 'Error: %', v_error_message;
                
                -- Update log with error
                update flow.job_execution_log
                set status = v_status,
                    end_time = v_end_time,
                    rows_affected = v_rows_affected,
                    error_message = v_error_message
                where execution_id = v_execution_id;
                
                -- Return current step result
                return query select 
                    v_step.step_order,
                    v_step.pipeline_name,
                    v_status,
                    v_start_time,
                    v_end_time,
                    v_end_time - v_start_time as duration,
                    v_rows_affected,
                    v_error_message;
                
                if p_stop_on_error then
                    raise notice '';
                    raise notice '=================================================================';
                    raise notice 'Job FAILED: % (stopped on error)', p_job_name;
                    raise notice '=================================================================';
                    return;
                else
                    continue;
                end if;
        end;
        
        -- Update log with success
        update flow.job_execution_log
        set status = v_status,
            end_time = v_end_time,
            rows_affected = v_rows_affected
        where execution_id = v_execution_id;
        
        -- Return step result
        return query select 
            v_step.step_order,
            v_step.pipeline_name,
            v_status,
            v_start_time,
            v_end_time,
            v_end_time - v_start_time as duration,
            v_rows_affected,
            v_error_message;
    end loop;
    
    raise notice '';
    raise notice '=================================================================';
    raise notice 'Job completed successfully: %', p_job_name;
    raise notice '=================================================================';
end;
$body$;

comment on function flow.run_job(text, jsonb, boolean)
is $comment$@category Jobs: Execution

Execute all pipelines in a job in sequence order.

Logs execution details including start time, end time, duration, and rows affected.
Each execution is recorded in flow.job_execution_log.

Parameters:
  p_job_name      - Name of the job to execute
  p_variables     - JSONB object with variables to pass to all pipelines
  p_stop_on_error - Stop execution if any pipeline fails (default: true)

Returns table with:
- step_order: Execution order
- pipeline_name: Name of the pipeline
- status: 'running', 'success', or 'failed'
- start_time: When pipeline started
- end_time: When pipeline finished
- duration: How long it took
- rows_affected: Number of rows affected
- error_message: Error if failed

Examples:
  -- Run a job
  SELECT * FROM flow.run_job('daily_etl');
  
  -- Run with variables
  SELECT * FROM flow.run_job(
      'monthly_report',
      jsonb_build_object('report_date', '2025-01-01')
  );
  
  -- Continue on error
  SELECT * FROM flow.run_job('data_cleanup', '{}'::jsonb, false);
  
  -- View execution history
  SELECT 
      pipeline_name,
      status,
      start_time,
      duration,
      rows_affected
  FROM flow.job_execution_log
  WHERE job_id = (SELECT job_id FROM flow.job WHERE job_name = 'daily_etl')
  ORDER BY start_time DESC
  LIMIT 10;
$comment$;
