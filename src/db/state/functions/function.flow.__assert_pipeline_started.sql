CREATE OR REPLACE FUNCTION flow.__assert_pipeline_started()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_exists boolean;
BEGIN
    -- Does the session steps temp table exist?
    SELECT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class c
        WHERE c.relname = '__session_steps'
          AND c.relpersistence = 't'
    )
    INTO v_exists;

    IF NOT v_exists THEN
        RAISE EXCEPTION
            'No active pipeline. Call flow.read_*() before adding transformations.';
    END IF;

    -- Does it contain at least one step?
    IF NOT EXISTS (SELECT 1 FROM pg_temp.__session_steps) THEN
        RAISE EXCEPTION
            'Pipeline has not been initialized. Call flow.read_*() first.';
    END IF;
END;
$$;
