-- List all functions (no arguments)
create or replace function flow.help()
returns void
language plpgsql
as $body$
declare
    v_rec record;
    v_current_category text := '';
    v_line text;
begin
    raise notice '';
    raise notice '=== pgFlow Function Reference ===';
    raise notice '';
    
    for v_rec in
        with function_info as (
            select
                p.proname::text as func_name,
                pg_catalog.pg_get_function_identity_arguments(p.oid)::text as signature,
                pg_catalog.obj_description(p.oid, 'pg_proc') as full_comment
            from pg_catalog.pg_proc p
            join pg_catalog.pg_namespace n on n.oid = p.pronamespace
            where n.nspname = 'flow'
              and substring(p.proname, 1, 2) != '__'  -- exclude internal functions
              and p.proname != 'help'                 -- exclude help itself
        )
        select
            coalesce(
                nullif(trim(split_part(split_part(fi.full_comment, '@category', 2), E'\n', 1)), ''),
                'Other'
            ) as category,
            fi.func_name as function_name,
            fi.signature,
            coalesce(
                split_part(trim(regexp_replace(fi.full_comment, '@category[^\n]*\n?', '', 'g')), E'\n', 1),
                '(no description)'
            ) as description
        from function_info fi
        order by
            category,
            fi.func_name
    loop
        -- Print category header when it changes
        if v_rec.category != v_current_category then
            if v_current_category != '' then
                raise notice '';
            end if;
            raise notice '--- % ---', v_rec.category;
            v_current_category := v_rec.category;
        end if;
        
        -- Format and print function entry
        if v_rec.signature = '' then
            v_line := format('  flow.%s()', v_rec.function_name);
        else
            v_line := format('  flow.%s(%s)', v_rec.function_name, v_rec.signature);
        end if;
        
        raise notice '%', v_line;
        raise notice '    %', v_rec.description;
    end loop;
    
    raise notice '';
    raise notice 'Use flow.help(''function_name'') for detailed help on a specific function.';
    raise notice '';
end;
$body$;

-- Show detailed help for specific function
create or replace function flow.help(function_name text)
returns void
language plpgsql
as $body$
declare
    v_full_sig text;
    v_description text;
    v_full_comment text;
    v_category text;
    v_return_type text;
begin
    -- Get function details
    select
        pg_catalog.pg_get_function_identity_arguments(p.oid),
        pg_catalog.obj_description(p.oid, 'pg_proc'),
        pg_catalog.pg_get_function_result(p.oid)
    into
        v_full_sig,
        v_full_comment,
        v_return_type
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'flow'
      and p.proname = function_name
    limit 1;

    if v_full_sig is null then
        raise notice 'Function "flow.%" not found.', function_name;
        return;
    end if;

    -- Extract category from @category tag
    v_category := coalesce(
        nullif(trim(split_part(split_part(v_full_comment, '@category', 2), E'\n', 1)), ''),
        'Other'
    );

    -- Remove @category tag from description
    v_description := coalesce(
        trim(regexp_replace(v_full_comment, '@category[^\n]*\n?', '', 'g')),
        '(no description available)'
    );

    -- Print help text
    raise notice '';
    raise notice '=== flow.% ===', function_name;
    raise notice '';
    raise notice 'Category: %', v_category;
    raise notice '';
    raise notice 'Signature:';
    raise notice '  flow.%(%)', function_name, v_full_sig;
    raise notice '';
    raise notice 'Returns: %', v_return_type;
    raise notice '';
    raise notice 'Description:';
    raise notice '%', v_description;
    raise notice '';
end;
$body$;

comment on function flow.help()
is 'List all user-facing pgFlow functions grouped by category. Use flow.help(''function_name'') for detailed help on a specific function.';

comment on function flow.help(text)
is 'Show detailed help for a specific pgFlow function including signature, description, and usage examples.';
