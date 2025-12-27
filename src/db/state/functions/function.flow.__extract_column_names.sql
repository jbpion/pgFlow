create or replace function flow.__extract_column_names(
    select_cols text[]
)
returns text[]
language plpgsql
immutable
as $body$
begin
    -- Extract column names from select expressions
    -- Handles both "expr AS alias" and simple column names
    return (
        select array_agg(
            case
                when sc ~ $re$ AS $re$ then split_part(sc, ' AS ', 2)
                else regexp_replace(sc, '^[^.]+\.', '')  -- Strip table prefix if present
            end
        )
        from unnest(select_cols) sc
    );
end;
$body$;

comment on function flow.__extract_column_names(text[])
is 'Helper function to extract column names from SELECT expression array. Handles "expr AS alias" syntax and strips table prefixes.';
