/*
******************************
** File: query_builder.sql
** Name: Oliver Rice
** Date: 2019-12-11
** Desc: Build a SQL query from a parsed GraphQL AST
******************************


Public API:
    - gql.execute_graphql(graphql_query: text) returns jsonb
*/


create or replace function gql.execute_sql (sql_query TEXT) returns setof jsonb as
$$
    begin
	return query execute sql_query;
	end;
$$ language plpgsql stable;



-- Extract Attributes From Selection (AST)
create or replace function gql.field_to_query_block_name(jsonb) returns text as
$$ select 'q' || substring(md5($1::text), 1, 5);
$$ language sql immutable returns null on null input;


create or replace function gql.field_to_name(field jsonb) returns text as
$$ select (field ->> 'name')::text;
$$ language sql immutable returns null on null input;

create or replace function gql.field_to_alias(field jsonb) returns text as
$$ select (field ->> 'alias')::text;
$$ language sql immutable returns null on null input;

create or replace function gql.field_to_args(field jsonb) returns jsonb as
$$ select (field ->> 'args')::jsonb;
$$ language sql immutable returns null on null input;


create or replace function gql.field_to_fields(field jsonb) returns jsonb as
$$ select (field ->> 'fields')::jsonb;
$$ language sql immutable returns null on null input;

-- Convert Selection Attributes to SQL Clauses
create or replace function gql.sqlize_to_filter_clause(field jsonb) returns text as
$$  with kv_pairs as (
		select
			(ar.cont -> 'key')::text as _key, -- column name
			$a$'$a$ || (ar.cont ->> 'value')::text || $b$'$b$ as _value
		from
			jsonb_array_elements(gql.field_to_args(field)) ar(cont)
	)
	select
		coalesce(string_agg(_key || ' = ' || _value, ' and '), 'true')
	from
		kv_pairs;
$$ language sql immutable returns null on null input;

create or replace function gql.sqlize_to_name(field jsonb) returns text as
$$ select split_part(split_part(gql.field_to_name(field), '_collection_by_', 1), '_by_', 1)
$$ language sql immutable returns null on null input;


create or replace function gql.requires_subquery(field jsonb, parent_block_name text) returns bool as
$$ select (gql.field_to_name(field) like '%_by_%_to_%' or parent_block_name is null);
$$ language sql immutable returns null on null input;

create or replace function gql.requires_array(field jsonb) returns bool as
$$ select gql.field_to_name(field) like '%_collection_by_%_to_%';
$$ language sql immutable returns null on null input;


-- Placeholder
create or replace function gql.sqlize_field(field jsonb, parent_block_name text) returns text as
$$ select null::text;
$$ language sql immutable returns null on null input;

create or replace function gql.single_quote(text) returns text as
$$ select $a$'$a$ || $1 || $a$'$a$
$$ language sql immutable strict;


create or replace function gql.sqlize_to_join_clause(field jsonb, parent_block_name text) returns text as
$$ select
    case
        when gql.requires_subquery(field, parent_block_name) then (
            with parts as (
                select
                    gql.sqlize_to_name(field) as local_table_name,
                    string_to_array(split_part(split_part(gql.field_to_name(field), '_by_', 2), '_to_', 1), '_and_')::text[] parent_keys,
                    string_to_array(split_part(split_part(gql.field_to_name(field), '_by_', 2), '_to_', 2), '_and_')::text[] local_keys
            )
            select string_agg((select local_table_name from parts) || '.' || local_key || ' = ' || parent_block_name || '.' || parent_key, ' and ')
            from (
                select
                    unnest((select local_keys from parts)) as local_key,
                    unnest((select parent_keys from parts)) as parent_key
            ) x(local_key, parent_key)
        )
        else 'true'
    end
$$ language sql immutable;



create or replace function gql.sqlize_to_selector_clause(field jsonb, parent_block_name text) returns text
    language plpgsql immutable as
$$
    declare
        subfields jsonb[] := (select array_agg(abc.b) from jsonb_array_elements(gql.field_to_fields(field)) abc(b));
        subfield jsonb;
        subfield_name text;
        response text := 'jsonb_build_object(';
        subfield_sql text;
    begin
    
    for subfield in select unnest(subfields) loop
        subfield_name := coalesce(
            gql.field_to_alias(subfield),
            gql.field_to_name(subfield)
        );
        response := response || gql.single_quote(subfield_name) || ',';
        response := response || gql.sqlize_field(subfield, parent_block_name) || ',';
    end loop;
    
    -- Remove trailing comma
    if subfield is not null then
        response := substring(response, 1, character_length(response)-1);
    end if;
    response := response || ')';

    return response;
    end;
$$;


create or replace function gql.sqlize_pkey_cols_to_cursor(pkey_cols text[]) returns text as 
$$ select '(' || string_agg(x, ',') || ')' from unnest(pkey_cols) ab(x);
$$ language sql immutable;

create or replace function gql.sqlize_connection_field(field jsonb, parent_block_name text) returns text
    language 'plpgsql'
    immutable
as $BODY$
    declare
        block_name text := gql.field_to_query_block_name(field);
        field_name text := gql.field_to_name(field); -- if this is not a subselect, field_name is a valid column_name
        table_name text := gql.sqlize_to_name(field);
        filter_clause text := gql.sqlize_to_filter_clause(field);
        _alias text := gql.field_to_alias(field);
        join_clause text := gql.sqlize_to_join_clause(field, parent_block_name);
        requires_subquery bool := gql.requires_subquery(field, parent_block_name);
        requires_array bool := gql.requires_array(field);
        pkey_cols text[] := gql.to_primary_key_cols('public', table_name);
        cursor_selector text := gql.sqlize_pkey_cols_to_cursor(pkey_cols);
        selector_clause text := null;
        subfields jsonb := null;
        edges jsonb := null;
        node jsonb := null;
    begin

    -- Get Edges Clause
    edges := (select b from jsonb_array_elements(gql.field_to_fields(field)) abc(b) where (b ->> 'name') = 'edges'); 
    raise notice 'Edges %', edges;
    node := (select b from jsonb_array_elements(gql.field_to_fields(edges)) abc(b) where (b ->> 'name') = 'node'); 
    raise notice 'Node %', node;
    selector_clause := gql.sqlize_to_selector_clause(node, block_name);

    return format($query$
            (
                with %s as ( -- block name
                    select
                        *,
                        1 as total_count,
                        %s as cursor
                    from
                        %s -- table name
                    where
                        (%s) -- key clause
                        and (%s) -- join clause
                )
                
                select
                    jsonb_build_object(
                        'total_count', total_count,
                        'edges', json_agg(
                            jsonb_build_object(
                                'cursor', cursor,
                                'node', %s
                            )
                        )
                    ) as response -- selector clause
                from
                   %s -- block name
                group by
                    total_count
            )
        $query$,block_name, cursor_selector, table_name, filter_clause, join_clause, selector_clause, block_name);

    end;
$BODY$;


 

create or replace function gql.sqlize_field(field jsonb, parent_block_name text) returns text
    language 'plpgsql'
    immutable
as $BODY$
    declare
        block_name text := gql.field_to_query_block_name(field);
        field_name text := gql.field_to_name(field); -- if this is not a subselect, field_name is a valid column_name
        table_name text := gql.sqlize_to_name(field);
        filter_clause text := gql.sqlize_to_filter_clause(field);
        _alias text := gql.field_to_alias(field);
        join_clause text := gql.sqlize_to_join_clause(field, parent_block_name);
        selector_clause text := gql.sqlize_to_selector_clause(field, parent_block_name);
        requires_subquery bool := gql.requires_subquery(field, parent_block_name);
        requires_array bool := gql.requires_array(field);
    begin
      
    -- Basic field 
    if not requires_subquery then
        return field_name;
    end if;
    
    if field_name like '%_by_%' then
        selector_clause := gql.sqlize_to_selector_clause(field, block_name);
    else
        selector_clause := gql.sqlize_to_selector_clause(field, block_name);
    end if;

    -- Query 1 (Entrypoint 1) 
    if not requires_array then
        return format($query$
                (
                    with %s as ( -- block name
                        select
                            *
                        from
                            %s -- table name
                        where
                            (%s) -- key clause
                            and (%s) -- join clause
                    )
                    select
                       %s as response -- selector clause
                    from
                       %s -- block name
                )
            $query$,block_name, table_name, filter_clause, join_clause, selector_clause, block_name);
    end if;

    if requires_array then
        return gql.sqlize_connection_field(field, parent_block_name);
    end if;


    return 'UNREACHABLE';

    end;
$BODY$;


create or replace function gql.execute(operation text) returns jsonb as
$$
    declare
        tokens gql.token[] := gql.tokenize_operation(operation);
        ast jsonb := gql.parse_operation(tokens);
        sql_query text := gql.sqlize_field(ast, parent_block_name := null);
    begin
		raise notice 'Tokens %', tokens::text;
		raise notice 'AST %', jsonb_pretty(ast);
		raise notice 'SQL %', sql_query;
        return gql.execute_sql(sql_query)::jsonb;
	end;
$$ language plpgsql stable;
