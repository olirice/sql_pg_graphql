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
$$ language plpgsql;



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


create or replace function gql.requires_subquery(field jsonb) returns bool as
$$ select
    (gql.field_to_name(field) like '%_collection_by_%_to_%')
    or (gql.field_to_name(field) like '%_by_%_to_%');
$$ language sql immutable returns null on null input;

create or replace function gql.requires_array(field jsonb) returns bool as
$$ select gql.field_to_name(field) like '%_collection_by_%_to_%';
$$ language sql immutable returns null on null input;


-- Placeholder
create or replace function gql.sqlize_field(field jsonb, parent_block_name text) returns text as
$$ select null::text;
$$ language sql immutable returns null on null input;


create or replace function gql.sqlize_to_selector_clause(field jsonb) returns text as
$$
	with subfields as (
		select
			coalesce(
				gql.field_to_alias(ar._field),
                gql.field_to_name(ar._field)
			) as field_name,
            gql.sqlize_field(ar._field, gql.field_to_query_block_name(field)) as field_sql,
            gql.field_to_name(ar._field) like '%_collection_by_%' as requires_array
		from
			jsonb_array_elements(gql.field_to_fields(field)) ar(_field)
	)

	select
        'jsonb_build_object(' || string_agg(
                $a$'$a$|| field_name || $a$'$a$ || ', ' || case when requires_array then '( select jsonb_agg(response) from' || field_sql  || 'abc(response) )' else field_sql end,
			    ', '
            ) || ')'
	from
		subfields

$$ language sql immutable;


create or replace function gql.sqlize_to_join_clause(field jsonb, parent_block_name text) returns text as
$$ select
    case
        when gql.requires_subquery(field) then (
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
        selector_clause text := gql.sqlize_to_selector_clause(field);
        requires_subquery bool := gql.requires_subquery(field);
    begin

    if (not requires_subquery and parent_block_name is not null) then
        -- Scalar field
        return field_name;
    -- Table field
    end if;
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
    end;
$BODY$;


create or replace function gql.execute(operation text) returns jsonb as
$$
    declare
        tokens gql.token[] := gql.tokenize_operation(operation);
        ast jsonb := gql.parse_operation(tokens);
        sql_query text := gql.sqlize_field(ast, parent_block_name := null);
    begin
		-- raise notice 'Tokens %s', tokens::text;
		-- raise notice 'AST %s', jsonb_pretty(ast);
		-- raise notice 'SQL %s', sql_query;
        return gql.execute_sql(sql_query)::jsonb;
	end;
$$ language plpgsql;
