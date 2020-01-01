/*
******************************
** File: schema.sql
** Name: Oliver Rice
** Date: 2019-12-11
** Desc: Parse a token array into an abstract syntax tree (AST)
******************************

Public API:
    - gql.get_schema(schema_name text) returns jsonb

Usage:
    select gql.get_schema('public')
*/

/*
	GRAPHQL Type Names
*/


-- base
-- edge
-- connection
-- condition
-- input
-- patch
-- entrypoint one
-- entrypoint connection


create or replace function gql.to_base_name(_table_name text) returns text as
$$ select gql.pascal_case(_table_name)
$$ language sql immutable returns null on null input;

create or replace function gql.to_edge_name(_table_name text) returns text as
$$ select gql.pascal_case(_table_name || '_edge')
$$ language sql immutable returns null on null input;

create or replace function gql.to_connection_name(_table_name text) returns text as
$$ select gql.pascal_case(_table_name || '_connection')
$$ language sql immutable returns null on null input;

create or replace function gql.to_entrypoint_one_name(_table_name text) returns text as
$$ select gql.camel_case(_table_name || '_one')
$$ language sql immutable returns null on null input;

create or replace function gql.to_entrypoint_connection_name(_table_name text) returns text as
$$ select gql.camel_case(_table_name || '_connection')
$$ language sql immutable returns null on null input;

create or replace function gql.to_field_name(_column_name text) returns text as
$$ select _column_name
$$ language sql immutable returns null on null input;



create or replace function gql.to_gql_type(_table_schema text, _table_name text, _column_name text) returns text as $$
	/* Assign a concrete graphql data type (non-connection) from a sql datatype e.g. 'int4' -> 'Integer!' */
	select case
		when sql_data_type in ('integer', 'int4', 'smallint', 'serial') then 'Int'
		else 'String'

	end || case when not_null then '!' else '' end gql_data_type
	from
		gql.column_info fm
	where
		fm.table_schema = _table_schema
		and fm.table_name = _table_name
		and fm.column_name = _column_name
$$ language sql stable returns null on null input;


create or replace function gql.to_gql_type(sql_type text, not_null bool) returns text as
$$ select
		case
			when sql_type in ('integer', 'int4', 'smallint', 'serial') then 'Int'
			else 'String'
		end || case
			when not_null then '!' else '' end;
$$ language sql immutable strict;


create or replace function gql.relationship_to_gql_field_name(_constraint_name text) returns text
	language plpgsql as
$$
declare
	field_name text := null;
	rec record := null;
begin
	select *
	from gql.relationship_info ri
	where ri.constraint_name = _constraint_name
	limit 1
	into rec;
	
	field_name := rec.foreign_table;
	field_name := field_name || (select case
		when rec.foreign_cardinality = 'MANY' then '_collection_by_'
		when rec.foreign_cardinality = 'ONE' then '_by_'
		else '_UNREACHABLE_'
	end);

	field_name := field_name || array_to_string(rec.local_columns, '_and_');
	field_name := field_name || '_to_';
	field_name := field_name || array_to_string(rec.foreign_columns, '_and_');	
	return field_name;
end;
$$;

create or replace function gql.relationship_to_gql_field_def(_constraint_name text) returns text
	language plpgsql as
$$
declare
	field_name text := null;
	rec record := null;
begin
	select *
	from gql.relationship_info ri
	where ri.constraint_name = _constraint_name
	limit 1
	into rec;
	
	field_name := gql.relationship_to_gql_field_name(_constraint_name);

    if rec.foreign_cardinality = 'MANY' then
        field_name := field_name || '(first: Int after: Cursor last: Int before: Cursor)';
    end if;
	return field_name;
end;
$$;



create or replace function gql.relationship_to_gql_type(_constraint_name text) returns text
	language plpgsql as
$$
declare
	data_type text := null;
	rec record := null;
	foreign_base_type_name text := null;
begin
	select *
	from gql.relationship_info ri
	where ri.constraint_name = _constraint_name
	limit 1
	into rec;
	foreign_base_type_name := gql.to_base_name(rec.foreign_table);
	return (select case
		when rec.foreign_cardinality = 'MANY' then gql.to_connection_name(rec.foreign_table) || '!'
		when rec.foreign_cardinality = 'ONE' then foreign_base_type_name || '!'
		else 'UNREACHABLE'
	end);
end;
$$;



create or replace function gql.to_base_type(_table_schema text, _table_name text) returns text
	language plpgsql as
$$
declare
	base_type_name text := gql.to_base_name(_table_name);
	column_arr text[] := gql.list_columns(_table_schema, _table_name);
	col_name text := null;
	col_gql_type text := null;
	res text := 'type ' || base_type_name || e' {\n';
	relation_arr text[] := gql.list_relationships(_table_schema, _table_name);
	relation_name text := null;
	relation_field_def text := null;
	relation_gql_type text := null;
begin
	for col_name in select unnest(column_arr) loop
		raise notice 'Column %', col_name;
		
		col_gql_type := gql.to_gql_type(_table_schema, _table_name, col_name);
		
		-- Add column to result type
		res := res || e'\t' || col_name || ': ' || col_gql_type || e'\n';
	end loop;
	
	for relation_name in select unnest(relation_arr) loop
		 relation_field_def := gql.relationship_to_gql_field_def(relation_name);
		 relation_gql_type := gql.relationship_to_gql_type(relation_name);
		 
		 res := res || e'\t' || relation_field_def || ': ' || relation_gql_type || e'\n';
	end loop;
	res := res || '}';	
	return res;
end;
$$;


create or replace function gql.to_edge_type(_table_schema text, _table_name text) returns text
	language plpgsql as
$$
declare
	base_type_name text := gql.to_base_name(_table_name);
	edge_type_name text := gql.to_edge_name(_table_name);
begin
    return format($typedef$
type %s {
    cursor: Cursor
    node: %s
}    
$typedef$, edge_type_name, base_type_name);
end;
$$;

create or replace function gql.to_connection_type(_table_schema text, _table_name text) returns text
	language plpgsql as
$$
declare
	edge_type_name text := gql.to_edge_name(_table_name);
	connection_type_name text := gql.to_connection_name(_table_name);
    -- TODO
	page_info text := null;
begin
    return format($typedef$
type %s {
    edges: [%s!]!
    total_count: Int!
}    
$typedef$, connection_type_name, edge_type_name);
end;
$$;


create or replace function gql.to_entrypoint_one(_table_schema text, _table_name text) returns text as $$
	select
		gql.to_entrypoint_one_name(ti.table_name) || '('
		|| (
				select
					string_agg('  ' || gql.to_field_name(pk._column_name)  || ': ' || gql.to_gql_type(ti.table_schema, ti.table_name, pk._column_name), ', ')
				from
					unnest(ti.pkey_cols) pk(_column_name)
			)
		|| ' ): ' || gql.to_base_name(ti.table_name) || E'\n' as entry_single_row
	from
		gql.table_info ti
	where
		ti.table_schema = _table_schema
		and ti.table_name = _table_name
	group by
		ti.table_schema,
		ti.table_name,
		ti.pkey_cols
$$ language sql stable returns null on null input;


create or replace function gql.to_query(_table_schema text) returns text as $$
	select
		E'type Query {\n'
		|| string_agg('  ' || gql.to_entrypoint_one(ti.table_schema, ti.table_name), E'\n')
		|| E'\n}' as def
	from
		gql.table_info ti
	where
		ti.table_schema = _table_schema
$$ language sql stable returns null on null input;


create or replace function gql.to_schema(_table_schema text) returns text as $$
    
	select
	    e'scalar Cursor\n\n' ||

        string_agg(zzz.def, E'\n')
	from
		(
			select gql.to_base_type(table_schema, table_name) def from gql.table_info where table_schema = _table_schema
			union all
			select gql.to_edge_type(table_schema, table_name) def from gql.table_info where table_schema = _table_schema
			union all
			select gql.to_connection_type(table_schema, table_name) def from gql.table_info where table_schema = _table_schema
			union all
			select gql.to_query(_table_schema)
		 ) zzz
$$ language sql stable returns null on null input;