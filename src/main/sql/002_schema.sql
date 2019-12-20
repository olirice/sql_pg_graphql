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

create or replace function gql.pascal_case(entity_name text) returns text as $$
	SELECT replace(initcap(replace(entity_name, '_', ' ')), ' ', '')
$$ language sql immutable returns null on null input;


create or replace function gql.camel_case(entity_name text) returns text as $$
	select lower(substring(pascal_name,1,1)) || substring(pascal_name,2)
	from
		(select gql.pascal_case(entity_name) as pascal_name) pn
$$ language sql immutable returns null on null input;

/*
	GRAPHQL Type Names
*/
create or replace function gql.to_base_type_name(_table_schema text, _table_name text) returns text as
$$ select gql.pascal_case(_table_name)
$$ language sql immutable returns null on null input;


create or replace function gql.to_connection_type_name(_table_schema text, _table_name text) returns text as
$$ select gql.pascal_case(_table_name || '_connection')
$$ language sql immutable returns null on null input;


create or replace function gql.to_edge_type_name(_table_schema text, _table_name text) returns text as
$$ select gql.pascal_case(_table_name || '_edge')
$$ language sql immutable returns null on null input;


create or replace function gql.to_query_single_row(_table_schema text, _table_name text) returns text as
$$ select gql.camel_case(_table_name)
$$ language sql immutable returns null on null input;


create or replace function gql.to_field_name(_table_schema text, _table_name text, _column_name text) returns text as
$$ select _column_name
$$ language sql immutable returns null on null input;


/*
	Table Info
*/
create or replace function gql.to_primary_key_cols(_table_schema text, _table_name text) returns text[] as $$
	select
		array_agg(column_name)::text[]
	from (
		select
			c.table_schema,
			c.table_name,
			c.column_name
		from
			information_schema.table_constraints tc
			join information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
			join information_schema.columns AS c ON c.table_schema = tc.constraint_schema
  			and tc.table_name = c.table_name and ccu.column_name = c.column_name
			where constraint_type = 'PRIMARY KEY'
	) constr
	where
		constr.table_schema = _table_schema
		and constr.table_name = _table_name
$$ language sql stable returns null on null input;


create or replace view gql.table_info as
    select tab.table_schema::text,
        tab.table_name::text,
        gql.to_primary_key_cols(tab.table_schema, tab.table_name) pkey_cols
    from information_schema.tables tab
    where tab.table_schema not in ('pg_catalog','information_schema','gql')
    group by tab.table_schema,
        tab.table_name,
        tab.table_type;

/*
	Column Info
*/
create or replace view gql.column_info as
    select table_schema::text,
        table_name::text,
        column_name::text,
        is_nullable='NO' as not_null,
        data_type::text sql_data_type
    from information_schema.columns
    where table_schema not in ('pg_catalog','information_schema','gql')
    order by table_name,
        ordinal_position;

create type gql.cardinality as enum ('ONE', 'MANY');

create or replace view gql.relationship_info as
    with constraint_cols as (
        select
            table_schema::text,
            table_name::text,
            constraint_name::text,
            array_agg(column_name::text) column_names
        from information_schema.constraint_column_usage
        group by table_schema,
            table_name,
            constraint_name
    ),
	directional as (
        select 
            tc.constraint_name::text,
            tc.table_schema::text,
            tc.table_name::text local_table,
            array_agg(kcu.column_name) local_columns,
            'MANY'::gql.cardinality as local_cardinality,
            ccu.table_name::text as foreign_table,
            ccu.column_names::text[] as foreign_columns,
            'ONE'::gql.cardinality as foreign_cardinality
        from
            information_schema.table_constraints as tc
        join
            information_schema.key_column_usage as kcu
            on tc.constraint_name = kcu.constraint_name
            and tc.table_schema = kcu.table_schema
        join constraint_cols as ccu
            on ccu.constraint_name = tc.constraint_name
            and ccu.table_schema = tc.table_schema
        where
            tc.constraint_type = 'FOREIGN KEY'
        group by
            tc.constraint_name,
            tc.table_schema,
            tc.table_name,
            ccu.table_schema,
            ccu.table_name,
            ccu.column_names
    )
    select *
    from
        directional
    union all
    select
        'reverse_' || constraint_name,
	    table_schema,
	    foreign_table as local_table,
	    foreign_columns as local_columns,
        foreign_cardinality as local_cardinality,
	    local_table as foreign_table,
	    local_columns as foreign_columns,
        local_cardinality as foreign_cardinality
    from
        directional;



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


create or replace function gql.list_tables(_table_schema text) returns text[] as
$$ select array_agg(table_name) from gql.table_info ti where ti.table_schema = _table_schema;
$$ language sql strict;

create or replace function gql.list_columns(_table_schema text, _table_name text) returns text[] as
$$  select
		array_agg(column_name)
	from
		gql.column_info ti
	where
		ti.table_schema = _table_schema
		and ti.table_name = _table_name;
$$ language sql strict;

create or replace function gql.list_primary_key_columns(_table_schema text, _table_name text) returns text[] as
$$  select
		pkey_cols
	from
		gql.table_info ti
	where
		ti.table_schema = _table_schema
		and ti.table_name = _table_name;
$$ language sql strict;

create or replace function gql.list_relationships(_table_schema text, _table_name text) returns text[] as
$$  select
		array_agg(constraint_name)
	from
		gql.relationship_info ti
	where
		ti.table_schema = _table_schema
		and ti.local_table = _table_name;
$$ language sql strict;

create or replace function gql.get_column_sql_type(_table_schema text, _table_name text, _column_name text) returns text as
$$  select
		sql_data_type
	from
		gql.column_info ti
	where
		ti.table_schema = _table_schema
		and ti.table_name = _table_name
		and ti.column_name = _column_name;
$$ language sql strict;


create or replace function gql.to_gql_type(sql_type text, not_null bool) returns text as
$$ select
		case
			when sql_type in ('integer', 'int4', 'smallint', 'serial') then 'Int'
			else 'String'
		end || case
			when not_null then '!' else '' end;
$$ language sql immutable strict;


create or replace function gql.get_column_gql_type(_table_schema text, _table_name text, _column_name text) returns text as
$$  select
		gql.to_gql_type(sql_data_type, not_null)
	from
		gql.column_info ti
	where
		ti.table_schema = _table_schema
		and ti.table_name = _table_name
		and ti.column_name = _column_name;
$$ language sql strict;

create or replace function gql.is_not_null(table_schema text, table_name text, column_name text) returns bool as
$$  select
		not_null
	from
		gql.column_info ti
	where
		ti.table_schema = table_schema
		and ti.table_name = table_name
		and ti.column_name = column_name;
$$ language sql strict;

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
	foreign_base_type_name := gql.table_name_to_base_type_name(rec.foreign_table);
	return (select case
		when rec.foreign_cardinality = 'MANY' then '[' || foreign_base_type_name || '!]'
		when rec.foreign_cardinality = 'ONE' then foreign_base_type_name || '!'
		else 'UNREACHABLE'
	end);
end;
$$;


create or replace function gql.table_name_to_base_type_name(_table_name text) returns text as
$$ select _table_name;
$$ language sql immutable;



create or replace function gql.to_base_type(_table_schema text, _table_name text) returns text
	language plpgsql as
$$
declare
	base_type_name text := _table_name;
	column_arr text[] := gql.list_columns(_table_schema, _table_name);
	col_name text := null;
	col_gql_type text := null;
	res text := 'type ' || base_type_name || e' {\n';
	relation_arr text[] := gql.list_relationships(_table_schema, _table_name);
	relation_name text := null;
	relation_field_name text := null;
	relation_gql_type text := null;
begin
	for col_name in select unnest(column_arr) loop
		raise notice 'Column %', col_name;
		
		col_gql_type := gql.get_column_gql_type(_table_schema, _table_name, col_name);
		
		-- Add column to result type
		res := res || e'\t' || col_name || ': ' || col_gql_type || e'\n';
	end loop;
	
	for relation_name in select unnest(relation_arr) loop
		 relation_field_name := gql.relationship_to_gql_field_name(relation_name);
		 relation_gql_type := gql.relationship_to_gql_type(relation_name);
		 
		 res := res || e'\t' || relation_field_name || ': ' || relation_gql_type || e'\n';
	end loop;
	res := res || '}';	
	return res;
end;
$$;



create or replace function gql.to_entrypoint_one(_table_schema text, _table_name text) returns text as $$
	select
		gql.camel_case(ti.table_name) || '('
		|| (
				select
					string_agg('  ' || gql.to_field_name(ti.table_schema, ti.table_name, pk._column_name)  || ': ' || gql.to_gql_type(ti.table_schema, ti.table_name, pk._column_name), ', ')
				from
					unnest(ti.pkey_cols) pk(_column_name)
			)
		|| ' ): ' || gql.table_name_to_base_type_name(ti.table_name) || E'\n' as entry_single_row
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
		string_agg(zzz.def, E'\n')
	from
		(
			select gql.to_base_type(table_schema, table_name) def from gql.table_info where table_schema = _table_schema
			union all
			select gql.to_query(_table_schema)
		 ) zzz
$$ language sql stable returns null on null input;
