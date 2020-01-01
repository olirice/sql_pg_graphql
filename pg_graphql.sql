/*
******************************
** File: setup.sql
** Name: Oliver Rice
** Date: 2019-12-11
** Desc: Setup for gql SQL schema
******************************

*/

drop schema if exists gql cascade;

create schema gql;


/*
******************************
** File: utils.sql
** Name: Oliver Rice
** Date: 2019-12-11
** Desc: Utilities
******************************
*/


create or replace function gql.format_sql(text)
returns text as
$$
   DECLARE
      v_ugly_string       ALIAS FOR $1;
      v_beauty            text;
      v_tmp_name          text;
   BEGIN
      -- let us create a unique view name
      v_tmp_name := 'temp_' || md5(v_ugly_string);
      EXECUTE 'CREATE TEMPORARY VIEW ' ||
      v_tmp_name || ' AS ' || v_ugly_string;

      -- the magic happens here
      SELECT pg_get_viewdef(v_tmp_name) INTO v_beauty;

      -- cleanup the temporary object
      EXECUTE 'DROP VIEW ' || v_tmp_name;
      RETURN v_beauty;
   EXCEPTION WHEN OTHERS THEN
      RAISE EXCEPTION 'you have provided an invalid string: % / %',
            sqlstate, sqlerrm;
   END;
$$ language 'plpgsql';


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
    select
        table_schema::text,
        table_name::text,
        column_name::text,
        is_nullable='NO' as not_null,
        data_type::text sql_data_type,
        column_name::text = any(pk.pk_cols) is_pkey,
        ordinal_position
    from
        information_schema.columns,
        lateral gql.to_primary_key_cols(table_schema::text, table_name::text) pk(pk_cols)
    where
        table_schema not in ('pg_catalog','information_schema','gql')
    order by
        table_name,
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




create or replace function gql.pascal_case(entity_name text) returns text as $$
	SELECT replace(initcap(replace(entity_name, '_', ' ')), ' ', '')
$$ language sql immutable returns null on null input;


create or replace function gql.camel_case(entity_name text) returns text as $$
	select lower(substring(pascal_name,1,1)) || substring(pascal_name,2)
	from
		(select gql.pascal_case(entity_name) as pascal_name) pn
$$ language sql immutable returns null on null input;



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


/*
******************************
** File: tokenizer.sql
** Name: Oliver Rice
** Date: 2019-12-11
** Desc: Tokenize a graphql query operation
******************************


select
    gql.tokenize_operation('
        query {
            account(id: 1, name: "Oliver") {
                # accounts have comments!
                id
                name
                createdAt
            }
        }'
    )
*/

create type gql.token_kind as enum (
	'BANG', 'DOLLAR', 'AMP', 'PAREN_L', 'PAREN_R',
	'COLON', 'EQUALS', 'AT', 'BRACKET_L', 'BRACKET_R',
	'COMMA','BRACE_L', 'BRACE_R', 'PIPE', 'SPREAD',
	'NAME', 'INT', 'FLOAT', 'STRING', 'BLOCK_STRING',
	'COMMENT', 'WHITESPACE', 'ERROR'
);

create type gql.token as (
	kind gql.token_kind,
	content text
);


create or replace function gql.tokenize_operation(payload text) returns gql.token[]
    language plpgsql immutable strict parallel safe
as $BODY$
    declare
        tokens gql.token[] := Array[]::gql.token[];
        cur_token gql.token;
        first_char char := null;
        maybe_tok text;
    begin
        loop
            exit when payload = '';
           
            maybe_tok = substring(payload from '^\s+');
            if maybe_tok is not null then
                payload := substring(payload, character_length(maybe_tok)+1, 99999);
                continue;
            end if;

            maybe_tok = substring(payload from '^[_A-Za-z][_0-9A-Za-z]*');
            if maybe_tok is not null then
                payload := substring(payload, character_length(maybe_tok)+1, 99999);
                tokens := tokens || ('NAME', maybe_tok)::gql.token;
                continue;
            end if;

            first_char := substring(payload, 1, 1);
            
            if first_char = '{' then
                payload := substring(payload, 2, 99999);
                tokens := tokens || ('BRACE_L', '{')::gql.token;
                continue;
            end if;

            if first_char = '}' then
                payload := substring(payload, 2, 99999);
                tokens := tokens || ('BRACE_R', '}')::gql.token;
                continue;
            end if;

            if first_char = '(' then
                payload := substring(payload, 2, 99999);
                tokens := tokens || ('PAREN_L', '(')::gql.token;
                continue;
            end if;

            if first_char = ')' then
                payload := substring(payload, 2, 99999);
                tokens := tokens || ('PAREN_R', ')')::gql.token;
                continue;
            end if;

            if first_char = ':' then
                payload := substring(payload, 2, 99999);
                tokens := tokens || ('COLON', ':')::gql.token;
                continue;
            end if;

            maybe_tok = substring(payload from '^""".*?"""');
            if maybe_tok is not null then
                payload := substring(payload, character_length(maybe_tok)+1, 99999);
                tokens := tokens || ('BLOCK_STRING', maybe_tok)::gql.token;
                continue;
            end if;

            maybe_tok = substring(payload from '^".*?"');
            if maybe_tok is not null then
                payload := substring(payload, character_length(maybe_tok)+1, 99999);
                tokens := tokens || ('STRING', maybe_tok)::gql.token;
                continue;
            end if;

            maybe_tok = substring(payload from '^\-?[0-9]+[\.][0-9]+');
            if maybe_tok is not null then
                payload := substring(payload, character_length(maybe_tok)+1, 99999);
                tokens := tokens || ('FLOAT', maybe_tok)::gql.token;
                continue;
            end if;

            maybe_tok = substring(payload from '^\-?[0-9]+');
            if maybe_tok is not null then
                payload := substring(payload, character_length(maybe_tok)+1, 99999);
                tokens := tokens || ('INT', maybe_tok)::gql.token;
                continue;
            end if;

            maybe_tok = substring(payload from '^#[^\u000A\u000D]*');
            if maybe_tok is not null then
                payload := substring(payload, character_length(maybe_tok)+1, 99999);
                continue;
            end if;

            if first_char = ',' then
                payload := substring(payload, 2, 99999);
                continue;
            end if;

            cur_token := coalesce(case
                    when first_char = '[' then ('BRACKET_L', ']')::gql.token
                    when first_char = ']' then ('BRACKET_R', '[')::gql.token
                    when first_char = '!' then ('BANG', '!')::gql.token
                    when first_char = '$' then ('DOLLAR', '$')::gql.token
                    when first_char = '&' then ('AMP', '&')::gql.token
                    when first_char = '=' then ('EQUALS', '=')::gql.token
                    when first_char = '@' then ('AT', '@')::gql.token
                    when first_char = '|' then ('PIPE', '|')::gql.token
                    when substring(payload, 1, 3) = '...' then ('SPREAD', '...')::gql.token
                    else null::gql.token
                end::gql.token,
                ('ERROR', substring(payload from '^.*'))::gql.token
            );

            payload := substring(payload, character_length(cur_token.content)+1, 99999);
            tokens := tokens || cur_token;
        end loop;
        return tokens;
    end;
$BODY$;


comment on function gql.tokenize_operation is $comment$
	Tokenizes a string containing a valid GraphQL operation
	https://graphql.github.io/graphql-spec/June2018/#sec-Language.Operations
	into an array of gql.token

	Example:
		select
			gql.tokenize_operation('
				query {
					account(id: 1) {
					# queries have comments
					id
					name
				}
			')
	Returns:
		Array[('NAME', 'query'), ('BRACE_L', '{'), ('NAME', 'account'), ...]::gql.token[]
$comment$;


/*
******************************
** File: parser.sql
** Name: Oliver Rice
** Date: 2019-12-11
** Desc: Parse a token array into an abstract syntax tree (AST)
******************************

Public API:
    - gql.parse_operation(tokens gql.token[]) returns jsonb


select
	jsonb_pretty(
		gql.parse_operation(
            gql.tokenize_operation('
                query {
                    acct: account(id: 1, name: "Oliver") {
                        id
                        photo(px: 240)
                        created_at
                    }
                }'
            ),
        )
    )
*/

create type gql.partial_parse as (
	contents jsonb,
	remaining gql.token[]
);


create or replace function gql.parse_field(tokens gql.token[]) returns gql.partial_parse
    language plpgsql immutable strict parallel safe
as $BODY$
    declare
        _alias text;
        _name text;
        cur jsonb;
        args jsonb := '{}';
        last_iter_args jsonb := null;
        last_iter_fields jsonb := null;
        fields jsonb := '{}';
        cur_field gql.partial_parse;
    begin
    -- Read Alias
    if (tokens[1].kind, tokens[2].kind) = ('NAME', 'COLON') then
        _alias = tokens[1].content;
        tokens = tokens[3:];
    else
        _alias := null;
    end if;

    -- Read Name
    _name := tokens[1].content;

    raise notice '_name: %', tokens;
    tokens := tokens[2:];

    -- Read Args
    if tokens[1].kind = 'PAREN_L' then
        -- Skip over the PAREN_L
        tokens := tokens[2:];
        loop
            exit when (tokens[1].kind = 'PAREN_R' or args = last_iter_args);

            if (tokens[1].kind, tokens[2].kind) = ('NAME', 'COLON') then
                -- Special handling of string args to strip the double quotes
                if tokens[3].kind = 'STRING' then
                    cur := jsonb_build_object(
                        tokens[1].content,
                        substring(tokens[3].content, 2, character_length(tokens[3].content)-2)
                    );
                -- Any other scalar arg
                else
                    cur := jsonb_build_object(
                        tokens[1].content,
                        tokens[3].content
                    );
                end if;
                tokens := tokens[4:];
            else
                raise notice 'gql.parse_field: invalid state parsing arg with tokens %', tokens;
            end if;

            last_iter_args := args;
            args := args || cur;
        end loop;
        -- Advance past the PAREN_R
        tokens := tokens[2:];
    else
        args := '{}'::jsonb;
    end if;

    -- Read Fields
    if tokens[1].kind = 'BRACE_L' then
        tokens := tokens[2:];
        loop
            exit when (tokens[1].kind = 'BRACE_R' or fields = last_iter_fields);
            cur_field := gql.parse_field(tokens);
            last_iter_fields := fields;
            fields := fields || cur_field.contents;
            tokens := cur_field.remaining;
        end loop;
    else
        fields := '{}'::jsonb;
    end if;

    raise notice '_name: %', _name;

	return (
        select
            (
		        jsonb_build_object(
                    _name, jsonb_build_object(
                    'alias', _alias,
                    'name', _name,
                    'args', args,
                    'fields', fields
                )),
	            tokens
	        )::gql.partial_parse
    );
    end;
$BODY$;

create or replace function gql.parse_operation(tokens gql.token[]) returns jsonb
    language plpgsql immutable strict parallel safe
as $BODY$
    begin
        -- Standard syntax: "query { ..."
        if (tokens[1].kind, tokens[2].kind) = ('NAME', 'BRACE_L') and tokens[1].content = 'query'
            then tokens := tokens[3:];
        end if;

        -- Simplified syntax for single queries: "{ ..."
        if tokens[1].kind = 'BRACE_L'
            then tokens := tokens[2:];
        end if;

        return (select gql.parse_field(tokens)).contents;
    end;
$BODY$;


/*
******************************
** File: resolver.sql
** Name: Oliver Rice
** Date: 2019-12-11
** Desc: Build a SQL query from a parsed GraphQL AST
******************************


Public API:
    - gql.execute(operation: text) returns jsonb
*/

create or replace function gql.to_resolver_name(field_name text) returns text as
$$ select 'gql."resolve_' || field_name || '"'
$$ language sql immutable returns null on null input;


create or replace function gql.build_resolve_rows(_table_schema text) returns void
	language plpgsql as
$body$
	declare
		tab_rec record;
		col_rec record;
		rel_rec record;
		clause text;
		func_def text;
		relationship_field_name text;
        resolver_name text;
	begin
		for tab_rec in select * from gql.table_info ci where ci.table_schema=table_schema loop

            resolver_name := gql.to_resolver_name(gql.to_base_name(tab_rec.table_name));
		
			func_def := format(e'
			create or replace function %s(rec %s.%s, field jsonb)
				returns jsonb
				language plpgsql
			    immutable
				parallel safe
				as
			$$
            begin
				return jsonb_build_object(
							   coalesce(field ->> \'alias\', field ->> \'name\'), (
			', resolver_name, tab_rec.table_schema, tab_rec.table_name);
			
			-- column resolvers
			for col_rec in (
				select *
				from gql.column_info ci
				where
					ci.table_schema=tab_rec.table_schema
					and ci.table_name = tab_rec.table_name)
				loop
					clause := format(e'
					case when field #> \'{fields,%s}\' is not null
									 then jsonb_build_object(
										coalesce(
									 		field #>> \'{fields,%s,alias}\',
									 		field #>> \'{fields,%s,name}\'
										),
										rec.%s
									 )
									 else \'{}\'::jsonb
									 end ||',
					col_rec.column_name, col_rec.column_name, col_rec.column_name, col_rec.column_name
					);
					func_def := func_def || clause;
				end loop;
			
			-- Relationship Resolvers
			for rel_rec in (
				select *
				from gql.relationship_info ci
				where
					ci.table_schema=tab_rec.table_schema
					and ci.local_table = tab_rec.table_name)
				loop
					relationship_field_name := gql.relationship_to_gql_field_name(rel_rec.constraint_name);
                    resolver_name := gql.to_resolver_name(relationship_field_name);
					clause := format(e'
					case when field #> \'{fields,%s}\' is not null
									 then %s(rec, field #> \'{fields,%s}\' )
									 else \'{}\'::jsonb
									 end ||',
					relationship_field_name, resolver_name, relationship_field_name);
					func_def := func_def || clause;
				end loop;
				
				func_def := substring(func_def, 1, character_length(func_def)-3);
                func_def := func_def || e')); end;$$;';
			raise notice 'Function %', func_def;
			execute func_def;
		end loop;
	end;
$body$;



create or replace function gql.build_resolve_stubs(_table_schema text) returns void
	language plpgsql as
$body$
	declare
		rec record;
		func_def text;
		resolver_name text;
	begin
		for rec in select * from gql.table_info ci where ci.table_schema=table_schema loop
            resolver_name := gql.to_resolver_name(gql.to_base_name(rec.table_name));
			func_def := format(e'
create or replace function %s(rec %s.%s, field jsonb) returns jsonb
language plpgsql stable as
$$ begin return null::jsonb; end;
$$;', resolver_name, rec.table_schema, rec.table_name);
			execute func_def;
		end loop;

		for rec in select * from gql.relationship_info ci where ci.table_schema=table_schema loop
            resolver_name := gql.to_resolver_name(gql.relationship_to_gql_field_name(rec.constraint_name));
			func_def := format(e'
create or replace function %s(rec %s.%s, field jsonb) returns jsonb
	language plpgsql stable as
    $$ begin return null::jsonb; end;
$$;', resolver_name, rec.table_schema, rec.local_table);
			execute func_def;
		end loop;
	end;
$body$;


create or replace function gql.build_resolve_entrypoint_one(_table_schema text) returns void
	language plpgsql as
$body$
	declare
		tab_rec record;
		func_def text;
		resolver_name text;
		pkey_col text;
		pkey_clause text := '';
	begin
		for tab_rec in select * from gql.table_info ci where ci.table_schema=table_schema loop
			
			pkey_clause := '';
			for pkey_col in select b from unnest(tab_rec.pkey_cols) r(b) loop
				raise notice 'Note: %', pkey_col;
				pkey_clause := pkey_clause || format(e'%s = (field #>> \'{args,%s}\')::%s and ',
													 pkey_col, pkey_col, gql.get_column_sql_type(_table_schema, tab_rec.table_name, pkey_col));
				raise notice 'Note2: %', pkey_clause;
			end loop;
			pkey_clause := substring(pkey_clause, 1, character_length(pkey_clause) -4);
			raise notice 'Note3: %', pkey_clause;
			
			func_def := format(e'
			create or replace function %s(field jsonb)
				returns jsonb
				language plpgsql
			    immutable
				parallel safe
				as
			$$
            begin
				return
					%s(%s, field) -> coalesce(field->>\'alias\', field->>\'name\')
				from
					%s.%s
				where
					%s
				limit 1;
            end;
		   $$;', gql.to_resolver_name(gql.to_entrypoint_one_name(tab_rec.table_name)),
			   gql.to_resolver_name(gql.to_base_name(tab_rec.table_name)), tab_rec.table_name,
			   tab_rec.table_schema, tab_rec.table_name,
			   pkey_clause
			);
			raise notice 'Function %', func_def;
			execute func_def;
		end loop;
	end;
$body$;


create or replace function gql.build_resolve(_table_schema text) returns void
	language plpgsql as
$body$
	declare
		rec record;
		func_def text;
		field_name text;
		resolver_name text;
	begin
		func_def := '
			create or replace function gql.resolve(field jsonb)
				returns jsonb
				language plpgsql
			    immutable
				parallel safe
				as
			$$
            begin
			return 
				case (select * from jsonb_object_keys(field) limit 1)
			';
		for rec in select * from gql.table_info ci where ci.table_schema=table_schema loop
			field_name := gql.to_entrypoint_one_name(rec.table_name);
			resolver_name := gql.to_resolver_name(field_name);
			func_def := func_def || format(e'
				when \'%s\' then %s(field := field -> \'%s\')',
			field_name, resolver_name, field_name);
		end loop;
			
        func_def := func_def || e'\nelse null end; end;$$;';
		raise notice 'Function %', func_def;
		execute func_def;
	end;
$body$;


create or replace function gql.to_cursor_type_name(_table_name text) returns text
	language sql immutable as
$$ select 'gql.' || _table_name || '_cursor';
$$;



CREATE OR REPLACE FUNCTION gql.build_cursor_types(_table_schema text) RETURNS void
    LANGUAGE 'plpgsql'
AS $BODY$
	declare
		rec record;
        func_def text;
        col_clause text;
	begin
		for rec in select * from gql.table_info ti where ti.table_schema=table_schema loop

            
            select
                string_agg(ci.column_name || ' ' || ci.sql_data_type, ', ' order by ci.ordinal_position)
            from
                gql.column_info ci
            where
                rec.table_schema=ci.table_schema and rec.table_name=ci.table_name
                and ci.is_pkey
            into col_clause;

            func_def := format(e'create type %s as (%s);',
                gql.to_cursor_type_name(rec.table_name), col_clause
            );

			execute func_def;
			
        end loop;
    end;
$BODY$;
			

CREATE OR REPLACE FUNCTION gql.build_to_cursor(_table_schema text) RETURNS void
    LANGUAGE 'plpgsql'
AS $BODY$
	declare
		rec record;
        func_def text;
        col_clause text;
	begin
		for rec in select * from gql.table_info ti where ti.table_schema=table_schema loop

            
            select
                string_agg('rec.' || ci.column_name, ', ' order by ci.ordinal_position)
            from
                gql.column_info ci
            where
                rec.table_schema=ci.table_schema and rec.table_name=ci.table_name
                and ci.is_pkey
            into col_clause;

            func_def := format(e'
create or replace function gql.to_cursor(rec %s.%s) returns %s
language plpgsql immutable
as $$
begin
    return row(%s)::%s; 
end;
$$;',
rec.table_schema, rec.table_name, gql.to_cursor_type_name(rec.table_name),
col_clause, gql.to_cursor_type_name(rec.table_name)
);

			execute func_def;
        end loop;
    end;
$BODY$;
	
	

CREATE OR REPLACE FUNCTION gql.build_resolve_connection_relationships(_table_schema text) RETURNS void
    LANGUAGE 'plpgsql'
AS $BODY$
	declare
		rec record;
		col_rec record;
		tab_rec gql.table_info;
		join_clause text;
		condition_clause text;
		order_clause text;
		cursor_extractor text;
		pagination_clause text;
		cursor_selector text;
		cursor_type_name text;
		func_def text;
		ix int;
		
	begin
		for rec in select * from gql.relationship_info ri
			where
				ri.table_schema=table_schema
				and ri.foreign_cardinality = 'MANY'
			loop
			
			-- Build Join Clause
			join_clause := '';
			for ix in select generate_series(1, array_length(rec.local_columns, 1)) loop
				join_clause := join_clause || format('rec.%s = %s.%s and ',
													 rec.local_columns[ix],
													 rec.foreign_table,
													 rec.foreign_columns[ix]);
			end loop;
			join_clause := substring(join_clause, 1, character_length(join_clause)-4);
			
			
			-- Build Condition Clause
			condition_clause := '';
			for col_rec in select * from gql.column_info ci where ci.table_schema=rec.table_schema and ci.table_name=rec.local_table loop
				condition_clause := format(
					e'and coalesce(%s = (filter_condition ->> \'%s\')::%s, true)\n',
					col_rec.column_name, gql.to_field_name(col_rec.column_name), col_rec.sql_data_type
				);
			end loop;
			
			
			select * from gql.table_info ti where ti.table_schema=rec.table_schema and ti.table_name=rec.foreign_table limit 1 into tab_rec;			
			-- Ordering Clause
			order_clause := (select '(' || string_agg(x, ',') || ')' from unnest(tab_rec.pkey_cols) abc(x));
			
			-- Pagination Clause
			cursor_type_name := gql.to_cursor_type_name(tab_rec.table_name);
			cursor_extractor := format(
				e'(select (%s) from (select (after_cursor::%s)) abc(r))',
				(select string_agg('(abc.r).' || x, ', ') from unnest(tab_rec.pkey_cols) abc(x)),
				cursor_type_name
			);
			pagination_clause := format(e'and coalesce(%s > %s, true)\n', order_clause, cursor_extractor);
			cursor_extractor := format(
				e'(select (%s) from (select (before_cursor::%s)) abc(r))',
				(select string_agg('(abc.r).' || x, ', ') from unnest(tab_rec.pkey_cols) abc(x)),
				gql.to_cursor_type_name(tab_rec.table_name)
			);
			pagination_clause := pagination_clause || format(e'\t\t\t\tand coalesce(%s > %s, true)\n', order_clause, cursor_extractor);
			
			-- Cursor Selector
			cursor_selector := format(e'\ngql.to_cursor(subq_sorted::%s.%s)::text\n', rec.table_schema, rec.foreign_table);
			
			
			func_def := format(e'
create or replace function %s(rec %s.%s, field jsonb) returns jsonb
	language plpgsql stable as
$$
declare
	-- Convenience
	field_name text := coalesce(field ->> \'alias\', field->> \'name\'); 

	-- Pagination
	arg_first int := (field #>> \'{args,first}\')::int;
	arg_last int := (field #>> \'{args,last}\')::int;
	before_cursor %s := (field #>> \'{args,before}\')::%s;
	after_cursor %s :=  (field #>> \'{args,after}\')::%s;

	-- Conditions
	filter_condition jsonb := (field #> \'{args,condition}\');

	-- Selection Set
	has_cursor bool := field #>> \'{fields,edges,fields,cursor}\' is not null;
	has_node bool := field #>> \'{fields,edges,fields,node}\' is not null;
	has_total bool := field #>> \'{fields,total_count}\' is not null;

	edges jsonb := field #> \'{fields,edges}\';
	node jsonb := edges #> \'{fields,node}\';

	cursor_field_name text := coalesce(edges #>> \'{fields,cursor,alias}\', field #>> \'{fields,cursor,name}\');
	edges_field_name text := coalesce(edges ->> \'alias\', edges ->> \'name\');
	node_field_name text := coalesce(edges ->> \'node\', edges ->> \'node\');
	
	total_field_name text := coalesce(field #>> \'{fields,total_count,alias}\', field #>> \'{fields,total_count,name}\');
							   
	total_count bigint;
begin
	-- Compute total
	if has_total is not null then
		total_count := (
			select
				count(*)
			from
				%s.%s
			where
				-- Join Clause
				%s
				-- Conditions
				%s
		);
	end if;

    return ( 
		with subq as (
			select
				*
			from
				%s.%s
			where
				-- Join Clause
				%s
				-- Pagination
				%s
				-- Conditions
				%s
			order by
				case when before_cursor is not null then %s end desc,
				%s asc
			limit
				coalesce(arg_first, arg_last, 20)
		),
		subq_sorted as (
			select * from subq order by %s asc
		)
		select jsonb_build_object(
			field_name,
			( select
                case
                    when total_field_name is not null then jsonb_build_object(total_field_name, total_count)
                    else \'{}\'::jsonb
                end ||
                jsonb_build_object(
				    edges_field_name, jsonb_agg(
                        case when has_cursor then jsonb_build_object(
                            cursor_field_name,
                            %s::text) else \'{}\'::jsonb end ||
                        case when has_node then %s(
                                rec:=subq_sorted,
                                field:=node
                                ) else \'{}\'::jsonb end 
                    )
                )
			)
		)
		from
			subq_sorted
	);
end;
$$;',
			gql.to_resolver_name(gql.relationship_to_gql_field_name(rec.constraint_name)),
			rec.table_schema, rec.local_table,
            cursor_type_name, cursor_type_name, cursor_type_name, cursor_type_name, 
			rec.table_schema, rec.foreign_table,
            join_clause, condition_clause,
			rec.table_schema, rec.foreign_table, join_clause, pagination_clause, condition_clause,
            order_clause, order_clause, order_clause,
			cursor_selector, gql.to_resolver_name(gql.to_base_name(rec.foreign_table))		
			);

			raise notice 'Function %', func_def;
			execute func_def;
		end loop;
	end;
$BODY$;







create or replace function gql.build_resolvers(_table_schema text) returns void
	language plpgsql stable as
$$
	begin
		perform gql.build_resolve_stubs(_table_schema);
		perform gql.build_cursor_types(_table_schema);
		perform gql.build_to_cursor(_table_schema);
		perform gql.build_resolve_connection_relationships(_table_schema);
		perform gql.build_resolve_rows(_table_schema);
		perform gql.build_resolve_entrypoint_one(_table_schema);
		perform gql.build_resolve(_table_schema);
	end;
$$;

create or replace function gql.execute(operation text) returns jsonb as
$$
    declare
        tokens gql.token[] := gql.tokenize_operation(operation);
        ast jsonb := gql.parse_operation(tokens);
        -- sql_query text := gql.sqlize_field(ast, parent_block_name := null);
    begin
        -- Raising these notices takes about 0.1 milliseconds
		--raise notice 'Tokens %', tokens::text;
		raise notice 'AST %', jsonb_pretty(ast);
		--raise notice 'SQL %', sql_query;
        return gql.resolve(ast);
	end;
$$ language plpgsql stable;


