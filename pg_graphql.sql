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
    select tab.table_schema,
        tab.table_name,
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


create or replace view gql.relationship_info as with constraint_cols as
				(-- Aggregate columns for each constraint into an array
 select table_schema::text,
						table_name::text,
						constraint_name::text,
						array_agg(column_name::text) column_names
					from information_schema.constraint_column_usage
					group by table_schema,
						table_name,
						constraint_name),
	directional as
				(-- List foreign keys with their local + foreign columns
 select tc.constraint_name,
						tc.table_schema,
						tc.table_name local_table,
						array_agg(kcu.column_name) local_columns,
						ccu.table_name as foreign_table,
						ccu.column_names as foreign_columns
					from information_schema.table_constraints as tc
					join information_schema.key_column_usage as kcu on tc.constraint_name = kcu.constraint_name
					and tc.table_schema = kcu.table_schema
					join constraint_cols as ccu on ccu.constraint_name = tc.constraint_name
					and ccu.table_schema = tc.table_schema
					where tc.constraint_type = 'FOREIGN KEY'
					group by tc.constraint_name,
						tc.table_schema,
						tc.table_name,
						ccu.table_schema,
						ccu.table_name,
						ccu.column_names)
select *
from directional
union all
select 'reverse_' || constraint_name,
	table_schema,
	foreign_table as local_table,
	foreign_columns as local_columns,
	local_table as foreign_table,
	local_columns as foreign_columns
from directional;




create or replace function gql.to_data_type(_table_schema text, _table_name text, _column_name text) returns text as $$
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


create or replace function gql.to_base_type(_table_schema text, _table_name text) returns text as $$
	select
		'type ' || gql.to_base_type_name(ti.table_schema, ti.table_name) || E' {\n'
		|| string_agg('  ' || gql.to_field_name(ti.table_schema, ti.table_name, ci.column_name) || ': ' || gql.to_data_type(ti.table_schema, ti.table_name, ci.column_name), E'\n')
		|| E'\n}' as def
	from
		gql.table_info ti
		left join gql.column_info ci
		on ti.table_schema = ci.table_schema
			and ti.table_name = ci.table_name
	where
		ti.table_schema = _table_schema
		and ti.table_name = _table_name
	group by
		ti.table_schema,
		ti.table_name
$$ language sql stable returns null on null input;


create or replace function gql.to_entrypoint_one(_table_schema text, _table_name text) returns text as $$
	select
		gql.camel_case(ti.table_name) || '('
		|| (
				select
					string_agg('  ' || gql.to_field_name(ti.table_schema, ti.table_name, pk._column_name)  || ': ' || gql.to_data_type(ti.table_schema, ti.table_name, pk._column_name), ', ')
				from
					unnest(ti.pkey_cols) pk(_column_name)
			)
		|| ' ): ' || gql.to_base_type_name(ti.table_schema, ti.table_name) || E'\n' as entry_single_row
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
    language 'plpgsql'
    immutable
as $BODY$
    declare
        tokens gql.token[] := Array[]::gql.token[];
        cur_token gql.token;
        first_char char := null;
        first_three_chars char(3) := null;
        first_char_code smallint := null;
        maybe_tok text;
    begin
        loop
            exit when payload = '';
            first_char := substring(payload, 1, 1);
            first_char_code := ascii(first_char);
            first_three_chars := substring(payload, 1, 3);
            
            maybe_tok = substring(payload from '^(\s+)');
            if maybe_tok is not null then
                payload := substring(payload, character_length(maybe_tok)+1, 99999);
                -- Whitespace is not a used token
                maybe_tok = substring(payload from '^[_A-Za-z][_0-9A-Za-z]*');
                continue;
            end if;

            maybe_tok = substring(payload from '^[_A-Za-z][_0-9A-Za-z]*');
            if maybe_tok is not null then
                payload := substring(payload, character_length(maybe_tok)+1, 99999);
                tokens := tokens || ('NAME', maybe_tok)::gql.token;
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
                tokens := tokens || ('COMMENT', maybe_tok)::gql.token;
                continue;
            end if;

            cur_token := (
                select coalesce(
                    case
                        when first_char = '{' then ('BRACE_L', '{')::gql.token
                        when first_char = '}' then ('BRACE_R', '}')::gql.token
                        when first_char = '(' then ('PAREN_L', '(')::gql.token
                        when first_char = ')' then ('PAREN_R', ')')::gql.token
                        when first_char = ':' then ('COLON', ':')::gql.token
                        when first_char = '[' then ('BRACKET_L', ']')::gql.token
                        when first_char = ']' then ('BRACKET_R', '[')::gql.token
                        when first_char = '!' then ('BANG', '!')::gql.token
                        when first_char = '$' then ('DOLLAR', '$')::gql.token
                        when first_char = '&' then ('AMP', '&')::gql.token
                        when first_char = '=' then ('EQUALS', '=')::gql.token
                        when first_char = '@' then ('AT', '@')::gql.token
                        when first_char = '|' then ('PIPE', '|')::gql.token
                        when first_char = ',' then ('COMMA', ',')::gql.token
                        when first_three_chars = '...' then ('SPREAD', '...')::gql.token
                        else null::gql.token
                    end::gql.token,
                    ('ERROR', substring(payload from '^.*'))::gql.token
                )
            );
            payload := substring(payload, character_length(cur_token.content)+1, 99999);

			if cur_token.kind not in ('WHITESPACE', 'COMMA', 'COMMENT') then
            	tokens := tokens || cur_token;
			end if;
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
		Array[('NAME', 'query'), ('WHITESPACE', '  '), ('BRACE_L', '{'),
		('WHITESPACE', '  '), ('NAME', 'account'), ...]::gql.token[]
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


create or replace function gql.parse_fields(tokens gql.token[]) returns gql.partial_parse as
$$ select null::gql.partial_parse $$ language sql immutable returns null on null input;


create or replace function gql.parse_field(tokens gql.token[]) returns gql.partial_parse
    language 'plpgsql'
    immutable
as $BODY$
    declare
        _alias text;
        _name text;
        cur jsonb;
        args jsonb := '[]';
        last_iter_args jsonb := null;
        last_iter_fields jsonb := null;
        fields jsonb := '[]';
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
                        'key', tokens[1].content,
                        'value', substring(tokens[3].content, 2, character_length(tokens[3].content)-2)
                    );
                -- Any other scalar arg
                else
                    cur := jsonb_build_object(
                        'key', tokens[1].content,
                        'value', tokens[3].content
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
        args := '[]'::jsonb;
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
        fields := '[]'::jsonb;
    end if;

	return (
        select
            (
		        jsonb_build_object(
                    'alias', _alias,
                    'name', _name,
                    'args', args,
                    'fields', fields
                ),
	            tokens
	        )::gql.partial_parse
    );
    end;
$BODY$;

create or replace function gql.parse_operation(tokens gql.token[]) returns jsonb
    language 'plpgsql'
    immutable
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
** File: graphiql.sql
** Name: Oliver Rice
** Date: 2019-12-11
** Desc: Serve graphiql HTML
******************************

Public API:
    - gql.get_graphiql_html() returns text

Usage:
    select gql.get_graphiql_html()
*/

create or replace function gql.get_graphiql_html() returns text as
$body$
	select
       $$
        <!--
         *  Copyright (c) Facebook, Inc.
         *  All rights reserved.
         *
         *  This source code is licensed under the license found in the
         *  LICENSE file in the root directory of this source tree.
        -->
        <!DOCTYPE html>
        <html>
          <head>
            <style>
              body {
                height: 100%;
                margin: 0;
                width: 100%;
                overflow: hidden;
              }
              #graphiql {
                height: 100vh;
              }
            </style>
            <!--
              This GraphiQL example depends on Promise and fetch, which are available in
              modern browsers, but can be "polyfilled" for older browsers.
              GraphiQL itself depends on React DOM.
              If you do not want to rely on a CDN, you can host these files locally or
              include them directly in your favored resource bunder.
            -->
            <link href="//cdn.jsdelivr.net/npm/graphiql@0.12.0/graphiql.css" rel="stylesheet"/>
            <script src="//cdn.jsdelivr.net/npm/whatwg-fetch@2.0.3/fetch.min.js"></script>
            <script src="//cdn.jsdelivr.net/npm/react@16.2.0/umd/react.production.min.js"></script>
            <script src="//cdn.jsdelivr.net/npm/react-dom@16.2.0/umd/react-dom.production.min.js"></script>
            <script src="//cdn.jsdelivr.net/npm/graphiql@0.12.0/graphiql.min.js"></script>
          </head>
          <body>
            <div id="graphiql">Loading...</div>
            <script>
              /**
               * This GraphiQL example illustrates how to use some of GraphiQL's props
               * in order to enable reading and updating the URL parameters, making
               * link sharing of queries a little bit easier.
               *
               * This is only one example of this kind of feature, GraphiQL exposes
               * various React params to enable interesting integrations.
               */
              // Parse the search string to get url parameters.
              var search = window.location.search;
              var parameters = {};
              search.substr(1).split('&').forEach(function (entry) {
                var eq = entry.indexOf('=');
                if (eq >= 0) {
                  parameters[decodeURIComponent(entry.slice(0, eq))] =
                    decodeURIComponent(entry.slice(eq + 1));
                }
              });
              // if variables was provided, try to format it.
              if (parameters.variables) {
                try {
                  parameters.variables =
                    JSON.stringify(JSON.parse(parameters.variables), null, 2);
                } catch (e) {
                  // Do nothing, we want to display the invalid JSON as a string, rather
                  // than present an error.
                }
              }
              // When the query and variables string is edited, update the URL bar so
              // that it can be easily shared
              function onEditQuery(newQuery) {
                parameters.query = newQuery;
                updateURL();
              }
              function onEditVariables(newVariables) {
                parameters.variables = newVariables;
                updateURL();
              }
              function onEditOperationName(newOperationName) {
                parameters.operationName = newOperationName;
                updateURL();
              }
              function updateURL() {
                var newSearch = '?' + Object.keys(parameters).filter(function (key) {
                  return Boolean(parameters[key]);
                }).map(function (key) {
                  return encodeURIComponent(key) + '=' +
                    encodeURIComponent(parameters[key]);
                }).join('&');
                history.replaceState(null, null, newSearch);
              }
              // Defines a GraphQL fetcher using the fetch API. You're not required to
              // use fetch, and could instead implement graphQLFetcher however you like,
              // as long as it returns a Promise or Observable.
              function graphQLFetcher(graphQLParams) {
                // This example expects a GraphQL server at the path /graphql.
                // Change this to point wherever you host your GraphQL server.
                return fetch({{REQUEST_PATH}}, {
                  method: 'post',
                  headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                  },
                  body: JSON.stringify(graphQLParams),
                  credentials: 'include',
                }).then(function (response) {
                  return response.text();
                }).then(function (responseBody) {
                  try {
                    return JSON.parse(responseBody);
                  } catch (error) {
                    return responseBody;
                  }
                });
              }
              // Render <GraphiQL /> into the body.
              // See the README in the top level of this module to learn more about
              // how you can customize GraphiQL by providing different values or
              // additional child elements.
              ReactDOM.render(
                React.createElement(GraphiQL, {
                  fetcher: graphQLFetcher,
                  query: parameters.query,
                  variables: parameters.variables,
                  operationName: parameters.operationName,
                  onEditQuery: onEditQuery,
                  onEditVariables: onEditVariables,
                  onEditOperationName: onEditOperationName
                }),
                document.getElementById('graphiql')
              );
            </script>
          </body>
        </html>
       $$
$body$ language sql immutable returns null on null input;




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
$$ with res as (select (field ->> 'name')::text)
	select
		case
			when (select * from res) <> 'null' then (select * from res)
			else null
		end
$$ language sql immutable returns null on null input;

create or replace function gql.field_to_alias(field jsonb) returns text as
$$ with res as (select (field ->> 'alias')::text)
	select
		case
			when (select * from res) <> 'null' then (select * from res)
			else null
		end
$$ language sql immutable returns null on null input;

create or replace function gql.field_to_args(field jsonb) returns jsonb as
$$ select jsonb_path_query(field, '$.args');
$$ language sql immutable returns null on null input;


create or replace function gql.field_to_fields(field jsonb) returns jsonb as
$$ select jsonb_path_query(field, '$.fields');
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
    case
        when gql.field_to_name(field) like '%_collection_by_%_to_%' then true
        when gql.field_to_name(field) like '%_by_%_to_%' then true
        else false
    end
$$ language sql immutable returns null on null input;

create or replace function gql.requires_array(field jsonb) returns bool as
$$ select
    case
        when gql.field_to_name(field) like '%_collection_by_%_to_%' then true
        else false
    end
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


create or replace function gql.execute_operation(graphql_query text) returns jsonb as
$$
    declare
        tokens gql.token[] := gql.tokenize_operation(graphql_query);
        ast jsonb := gql.parse_operation(tokens);
        sql_query text := gql.sqlize_field(ast, parent_block_name := null);
    begin
		-- raise notice 'Tokens %s', tokens::text;
		-- raise notice 'AST %s', jsonb_pretty(ast);
		-- raise notice 'SQL %s', sql_query;
        return gql.execute_sql(sql_query)::jsonb;
	end;
$$ language plpgsql;
