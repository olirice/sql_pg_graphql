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


