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



create or replace function gql.execute(operation text) returns jsonb as
$$
    declare
        tokens gql.token[] := gql.tokenize_operation(operation);
        ast jsonb := gql.parse_operation(tokens);
        sql_query text := gql.sqlize_field(ast, parent_block_name := null);
    begin
        -- Raising these notices takes about 0.1 milliseconds
		--raise notice 'Tokens %', tokens::text;
		--raise notice 'AST %', jsonb_pretty(ast);
		raise notice 'SQL %', sql_query;
        return gql.execute_sql(sql_query)::jsonb;
	end;
$$ language plpgsql stable;


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
	begin
		for tab_rec in select * from gql.table_info ci where ci.table_schema=table_schema loop
		
			func_def := format(e'
			create or replace function gql.resolve_%s_%s(rec %s.%s, field jsonb)
				returns jsonb
				language sql
			    immutable
				parallel safe
				as
			$$
				select jsonb_build_object(
							   coalesce(field ->> \'alias\', field ->> \'name\'), (
			', tab_rec.table_schema, tab_rec.table_name, tab_rec.table_schema, tab_rec.table_name);
			
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
					clause := format(e'
					case when field #> \'{fields,%s}\' is not null
									 then gql.resolve_%s_%s_%s(rec, field #> \'{fields,%s}\' )
									 else \'{}\'::jsonb
									 end ||',
					relationship_field_name, rel_rec.table_schema, rel_rec.local_table,
					relationship_field_name, relationship_field_name);
					func_def := func_def || clause;
				end loop;
				
				func_def := substring(func_def, 1, character_length(func_def)-3);
				func_def := func_def || e'));$$;';
			raise notice 'Function %', func_def;
			execute func_def;
		end loop;
	end;
$body$;


create or replace function gql.build_resolve_connection_relationships(_table_schema text) returns void
	language plpgsql as
$body$
	declare
		rec record;
		join_clause text;
		func_def text;
		ix int;
	begin
		for rec in select * from gql.relationship_info ci
			where
				ci.table_schema=table_schema
				and ci.foreign_cardinality = 'MANY'
			loop
			join_clause := '';
			for ix in select generate_series(1, array_length(rec.local_columns, 1)) loop
				join_clause := join_clause || format('rec.%s = %s.%s and ',
													 rec.local_columns[ix],
													 rec.foreign_table,
													 rec.foreign_columns[ix]);
			end loop;
			join_clause := substring(join_clause, 1, character_length(join_clause)-4);
			
			func_def := format(e'
create or replace function gql.resolve_%s_%s_%s(rec %s.%s, field jsonb) returns jsonb
	language sql stable as
$$
	select 
		jsonb_build_object(
			coalesce(field ->> \'alias\', field->> \'name\'),
			jsonb_build_object(
				-- TODO: Allow aliases
				\'total_count\', 1,
				\'edges\', jsonb_agg(
					jsonb_build_object(
						\'cursor\', \'my_cursor\'
					) ||
					-- TODO: This returns an object 
					gql.resolve_%s_%s(
						rec:=%s,
						field:=field #> \'{fields,edges,fields,node}\'
					)
				)
			)
		)
	from
		%s.%s
	where
		%s
$$;', rec.table_schema, rec.local_table, gql.relationship_to_gql_field_name(rec.constraint_name), rec.table_schema,
	rec.local_table, rec.table_schema, rec.foreign_table, rec.foreign_table, rec.table_schema, rec.foreign_table,
	join_clause);
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
	begin
		for rec in select * from gql.table_info ci where ci.table_schema=table_schema loop
			func_def := format(e'
create or replace function gql.resolve_%s_%s(rec %s.%s, field jsonb) returns jsonb
	language sql stable as
$$ select null::jsonb;
$$;', rec.table_schema, rec.table_name, rec.table_schema, rec.table_name);
			execute func_def;
		end loop;

		for rec in select * from gql.relationship_info ci where ci.table_schema=table_schema loop
			func_def := format(e'
create or replace function gql.resolve_%s_%s_%s(rec %s.%s, field jsonb) returns jsonb
	language sql stable as
$$ select null::jsonb;
$$;', rec.table_schema, rec.local_table, gql.relationship_to_gql_field_name(rec.constraint_name), rec.table_schema,
	rec.local_table);
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
			create or replace function gql.resolve_entrypoint_one_%s_%s(field jsonb)
				returns jsonb
				language sql
			    immutable
				parallel safe
				as
			$$
				select
					gql.resolve_%s_%s(%s, field) -> coalesce(field->>\'alias\', field->>\'name\')
				from
					%s.%s
				where
					%s
				limit 1;
		   $$;
			', tab_rec.table_schema, tab_rec.table_name,
			   tab_rec.table_schema, tab_rec.table_name, tab_rec.table_name,
			   tab_rec.table_schema, tab_rec.table_name,
			   pkey_clause
			);
			raise notice 'Function %', func_def;
			execute func_def;
		end loop;
	end;
$body$;


create or replace function gql.build_resolve_all(_table_schema text) returns void
	language plpgsql stable as
$$
	begin
		perform gql.build_resolve_stubs(_table_schema);
		perform gql.build_resolve_rows(_table_schema);
		perform gql.build_resolve_connection_relationships(_table_schema);
		perform gql.build_resolve_entrypoint_one(_table_schema);
	end;
$$;

