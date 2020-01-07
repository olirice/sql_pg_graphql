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
        cursor_selector text;
        column_selector text;
        relationship_selector text;
	begin
		for tab_rec in select * from gql.table_info ci where ci.table_schema=table_schema loop

			cursor_selector := gql.record_to_cursor_select_clause(tab_rec.table_schema, tab_rec.table_name, 'rec');

            -- column resolvers
            column_selector := '';
			for col_rec in (
				select *
				from gql.column_info ci
				where
					ci.table_schema=tab_rec.table_schema
					and ci.table_name = tab_rec.table_name)
				loop
					clause := format(e'
            || case when field #> \'{fields,%s}\' is not null
                 then jsonb_build_object(
                    coalesce(
                        field #>> \'{fields,%s,alias}\',
                        field #>> \'{fields,%s,name}\'
                    ),
                    rec.%s
                 )
                 else \'{}\'::jsonb
                     end',
					col_rec.column_name, col_rec.column_name, col_rec.column_name, col_rec.column_name
					);
					column_selector := column_selector || clause;
				end loop;
			
			-- Relationship Resolvers
            relationship_selector := '';
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
            || case when field #> \'{fields,%s}\' is not null
									 then %s(rec, field #> \'{fields,%s}\' )
									 else \'{}\'::jsonb
									 end',
					relationship_field_name, resolver_name, relationship_field_name);
					relationship_selector := relationship_selector || clause;
				end loop;
				

		
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
                        -- NodeId
                        case when field #> \'{fields,nodeId}\' is not null
                             then jsonb_build_object(
                                coalesce(
                                    field #>> \'{fields,nodeId,alias}\',
                                    field #>> \'{fields,nodeId,name}\'
                                ),
                                %s
                             )
                             else \'{}\'::jsonb
                             end
%s
%s
                   )
            );
end;
$$;',
                gql.to_resolver_name(gql.to_base_name(tab_rec.table_name)), tab_rec.table_schema, tab_rec.table_name,
                cursor_selector, column_selector, relationship_selector
            );
            raise notice 'Function %', func_def;
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
		cursor_arg_parsed text;
        cursor_selector text;
	begin
		for tab_rec in select * from gql.table_info ci where ci.table_schema=table_schema loop
			
            cursor_arg_parsed := gql.text_to_cursor_clause(
                tab_rec.table_schema,
                tab_rec.table_name,
                e'(field #>> \'{args,nodeId}\')'
            );

            cursor_selector := gql.to_cursor_clause(
                tab_rec.table_schema,
                tab_rec.table_name,
                'tab',
                as_row := false
            );


			
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
					%s(tab, field) -> coalesce(field->>\'alias\', field->>\'name\')
				from
					%s.%s tab
				where
					%s = %s
				limit 1;
            end;
		   $$;', gql.to_resolver_name(gql.to_entrypoint_one_name(tab_rec.table_name)),
			   gql.to_resolver_name(gql.to_base_name(tab_rec.table_name)),
			   tab_rec.table_schema, tab_rec.table_name,
			   cursor_selector, cursor_arg_parsed

			);
			--raise notice 'Function %', func_def;
			execute func_def;
		end loop;
	end;
$body$;

create or replace function gql.build_resolve_entrypoint_connection(_table_schema text) returns void
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

			
			func_def := format(e'
			create or replace function %s(field jsonb)
				returns jsonb
				language plpgsql
			    immutable
				parallel safe
				as
			$$
            begin
				return %s(field) -> coalesce(field->>\'alias\', field->>\'name\');
            end;
		   $$;', gql.to_resolver_name(gql.to_entrypoint_connection_name(tab_rec.table_name)),
			   gql.to_resolver_name(gql.to_connection_name(tab_rec.table_name))
            );
			--raise notice 'Function %', func_def;
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
            declare
                val_to_match text := (select * from jsonb_object_keys(field) limit 1);
            begin
			return 
				case 
			';
		for rec in select * from gql.table_info ci where ci.table_schema=table_schema loop
			field_name := gql.to_entrypoint_one_name(rec.table_name);
			resolver_name := gql.to_resolver_name(field_name);
			func_def := func_def || format(e'
				when val_to_match = \'%s\' then %s(field := field -> \'%s\')',
			field_name, resolver_name, field_name);

            field_name := gql.to_entrypoint_connection_name(rec.table_name);
			resolver_name := gql.to_resolver_name(field_name);
			func_def := func_def || format(e'
				when val_to_match = \'%s\' then %s(field := field -> \'%s\')',
			field_name, resolver_name, field_name);

		end loop;
			
        func_def := func_def || e'\nelse \'{"error": "no resolver matched"}\'::jsonb end; end;$$;';
		--raise notice 'Function %', func_def;
		execute func_def;
	end;
$body$;




CREATE OR REPLACE FUNCTION gql.build_resolve_connections(_table_schema text) RETURNS void
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
		start_cursor_selector text;
        end_cursor_selector text;
		cursor_type_name text;
		func_def text;
		resolver_function_args text;
		ix int;
        template text;
		
	begin


            template := e'
create or replace function %s(%s) returns jsonb
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
    row_limit int := least(coalesce(arg_first, arg_last), 20);

	-- Conditions
	filter_condition jsonb := (field #> \'{args,condition}\');

	-- Selection Set
	has_cursor bool := field #>> \'{fields,edges,fields,cursor}\' is not null;
	has_node bool := field #>> \'{fields,edges,fields,node}\' is not null;
	has_total bool := field #>> \'{fields,total_count}\' is not null;
	has_edges bool := field #>> \'{fields,edges}\' is not null;
	has_page_info bool := field #>> \'{fields,pageInfo}\' is not null;
    has_next_page bool := field #>> \'{fields,pageInfo,fields,hasNextPage}\' is not null;
    has_prev_page bool := field #>> \'{fields,pageInfo,fields,hasNextPage}\' is not null;
    has_start_cursor bool := field #>> \'{fields,pageInfo,fields,startCursor}\' is not null;
    has_end_cursor bool := field #>> \'{fields,pageInfo,fields,endCursor}\' is not null;

	edges jsonb := field #> \'{fields,edges}\';
	node jsonb := edges #> \'{fields,node}\';

	page_info_field_name text := coalesce(field #>> \'{fields,pageInfo,alias}\', field #>> \'{fields,pageInfo,name}\');
	cursor_field_name text := coalesce(edges #>> \'{fields,cursor,alias}\', edges #>> \'{fields,cursor,name}\');
	edges_field_name text := coalesce(edges ->> \'alias\', edges ->> \'name\');
	node_field_name text := coalesce(edges #>> \'{fields,node,alias}\', edges #>> \'{fields,node,name}\');
	next_page_field_name text := coalesce(field #>> \'{fields,pageInfo,fields,hasNextPage,alias}\', \'hasNextPage\');
	previous_page_field_name text := coalesce(field #>> \'{fields,pageInfo,fields,hasPreviousPage,alias}\', \'hasPreviousPage\');
    start_cursor_field_name text := coalesce(field #>> \'{fields,pageInfo,fields,startCursor,alias}\', \'startCursor\');
    end_cursor_field_name text := coalesce(field #>> \'{fields,pageInfo,fields,endCursor,alias}\', \'endCursor\');
	total_field_name text := coalesce(field #>> \'{fields,total_count,alias}\', field #>> \'{fields,total_count,name}\');
							   
begin
	-- Compute total
    return ( 
        with total as (
            select
				count(*) total_count
			from
				%s.%s
			where
				-- Join Clause
				%s
				-- Conditions
				%s
                -- Skip if not requested
                and has_total
        ),
        -- Query for rows and apply pagination
		subq as (
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
                -- Retrieve extra row to check if there is another page
				row_limit + 1
		),
        
        -- Ensure deterministic sort order
		subq_sorted as (
			select * from subq order by %s asc limit row_limit
		),
        -- Check if has next page
        has_next as (
            select (select count(*) from subq) > row_limit as val
        ),
        -- Check if has previous page
        has_previous as (
            select 
                case
                    -- If a cursor is provided, that row appears on the previous page
                    when coalesce(before_cursor, after_cursor) is null then true
                    -- If no cursor is provided, no previous page
                    else false
                end val
        ),
        -- Page Info Cursors
        start_cursor as (
            select
                %s::text as val
            from 
                subq_sorted
            order by
                %s asc
            limit 1
        ),
        end_cursor as (
            select
                %s::text as val
            from 
                subq_sorted
            order by
                %s desc
            limit 1
        )
        -- Build result
		select jsonb_build_object(
			field_name,
			( select
                case
                    when has_edges then jsonb_build_object(
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
                    else \'{}\'::jsonb
                end ||
                case
                    when has_total is not null then jsonb_build_object(total_field_name, (select total_count from total))
                    else \'{}\'::jsonb
                end ||
                case
                    when has_page_info then jsonb_build_object(
                        page_info_field_name, ( 
                            case
                                when has_next_page then jsonb_build_object(
                                    next_page_field_name, (select val from has_next)
                                ) 
                                else \'{}\'::jsonb
                            end ||
                            case
                                when has_prev_page then jsonb_build_object(
                                    previous_page_field_name, (select val from has_previous)
                                )
                            else \'{}\'::jsonb
                            end ||
                            case
                                when has_start_cursor then jsonb_build_object(
                                    start_cursor_field_name, (select val from start_cursor)
                                ) else \'{}\'::jsonb
                            end ||
                            case
                                when has_end_cursor then jsonb_build_object(
                                    end_cursor_field_name, (select val from end_cursor)
                                )  else \'{}\'::jsonb
                            end 
                        )
                    ) 
                    else \'{}\'::jsonb
                end

			)
		)
		from
			subq_sorted
	);
end;
$$;';


		for tab_rec in (select * from gql.table_info ti where ti.table_schema=_table_schema) loop


            -- Condition Clause
            condition_clause := '';
            for col_rec in (
                select *
                from gql.column_info ci
                where ci.table_schema=tab_rec.table_schema
                    and ci.table_name=tab_rec.table_name
                ) loop

                condition_clause := condition_clause || format(
                    e'and coalesce(%s = (filter_condition ->> \'%s\')::%s, true)\n',
                    col_rec.column_name, gql.to_field_name(col_rec.column_name), col_rec.sql_data_type
                );
            end loop;
            
            
            -- Ordering Clause
            order_clause := (select '(' || string_agg(x, ',') || ')' from unnest(tab_rec.pkey_cols) abc(x));
            
            -- Pagination Clause
            cursor_type_name := gql.to_cursor_type_name(tab_rec.table_name);
            cursor_extractor := gql.text_to_cursor_clause(
                tab_rec.table_schema,
                tab_rec.table_name,
                'after_cursor'
            );
            
            pagination_clause := format(e'and coalesce(%s > %s, true)\n', order_clause, cursor_extractor);
            cursor_extractor := gql.text_to_cursor_clause(
                tab_rec.table_schema,
                tab_rec.table_name,
                'before_cursor'
            );
            pagination_clause := pagination_clause || format(e'\t\t\t\tand coalesce(%s > %s, true)\n', order_clause, cursor_extractor);
            
            -- Cursor Selector
            cursor_selector := gql.to_cursor_clause(tab_rec.table_schema, tab_rec.table_name, 'subq_sorted');
            
            -- Function Signature
            resolver_function_args := 'field jsonb';

            -- Entrypoint Connection
			func_def := format(template,
                gql.to_resolver_name(gql.to_connection_name(tab_rec.table_name)),
                resolver_function_args,
                cursor_type_name, cursor_type_name, cursor_type_name, cursor_type_name, 
                tab_rec.table_schema, tab_rec.table_name,
                'true', condition_clause,
                tab_rec.table_schema, tab_rec.table_name, 'true', pagination_clause, condition_clause,
                order_clause, order_clause, order_clause,
                cursor_selector, order_clause,
                cursor_selector, order_clause,
                cursor_selector, gql.to_resolver_name(gql.to_base_name(tab_rec.table_name))		
            );
			execute func_def;

            -- For each relationship, make a connection resolver
            for rec in select *
                    from gql.relationship_info ri
                    where ri.table_schema = tab_rec.table_schema
                            and ri.foreign_table = tab_rec.table_name
				            and ri.foreign_cardinality = 'MANY' loop
			
                -- Build Join Clause
                join_clause := '';
                for ix in select generate_series(1, array_length(rec.local_columns, 1)) loop
                    join_clause := join_clause || format('rec.%s = %s.%s and ',
                                                         rec.local_columns[ix],
                                                         rec.foreign_table,
                                                         rec.foreign_columns[ix]);
                end loop;
                join_clause := substring(join_clause, 1, character_length(join_clause)-4);
                
                
                -- Function Signature
                resolver_function_args := format(
                    'rec %s.%s, field jsonb',
                    rec.table_schema,
                    rec.local_table
                );
                
                
                func_def := format(template,
                    gql.to_resolver_name(gql.relationship_to_gql_field_name(rec.constraint_name)),
                    resolver_function_args,
                    cursor_type_name, cursor_type_name, cursor_type_name, cursor_type_name, 
                    rec.table_schema, rec.foreign_table,
                    join_clause, condition_clause,
                    rec.table_schema, rec.foreign_table, join_clause, pagination_clause, condition_clause,
                    order_clause, order_clause, order_clause,
                    cursor_selector, order_clause,
                    cursor_selector, order_clause,
                    cursor_selector, gql.to_resolver_name(gql.to_base_name(rec.foreign_table))		
                );

                --raise notice 'Function %', func_def;
                execute func_def;

		    end loop;
		end loop;
	end;
$BODY$;



CREATE OR REPLACE FUNCTION gql.build_resolve_relationship_to_one(_table_schema text) RETURNS void
    LANGUAGE 'plpgsql'
AS $BODY$
	declare
		rec record;
		join_clause text;
		func_def text;
		ix int;
        template text;
		
    begin

            template := e'
create or replace function %s(rec %s.%s, field jsonb) returns jsonb
	language plpgsql stable as
$$
begin
	-- Compute total
    return ( 
		select
            %s(
                rec:=%s,
                field:=field
            )
		from
			%s.%s
        where
            -- Join clause
            %s
        -- Only returns 1 row due to join clause
        limit 1
    );
end;
$$;';


            -- For each relationship, make a connection resolver
        for rec in select *
                from gql.relationship_info ri
                where ri.table_schema = _table_schema
                        and ri.foreign_cardinality = 'ONE' loop
        
                -- Build Join Clause
                join_clause := '';
                for ix in select generate_series(1, array_length(rec.local_columns, 1)) loop
                    join_clause := join_clause || format('rec.%s = %s.%s and ',
                                                         rec.local_columns[ix],
                                                         rec.foreign_table,
                                                         rec.foreign_columns[ix]);
                end loop;
                join_clause := substring(join_clause, 1, character_length(join_clause)-4);
 
            -- Entrypoint Connection
			func_def := format(template,
                gql.to_resolver_name(gql.relationship_to_gql_field_name(rec.constraint_name)),
                rec.table_schema, rec.local_table,
                gql.to_resolver_name(gql.to_base_name(rec.foreign_table)),
                rec.foreign_table,
                rec.table_schema, rec.foreign_table,
                join_clause
            );
			execute func_def;
        end loop;
	end;
$BODY$;
