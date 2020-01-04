create or replace function gql.build_resolvers(_table_schema text) returns void
	language plpgsql stable as
$$
	begin
		perform gql.build_resolve_stubs(_table_schema);
		perform gql.build_cursor_types(_table_schema);
		perform gql.build_resolve_cursor(_table_schema);
		perform gql.build_resolve_connection_relationships(_table_schema);
		perform gql.build_resolve_rows(_table_schema);
		perform gql.build_resolve_entrypoint_one(_table_schema);
		perform gql.build_resolve_entrypoint_connection(_table_schema);
		perform gql.build_resolve(_table_schema);
	end;
$$;


create or replace function gql.drop_resolvers() returns void
	language plpgsql 
as $body$
declare
	function_oid text;
	type_oid text;
begin

	-- Drop resolver functions
	for function_oid in (
		select
			p.oid::regprocedure --,
		from 
			pg_catalog.pg_namespace n
			join pg_catalog.pg_proc p
				on pronamespace = n.oid
		where
			nspname = 'gql'
			and proname like 'resolve_%') loop
	    execute format('drop function if exists %s;', function_oid);
    end loop;

    for type_oid in (
        select 
            t.oid::regtype
        from
            pg_type t
            inner join pg_namespace n on t.typnamespace = n.oid
        where
            n.nspname = 'gql'
            and typname like '%_cursor'
            and typcategory = 'C'
        ) loop
	    execute format('drop type %s;', type_oid);
	end loop;
end;
$body$;


