/*
******************************
** File: cursor.sql
** Name: Oliver Rice
** Date: 2019-12-11
** Desc: Parse a token array into an abstract syntax tree (AST)
******************************

*/


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
			

CREATE OR REPLACE FUNCTION gql.build_resolve_cursor(_table_schema text) RETURNS void
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
create or replace function gql.resolve_cursor(rec %s.%s) returns %s
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



create or replace function gql.record_to_cursor_select_clause(_table_schema text, _table_name text, rec_name text default 'rec') RETURNS text
    LANGUAGE sql immutable as
$$ 
-- For selecting a cursor from a record to return to a user. Not suitable for filtering
-- Due to inability to work with indexes
select format(e'gql.resolve_cursor(%s::%s.%s)::text', rec_name, _table_schema, _table_name);
$$;



CREATE OR REPLACE FUNCTION gql.to_cursor_clause(
    _table_schema text,
    _table_name text,
    source_name text,
    as_row bool default true 
) returns text
    LANGUAGE 'plpgsql'
AS $BODY$
    -- creates an entry to a select clause to select primary key values
    -- as cursor. Use 'as_row' to determine if the result is packed into
    -- a row() call
	declare
        col_clause text;
	begin
        select
            string_agg(source_name || '.' || ci.column_name, ', ' order by ci.ordinal_position)
        from
            gql.column_info ci
        where
            _table_schema=ci.table_schema and _table_name=ci.table_name
            and ci.is_pkey
        into col_clause;
        
        if as_row then
            return format(e'row(%s)', col_clause);
        end if;
        return format(e'(%s)', col_clause);

    end;
$BODY$;


CREATE OR REPLACE FUNCTION gql.text_to_cursor_clause(
    _table_schema text,
    _table_name text,
    source_name text default 'after_cursor'
) returns text
    LANGUAGE 'plpgsql'
AS $BODY$
	declare
        col_clause text;
	begin
        select 
            string_agg('(abc.r).' || ci.column_name, ', ' order by ci.ordinal_position)
        from
            gql.column_info ci
        where
            _table_schema=ci.table_schema and _table_name=ci.table_name
            and ci.is_pkey
        into col_clause;
        
        return format(e'(select (%s) from (select (%s::%s)) abc(r))',
                col_clause,
                source_name,
                gql.to_cursor_type_name(_table_name)
        );


    end;
$BODY$;
