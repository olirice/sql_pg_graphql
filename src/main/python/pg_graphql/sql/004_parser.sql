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
