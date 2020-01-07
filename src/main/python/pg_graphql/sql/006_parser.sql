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
        field_depth int := 0;
        condition jsonb := '{}';
        ix int;
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

        -- Find the end of the arguments clause
        -- Avoid infinite loop. ix is unused.
        for ix in (select * from generate_series(1, array_length(tokens,1))) loop
            if tokens[1].kind = 'PAREN_R' then
                tokens := tokens[2:];
                exit;
            end if;

            -- Parse condition argument 
            if (tokens[1].kind, tokens[1].content, tokens[2].kind, tokens[3].kind) = ('NAME', 'condition', 'COLON', 'BRACE_L') then
                tokens := tokens[4:];
                for ix in (select * from generate_series(1, array_length(tokens,1))) loop
                    if tokens[1].kind = 'BRACE_R' then
                        tokens := tokens[2:];
                        exit;
                    end if;
                    if (tokens[1].kind, tokens[2].kind) = ('NAME', 'COLON') then
				        cur := jsonb_build_object(
                            tokens[1].content,
                            tokens[3].content
                        );
                    end if;
                    condition := condition || cur;
                    tokens := tokens[4:];
                end loop;

                cur := jsonb_build_object('condition', cur);

            -- Parse a standard argument
            elsif (tokens[1].kind, tokens[2].kind) = ('NAME', 'COLON') then
				cur := jsonb_build_object(
                    tokens[1].content,
                    tokens[3].content
                );
                tokens := tokens[4:];
            else
                raise exception 'gql.parse_field: invalid state parsing arg with tokens %', tokens;
            end if;

            args := args || cur;
        end loop;
    else
        args := '{}'::jsonb;
    end if;

    -- Read Fields
    if tokens[1].kind = 'BRACE_L' then
        for ix in (select * from generate_series(1, array_length(tokens,1))) loop

            if (tokens[1].kind = 'BRACE_L') then
                tokens := tokens[2:];
                field_depth := field_depth + 1;
            end if;
            if (tokens[1].kind = 'BRACE_R') then
                tokens := tokens[2:];
                field_depth := field_depth - 1;
            end if;

            exit when field_depth = 0;

            cur_field := gql.parse_field(tokens);
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
