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
    language plpgsql immutable parallel safe
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

        include jsonb := to_jsonb(true);
        skip jsonb := to_jsonb(false);
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

    
    -- Handle Inline Fragments
    -- Given that all pg_graphql endpoints only ever return one type, they are not yet supported
    if (_name, tokens[1].content) = ('...', 'on') then
            raise exception 'gql.parse_field: Inline query fragments are not necessary for this query %', tokens;
    end if;

    -- https://graphql.org/learn/queries/#fragments
    if _name = '...' then
        _name := _name || tokens[2].content;
        -- Advance past the spread marker
        tokens := tokens[2:];
    end if;

    -- Advance past name
    tokens := tokens[2:];


    -- Handle Directives
    -- https://graphql.org/learn/queries/#directives
    if tokens[1].kind = 'AT' then
        -- Looks like @<BLANK>(if: <BLANK>)
        if (tokens[3].kind, tokens[4].content, tokens[5].kind, tokens[7].kind) = ('PAREN_L', 'if', 'COLON', 'PAREN_R') 
            and tokens[2].content in ('include', 'skip') then

            include := case tokens[2].content when 'include' then to_jsonb(tokens[6].content) else include end;
            skip := case tokens[2].content when 'skip' then to_jsonb(tokens[6].content) else skip end;

        else
            raise exception 'gql.parse_field: invalid state while parsing directive %', tokens;
        end if;
        tokens := tokens[8:];

    end if;



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
                    -- Parse a variable argument
                    if (tokens[1].kind, tokens[2].kind, tokens[3].kind) = ('NAME', 'COLON', 'DOLLAR') then
				        cur := jsonb_build_object(
                            tokens[1].content,
                            '$' || tokens[4].content
                        );
                        tokens := tokens[5:];
                    -- Parse a standard argument
                    elsif (tokens[1].kind, tokens[2].kind) = ('NAME', 'COLON') then
                        cur := jsonb_build_object(
                            tokens[1].content,
                            tokens[3].content
                        );
                        tokens := tokens[4:];
                    end if;
                    condition := condition || cur;
                end loop;

                cur := jsonb_build_object('condition', cur);

            -- Parse a variable argument
            elsif (tokens[1].kind, tokens[2].kind, tokens[3].kind) = ('NAME', 'COLON', 'DOLLAR') then
                cur := jsonb_build_object(
                    tokens[1].content,
                    '$' || tokens[4].content
                );
                tokens := tokens[5:];
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
                    'fields', fields,
                    'include', include,
                    'skip', skip
                )),
	            tokens
	        )::gql.partial_parse
    );
    end;
$BODY$;




create or replace function gql.parse_fragments(tokens gql.token[]) returns jsonb
    language plpgsql immutable strict parallel safe
as $BODY$
/*
{   
    fragment_1: {
        "field1": # Field def,
    }
}
*/
declare
    ix int;
    fragments jsonb := '{}';

begin
    for ix in (select * from generate_series(1, array_length(tokens,1))) loop
        if (
            tokens[1].kind, tokens[1].content, tokens[2].kind,
            tokens[3].kind, tokens[3].content, tokens[4].kind,
            tokens[5].kind
           ) = (
            'NAME', 'fragment', 'NAME',
            'NAME', 'on', 'NAME', 'BRACE_L'
           ) then

            -- Make the fragemnt look like a field so we can parse it
            fragments := fragments || (
                gql.parse_field(
                    -- Make the fragemnt look like a field so we can parse it
                    ('NAME', '...' || tokens[2].content)::gql.token || tokens[5:]
                )
            ).contents; 

        end if;
        tokens := tokens[2:array_length(tokens,1)];
        exit when tokens is null;
    end loop;
    return fragments; 
end;
$BODY$;



