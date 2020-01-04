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

            maybe_tok = substring(payload from '^"""(.*)?"""');
            if maybe_tok is not null then
                payload := substring(payload, character_length(maybe_tok)+7, 99999);
                tokens := tokens || ('BLOCK_STRING', maybe_tok)::gql.token;
                continue;
            end if;

            maybe_tok = substring(payload from '^"(.*)?"');
            if maybe_tok is not null then
                payload := substring(payload, character_length(maybe_tok)+3, 99999);
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
