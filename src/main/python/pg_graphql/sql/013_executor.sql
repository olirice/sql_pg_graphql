create or replace function gql.parse_operation(tokens gql.token[], variables jsonb default '{}'::jsonb) returns jsonb
    language plpgsql immutable strict parallel safe
as $BODY$
declare
    ast jsonb;
    fragments jsonb := gql.parse_fragments(tokens);
begin
    -- Standard syntax: "query { ..."
    if (tokens[1].kind, tokens[2].kind) = ('NAME', 'BRACE_L') and tokens[1].content = 'query'
        then tokens := tokens[3:];
    end if;

    -- Simplified syntax for single queries: "{ ..."
    if tokens[1].kind = 'BRACE_L'
        then tokens := tokens[2:];
    end if;

    -- Parse request
    ast := (gql.parse_field(tokens)).contents;

    -- Expand query fragments
    ast := gql.ast_expand_fragments(ast, fragments); 

    -- Insert variable values
    ast := gql.ast_substitute_variables(ast, variables); 

    -- Apply skip and include directives
    ast := gql.ast_apply_directives(ast); 
    return ast;
end;
$BODY$;






create or replace function gql.execute(operation text, variables text default '{}') returns jsonb as
$$
    declare
        vars jsonb := variables::jsonb;
        tokens gql.token[] := gql.tokenize_operation(operation);
        ast jsonb := gql.parse_operation(tokens, vars);
    begin
        -- Raising these notices takes about 0.1 milliseconds
		--raise notice 'Tokens %', tokens::text;
        return gql.resolve(ast);
	end;
$$ language plpgsql stable;


