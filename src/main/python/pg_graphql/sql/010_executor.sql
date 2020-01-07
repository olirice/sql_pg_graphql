create or replace function gql.execute(operation text) returns jsonb as
$$
    declare
        tokens gql.token[] := gql.tokenize_operation(operation);
        ast jsonb := gql.parse_operation(tokens);
    begin
        -- Raising these notices takes about 0.1 milliseconds
		--raise notice 'Tokens %', tokens::text;
	    -- raise notice 'AST %', jsonb_pretty(ast);
        return gql.resolve(ast);
	end;
$$ language plpgsql stable;


