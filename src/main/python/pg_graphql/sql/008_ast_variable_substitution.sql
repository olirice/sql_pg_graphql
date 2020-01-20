CREATE or replace FUNCTION gql.ast_replace_value(ast jsonb, search jsonb, substitute jsonb) RETURNS jsonb
STRICT LANGUAGE plpgsql AS $$
/*
Anywhere 'search' is found as a key, 'substitute' is unpacked in its place.
Intended for unpacking query fragments on an AST
 */
 begin
  return
    CASE jsonb_typeof(ast)
        WHEN 'object' THEN
          (
            SELECT
                jsonb_object_agg(
                    key,
                    gql.ast_replace_value(value, search, substitute)
                )
            FROM
                jsonb_each(ast)
          )
        -- AST does not contain array types 
        -- WHEN 'array' THEN
        -- TODO(OR): A literal string argument passed as a 
        when 'string' then case when ast = search then substitute else ast end
        else ast
    end;
end;
$$;




CREATE or replace FUNCTION gql.ast_substitute_variables(ast jsonb, variables jsonb) RETURNS jsonb
language plpgsql immutable parallel safe as
$BODY$
declare
    variable record;
begin
    -- AST Pass to populate variables
    for variable in select key, value from jsonb_each(variables) loop
        ast := gql.ast_replace_value(
            ast := ast,
            search := to_jsonb(('$' || variable.key)::text),
            substitute := variable.value
        );
    end loop;

    return ast;
end;
$BODY$;


