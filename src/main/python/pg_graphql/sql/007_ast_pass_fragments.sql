
create or replace function gql.ast_recursive_merge(a jsonb, b jsonb)
returns jsonb language sql as $$
    select 
        jsonb_object_agg(
            coalesce(ka, kb), 
            case 
                when va is null then vb 
                when vb is null then va 
                when va = vb then va
                --when jsonb_typeof(va) <> 'object' then va || vb
                when (jsonb_typeof(va) = 'object' and jsonb_typeof(vb) = 'object') then gql.ast_recursive_merge(va, vb)
                else coalesce(va, vb)
            end
        )
    from jsonb_each(a) e1(ka, va)
    full join jsonb_each(b) e2(kb, vb) on ka = kb;
$$;


CREATE or replace FUNCTION gql.ast_merge_at_key(obj jsonb, search text, substitute jsonb) RETURNS jsonb
STRICT LANGUAGE SQL AS $$
/*
Anywhere 'search' is found as a key, 'substitute' is unpacked in its place.
Intended for unpacking query fragments on an AST
 */
  SELECT
    CASE jsonb_typeof(obj)
        
        WHEN 'object' THEN
            gql.ast_recursive_merge(
                (
                    SELECT
                        jsonb_object_agg(
                            key,
                            gql.ast_merge_at_key(
                                value,
                                search,
                                substitute
                            )
                        )
                    FROM
                        jsonb_each(obj)
                    WHERE
                        key <> search
                ),
                CASE
                    -- Extract fields from the fragment into the current level
                    WHEN obj ? search THEN substitute
                    ELSE '{}'
                END
            )
        -- AST does not contain array types 
        -- WHEN 'array' THEN

        -- Scalar
        ELSE
          obj
    END;
$$;

CREATE or replace FUNCTION gql.ast_expand_fragments(ast jsonb, fragments jsonb) RETURNS jsonb
language plpgsql immutable parallel safe as
$BODY$
declare
    fragment record;
    ast_prior jsonb;
    ix int;
begin
/*
    AST Passes to populate query fragments
    --------------------------------------
    Fragments may be nested but nested fragments are
    forbidden from forming cylces in the specification
    */
    
    -- Set maximum depth of nested query fragments
    for ix in select * from generate_series(1, 10) loop
        ast_prior = ast;
        -- Expand fragments in the AST
        for fragment in select key, value from jsonb_each(fragments) loop
            ast := gql.ast_merge_at_key(ast, fragment.key, fragment.value -> 'fields');
        end loop;
        -- If nothing happend, we're done
        if ast = ast_prior then
            exit;
        end if;
    end loop;
    return ast;
end;
$BODY$;


