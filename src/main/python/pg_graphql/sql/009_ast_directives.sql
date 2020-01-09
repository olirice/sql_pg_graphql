


create or replace function gql.to_bool(jsonb) returns bool
language sql immutable as 
$$
    select
        case
            when $1 = to_jsonb('true'::text) then true
            when $1 = to_jsonb('false'::text) then false
            when $1 = to_jsonb(true::bool) then true
            when $1 = to_jsonb(false::bool) then false
        end;
$$;



create or replace function gql.ast_is_skip(ast jsonb) returns bool
language sql immutable as
$$
    select 
        case
            when (jsonb_typeof(ast) = 'object'
                    and ast ? 'fields'
                    and ast ? 'args'
                    and ast ? 'include'
                    and ast ? 'skip'
                ) then gql.to_bool(ast -> 'include') and not gql.to_bool(ast -> 'skip')
            else true
        end
$$;


CREATE or replace FUNCTION gql.ast_apply_directives(ast jsonb) RETURNS jsonb
STRICT LANGUAGE SQL AS $$
/*
Skip fields where skip = true or include = false
 */
  SELECT
    CASE 
        WHEN jsonb_typeof(ast) = 'object' THEN
          coalesce((
            SELECT
                jsonb_object_agg(
                    key,
                    gql.ast_apply_directives(value)
                )
            FROM
                jsonb_each(ast)
            WHERE
                gql.ast_is_skip(value)
        ), 
        '{}'::jsonb)
        else ast
    end;

$$;
