import pytest

SQL_UP = """
drop table if exists post cascade;
drop table if exists account cascade;

create table account(
    id serial primary key,
    name text not null,
    created_at timestamp default now()
);

create table post(
    id serial primary key,
    owner_id integer references account(id),
    title text not null,
    body text,
    created_at timestamp default now()
);

insert into account(id, name) values
(1, 'Oliver'),
(2, 'Rach');

insert into post(id, owner_id, title) values
(1, 1, 'First Post'),
(2, 1, 'Second Post'),
(3, 2, 'Wrong Post');


select gql.build_resolve_all('public');

CREATE OR REPLACE FUNCTION gql.execute(
	operation text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'

    COST 100
    STABLE 
    
AS $BODY$
    declare
        tokens gql.token[] := gql.tokenize_operation(operation);
        ast jsonb := gql.parse_operation(tokens);
        --sql_query text := gql.sqlize_field(ast, parent_block_name := null);
    begin
        -- Raising these notices takes about 0.1 milliseconds
		--raise notice 'Tokens %', tokens::text;
		--raise notice 'AST %', jsonb_pretty(ast);
		--raise notice 'SQL %', sql_query;
		return gql.resolve_entrypoint_one_public_account(field:=ast->'account');
	end;
$BODY$;
"""

SQL_DOWN = """
drop table post cascade;
drop table account cascade;
"""


INTEGRATION_QUERY = """
select gql.execute($$
    query {
        account(id: 1) {
            name
            post_collection_by_id_to_owner_id {
                edges{
                    node{
                        title
                    }
                }
            }
            created_at
        }
    }
$$);
"""

@pytest.fixture
def build_tables(session):
    """Side effect fixture create and populate a SQL schema to query"""
    session.execute(SQL_UP)
    session.commit()
    yield
    session.execute(SQL_DOWN)


def test_operation(session, build_tables):
    query = """
        select gql.execute($$
            query { account(id: 1) { name my_id: id, created_at} }
        $$);
    """
    (result,) = session.execute(query).fetchone()
    assert isinstance(result, dict)
    assert len(result) == 3
    assert result["name"] == "Oliver"
    assert result["my_id"] == 1
    assert isinstance(result["created_at"], str)




def test_nested_operation(session, build_tables):
    (result,) = session.execute(INTEGRATION_QUERY).fetchone()
    assert isinstance(result, dict)
    assert isinstance(result["post_collection_by_id_to_owner_id"], dict)
    assert isinstance(result["post_collection_by_id_to_owner_id"]["edges"], list)


def test_nested_operation_join_correctness(session, build_tables):
    (result,) = session.execute(INTEGRATION_QUERY).fetchone()
    assert isinstance(result, dict)
    # There are 3 entries in the post table but only 2 have account.id=1
    assert len(result["post_collection_by_id_to_owner_id"]["edges"]) == 2



def _query_integration(session):
    (result,) = session.execute(INTEGRATION_QUERY).fetchone()
    assert result is not None


def test_query_speed(session, build_tables, benchmark):
    benchmark(_query_integration, session)
