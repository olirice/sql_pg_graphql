import pytest

SQL_UP = """
drop table if exists post;
drop table if exists account;

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
"""

SQL_DOWN = """
drop table post;
drop table account;
"""


@pytest.fixture
def build_tables(session):
    """Side effect fixture create and populate a SQL schema to query"""
    session.execute(SQL_UP)
    yield
    session.execute(SQL_DOWN)


def test_to_query_block_name(session):
    query = """
        select gql.field_to_query_block_name('{"hello": "world"}'::jsonb);
    """
    (result,) = session.execute(query).fetchone()
    assert isinstance(result, str)
    assert len(result) > 4


def test_execute_sql(session):
    query = """
        select gql.execute_sql($$
            select '{"hello": "world"}'::jsonb;
        $$);
    """
    (result,) = session.execute(query).fetchone()
    assert isinstance(result, dict)
    assert result["hello"] == "world"


def test_operation(session, build_tables):
    query = """
        select gql.execute_operation($$
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
    query = """
        select gql.execute_operation($$
            query {
                account(id: 1) {
                    name
                    post_collection_by_id_to_owner_id {
                        title
                    }
                    created_at
                }
            }
        $$);
    """
    (result,) = session.execute(query).fetchone()
    assert isinstance(result, dict)
    assert isinstance(result["post_collection_by_id_to_owner_id"], list)


def test_nested_operation_join_correctness(session, build_tables):
    query = """
        select gql.execute_operation($$
            query {
                account(id: 1) {
                    name
                    post_collection_by_id_to_owner_id {
                        title
                    }
                    created_at
                }
            }
        $$);
    """
    (result,) = session.execute(query).fetchone()
    assert isinstance(result, dict)
    assert isinstance(result["post_collection_by_id_to_owner_id"], list)
    # There are 3 entries in the post table but only 2 have account.id=1
    assert len(result["post_collection_by_id_to_owner_id"]) == 2


def _builder_integration(session):
    query = """
        select 
            gql.sqlize_field(
                gql.parse_operation(
                    gql.tokenize_operation($$
            query {
                account(id: 1) {
                    name
                    post_collection_by_id_to_owner_id {
                        title
                    }
                    created_at
                }
            }
        $$)
        ), null)
        ;
    """
    (result,) = session.execute(query).fetchone()
    assert result is not None


def test_builder_speed(session, build_tables, benchmark):
    benchmark(_builder_integration, session)


def _query_integration(session):
    query = """
        select gql.execute_operation($$
            query {
                account(id: 1) {
                    name
                    post_collection_by_id_to_owner_id {
                        title
                    }
                    created_at
                }
            }
        $$);
    """
    (result,) = session.execute(query).fetchone()
    assert result is not None


def test_query_speed(session, build_tables, benchmark):
    benchmark(_query_integration, session)
