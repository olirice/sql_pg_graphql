import pytest
from graphql import build_schema
from graphql.error import GraphQLError

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
"""

SQL_DOWN = """
drop table post cascade;
drop table account cascade;
"""


@pytest.fixture
def build_tables(session):
    """Side effect fixture create and populate a SQL schema to query"""
    session.execute(SQL_UP)
    session.commit()
    yield
    session.execute(SQL_DOWN)
    session.commit()


def test_schema_no_error(session, build_tables):
    query = "select gql.to_schema('public');"
    (result,) = session.execute(query).fetchone()
    print(result)
    assert isinstance(result, str)


def test_schema_is_valid(session, build_tables):
    query = "select gql.to_schema('public');"
    (result,) = session.execute(query).fetchone()
    print(result)
    try:
        build_schema(result)
    except GraphQLError:
        pytest.fail("Building schema resulted in an error")
