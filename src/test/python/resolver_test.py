import pytest

SQL_UP = """
select gql.drop_resolvers();

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
(3, 2, 'Wrong Post'),
(4, 2, 'Post4'),
(5, 2, 'Post5'),
(6, 2, 'Post6'),
(7, 2, 'Post7'),
(8, 2, 'Post8'),
(9, 2, 'Post9'),
(10, 2, 'Post10');

select gql.build_resolvers('public');
"""

SQL_DOWN = """
select gql.drop_resolvers();
drop table post cascade;
drop table account cascade;
"""


INTEGRATION_QUERY = """
select gql.execute($$
    query {
        account(id: 1) {
            name
            post_collection_by_id_to_owner_id {
                total_count
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
    session.commit()


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
