import pytest
from graphql import build_schema
from graphql import graphql_sync as graphql_exec

from pg_graphql.executor import execute_operation, get_schema

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


INTEGRATION_QUERY = execute_operation(
    """
    query {
        account(nodeId: "(1)") {
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
""",
    {},
)


@pytest.fixture
def build_tables(session):
    """Side effect fixture create and populate a SQL schema to query"""
    session.execute(SQL_UP)
    session.commit()
    yield
    session.execute(SQL_DOWN)
    session.commit()


@pytest.fixture
def validation_executor(session, build_tables):
    def resolver(root, info, **kwargs):
        context = info.context
        session = context["session"]
        query = info.context["query"]
        variables = info.context["variables"]
        result = session.execute(execute_operation(query, variables)).fetchone()["result"]
        context["result"] = result
        return result

    # Build a graphql package schema
    # It validates queries and respones match the spec
    schema_doc = session.execute(get_schema("public")).fetchone()["schema"]
    schema = build_schema(schema_doc)

    def default_resolver(_, info, **kwargs):
        path = info.path

        result = info.context["result"]
        remain = result
        for elem in path.as_list():
            remain = remain[elem]
        return remain

    # Use the resolver for every entrypoint
    entrypoints = schema.query_type.fields
    for key, entrypoint in entrypoints.items():
        entrypoint.resolve = resolver

        if hasattr(entrypoint, "type"):
            if hasattr(entrypoint.type, "fields"):
                for k, v in entrypoint.type.fields.items():
                    print(k)
                    v.resolve = default_resolver

    def validation_executor(query: str, variables: dict):
        context = {
            "query": query,
            "variables": variables,
            # From outer scope
            "session": session,
        }
        result = graphql_exec(
            schema=schema,
            source=query,
            context_value=context,
            variable_values=variables,
            field_resolver=None,
        )
        errors = result.errors
        result_dict = {
            "data": result.data,
            "errors": [error.formatted for error in errors or []],
        }
        return result_dict

    return validation_executor


def test_operation(session, build_tables):
    query = execute_operation(
        """query { account(nodeId: "(1)") { name my_id: id, created_at} }""", {}
    )
    (result,) = session.execute(query).fetchone()
    assert isinstance(result, dict)
    assert len(result) == 1
    assert "account" in result
    assert result["account"]["name"] == "Oliver"
    assert result["account"]["my_id"] == 1
    assert isinstance(result["account"]["created_at"], str)


def test_nested_operation(session, build_tables):
    (result,) = session.execute(INTEGRATION_QUERY).fetchone()
    print(result)
    assert isinstance(result, dict)
    assert isinstance(result["account"]["post_collection_by_id_to_owner_id"], dict)
    assert isinstance(result["account"]["post_collection_by_id_to_owner_id"]["edges"], list)


def test_nested_operation_join_correctness(session, build_tables):
    (result,) = session.execute(INTEGRATION_QUERY).fetchone()
    assert isinstance(result, dict)
    # There are 3 entries in the post table but only 2 have account.id=1
    assert len(result["account"]["post_collection_by_id_to_owner_id"]["edges"]) == 2


def _query_integration(session):
    (result,) = session.execute(INTEGRATION_QUERY).fetchone()
    assert result is not None


def test_query_speed(session, build_tables, benchmark):
    benchmark(_query_integration, session)


def test_validation_executor(validation_executor):
    result = validation_executor(
        """query { account(nodeId: "(1)") { name, my_id: id, created_at} }""", {}
    )
    print(result)
    assert "data" in result
    assert "errors" in result
    assert len(result["errors"]) == 0
    assert isinstance(result["data"]["account"]["created_at"], str)
