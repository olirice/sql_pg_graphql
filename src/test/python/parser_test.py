import json
from typing import Any, Dict

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause


def is_valid_ast(ast):
    """Validate that a graphql operation ast contains all AST keys"""
    keys = ["args", "alias", "name", "fields"]

    for key in keys:
        if key not in ast:
            return False

    # Empty list is ok, null is not.
    # assert ast["fields"] is not None
    # assert ast["args"] is not None
    if ast["fields"]:
        for key, field in ast["fields"].items():
            if not is_valid_ast(field):
                return False

    if ast["args"]:
        for key, val in ast["args"].items():
            if not isinstance(key, str):
                return False
            if not isinstance(val, (str, dict)):
                return False
    return True


def build_sql_query(text_to_parse, variables: Dict[str, Any] = None) -> TextClause:
    variables = variables or dict()
    variable_str = json.dumps(variables)
    return text(
        f"""
        select
            jsonb_pretty(
                gql.parse_operation(
                    gql.tokenize_operation($ttp${text_to_parse}$ttp$::text),
                    '{variable_str}'::jsonb
                )
            )
        """
    )


def test_parser_integration(session):
    query = build_sql_query(
        """
        query {
            account(id: 1, name: "Oliver") {
                # accounts have comments!
                id
                name
                photo(pixels: 240)
                created_at
            }
        }
    """
    )
    (result,) = session.execute(query).fetchone()
    result = json.loads(result)
    print(json.dumps(result, indent=2))
    assert is_valid_ast(result["account"])


def test_parser_speed(session, benchmark):
    result = benchmark(test_parser_integration, session)
    assert True


def test_parse_variable(session):
    myvar = "myvar"
    variables = {myvar: "(1)"}
    query = build_sql_query(
        """
        {
            account(nodeId: $myvar) {
                id
                created_at
            }
        }
    """,
        variables=variables,
    )
    (result,) = session.execute(query).fetchone()
    result = json.loads(result)
    print(json.dumps(result, indent=2))
    assert is_valid_ast(result["account"])
    assert result["account"]["args"]["nodeId"] == variables[myvar]


def test_parse_fragment(session):
    query = build_sql_query(
        """
        {
            account(nodeId: "(1)") {
                id
                created_at
                ...acctFrag
            }
        }

        fragment acctFrag on Account {
            title
        }
    """
    )
    (result,) = session.execute(query).fetchone()
    result = json.loads(result)
    print(json.dumps(result, indent=2))
    assert is_valid_ast(result["account"])
    assert "title" in result["account"]["fields"]


def test_parse_nested_fragment(session):
    query = build_sql_query(
        """
        {
            account(nodeId: "(1)") {
                id
                ...acctFrag
            }
        }

        fragment acctFrag on Account {
            title
            ...acctFragExt
        }

        fragment acctFragExt on Account {
            created_at
        }
    """
    )
    (result,) = session.execute(query).fetchone()
    result = json.loads(result)
    print(json.dumps(result, indent=2))
    assert is_valid_ast(result["account"])
    assert "title" in result["account"]["fields"]
    assert "created_at" in result["account"]["fields"]


def test_parse_directive_skip_true(session):
    query = build_sql_query(
        """
        {
            account(nodeId: "(1)") {
                id
                created_at @skip(if: true)
            }
        }
    """
    )
    (result,) = session.execute(query).fetchone()
    result = json.loads(result)
    print(json.dumps(result, indent=2))
    assert is_valid_ast(result["account"])
    assert "id" in result["account"]["fields"]
    assert "created_at" not in result["account"]["fields"]


def test_parse_directive_skip_false(session):
    query = build_sql_query(
        """
        {
            account(nodeId: "(1)") {
                id
                created_at @skip(if: false)
            }
        }
    """
    )
    (result,) = session.execute(query).fetchone()
    result = json.loads(result)
    print(json.dumps(result, indent=2))
    assert is_valid_ast(result["account"])
    assert "id" in result["account"]["fields"]
    assert "created_at" in result["account"]["fields"]


def test_parse_directive_include_true(session):
    query = build_sql_query(
        """
        {
            account(nodeId: "(1)") {
                id
                created_at @include(if: true)
            }
        }
    """
    )
    (result,) = session.execute(query).fetchone()
    result = json.loads(result)
    print(json.dumps(result, indent=2))
    assert is_valid_ast(result["account"])
    assert "id" in result["account"]["fields"]
    assert "created_at" in result["account"]["fields"]


def test_parse_directive_include_false(session):
    query = build_sql_query(
        """
        {
            account(nodeId: "(1)") {
                id
                created_at @include(if: false)
            }
        }
    """
    )
    (result,) = session.execute(query).fetchone()
    result = json.loads(result)
    print(json.dumps(result, indent=2))
    assert is_valid_ast(result["account"])
    assert "id" in result["account"]["fields"]
    assert "created_at" not in result["account"]["fields"]
