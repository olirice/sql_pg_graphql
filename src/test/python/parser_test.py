import json

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause


def is_valid_ast(ast):
    """Validate that a graphql operation ast contains all AST keys"""
    keys = ["args", "alias", "name", "fields"]

    for key in keys:
        if key not in ast:
            return False

    # Empty list is ok, null is not.
    assert ast["fields"] is not None
    assert ast["args"] is not None

    for key, field in ast["fields"].items():
        if not is_valid_ast(field):
            return False

    for key, val in ast["args"].items():
        if not isinstance(key, str):
            return False
        if not isinstance(val, (str, dict)):
            return False
    return True


def build_sql_query(text_to_parse) -> TextClause:
    return text(
        f"""
        select
            jsonb_pretty(
                gql.parse_operation(
                    gql.tokenize_operation($ttp${text_to_parse}$ttp$)
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
                createdAt
            }
        }
    """
    )
    (result,) = session.execute(query).fetchone()
    result = json.loads(result)
    print(json.dumps(result, indent=2))
    assert is_valid_ast(result['account'])


def test_parser_speed(session, benchmark):
    result = benchmark(test_parser_integration, session)
    assert True
