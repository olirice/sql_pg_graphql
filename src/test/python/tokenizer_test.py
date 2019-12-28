import pytest
from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause

from pg_graphql.types import Token, TokenKind


def build_sql_query(text_to_tokenize) -> TextClause:
    return text(
        f"""
        select gql.tokenize_operation('{text_to_tokenize}');
    """
    )


def test_tokenizer_integration(session):
    query = text("""
        select gql.tokenize_operation($q$
            query {
                account(id: 1, name: "Oliver") {
                    # accounts have comments!
                    id
                    name
                    photo(pixels: 240)
                    createdAt
                    post(first: 5) {
                        title
                    }
                }
            }
        $q$)
    """)
    (result,) = session.execute(query).fetchone()
    tokens = Token.from_text(result)
    assert tokens[-1].token_kind != TokenKind.ERROR


def _integration(session):
    query = text("""
        select gql.tokenize_operation($q$
            query {
                account(id: 1, name: "Oliver") {
                    # accounts have comments!
                    id
                    name
                    photo(pixels: 240)
                    createdAt
                }
            }
        $q$)
    """)
    (result,) = session.execute(query).fetchone()

    assert True


def test_tokenizer_speed(session, benchmark):
    result = benchmark(_integration, session)
    assert True


@pytest.mark.parametrize(
    "text_to_tokenize,token_kind",
    [
        ("! ", TokenKind.BANG),
        ("$ ", TokenKind.DOLLAR),
        ("& ", TokenKind.AMP),
        ("( ", TokenKind.PAREN_L),
        (") ", TokenKind.PAREN_R),
        (": ", TokenKind.COLON),
        ("= ", TokenKind.EQUALS),
        ("@ ", TokenKind.AT),
        ("[ ", TokenKind.BRACKET_L),
        ("] ", TokenKind.BRACKET_R),
        ("{ ", TokenKind.BRACE_L),
        ("} ", TokenKind.BRACE_R),
        ("| ", TokenKind.PIPE),
        ("... ", TokenKind.SPREAD),
        (".. ", TokenKind.ERROR),
        (". ", TokenKind.ERROR),
        ("_s0me_name ", TokenKind.NAME),
        ("1234 ", TokenKind.INT),
        ("-1234 ", TokenKind.INT),
        ("12.34 ", TokenKind.FLOAT),
        ("-12.34 ", TokenKind.FLOAT),
        ('"I am a string" ', TokenKind.STRING),
        ('"""I am a block string""" ', TokenKind.BLOCK_STRING),
    ],
)
def test_tokenize_input(text_to_tokenize, token_kind, session):
    query = build_sql_query(text_to_tokenize)
    (result,) = session.execute(query).fetchone()
    print(result)
    tokens = Token.from_text(result)
    assert tokens[0].token_kind == token_kind
