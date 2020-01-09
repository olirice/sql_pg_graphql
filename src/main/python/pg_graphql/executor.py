from sqlalchemy import text
import json
import secrets
import string


def secure_token(n=16):
    return ''.join(secrets.choice(string.ascii_letters) for i in range(n))


def execute_operation(gql_query: str, variables: dict):

    if variables is None:
        variable_str = 'null::text'
    else: 
        variable_str = json.dumps(variables);

    escape_1 = secure_token()
    escape_2 = secure_token()

    return text(f"""
    select gql.execute(
        ${escape_1}${gql_query}${escape_1}$::text,
        ${escape_2}${variable_str}${escape_2}$::text
    ) as result
    """)
