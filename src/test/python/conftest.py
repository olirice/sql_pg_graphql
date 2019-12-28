# pylint: disable=redefined-outer-name,no-member
import pytest
import sqlalchemy
from sqlalchemy.orm import scoped_session, sessionmaker

from pg_graphql.utils import Context, build_sql_source


@pytest.fixture(scope="session")
def connection():
    return Context.connection


@pytest.fixture(scope="session")
def engine(connection):
    _engine = sqlalchemy.create_engine(connection, echo=False)
    yield _engine
    _engine.dispose()


@pytest.fixture(scope="session")
def session_maker(engine):
    smake = sessionmaker(bind=engine)
    _session = scoped_session(smake)

    # Build gql schema
    sql_source = build_sql_source()
    _session.execute(sqlalchemy.text(sql_source))
    _session.commit()
    yield _session


@pytest.fixture
def session(session_maker):
    _session = session_maker

    yield _session
    # _session.rollback()
    _session.close()
