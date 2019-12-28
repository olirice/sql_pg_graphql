from sqlalchemy import text

def test_select_one(session):
    query = text("select 1;")
    (result,) = session.execute(query).fetchone()
    assert result == 1

def test_select_speed(session, benchmark):
    result = benchmark(test_select_one, session)
    assert True
