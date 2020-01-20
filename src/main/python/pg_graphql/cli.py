# pylint: disable=invalid-name,redefined-outer-name
import sys
import time

import click
import pytest
import sqlparse
from flupy import walk_files
from sqlalchemy import create_engine
from sqlalchemy import text as sqla_text

from pg_graphql import SQL_DIR, SRC_DIR, TEST_DIR
from pg_graphql.utils import Context, build_sql_source, clear_terminal, most_recent_edit, run_test


@click.group()
@click.version_option(version="0.0.1")
def main(**kwargs):
    """Click command group"""


@main.command()
@click.option(
    "-c",
    "--connection",
    help="e.g. postgresql://postgres:@localhost:5432/pg_graphql",
    default="postgresql://postgres:@localhost:5432/pg_graphql",
)
def install(connection):
    """Install pg_graphql in *connection* database"""
    # Store connection string on class variable to make it accessible to pytest
    engine = create_engine(connection)
    con = engine.connect()
    with con.begin() as trans:
        con.execute(sqla_text(build_sql_source()))
        trans.commit()
    con.close()
    engine.dispose()


@main.command()
@click.option(
    "-c",
    "--connection",
    help="e.g. postgresql://postgres:@localhost:5432/pg_graphql",
    default="postgresql://postgres:@localhost:5432/pg_graphql",
)
def test(connection):
    """Run test suite"""
    # Store connection string on class variable to make it accessible to pytest
    Context.connection = connection
    exit_code = pytest.main([TEST_DIR, "-x", "-p", "no:cacheprovider",])
    sys.exit(exit_code)


@main.command()
@click.option(
    "-c",
    "--connection",
    help="e.g. postgresql://postgres:@localhost:5432/pg_graphql",
    default="postgresql://postgres:@localhost:5432/pg_graphql",
)
def watch(connection):
    """Run the test suite every time the `src/` directory changes"""

    clear_terminal()
    run_test(connection)
    last_edit = most_recent_edit(SRC_DIR)
    click.echo("File system monitor started")
    try:
        while True:
            time.sleep(1)
            current_edit = most_recent_edit(SRC_DIR)
            if current_edit != last_edit:
                clear_terminal()
                run_test(connection)
            last_edit = current_edit
    except KeyboardInterrupt:
        pass
    click.echo("File system monitor stopped")


@main.command()
@click.option("-o", "--output", type=click.File("w"))
def build(output):
    """Aggregate SQL source into *output* file or terminal"""
    sql_source = build_sql_source()

    if output is not None:
        output.write(sql_source)
    else:
        click.echo(sql_source)


@main.command()
def format():
    """Format SQL source code in place"""
    source_pathes = walk_files(SQL_DIR).filter(lambda x: x.endswith(".sql"))
    for source_path in source_pathes:
        with open(source_path, "r") as f:
            sql = f.read()
        formatted_sql = sqlparse.format(sql, keyword_case="lower", reindent=False, wrap_after=110)
        with open(source_path, "w") as f:
            f.write(formatted_sql)

    click.echo("Completed")


@main.command()
@click.option(
    "-c",
    "--connection",
    help="e.g. postgresql://postgres:@localhost:5432/pg_graphql",
    default="postgresql://postgres:@localhost:5432/pg_graphql",
)
def demo(connection):
    """Setup and populate demo schema"""
    # Store connection string on class variable to make it accessible to pytest

    engine = create_engine(connection)
    con = engine.connect()
    with con.begin() as trans:
        con.execute(
            sqla_text(
                """
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
            )
        )
        trans.commit()
    con.close()
    engine.dispose()

    engine.dispose()
