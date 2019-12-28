# pylint: disable=invalid-name,redefined-outer-name
import time

import sys

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
    """Run test suite"""
    # Store connection string on class variable to make it accessible to pytest
    engine = create_engine(connection)
    engine.execute(sqla_text(build_sql_source()))
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
    """Run test suite"""

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
