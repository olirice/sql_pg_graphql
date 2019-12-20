import os
import subprocess

from flupy import walk_files

from pg_graphql import SQL_DIR


def build_sql_source() -> str:
    """Collect SQL source files as a string"""
    source_pathes = walk_files(SQL_DIR).filter(lambda x: x.endswith(".sql")).sort()

    sql_components = []

    for source_path in source_pathes:
        with open(source_path) as source_file:
            contents = source_file.read()
        sql_components.append(contents)

    sql_source = "\n\n".join(sql_components)
    return sql_source


def run_test(connection):
    """Run test suite"""
    return subprocess.call(["pg_graphql", "test", "-c", connection])


def clear_terminal():
    """Clear terminal output"""
    os.system("cls" if os.name == "nt" else "clear")


def most_recent_edit(src_path):
    """Return timestamp of most recently edited file located within *src_path* (recursive)"""
    return (
        walk_files(src_path)
        .filter(lambda x: x.endswith(".py") or x.endswith(".sql"))
        .map(lambda x: os.stat(x).st_mtime)
        .max()
    )


class Context:
    """Global context for passing arguments and options to pytest"""

    connection = None
