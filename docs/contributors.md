# How to Contribute

While the source for pg_graphql is SQL and PL/pgSQL, the development repository
contains tooling written in python for testing, and building the SQL source into
a single file.

**Requirements**: Python 3.6+, Postgres


!!! note "Running Postgres in Docker"
    Using docker to expose a Postgres database is the recommended setup for rapid
    development. To spin up container matching the pg_graphql default connection
    string use:
    ```
    docker run --name pg -p 5432:5432 -d -e POSTGRES_DB=pg_graphql -d postgres
    ```


## Tooling
The `pg_graphql` command line tool consists of a few commands to aid in development.

### Installation

Check out the development repository
```shell
git clone https://github.com/olirice/pg_graphql.git
cd pg_graphql
```

Install the python package
```shell
python3 -m pip install ".[dev]"
```

Confirm the install by running the test suite
```shell
pg_graphql test -c <postgres_connection_string>
```

### Development Flow

The most useful development command is `watch` which monitors the project's `src/` directory
and re-runs the test suite when any python or SQL source code changes.

```shell
pg_graphql watch -c <postgres_connection_string>
```

Which results in an output like 
```
============================= test session starts ==============================
platform darwin -- Python 3.6.5, pytest-5.3.2, py-1.8.0, pluggy-0.13.1
rootdir: /Users/wova/Documents/pg_graphql
plugins: benchmark-3.2.2
collected 36 items

src/test/python/cli_test.py .                                            [  2%]
src/test/python/parser_test.py ..                                        [  8%]
src/test/python/resolver_test.py ....                                    [ 19%]
src/test/python/schema_test.py ..                                        [ 25%]
src/test/python/select_test.py ..                                        [ 30%]
src/test/python/tokenizer_test.py .........................              [100%]
============================== 36 passed in 0.86s ==============================
```
or a stacktrace if the change caused a test to break.


### Build `pg_graphql.sql`

To build your local copy of development repo into a single `pg_graphql.sql` migration use:
```shell
pg_graphql build -o pg_graphql.sql
```



## Source Guide

The SQL files that assemble into the final `pg_graphql.sql` file that can be used to install the pg_graphql library
are located in `src/main/python/pg_graphql/sql` and are namespaced by function. For example, the function responsible
for tokenizing GraphQL queries is located in `XXX_tokenizer.sql`. 

The files are prefixed with a number which controls the order they will be inserted into the build artifact, i.e. pg_graphql.sql.


