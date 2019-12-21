# pg_graphql

<p>
<a href="https://github.com/olirice/pg_graphql/actions">
    <img src="https://github.com/olirice/pg_graphql/workflows/Tests/badge.svg" alt="Test Status">
</a>
</p>

pg_graphql adds [GraphQL](https://graphql.org/learn/) support to [PostgreSQL](https://www.postgresql.org/).

It gives postgres the ability to describe its schema as a GraphQL API and performantly serve
requests against that API. This is a foundational utilty for building blazingly fast
GraphQL webservers in any programming language.

A python reference implementation is coming soon at [archetype](https://github/olirice/archetype).

pg_graphql is suitable for processing sanitized, syntactically correct, GraphQL operations. Under no circumstances should unsanitized input be accepted.

WARNING: This is pre-alpha software.

**Documentation**: [https://olirice.github.io/pg_graphql/](https://olirice.github.io/pg_graphql/)

**Requirements**: PostgresQL 9.6+

## Installation

pg_graphql is written in SQL and PL/pgSQL. Install it by adding [pg_graphql.sql](https://github.com/olirice/pg_graphql/blob/master/pg_graphql.sql) to your migrations or pasting it to your `psql` prompt.

Alternatively, if you are a python 3.6+ developer, you can install the development kit and install pg_graphql using its CLI.

```shell
$ pip install pg_graphql
$ pg_graphql install -c <database_connection>
```

The install can be berified by checking that the `gql` schema has been created.
```sql
select exists (select * from pg_catalog.pg_namespace where nspname = 'gql');
```

Next, check out the [quickstart](quickstart.md) guide for a small end-to-end example.

<p align="center">&mdash;&mdash;  &mdash;&mdash;</p>
<p align="center"><i>pg_graphql is <a href="https://github.com/olirice/pg_graphql/blob/master/LICENSE.md">BSD licensed</a> code.</i></p>