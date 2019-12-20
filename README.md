# pg_graphql

<p>
<a href="https://github.com/olirice/pg_graphql/actions">
    <img src="https://github.com/olirice/pg_graphql/workflows/Tests/badge.svg" alt="Test Status">
</a>
</p>

pg_graphql adds GraphQL support to PostgreSQL.

It allows you to serve GraphQL queries directly from your database can convert
any database schema into a GraphQL schema.

pg_graphql is suitable for processing sanitized, syntactically correct, GraphQL operations.

**Requirements**: PostgresQL 9.6+


## Installation

pg_graphql is written in SQL and PL/pgSQL. Install it by adding `pg_graphql.sql`, located in the root directory
of https://github.com/olirice/pg_graphql/, to your SQL migrations.

Alternatively, if you are a python 3.6+ developer:

Install pg_graphql into your environment
```shell
$ pip install pg_graphql
$ pg_graphql install -c <database_connection>
```

---

## Quickstart

For this example we'll create simple database schema, and query it using GraphQL.

```sql
CREATE TABLE account (
    id SERIAL PRIMARY KEY,
    name text not null,
    created_at timestamp not null default now()
);

CREATE TABLE blog_post (
    id SERIAL PRIMARY KEY,
    title text not null,
    body text,
    author_id references account(id),
    created_at timestamp not null default now()
);

INSERT INTO account (id, name) VALUES
(1, 'Oliver'),
(2, 'Buddy');

INSERT INTO blog_post (id, owner_id, title) VALUES
(1, 1, 'A GraphQL to SQL Webserver for every Language'),
(2, 1, 'Sanitize all the things!'),
(3, 2. 'To Bite or not to Bite');
```

We now have 2 tables, connected by a foreign key relationshp describing user accounts and
their associated blog posts.

Next, check out the [specification](specification.md) for more examples of how SQL schemas
translated to GraphQL schemas.

<p align="center">&mdash;&mdash;  &mdash;&mdash;</p>
<p align="center"><i>pg_graphql is <a href="https://github.com/olirice/pg_graphql/blob/master/LICENSE.md">BSD licensed</a> code.</i></p>
