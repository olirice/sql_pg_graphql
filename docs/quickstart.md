## Quickstart

For this example we're going to create a simple blogging platform exposed. We'll start with create a minimal database schema. Then we'll use pg_graphql retrieve our GraphQL API schema, and finally, we'll serve a GraphQL query from that API.

### Database Setup

First, we need to define our database schema. We need a table for accounts, and another for blog posts. All blog posts must be associated with an author in the accounts table.

```sql
CREATE TABLE public.account (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE public.blog_post (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    body TEXT,
    author_id INT REFERENCES account(id),
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

INSERT INTO account (id, name) VALUES
(1, 'Oliver'),
(2, 'Buddy');

INSERT INTO blog_post (id, author_id, title) VALUES
(1, 1, 'A GraphQL to SQL Webserver for every Language'),
(2, 1, 'Sanitize all the things!'),
(3, 2, 'To Bite or not to Bite');
```

### GraphQL Schema

Now we can reflect our new database schema as a GraphQL schema using the `gql.get_schema(schema_name text)` function. This is the document that describes our GraphQL API's types, and what data can be queried from it. If you're new to GraphQL check out their [documentation](https://graphql.org/learn/) for more information. 
```sql
SELECT gql.get_schema(schema_name TEXT);
```

An abridged version of the output is show below.
```gql
type account {
  id: Int!
  name: String!
  created_at: String!
  blog_post_collection_by_id_to_author_id: [blog_post!]
}
type bar {
  id: Int!
}
type blog_post {
  id: Int!
  title: String!
  body: String
  author_id: Int
  created_at: String!
  account_by_author_id_to_id: account!
}
type foo {
 id: Int!
}
type Query {
  account(id: Int! ): account
  blog_post(id: Int! ): blog_post
}
```

Notice that our API detected the foreign key relationship between `account` and `blog_post` and created `blog_post_collection_by_id_to_author_id` and `account_by_author_id_to_id` on their base types respectively.

### Query the API

Lets write a query against our shiny new API to retrieve information about our user with `account.id = 1`   including all of their blog posts. To serve a [query operation](https://graphql.github.io/graphql-spec/June2018/#sec-Language.Operations) we pass the query to `gql.execute_operation(operation TEXT)`.

```sql
SELECT gql.execute_operation('
    query {
        account(id: 1) {
            name
            created_at
            blog_post_collection_by_id_to_author_id {
                id
                title
            }
        }
    }
');
```

This returns a `jsonb` response with our query results:
```json
{
    "name": "Oliver",
    "created_at": "2019-12-20T16:02:59.783038",
    "blog_post_collection_by_id_to_author_id": [
        {
            "id": 1,
            "title": "A GraphQL to SQL Webserver for every Language"
        },
        {
            "id": 2,
            "title": "Sanitize all the things!"
        }
    ]
}
```
