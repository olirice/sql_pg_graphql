

Reccomended method to set up database for development
```shell
docker run --name pg -p 5432:5432 -d -e POSTGRES_DB=pg_graphql -d postgres
```
