# Postgres

In this example, postgres is run once per package, and the database is cloned for each individual test to ensure idempotency.


## Debugging Tips

1. Disable teardown `pgtest.NewPostgres(ctx, pgtest.OptSkipTearDown())`
2. `docker exec -it bash CONTAINER_ID`
3. `psql -Upostgres`
