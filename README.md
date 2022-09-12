# go-pgtest

Import `github.com/charlieparkes/go-pgtest`.

## Example

For a full example, see [examples/](./examples/).

```go
    p, err := pgtest.NewPostgres(ctx)
    pool, err := p.Connect(ctx)
    p.TearDown(ctx)
```