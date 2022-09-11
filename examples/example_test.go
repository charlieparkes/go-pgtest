package example

import (
	"context"
	"os"
	"testing"

	"github.com/charlieparkes/go-pgtest"
)

var p *pgtest.Postgres

func TestMain(m *testing.M) {
	ctx := context.Background()
	p = pgtest.Must(pgtest.NewPostgres(ctx))
	defer p.RecoverTearDown(ctx)
	status := m.Run()
	p.TearDown(ctx)
	os.Exit(status)
}

func TestExample(t *testing.T) {
	ctx := context.Background()
	pool := p.MustConnect(ctx)
	defer pool.Close()
}
