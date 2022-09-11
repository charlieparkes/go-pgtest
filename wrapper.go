package pgtest

import (
	"context"
	"fmt"

	"github.com/charlieparkes/go-fixtures/v2"
)

type Postgres struct {
	fixture
	f           *fixtures.Fixtures
	networkName string
}

func NewPostgres(ctx context.Context, opts ...Opt) (*Postgres, error) {
	p := &Postgres{}
	p.f = fixtures.NewFixtures()
	for _, opt := range opts {
		opt(p)
	}

	// Docker
	if err := p.f.Add(ctx, fixtures.NewDocker(
		fixtures.DockerNetworkName(p.networkName),
	)); err != nil {
		return nil, fmt.Errorf("failed to setup docker: %w", err)
	}
	p.docker = p.f.Docker()

	// Postgres
	if err := p.f.Add(ctx, &p.fixture); err != nil {
		return nil, fmt.Errorf("failed to setup postgres: %w", err)
	}

	// Healthcheck
	if err := p.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}
	return p, nil
}

func (p *Postgres) TearDown(ctx context.Context) error {
	return p.f.TearDown(ctx)
}

func (p *Postgres) RecoverTearDown(ctx context.Context) {
	if p == nil {
		return
	}
	p.f.RecoverTearDown(ctx)
}

func Must(p *Postgres, err error) *Postgres {
	if err != nil {
		panic(err)
	}
	return p
}
