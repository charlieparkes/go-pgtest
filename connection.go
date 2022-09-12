package pgtest

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// Since you shouldn't create a pgx.ConnConfig from scratch, ConnectionSettings exists to hold all the
// parameters we'll need to work with throughout the lifecycle of a postgres database throughout tests.
type ConnectionSettings struct {
	Host         string
	Port         string
	User         string
	Password     string
	Database     string
	DisableSSL   bool
	MaxOpenConns int
}

func (cs *ConnectionSettings) DSN() string {
	sslmode := "require"
	if cs.DisableSSL {
		sslmode = "disable"
	}
	return fmt.Sprintf("host=%v port=%v user=%v password=%v dbname=%v sslmode=%v",
		cs.Host,
		cs.Port,
		cs.User,
		cs.Password,
		cs.Database,
		sslmode,
	)
}

func (cs *ConnectionSettings) URL() string {
	sslmode := "require"
	if cs.DisableSSL {
		sslmode = "disable"
	}
	return fmt.Sprintf("postgresql://%v:%v@%v:%v/%v?sslmode=%v",
		cs.User,
		cs.Password,
		cs.Host,
		cs.Port,
		cs.Database,
		sslmode,
	)
}

func (cs *ConnectionSettings) String() string {
	return cs.DSN()
}

func (cs *ConnectionSettings) Config() (*pgx.ConnConfig, error) {
	return pgx.ParseConfig(cs.String())
}

func (cs *ConnectionSettings) PoolConfig() (*pgxpool.Config, error) {
	return pgxpool.ParseConfig(cs.String())
}

func (cs *ConnectionSettings) Copy() *ConnectionSettings {
	s := *cs
	return &s
}

func (cs *ConnectionSettings) Connect(ctx context.Context) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, cs.String())
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}
	return conn, nil
}
