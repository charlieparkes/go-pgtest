package pgtest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/charlieparkes/go-fixtures/v2"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/ory/dockertest/v3"
	"go.uber.org/zap"
)

const (
	DEFAULT_POSTGRES_REPO    = "postgres"
	DEFAULT_POSTGRES_VERSION = "13-alpine"
)

type fixture struct {
	fixtures.BaseFixture
	log          *zap.Logger
	docker       *fixtures.Docker
	settings     *ConnectionSettings
	resource     *dockertest.Resource
	repo         string
	version      string
	name         string
	expireAfter  uint
	timeoutAfter uint
	skipTearDown bool
	mounts       []string
}

func (f *fixture) Settings() *ConnectionSettings {
	return f.settings
}

func (f *fixture) SetUp(ctx context.Context) error {
	if f.log == nil {
		f.log = zap.Must(zap.NewDevelopment())
	}
	if f.repo == "" {
		f.repo = DEFAULT_POSTGRES_REPO
	}
	if f.version == "" {
		f.version = DEFAULT_POSTGRES_VERSION
	}
	if f.name == "" {
		f.name = "postgres"
	}
	if f.settings == nil {
		f.settings = &ConnectionSettings{
			User:       "postgres",
			Password:   fixtures.GenerateString(),
			Database:   f.name,
			DisableSSL: true,
		}
	}

	networks := make([]*dockertest.Network, 0)
	if f.docker.Network() != nil {
		networks = append(networks, f.docker.Network())
	}
	opts := dockertest.RunOptions{
		Name:       f.name + "_" + fixtures.GetRandomName(0),
		Repository: f.repo,
		Tag:        f.version,
		Env: []string{
			"POSTGRES_USER=" + f.settings.User,
			"POSTGRES_PASSWORD=" + f.settings.Password,
			"POSTGRES_DB=" + f.settings.Database,
		},
		Networks: networks,
		Cmd: []string{
			// https://www.postgresql.org/docs/current/non-durability.html
			"-c", "fsync=off",
			"-c", "synchronous_commit=off",
			"-c", "full_page_writes=off",
			"-c", "random_page_cost=1.1",
			"-c", fmt.Sprintf("shared_buffers=%vMB", fixtures.MemoryMB()/8),
			"-c", fmt.Sprintf("work_mem=%vMB", fixtures.MemoryMB()/8),
		},
		Mounts: f.mounts,
	}

	var err error
	f.resource, err = f.docker.Pool().RunWithOptions(&opts)
	if err != nil {
		return err
	}

	f.settings.Host = fixtures.ContainerAddress(f.resource, f.docker.Network())

	if f.expireAfter == 0 {
		f.expireAfter = 600
	}
	f.resource.Expire(f.expireAfter)

	if f.timeoutAfter == 0 {
		f.timeoutAfter = 30
	}
	if err := f.WaitForReady(ctx, time.Second*time.Duration(f.timeoutAfter)); err != nil {
		return err
	}
	return nil
}

func (f *fixture) TearDown(ctx context.Context) error {
	if f.skipTearDown {
		return nil
	}
	f.docker.Purge(f.resource)
	return nil
}

// RecoverTearDown returns a deferrable function that will teardown in the event of a panic.
func (f *fixture) RecoverTearDown(ctx context.Context) {
	if r := recover(); r != nil {
		if err := f.TearDown(ctx); err != nil {
			f.log.Warn("failed to tear down", zap.Error(err))
		}
		panic(r)
	}
}

type connConfig struct {
	poolConfig *pgxpool.Config
	role       string
	database   string
	createCopy bool
}

type ConnOpt func(*connConfig)

func ConnOptRole(role string) ConnOpt {
	return func(f *connConfig) {
		f.role = role
	}
}

func ConnOptDatabase(database string) ConnOpt {
	return func(f *connConfig) {
		if database != "" {
			f.database = database
		}
	}
}

func ConnOptCreateCopy() ConnOpt {
	return func(f *connConfig) {
		f.createCopy = true
	}
}

func (f *fixture) Connect(ctx context.Context, opts ...ConnOpt) (*pgxpool.Pool, error) {
	poolConfig, err := f.Settings().PoolConfig()
	if err != nil {
		return nil, err
	}
	cfg := &connConfig{
		poolConfig: poolConfig,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.database != "" {
		cfg.poolConfig.ConnConfig.Database = cfg.database
	}
	if cfg.createCopy {
		copiedDatabaseName := fixtures.GetRandomName(0)
		if err := f.CopyDatabase(ctx, cfg.database, copiedDatabaseName); err != nil {
			return nil, err
		}
		cfg.poolConfig.ConnConfig.Database = copiedDatabaseName
	}
	pool, err := pgxpool.ConnectConfig(ctx, cfg.poolConfig)
	if err != nil {
		return nil, err
	}
	if cfg.role != "" {
		_, err := pool.Exec(ctx, "set role "+cfg.role)
		if err != nil {
			return nil, fmt.Errorf("failed to assume role '%v': %w", cfg.role, err)
		}
	}
	return pool, nil
}

func (f *fixture) MustConnect(ctx context.Context, opts ...ConnOpt) *pgxpool.Pool {
	pool, err := f.Connect(ctx, opts...)
	if err != nil {
		panic(err)
	}
	return pool
}

func (f *fixture) HostName() string {
	return fixtures.HostName(f.resource)
}

func (f *fixture) Psql(ctx context.Context, cmd []string, mounts []string, quiet bool) (int, error) {
	// We're going to connect over the docker network
	settings := f.settings.Copy()
	settings.Host = fixtures.HostIP(f.resource, f.docker.Network())
	var err error
	opts := dockertest.RunOptions{
		Name:       "psql_" + fixtures.GetRandomName(0),
		Repository: "governmentpaas/psql", // God save the queen. 🇬🇧
		Tag:        "latest",
		Env: []string{
			"PGUSER=" + settings.User,
			"PGPASSWORD=" + settings.Password,
			"PGDATABASE=" + settings.Database,
			"PGHOST=" + settings.Host,
			"PGPORT=5432",
		},
		Mounts: mounts,
		Networks: []*dockertest.Network{
			f.docker.Network(),
		},
		Cmd: cmd,
	}
	resource, err := f.docker.Pool().RunWithOptions(&opts)
	if err != nil {
		return 0, err
	}
	exitCode, err := fixtures.WaitForContainer(f.docker.Pool(), resource)
	containerName := resource.Container.Name[1:]
	containerID := resource.Container.ID[0:11]
	if err != nil || exitCode != 0 && !quiet {
		f.log.Debug("psql failed", zap.Int("status", exitCode), zap.String("container_name", containerName), zap.String("container_id", containerID), zap.String("cmd", strings.Join(cmd, " ")))
		return exitCode, fmt.Errorf("psql exited with error (code: %v)", exitCode)
	}
	if f.skipTearDown {
		// If there was an issue, and debug is enabled, don't destroy the container.
		return exitCode, nil
	}
	f.docker.Purge(resource)
	return exitCode, nil
}

func (f *fixture) PingPsql(ctx context.Context) error {
	_, err := f.Psql(ctx, []string{"psql", "-c", ";"}, []string{}, false)
	return err
}

func (f *fixture) Ping(ctx context.Context) error {
	db, err := f.Connect(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Ping(ctx)
}

func (f *fixture) CreateDatabase(ctx context.Context, name string) error {
	if name == "" {
		return errors.New("must provide a database name")
	}
	exitCode, err := f.Psql(ctx, []string{"createdb", "--template=template0", name}, []string{}, false)
	f.log.Debug("create database", zap.Int("status", exitCode), zap.String("database", name), zap.String("container", f.HostName()))
	return err
}

// CopyDatabase creates a copy of an existing postgres database using `createdb --template={source} {target}`
// source will default to the primary database
func (f *fixture) CopyDatabase(ctx context.Context, source string, target string) error {
	if source == "" {
		source = f.settings.Database
	}
	exitCode, err := f.Psql(ctx, []string{"createdb", fmt.Sprintf("--template=%v", source), target}, []string{}, false)
	f.log.Debug("copy database", zap.Int("status", exitCode), zap.String("source", source), zap.String("target", target), zap.String("container", f.HostName()))
	return err
}

func (f *fixture) DropDatabase(ctx context.Context, name string) error {
	db, err := f.Connect(ctx, ConnOptDatabase(name))
	if err != nil {
		return err
	}
	defer db.Close()

	// Revoke future connections.
	_, err = db.Exec(ctx, fmt.Sprintf("REVOKE CONNECT ON DATABASE %v FROM public", name))
	if err != nil {
		return err
	}

	// Terminate all connections.
	_, err = db.Exec(ctx, "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()")
	if err != nil {
		return err
	}

	exitCode, err := f.Psql(ctx, []string{"dropdb", name}, []string{}, false)
	f.log.Debug("drop database", zap.Int("status", exitCode), zap.String("database", name), zap.String("container", f.HostName()))
	return err
}

func (f *fixture) Dump(ctx context.Context, dir string, filename string) error {
	path := fixtures.FindPath(dir)
	if path == "" {
		return fmt.Errorf("could not resolve path: %v", dir)
	}
	exitCode, err := f.Psql(ctx, []string{"sh", "-c", fmt.Sprintf("pg_dump -Fc -Z0 %v > /tmp/%v", f.settings.Database, filename)}, []string{fmt.Sprintf("%v:/tmp", path)}, false)
	f.log.Debug("dump database", zap.Int("status", exitCode), zap.String("database", f.settings.Database), zap.String("container", f.HostName()), zap.String("path", path))
	return err
}

func (f *fixture) Restore(ctx context.Context, dir string, filename string) error {
	path := fixtures.FindPath(dir)
	if path == "" {
		return fmt.Errorf("could not resolve path: %v", dir)
	}
	exitCode, err := f.Psql(ctx, []string{"sh", "-c", fmt.Sprintf("pg_restore --dbname=%v --verbose --single-transaction /tmp/%v", f.settings.Database, filename)}, []string{fmt.Sprintf("%v:/tmp", path)}, false)
	f.log.Debug("restore database", zap.Int("status", exitCode), zap.String("database", f.settings.Database), zap.String("container", f.HostName()), zap.String("path", path))
	return err
}

// LoadSql runs a file or directory of *.sql files against the default postgres database.
func (f *fixture) LoadSql(ctx context.Context, path string) error {
	load := func(p string) error {
		dir, err := filepath.Abs(filepath.Dir(p))
		if err != nil {
			return err
		}
		name := filepath.Base(p)
		exitCode, err := f.Psql(ctx, []string{"psql", fmt.Sprintf("--file=/tmp/%v", name)}, []string{fmt.Sprintf("%v:/tmp", dir)}, false)
		f.log.Debug("load sql", zap.Int("status", exitCode), zap.String("database", f.settings.Database), zap.String("container", f.HostName()), zap.String("name", name))
		if err != nil {
			return fmt.Errorf("failed to run psql (load sql): %w", err)
		}
		return nil
	}

	if info, err := os.Stat(path); err == nil {
		if info.IsDir() {
			files, err := filepath.Glob(filepath.Join(path, "*.sql"))
			if err != nil {
				return err
			}
			for _, path := range files {
				if err := load(path); err != nil {
					return err
				}
			}
		} else {
			return load(path)
		}
	}
	return nil
}

// LoadSqlPattern finds files matching a custom pattern and runs them against the default database.
func (f *fixture) LoadSqlPattern(ctx context.Context, pattern string) error {
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	for _, path := range files {
		err := f.LoadSql(ctx, path)
		if err != nil {
			return fmt.Errorf("failed to load test data: %w", err)
		}
	}
	return nil
}

// https://github.com/ory/dockertest/blob/v3/examples/PostgreSQL.md
// https://stackoverflow.com/a/63011266
func (f *fixture) WaitForReady(ctx context.Context, d time.Duration) error {
	if err := fixtures.Retry(d, func() error {
		var err error

		port := fixtures.ContainerTcpPort(f.resource, f.docker.Network(), "5432")
		if port == "" {
			err = fmt.Errorf("could not get port from container: %+v", f.resource.Container)
			return err
		}
		f.settings.Port = port

		status, err := f.Psql(ctx, []string{"pg_isready"}, []string{}, true)
		if err != nil {
			return err
		}
		if status != 0 {
			reason := "unknown"
			switch status {
			case 1:
				reason = "server is rejecting connections"
			case 2:
				reason = "no response"
			case 3:
				reason = "no attempt was made"
			}
			err = fmt.Errorf("postgres is not ready: (%v) %v", status, reason)
			return err
		}

		db, err := f.settings.Connect(ctx)
		if err != nil {
			return err
		}
		return db.Close(ctx)
	}); err != nil {
		return fmt.Errorf("gave up waiting for postgres: %w", err)
	}

	return nil
}

func (f *fixture) TableExists(ctx context.Context, database, schema, table string) (bool, error) {
	db, err := f.Connect(ctx, ConnOptDatabase(database))
	if err != nil {
		return false, err
	}
	defer db.Close()
	query := "SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname = $1 AND tablename = $2"
	count := 0
	if err := db.QueryRow(ctx, query, schema, table).Scan(&count); err != nil {
		return false, err
	}
	return count == 1, nil
}

func (f *fixture) TableColumns(ctx context.Context, database, schema, table string) ([]string, error) {
	db, err := f.Connect(ctx, ConnOptDatabase(database))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	var columnNames pgtype.TextArray
	query := fmt.Sprintf("SELECT array_agg(column_name::text) FROM information_schema.columns WHERE table_schema = '%v' AND table_name = '%v'", schema, table)
	if err := db.QueryRow(ctx,
		query,
	).Scan(&columnNames); err != nil {
		return nil, err
	}
	cols := make([]string, len(columnNames.Elements))
	for _, text := range columnNames.Elements {
		cols = append(cols, text.String)
	}
	return cols, nil
}

func (f *fixture) Tables(ctx context.Context, database string) ([]string, error) {
	db, err := f.Connect(ctx, ConnOptDatabase(database))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	tables := []string{}
	rows, err := db.Query(ctx, "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'information_schema' AND schemaname != 'pg_catalog'")
	if err != nil {
		return nil, fmt.Errorf("failed to query: %w", err)
	}
	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		if err != nil {
			return nil, fmt.Errorf("failed to scan: %w", err)
		}
		tables = append(tables, table)
	}
	return tables, nil
}
