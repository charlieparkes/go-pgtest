package pgtest

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/charlieparkes/go-fixtures/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgres(t *testing.T) {
	ctx := context.Background()
	opts := []Opt{OptNetworkName(os.Getenv("HOST_NETWORK_NAME"))}

	p, err := NewPostgres(ctx, opts...)
	require.NoError(t, err)
	defer p.RecoverTearDown(ctx)

	require.NoError(t, p.PingPsql(ctx))

	// Connect
	db, err := p.Connect(ctx)
	require.NoError(t, err)
	if err == nil {
		db.Close()
	}

	// LoadSqlPattern, Tables, TableExists
	exists, err := p.TableExists(ctx, "", "public", "address")
	assert.NoError(t, err)
	assert.False(t, exists)

	require.NoError(t, p.LoadSqlPattern(ctx, "./testdata/migrations/*.sql"))

	tables, err := p.Tables(ctx, "")
	require.NoError(t, err)
	assert.Len(t, tables, 2)

	exists, err = p.TableExists(ctx, "", "public", "address")
	assert.NoError(t, err)
	assert.True(t, exists)
	if !exists {
		tables, err := p.Tables(ctx, "")
		require.NoError(t, err)
		fmt.Println(tables)
	}

	// ValidateModel
	require.NoError(t, p.ValidateModels(ctx, "", &Person{}))

	// Dump
	require.NoError(t, p.Dump(ctx, "testdata/tmp", "test.pgdump"))

	// CreateDatabase
	name := fixtures.GetRandomName(0)
	require.NoError(t, p.CreateDatabase(ctx, name))

	db, err = p.Connect(ctx, PostgresConnDatabase(name))
	require.NoError(t, err)
	if err == nil {
		db.Close()
	}

	// CopyDatabase
	databaseName := fixtures.GetRandomName(0)
	require.NoError(t, p.CopyDatabase(ctx, "", databaseName))

	db, err = p.Connect(ctx, PostgresConnDatabase(databaseName))
	require.NoError(t, err)
	if err == nil {
		db.Close()
	}

	tables, err = p.Tables(ctx, databaseName)
	require.NoError(t, err)
	assert.Len(t, tables, 2)

	// Original exists.
	exists, err = p.TableExists(ctx, "", "public", "address")
	assert.NoError(t, err)
	assert.True(t, exists)
	// New copy exists.
	exists, err = p.TableExists(ctx, databaseName, "public", "address")
	assert.NoError(t, err)
	assert.True(t, exists)

	// ConnectCopyDatabase
	db, err = p.Connect(ctx, PostgresConnCreateCopy())
	require.NoError(t, err)
	if err == nil {
		db.Close()
	}

	tables, err = p.Tables(ctx, db.Config().ConnConfig.Database)
	require.NoError(t, err)
	assert.Len(t, tables, 2)

	// Original.
	exists, err = p.TableExists(ctx, "", "public", "address")
	assert.NoError(t, err)
	assert.True(t, exists)
	// New copy.
	exists, err = p.TableExists(ctx, db.Config().ConnConfig.Database, "public", "address")
	assert.NoError(t, err)
	assert.True(t, exists)

	// Teardown
	require.NoError(t, p.TearDown(ctx))

	// Restore
	p2, err := NewPostgres(ctx, opts...)
	require.NoError(t, err)
	defer p2.RecoverTearDown(ctx)

	assert.NoError(t, p2.Restore(ctx, "testdata/tmp", "test.pgdump"))

	db, err = p2.Connect(ctx)
	require.NoError(t, err)
	if err == nil {
		db.Close()
	}

	tables, err = p2.Tables(ctx, "")
	require.NoError(t, err)
	assert.Len(t, tables, 2)

	exists, err = p2.TableExists(ctx, "", "public", "address")
	assert.NoError(t, err)
	assert.True(t, exists)

	require.NoError(t, p2.TearDown(ctx))
}

type Person struct {
	Id        int64
	FirstName string
	LastName  string
	AddressId int64
	FooBar    bool `db:"-"`
}
