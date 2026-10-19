// Package postgres is the Postgres adapter: schema migrations and, in later
// phases, the notification store.
package postgres

import (
	"embed"
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // registers the pgx5:// driver
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// MigrateUp applies every pending migration to the database at databaseURL
// (a postgres:// URL). It is safe to call on an up-to-date database.
func MigrateUp(databaseURL string) error {
	return withMigrator(databaseURL, func(m *migrate.Migrate) error {
		if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return fmt.Errorf("apply migrations: %w", err)
		}
		return nil
	})
}

// withMigrator opens a migrator for databaseURL, runs fn and closes it.
func withMigrator(databaseURL string, fn func(*migrate.Migrate) error) (err error) {
	url, err := toPgx5(databaseURL)
	if err != nil {
		return err
	}
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("open migrations: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, url)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() {
		srcErr, dbErr := m.Close()
		err = errors.Join(err, srcErr, dbErr)
	}()
	return fn(m)
}

// toPgx5 rewrites a postgres:// or postgresql:// URL to the scheme
// golang-migrate's pgx v5 driver registers, and rejects anything else.
func toPgx5(u string) (string, error) {
	for _, p := range []string{"postgresql://", "postgres://"} {
		if rest, ok := strings.CutPrefix(u, p); ok {
			return "pgx5://" + rest, nil
		}
	}
	return "", errors.New("database URL must start with postgres:// or postgresql://")
}
