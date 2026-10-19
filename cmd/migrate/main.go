// Command migrate applies the database migrations and exits.
package main

import (
	"os"

	"github.com/mansonxasthur/eshaar/internal/adapters/postgres"
	"github.com/mansonxasthur/eshaar/internal/platform/config"
	"github.com/mansonxasthur/eshaar/internal/platform/logging"
)

func main() {
	log := logging.New().With("component", "migrate")

	cfg, err := config.Load(os.Getenv, config.NeedDatabase)
	if err != nil {
		log.Error("load config", "error", err)
		os.Exit(1)
	}
	if err := postgres.MigrateUp(cfg.DatabaseURL); err != nil {
		log.Error("migrate", "error", err)
		os.Exit(1)
	}
	log.Info("migrations applied")
}
