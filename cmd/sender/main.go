// Command sender is an Eshaar entry point.
package main

import (
	"os"

	"github.com/mansonxasthur/eshaar/internal/platform/config"
	"github.com/mansonxasthur/eshaar/internal/platform/logging"
)

func main() {
	log := logging.New().With("component", "sender")

	if _, err := config.Load(os.Getenv, config.NeedDatabase|config.NeedKafka); err != nil {
		log.Error("load config", "error", err)
		os.Exit(1)
	}
	log.Info("starting")
}
