// Command webhook is an Eshaar entry point.
package main

import (
	"os"

	"github.com/mansonxasthur/eshaar/internal/platform/config"
	"github.com/mansonxasthur/eshaar/internal/platform/logging"
)

func main() {
	log := logging.New().With("component", "webhook")

	if _, err := config.Load(os.Getenv, config.NeedDatabase|config.NeedHTTP); err != nil {
		log.Error("load config", "error", err)
		os.Exit(1)
	}
	log.Info("starting")
}
