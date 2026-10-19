// Command sender is an Eshaar entry point.
package main

import "github.com/mansonxasthur/eshaar/internal/platform/logging"

func main() {
	log := logging.New().With("component", "sender")
	log.Info("starting")
}
