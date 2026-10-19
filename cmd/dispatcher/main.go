// Command dispatcher is an Eshaar entry point.
package main

import "github.com/mansonxasthur/eshaar/internal/platform/logging"

func main() {
	log := logging.New().With("component", "dispatcher")
	log.Info("starting")
}
