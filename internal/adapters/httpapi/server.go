package httpapi

import (
	"net/http"
	"time"
)

// NewServer returns an http.Server for h with timeouts that bound slow clients.
func NewServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      20 * time.Second, // covers reading the body and the Kafka wait
		IdleTimeout:       60 * time.Second,
	}
}
