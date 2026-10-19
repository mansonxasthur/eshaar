package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

const (
	// maxBodyBytes caps a request body; a notification is a few kilobytes. It
	// sits well under Kafka's default 1 MB record limit, which a larger record
	// could never satisfy.
	maxBodyBytes = 512 << 10
	// publishTimeout bounds the wait for Kafka to confirm.
	publishTimeout = 5 * time.Second
)

// Handler serves the dispatcher API.
type Handler struct {
	publisher ports.Publisher
	log       *slog.Logger
}

// NewHandler returns the dispatcher handler. Authentication and rate limiting
// are middleware, applied by Routes.
func NewHandler(publisher ports.Publisher, log *slog.Logger) *Handler {
	return &Handler{publisher: publisher, log: log}
}

// Routes returns the HTTP routes. /v1/notifications runs behind mw (in order);
// /healthz does not.
func (h *Handler) Routes(mw ...Middleware) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /v1/notifications", Chain(http.HandlerFunc(h.createNotification), mw...))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

func (h *Handler) createNotification(w http.ResponseWriter, r *http.Request) {
	var req domain.Request
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		if errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "request body is empty")
			return
		}
		writeError(w, http.StatusBadRequest, "malformed JSON body")
		return
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "body must be a single JSON object")
		return
	}

	if err := req.Validate(); err != nil {
		var ve *domain.ValidationError
		if errors.As(err, &ve) {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid request", "fields": ve.Fields})
			return
		}
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), publishTimeout)
	defer cancel()
	if err := h.publisher.Publish(ctx, req); err != nil {
		// The details stay in the log; the caller can safely retry with the
		// same idempotency key, since the sender dedupes.
		h.log.ErrorContext(r.Context(), "publish failed", "idempotency_key", req.IdempotencyKey, "error", err)
		writeError(w, http.StatusServiceUnavailable, "could not accept the notification, retry later")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted", "idempotency_key": req.IdempotencyKey})
}
