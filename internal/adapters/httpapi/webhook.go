package httpapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/mansonxasthur/eshaar/internal/ports"
)

const (
	// maxWebhookBytes caps a webhook body; an event is a few hundred bytes.
	maxWebhookBytes = 64 << 10
	// receiveTimeout bounds the wait for an event to be saved: the batch filling
	// and one statement. Reading the body can take up to the server's read
	// timeout (10 s), so the two together stay under its write timeout (20 s).
	receiveTimeout = 5 * time.Second
	// SignatureHeader carries the hex HMAC-SHA256 of the raw body.
	SignatureHeader = "X-Signature"
)

// WebhookHandler serves the provider webhook endpoint.
type WebhookHandler struct {
	receiver ports.EventReceiver
	log      *slog.Logger
}

// NewWebhookHandler returns the webhook handler.
func NewWebhookHandler(receiver ports.EventReceiver, log *slog.Logger) *WebhookHandler {
	return &WebhookHandler{receiver: receiver, log: log}
}

// Routes returns the HTTP routes. Webhooks are authenticated by their
// signature, so there is no API key or rate limit in front of them.
func (h *WebhookHandler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/webhooks/{provider}", h.receive)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

// receive answers 200 once the event is stored, including a duplicate or one
// that cannot be linked yet. 401 covers a wrong signature and an unknown
// provider alike, so a caller cannot probe which providers exist; 400 is a
// signed body that is not a valid event; anything else is ours and is a 503,
// so the provider retries.
func (h *WebhookHandler) receive(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxWebhookBytes))
	if err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "could not read the request body")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), receiveTimeout)
	defer cancel()
	err = h.receiver.Receive(ctx, r.PathValue("provider"), body, r.Header.Get(SignatureHeader))
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case errors.Is(err, ports.ErrUnknownProvider), errors.Is(err, ports.ErrInvalidSignature):
		writeError(w, http.StatusUnauthorized, "invalid signature")
	case errors.Is(err, ports.ErrInvalidEvent):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ports.ErrShuttingDown), errors.Is(err, context.Canceled):
		// A restart or a caller that hung up: not a fault in the store, and the
		// provider retries.
		h.log.WarnContext(r.Context(), "webhook not stored", "error", err)
		writeError(w, http.StatusServiceUnavailable, "could not store the event, retry later")
	default:
		// Includes a wait that ran out (context.DeadlineExceeded), which means the
		// store stalled. The details stay in the log; the provider retries.
		h.log.ErrorContext(r.Context(), "webhook not stored", "error", err)
		writeError(w, http.StatusServiceUnavailable, "could not store the event, retry later")
	}
}
