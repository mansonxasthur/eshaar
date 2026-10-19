package ports

import (
	"context"

	"github.com/mansonxasthur/eshaar/internal/domain"
)

// Publisher hands a request to the queue between the dispatcher and the sender.
type Publisher interface {
	// Publish returns nil only after the queue has durably confirmed the
	// request. An error means the request may or may not have been stored (a
	// timeout can fire after the write lands), so callers must expect that a
	// retry with the same idempotency key produces a duplicate message, which
	// the sender dedupes.
	Publish(ctx context.Context, req domain.Request) error
}
