package ports

import (
	"context"

	"github.com/mansonxasthur/eshaar/internal/domain"
)

// Consumer reads batches of requests from the queue between the dispatcher and
// the sender. Each consumer of a group reads its own partitions in order.
type Consumer interface {
	// Poll blocks until a batch is available or ctx is done. Until the batch is
	// acked or released, the consumer reads nothing further from those
	// partitions.
	Poll(ctx context.Context) (Batch, error)
}

// Batch is a group of requests read together. Exactly one of Ack or Release
// must be called.
type Batch interface {
	Requests() []domain.Request

	// Ack tells the queue the whole batch is saved, so it is not redelivered.
	Ack(ctx context.Context) error

	// Release gives the batch back unacknowledged, so the queue redelivers it.
	Release()
}
