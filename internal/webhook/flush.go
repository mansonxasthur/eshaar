package webhook

import (
	"context"
	"errors"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
)

// isolateAfter is how many events in a row may fail on their own, after their
// batch failed, before the rest of the batch gets the batch's error without
// another try. One bad event should not fail its neighbors, but when the
// database is down every retry would fail and wait.
const isolateAfter = 2

// flushLoop takes events from the queue, gathers a batch of up to BatchSize
// for at most FlushInterval from its first event, and saves it. It returns when
// the queue is closed and empty.
func (s *Service) flushLoop() {
	for {
		first, ok := <-s.queue
		if !ok {
			return
		}
		batch := []*item{first}
		timer := time.NewTimer(s.cfg.FlushInterval)
	fill:
		for len(batch) < s.cfg.BatchSize {
			select {
			case it, ok := <-s.queue:
				if !ok {
					break fill
				}
				batch = append(batch, it)
			case <-timer.C:
				break fill
			}
		}
		timer.Stop()
		s.flush(batch)
	}
}

// flush saves a batch with one statement and answers every request in it. The
// statement is atomic, so a failed batch saved nothing. A batch of several
// events that fails is then tried one event at a time, so a single event the
// database refuses fails only its own request.
func (s *Service) flush(batch []*item) {
	batchErr := s.save(batch)
	if batchErr == nil {
		answer(batch, nil)
		return
	}
	if len(batch) == 1 {
		s.log.Error("save webhook event", "error", batchErr, "provider", batch[0].event.Provider, "event_id", batch[0].event.EventID)
		answer(batch, batchErr)
		return
	}
	if errors.Is(batchErr, context.DeadlineExceeded) || errors.Is(batchErr, context.Canceled) {
		// A slow database, not a bad event: retrying one by one would take
		// FlushTimeout again for each.
		s.log.Error("save webhook batch timed out", "events", len(batch), "error", batchErr)
		answer(batch, batchErr)
		return
	}
	s.log.Warn("save webhook batch failed, retrying its events one by one", "events", len(batch), "error", batchErr)
	failures := 0
	for _, it := range batch {
		if failures >= isolateAfter {
			it.done <- batchErr
			continue
		}
		err := s.save([]*item{it})
		if err != nil {
			failures++
			s.log.Error("save webhook event", "error", err, "provider", it.event.Provider, "event_id", it.event.EventID)
		} else {
			failures = 0
		}
		it.done <- err
	}
}

func (s *Service) save(batch []*item) error {
	events := make([]domain.WebhookEvent, len(batch))
	for i, it := range batch {
		events[i] = it.event
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.FlushTimeout)
	defer cancel()
	return s.store.RecordEvents(ctx, events)
}

func answer(batch []*item, err error) {
	for _, it := range batch {
		it.done <- err
	}
}
