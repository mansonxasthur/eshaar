package webhook

import (
	"context"
	"time"
)

// resolveLoop links events that arrived before their notification was accepted,
// every ResolveInterval, until ctx is done.
func (s *Service) resolveLoop(ctx context.Context) {
	t := time.NewTicker(s.cfg.ResolveInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.resolveOnce(ctx)
		}
	}
}

// resolveOnce runs passes while they come back full, up to maxResolvePasses. A
// failed pass is logged and left for the next tick.
func (s *Service) resolveOnce(ctx context.Context) {
	for pass := 0; pass < maxResolvePasses; pass++ {
		n, err := s.store.ResolvePending(ctx, s.cfg.ResolveBatch)
		if err != nil {
			if ctx.Err() == nil {
				s.log.WarnContext(ctx, "resolve pending webhook events", "error", err)
			}
			return
		}
		if n > 0 {
			s.log.InfoContext(ctx, "linked webhook events that arrived before their accept", "events", n)
		}
		if n < s.cfg.ResolveBatch {
			return
		}
	}
}
