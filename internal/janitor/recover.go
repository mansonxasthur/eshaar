package janitor

import (
	"context"
	"sync"
)

// recover claims a batch of stale rows and re-drives them, at most
// Concurrency at a time. A claim already used one of a row's attempts, so a
// shutdown does not drop claimed rows: the pass finishes them, which the
// batch-size rule keeps to about one send deadline.
func (j *Janitor) recover(ctx context.Context) {
	rows, err := j.store.ClaimStale(ctx, j.cfg.StaleAfter, j.cfg.BatchSize)
	if err != nil {
		if ctx.Err() == nil {
			j.log.ErrorContext(ctx, "claim stale rows", "error", err)
		}
		return
	}
	if len(rows) == 0 {
		return
	}
	j.log.InfoContext(ctx, "re-driving stale rows", "count", len(rows))

	var wg sync.WaitGroup
	slots := make(chan struct{}, j.cfg.Concurrency)
	for _, n := range rows {
		slots <- struct{}{}
		wg.Go(func() {
			defer func() { <-slots }()
			j.recoverRow(ctx, n)
		})
	}
	wg.Wait()
}
