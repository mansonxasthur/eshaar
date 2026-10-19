package janitor

import "context"

// sweep marks rows that used all their attempts unresolved, a batch at a time
// until a batch comes back short.
func (j *Janitor) sweep(ctx context.Context) {
	total := 0
	for ctx.Err() == nil {
		n, err := j.store.SweepExhausted(ctx, j.cfg.StaleAfter, j.cfg.BatchSize)
		total += n
		if err != nil {
			if ctx.Err() == nil {
				j.log.ErrorContext(ctx, "sweep exhausted rows", "error", err)
			}
			break
		}
		if n < j.cfg.BatchSize {
			break
		}
	}
	if total > 0 {
		j.log.WarnContext(ctx, "marked rows unresolved after their last attempt", "count", total)
	}
}
