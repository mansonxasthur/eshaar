package webhook

import (
	"sync"
	"time"
)

// logEvery is the shortest gap between two log lines of one kind.
const logEvery = time.Second

// throttle limits a log line to one per logEvery and counts what it held back,
// so a flood of refused or malformed events stays visible without filling the
// log.
type throttle struct {
	mu      sync.Mutex
	last    time.Time
	skipped int
}

// allow reports whether to log now. When it does, skipped is how many calls
// were held back since the last line.
func (t *throttle) allow(now time.Time) (ok bool, skipped int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.last.IsZero() && now.Sub(t.last) < logEvery {
		t.skipped++
		return false, 0
	}
	t.last, skipped, t.skipped = now, t.skipped, 0
	return true, skipped
}
