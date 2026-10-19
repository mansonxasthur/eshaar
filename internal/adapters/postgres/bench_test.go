package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/mansonxasthur/eshaar/internal/domain"
)

// These benchmarks run against a real Postgres 16 in a container with its
// default settings (durable commits), so they skip under -short or without
// Docker. They measure the writes the load test found to be the bottleneck.

var (
	benchOnce  sync.Once
	benchPool  *Store
	benchSkip  string
	benchFatal error
)

// benchStore returns a store on one migrated Postgres shared by every
// benchmark of the run, so the framework's repeated runs with a growing b.N do
// not each start a container. The container is not stopped explicitly: the
// testcontainers reaper removes it when the process exits.
func benchStore(b *testing.B) *Store {
	b.Helper()
	if testing.Short() {
		b.Skip("skipping Postgres container benchmark in -short mode")
	}
	benchOnce.Do(func() {
		ctx := context.Background()
		ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
			tcpostgres.WithDatabase("eshaar"), tcpostgres.WithUsername("eshaar"), tcpostgres.WithPassword("eshaar"),
			testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second)),
		)
		if err != nil {
			benchSkip = fmt.Sprintf("no Postgres container (is Docker running?): %v", err)
			return
		}
		url, err := ctr.ConnectionString(ctx, "sslmode=disable")
		if err == nil {
			err = MigrateUp(url)
		}
		if err == nil {
			benchPool, err = NewStore(ctx, url)
		}
		benchFatal = err
	})
	if benchSkip != "" {
		b.Skip(benchSkip)
	}
	if benchFatal != nil {
		b.Fatal(benchFatal)
	}
	return benchPool
}

// seq makes keys unique across the repeated runs the framework makes with a
// growing b.N.
var seq atomic.Int64

// requestRows returns the rows of one request with all three channels.
func requestRows() []domain.Notification {
	key := fmt.Sprintf("bench-%d", seq.Add(1))
	email, phone := "a@example.com", "+966500000000"
	now := time.Now().UTC().Truncate(time.Microsecond)
	mk := func(ch domain.Channel, content string) domain.Notification {
		n := domain.Notification{IdempotencyKey: key, RecipientID: "u1", NotificationType: "match_started", Channel: ch,
			Content: json.RawMessage(content), State: domain.StatePending, CreatedAt: now}
		if ch == domain.ChannelEmail {
			n.Email = &email
		} else {
			n.Phone = &phone
		}
		return n
	}
	return []domain.Notification{
		mk(domain.ChannelEmail, `{"template_id":"t","variables":{"team_a":"الهلال"}}`),
		mk(domain.ChannelSMS, `{"text":"مباراة الهلال والنصر تبدأ الساعة 8 مساءً"}`),
		mk(domain.ChannelWhatsApp, `{"template_id":"t","variables":{"team_a":"الهلال"}}`),
	}
}

// batchOf returns the rows of requests requests.
func batchOf(requests int) []domain.Notification {
	var rows []domain.Notification
	for range requests {
		rows = append(rows, requestRows()...)
	}
	return rows
}

// seedPending inserts n pending rows (outside the timed part) and returns their ids.
func seedPending(b *testing.B, s *Store, n int) []string {
	b.Helper()
	ids := make([]string, 0, n)
	for len(ids) < n {
		got, err := s.InsertPending(context.Background(), batchOf(100))
		if err != nil {
			b.Fatal(err)
		}
		for _, r := range got {
			ids = append(ids, r.ID)
		}
	}
	return ids[:n]
}

// BenchmarkInsertPending is the sender's save of one polled batch: 50 requests
// with three channels each, 150 rows in one statement.
func BenchmarkInsertPending(b *testing.B) {
	s := benchStore(b)
	const requests = 50
	batches := make([][]domain.Notification, b.N)
	for i := range batches {
		batches[i] = batchOf(requests)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for _, rows := range batches {
		if _, err := s.InsertPending(context.Background(), rows); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*requests*3)/b.Elapsed().Seconds(), "rows/s")
}

// BenchmarkMarkAccepted is the outcome write as the store does it today: one
// UPDATE and one commit per notification, from one goroutine.
func BenchmarkMarkAccepted(b *testing.B) {
	s := benchStore(b)
	ids := seedPending(b, s, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i, id := range ids {
		if err := s.MarkAccepted(context.Background(), id, "sms-a", fmt.Sprintf("m-%d-%d", seq.Add(1), i)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "rows/s")
}

// BenchmarkMarkAcceptedParallel is the same from many goroutines, which is how
// the sender's pool calls it. Concurrent commits share a WAL flush, so this is
// the figure that compares with the load test.
func BenchmarkMarkAcceptedParallel(b *testing.B) {
	s := benchStore(b)
	ids := seedPending(b, s, b.N)
	var next atomic.Int64
	b.SetParallelism(32)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := next.Add(1) - 1
			if err := s.MarkAccepted(context.Background(), ids[i], "sms-a", fmt.Sprintf("m-%d-%d", seq.Add(1), i)); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "rows/s")
}

// BenchmarkMarkAcceptedBatchedReference is NOT how the store writes outcomes. It
// is an experiment: the same update for 100 rows in one statement, to show what
// batching the outcome writes (the follow-up in docs/solution.md, on the Phase 7 branch until it merges) would be worth.
func BenchmarkMarkAcceptedBatchedReference(b *testing.B) {
	s := benchStore(b)
	ids := seedPending(b, s, b.N)
	const batch = 100
	b.ReportAllocs()
	b.ResetTimer()
	for lo := 0; lo < len(ids); lo += batch {
		hi := min(lo+batch, len(ids))
		msgs := make([]string, 0, hi-lo)
		for i := lo; i < hi; i++ {
			msgs = append(msgs, fmt.Sprintf("m-%d-%d", seq.Add(1), i))
		}
		_, err := s.pool.Exec(context.Background(), `
UPDATE notifications n
SET state = 'accepted', provider = 'sms-a', provider_message_id = u.msg, accepted_at = now(), updated_at = now()
FROM unnest($1::uuid[], $2::text[]) AS u(id, msg)
WHERE n.id = u.id AND n.state = 'pending'`, ids[lo:hi], msgs)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "rows/s")
}
