package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mansonxasthur/eshaar/internal/ports"
)

const testTopic = "notifications"

func publish(t *testing.T, brokers []string, keys ...string) {
	t.Helper()
	pub, err := NewPublisher(brokers, testTopic)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	for _, k := range keys {
		req := sampleRequest()
		req.IdempotencyKey = k
		if err := pub.Publish(context.Background(), req); err != nil {
			t.Fatalf("publish %s: %v", k, err)
		}
	}
}

func newTestConsumer(t *testing.T, brokers []string, group string) *Consumer {
	t.Helper()
	c, err := NewConsumer(brokers, testTopic, group, 100, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func keysOf(b ports.Batch) []string {
	var keys []string
	for _, r := range b.Requests() {
		keys = append(keys, r.IdempotencyKey)
	}
	sort.Strings(keys)
	return keys
}

// pollAll polls until it has want requests, acking each batch, or fails.
func pollAll(t *testing.T, c *Consumer, want int) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var got []string
	for len(got) < want {
		b, err := c.Poll(ctx)
		if err != nil {
			t.Fatalf("Poll after %d of %d: %v", len(got), want, err)
		}
		got = append(got, keysOf(b)...)
		if err := b.Ack(ctx); err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(got)
	return got
}

// expectNothing fails if the consumer returns a batch within d.
func expectNothing(t *testing.T, c *Consumer, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	b, err := c.Poll(ctx)
	if err == nil {
		t.Fatalf("unexpected batch %v", keysOf(b))
	}
}

func TestAckedBatchIsNotRedelivered(t *testing.T) {
	brokers := startKafka(t, testTopic)
	publish(t, brokers, "k1", "k2", "k3")

	first := newTestConsumer(t, brokers, "g")
	ctx := context.Background()
	b, err := first.Poll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := keysOf(b)
	for len(got) < 3 {
		if err := b.Ack(ctx); err != nil {
			t.Fatal(err)
		}
		if b, err = first.Poll(ctx); err != nil {
			t.Fatal(err)
		}
		got = append(got, keysOf(b)...)
	}
	if err := b.Ack(ctx); err != nil {
		t.Fatal(err)
	}
	first.Close()

	// A new member of the same group resumes after the committed offsets.
	expectNothing(t, newTestConsumer(t, brokers, "g"), 5*time.Second)
}

func TestReleasedBatchIsRedelivered(t *testing.T) {
	brokers := startKafka(t, testTopic)
	publish(t, brokers, "k1", "k2", "k3", "k4", "k5", "k6")

	c := newTestConsumer(t, brokers, "g")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Wait until the whole topic arrives in one batch so the comparison is exact.
	var want []string
	for {
		b, err := c.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		want = keysOf(b)
		b.Release()
		if len(want) == 6 {
			break
		}
	}

	// Everything released comes back, with nothing lost and nothing acked.
	var again []string
	for len(again) < 6 {
		b, err := c.Poll(ctx)
		if err != nil {
			t.Fatalf("Poll after release: %v (got %v)", err, again)
		}
		again = keysOf(b)
		b.Release()
	}
	if fmt.Sprint(again) != fmt.Sprint(want) {
		t.Errorf("redelivered %v, want %v", again, want)
	}
}

func TestPoisonRecordIsSkippedAndCommitted(t *testing.T) {
	brokers := startKafka(t, testTopic)
	raw, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(testTopic))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	ctx := context.Background()
	if err := raw.ProduceSync(ctx, &kgo.Record{Key: []byte("bad"), Value: []byte("not json")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	publish(t, brokers, "good")

	c := newTestConsumer(t, brokers, "g")
	got := pollAll(t, c, 1)
	if len(got) != 1 || got[0] != "good" {
		t.Fatalf("got %v, want only the good request", got)
	}
	c.Close()
	// The poison record was committed with its batch: it is not read again.
	expectNothing(t, newTestConsumer(t, brokers, "g"), 5*time.Second)
}

func TestGroupMembersShareTheTopic(t *testing.T) {
	brokers := startKafka(t, testTopic)
	var keys []string
	for i := range 30 {
		keys = append(keys, fmt.Sprintf("k%02d", i))
	}
	publish(t, brokers, keys...)

	var mu sync.Mutex
	seen := map[string]int{}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for range 2 {
		c := newTestConsumer(t, brokers, "g")
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				b, err := c.Poll(ctx)
				if err != nil {
					return
				}
				mu.Lock()
				for _, k := range keysOf(b) {
					seen[k]++
				}
				done := len(seen) == len(keys)
				mu.Unlock()
				_ = b.Ack(ctx)
				if done {
					cancel()
				}
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != len(keys) {
		t.Fatalf("read %d of %d keys", len(seen), len(keys))
	}
	for k, n := range seen {
		if n != 1 {
			t.Errorf("%s read %d times, want once across the group", k, n)
		}
	}
}

func TestNewConsumerRejectsBadBatchSize(t *testing.T) {
	if _, err := NewConsumer([]string{"127.0.0.1:1"}, "t", "g", 0, slog.New(slog.DiscardHandler)); err == nil {
		t.Fatal("accepted batch size 0")
	}
}
