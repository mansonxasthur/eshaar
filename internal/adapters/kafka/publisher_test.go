package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mansonxasthur/eshaar/internal/domain"
)

func sampleRequest() domain.Request {
	return domain.Request{
		IdempotencyKey:   "match-1",
		RecipientID:      "u1",
		NotificationType: "match_started",
		Phone:            "+966500000000",
		Content: map[domain.Channel]json.RawMessage{
			domain.ChannelSMS: json.RawMessage(`{"text":"مباراة الهلال والنصر تبدأ الساعة 8 مساءً <b>"}`),
		},
	}
}

func TestEncodeKeepsUTF8(t *testing.T) {
	b, err := encode(sampleRequest())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "مباراة الهلال والنصر") || strings.Contains(string(b), `\u`) {
		t.Errorf("non-ASCII text was escaped: %s", b)
	}
	got, err := decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Content[domain.ChannelSMS]) != string(sampleRequest().Content[domain.ChannelSMS]) {
		t.Errorf("round trip changed content: %s", got.Content[domain.ChannelSMS])
	}
}

// startKafka runs a throwaway broker with the topic created. The test is
// skipped under -short or without Docker.
func startKafka(t *testing.T, topic string) []string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping Kafka container test in -short mode")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := context.Background()
	ctr, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	brokers, err := ctr.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := kadm.NewClient(admin).CreateTopic(ctx, 3, 1, nil, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	return brokers
}

func TestPublisherWritesKeyedRecord(t *testing.T) {
	const topic = "notifications"
	brokers := startKafka(t, topic)

	pub, err := NewPublisher(brokers, topic)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := pub.Publish(ctx, sampleRequest()); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	fetches := consumer.PollRecords(ctx, 10)
	if err := fetches.Err(); err != nil {
		t.Fatalf("consume: %v", err)
	}
	recs := fetches.Records()
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
	if string(recs[0].Key) != "match-1" {
		t.Errorf("key = %q, want the idempotency key", recs[0].Key)
	}
	got, err := decode(recs[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Content[domain.ChannelSMS]) != string(sampleRequest().Content[domain.ChannelSMS]) {
		t.Errorf("content changed: %s", got.Content[domain.ChannelSMS])
	}
}

func TestPublishFailsWhenBrokerUnreachable(t *testing.T) {
	// No context deadline: the delivery timeout alone must end the call.
	pub, err := newPublisher([]string{"127.0.0.1:1"}, "notifications", kgo.RecordDeliveryTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)
	start := time.Now()
	if err := pub.Publish(context.Background(), sampleRequest()); err == nil {
		t.Fatal("Publish succeeded with no broker")
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Errorf("Publish took %v, want it bounded by the delivery timeout", d)
	}
}

func TestPublishRejectsEmptyKey(t *testing.T) {
	pub, err := NewPublisher([]string{"127.0.0.1:1"}, "notifications")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)
	req := sampleRequest()
	req.IdempotencyKey = ""
	if err := pub.Publish(context.Background(), req); err == nil {
		t.Fatal("Publish accepted an empty idempotency key")
	}
}
