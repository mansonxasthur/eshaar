// Package kafka implements the publishing port on top of franz-go.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

// Publisher writes requests to one topic, keyed by idempotency key.
type Publisher struct {
	client *kgo.Client
	topic  string
}

var _ ports.Publisher = (*Publisher)(nil)

// deliveryTimeout bounds how long a record may wait for Kafka, so a dead broker
// fails the request instead of hanging it and filling the producer buffer.
const deliveryTimeout = 10 * time.Second

// NewPublisher connects a producer to brokers. Records are acknowledged by all
// in-sync replicas. franz-go makes the producer idempotent when acks=all is in
// force (its default; do not add DisableIdempotentWrite), so an internal retry
// never duplicates a record. Batching and compression keep throughput up.
func NewPublisher(brokers []string, topic string) (*Publisher, error) {
	return newPublisher(brokers, topic)
}

// newPublisher is NewPublisher with extra client options, for tests. Later
// options override the defaults.
func newPublisher(brokers []string, topic string, extra ...kgo.Opt) (*Publisher, error) {
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.ProducerLinger(5 * time.Millisecond),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
	}, extra...)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &Publisher{client: client, topic: topic}, nil
}

// Publish sends req and returns once Kafka confirms the write. The message key
// is the idempotency key, which spreads load evenly across partitions.
func (p *Publisher) Publish(ctx context.Context, req domain.Request) error {
	if req.IdempotencyKey == "" {
		return errors.New("publish: empty idempotency key")
	}
	value, err := encode(req)
	if err != nil {
		return err
	}
	rec := &kgo.Record{Key: []byte(req.IdempotencyKey), Value: value}
	if err := p.client.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("publish to %s: %w", p.topic, err)
	}
	return nil
}

// Close flushes nothing (Publish is synchronous) and releases the connections.
func (p *Publisher) Close() { p.client.Close() }
