package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

// Consumer reads requests as one member of a consumer group. Each partition is
// read by one member, in order.
type Consumer struct {
	client    *kgo.Client
	batchSize int
	log       *slog.Logger
}

var _ ports.Consumer = (*Consumer)(nil)

// NewConsumer joins group on topic. Offsets are committed only by Batch.Ack,
// and a new group starts at the beginning of the topic so nothing published
// before it existed is skipped. batchSize caps the records per Poll.
func NewConsumer(brokers []string, topic, group string, batchSize int, log *slog.Logger) (*Consumer, error) {
	return newConsumer(brokers, topic, group, batchSize, log)
}

// newConsumer is NewConsumer with extra client options, for tests.
func newConsumer(brokers []string, topic, group string, batchSize int, log *slog.Logger, extra ...kgo.Opt) (*Consumer, error) {
	if batchSize < 1 {
		return nil, errors.New("kafka consumer: batch size must be at least 1")
	}
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		// A rebalance waits until the batch is acked or released, so a
		// partition is never handed to another member mid-batch.
		kgo.BlockRebalanceOnPoll(),
	}, extra...)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return &Consumer{client: client, batchSize: batchSize, log: log}, nil
}

// Poll implements ports.Consumer. A record that cannot be decoded is logged
// and skipped, and is committed with its batch, so one poison message cannot
// stall a partition. There is no dead-letter topic.
func (c *Consumer) Poll(ctx context.Context) (ports.Batch, error) {
	for {
		fetches := c.client.PollRecords(ctx, c.batchSize)
		if fetches.IsClientClosed() {
			return nil, errors.New("kafka consumer: closed")
		}
		if err := ctx.Err(); err != nil {
			c.client.AllowRebalance()
			return nil, err
		}
		recs := fetches.Records()
		if len(recs) == 0 {
			c.client.AllowRebalance()
			if err := fetches.Err0(); err != nil {
				return nil, fmt.Errorf("kafka fetch: %w", err)
			}
			continue
		}
		b := &batch{client: c.client, records: recs}
		for _, rec := range recs {
			req, err := decode(rec.Value)
			if err != nil {
				c.log.ErrorContext(ctx, "skip undecodable record",
					"topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "error", err)
				continue
			}
			b.requests = append(b.requests, req)
		}
		return b, nil
	}
}

// Close leaves the group and releases the connections.
func (c *Consumer) Close() { c.client.Close() }

// batch is the records of one Poll.
type batch struct {
	client   *kgo.Client
	records  []*kgo.Record
	requests []domain.Request
}

func (b *batch) Requests() []domain.Request { return b.requests }

// Ack commits the offsets after the last record of each partition, then lets a
// pending rebalance proceed.
func (b *batch) Ack(ctx context.Context) error {
	defer b.client.AllowRebalance()
	if err := b.client.CommitRecords(ctx, b.records...); err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}
	return nil
}

// Release moves each partition back to the first record of the batch, so the
// next Poll returns the same records, then lets a pending rebalance proceed.
func (b *batch) Release() {
	first := make(map[string]map[int32]kgo.EpochOffset)
	for _, rec := range b.records {
		parts := first[rec.Topic]
		if parts == nil {
			parts = make(map[int32]kgo.EpochOffset)
			first[rec.Topic] = parts
		}
		// Records of a partition arrive in offset order: keep the first.
		if _, seen := parts[rec.Partition]; !seen {
			parts[rec.Partition] = kgo.EpochOffset{Epoch: rec.LeaderEpoch, Offset: rec.Offset}
		}
	}
	b.client.SetOffsets(first)
	b.client.AllowRebalance()
}
