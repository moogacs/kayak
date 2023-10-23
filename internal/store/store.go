package store

import (
	"context"

	kayakv1 "github.com/kayak/gen/proto/kayak/v1"
)

type Store interface {
	CreateTopic(ctx context.Context, name string) error
	DeleteTopic(ctx context.Context, topic string, force bool) error
	AddRecords(ctx context.Context, topic string, records ...*kayakv1.Record) error
	GetRecords(ctx context.Context, topic string, start string, limit int) ([]*kayakv1.Record, error)
	ListTopics(ctx context.Context) ([]string, error)
	GetConsumerPosition(ctx context.Context, topic, group string) (string, error)
	CommitConsumerPosition(ctx context.Context, topic, consumerGroup, position string) error
	Stats() map[string]*kayakv1.TopicMetadata
	Impl() any
	Close()
	SnapshotItems() <-chan DataItem
}

type DataItem interface{}
