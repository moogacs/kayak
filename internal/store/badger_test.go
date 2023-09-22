package store

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"

	kayakv1 "github.com/binarymatt/kayak/gen/kayak/v1"
	"github.com/dgraph-io/badger/v4"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type BadgerTestSuite struct {
	suite.Suite
	store *badgerStore
	db    *badger.DB
	ctx   context.Context
	ts    *timestamppb.Timestamp
}

func (b *BadgerTestSuite) SetupSuite() {
	l := slog.New(slog.NewTextHandler(io.Discard, nil))
	slog.SetDefault(l)
}
func (b *BadgerTestSuite) SetupTest() {
	opt := badger.DefaultOptions("").WithInMemory(true).WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(opt)
	b.NoError(err)
	b.db = db
	b.ctx = context.Background()
	now := timestamppb.Now()
	b.ts = now
	b.store = &badgerStore{db: db, timeFunc: func() *timestamppb.Timestamp { return now }}
}

func (b *BadgerTestSuite) TestCreateTopic() {
	err := b.store.CreateTopic(b.ctx, testTopicName)
	b.NoError(err)

	err = b.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get(key("topics#test"))
		b.NoError(err)

		var meta kayakv1.TopicMetadata
		err = LoadMeta(item, &meta)
		b.NoError(err)
		b.Equal(int64(0), meta.RecordCount)
		b.Empty(cmp.Diff(b.ts, meta.CreatedAt, protocmp.Transform()))
		return nil
	})
	b.NoError(err)
}

func (b *BadgerTestSuite) TestAddRecords_HappyPath() {
	err := b.store.CreateTopic(b.ctx, testTopicName)
	b.NoError(err)

	records := createRecords()

	err = b.store.AddRecords(context.Background(), testTopicName, records...)
	b.NoError(err)

	err = b.db.View(func(tx *badger.Txn) error {
		// Validate metadata
		item, err := tx.Get([]byte("topics#test"))
		b.NoError(err)

		data, err := item.ValueCopy(nil)
		b.NoError(err)

		var meta kayakv1.TopicMetadata
		err = proto.Unmarshal(data, &meta)
		b.NoError(err)
		b.Equal(int64(3), meta.RecordCount)
		// Validate records exist
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte("test#")
		i := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			data, err := item.ValueCopy(nil)
			b.NoError(err)

			r, err := decode(nil, data)
			b.NoError(err)

			b.NotEmpty(r.Id)
			b.Equal(records[i].Topic, r.Topic)
			b.Equal(records[i].Payload, r.Payload)
			i++
		}

		return nil
	})
	b.NoError(err)

}
func (b *BadgerTestSuite) TestGetRecords_AllRecords() {
	ctx := context.Background()
	_ = b.store.CreateTopic(ctx, "test")
	records := createRecords()
	err := b.store.AddRecords(ctx, "test", records...)
	b.NoError(err)
	items, err := b.store.GetRecords(ctx, "test", "", 100)
	b.NoError(err)
	b.Len(items, 3)
}

func (b *BadgerTestSuite) TestGetRecords_TwoRecords() {
	ctx := context.Background()
	_ = b.store.CreateTopic(ctx, "test")
	records := createRecords()
	err := b.store.AddRecords(ctx, "test", records...)
	b.NoError(err)
	items, err := b.store.GetRecords(ctx, "test", "", 2)
	b.NoError(err)
	b.Len(items, 2)
	b.Empty(cmp.Diff(records[0], items[0], protocmp.Transform()))
	b.Empty(cmp.Diff(records[1], items[1], protocmp.Transform()))
}

func (b *BadgerTestSuite) TestGetRecords_Single() {
	ctx := context.Background()
	_ = b.store.CreateTopic(ctx, "test")
	records := createRecords()
	err := b.store.AddRecords(ctx, "test", records...)
	b.NoError(err)

	record, err := b.store.GetRecords(ctx, "test", "", 1)
	b.NoError(err)
	b.Len(record, 1)
	b.Empty(cmp.Diff(records[0], record[0], protocmp.Transform()))

	record2, err := b.store.GetRecords(ctx, "test", records[0].Id, 1)
	b.NoError(err)
	b.Empty(cmp.Diff(records[1], record2[0], protocmp.Transform()))

	record3, err := b.store.GetRecords(ctx, "test", records[1].Id, 1)
	b.NoError(err)
	b.Empty(cmp.Diff(records[2], record3[0], protocmp.Transform()))
}

func (b *BadgerTestSuite) TestGetRecords_WithStart() {
	ctx := context.Background()
	_ = b.store.CreateTopic(ctx, "test")
	records := createRecords()
	err := b.store.AddRecords(ctx, "test", records...)
	b.NoError(err)
	items, err := b.store.GetRecords(ctx, "test", records[0].Id, 100)
	b.NoError(err)
	b.Len(items, 2)
	b.Empty(cmp.Diff(records[1], items[0], protocmp.Transform()))
	b.Empty(cmp.Diff(records[2], items[1], protocmp.Transform()))
}

func (b *BadgerTestSuite) TestCommitConsumerPosition() {
	ctx := context.Background()
	err := b.store.CreateTopic(ctx, testTopicName)
	b.NoError(err)

	err = b.store.CommitConsumerPosition(ctx, testTopicName, "testConsumer", "1")
	b.NoError(err)
	err = b.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get(key("topics#test"))
		b.NoError(err)

		data, err := item.ValueCopy(nil)
		b.NoError(err)
		var meta kayakv1.TopicMetadata
		err = proto.Unmarshal(data, &meta)
		b.NoError(err)
		fmt.Println(meta)
		b.Equal("1", meta.Consumers["testConsumer"])

		return nil
	})
	b.NoError(err)
}

func (b *BadgerTestSuite) TestCommitConsumerPosition_ArchivedTopic() {
	ctx := context.Background()
	err := b.store.CreateTopic(ctx, testTopicName)
	b.NoError(err)

	err = b.store.DeleteTopic(ctx, testTopicName, true)
	b.NoError(err)

	err = b.store.CommitConsumerPosition(ctx, testTopicName, "testConsumer", "1")
	b.ErrorIs(err, ErrTopicArchived)
}

func (b *BadgerTestSuite) TestCommitConsumerPosition_NonExistentTopic() {
	err := b.store.CommitConsumerPosition(context.Background(), testTopicName, "testConsumer", "1")
	b.ErrorIs(err, ErrInvalidTopic)
}

func (b *BadgerTestSuite) TestGetConsumerPosition_NonExistentTopic() {
	pos, err := b.store.GetConsumerPosition(context.Background(), testTopicName, "test")
	b.ErrorIs(err, ErrInvalidTopic)
	b.Empty(pos)
}

func (b *BadgerTestSuite) TestGetConsumerPosition_NonExistentConsumer() {
	ctx := context.Background()
	err := b.store.CreateTopic(ctx, testTopicName)
	b.NoError(err)

	pos, err := b.store.GetConsumerPosition(context.Background(), testTopicName, "test")
	b.NoError(err)
	b.Empty(pos)
}

func (b *BadgerTestSuite) TestGetConsumerPosition_Happy() {
	ctx := context.Background()

	err := b.store.CreateTopic(ctx, testTopicName)
	b.NoError(err)

	err = b.store.CommitConsumerPosition(context.Background(), testTopicName, "test", "1")
	b.NoError(err)

	pos, err := b.store.GetConsumerPosition(context.Background(), testTopicName, "test")
	b.NoError(err)
	b.Equal("1", pos)
}

func (b *BadgerTestSuite) TestListTopics() {
	topics, err := b.store.ListTopics(context.Background())
	b.NoError(err)
	b.Empty(topics)
	err = b.store.CreateTopic(context.Background(), testTopicName)
	b.NoError(err)
	topics, err = b.store.ListTopics(context.Background())
	b.NoError(err)
	b.Equal([]string{testTopicName}, topics)

}

func TestBadgerTestSuite(t *testing.T) {
	suite.Run(t, new(BadgerTestSuite))
}
