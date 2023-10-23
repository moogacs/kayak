package store

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	kayakv1 "github.com/kayak/gen/proto/kayak/v1"
)

var (
	testTopicName = "test"
	testTopic     = []byte(testTopicName)
	testTopicMeta = []byte("test_meta")
)

type BoltTestSuite struct {
	suite.Suite
	store     *boltStore
	db        *bolt.DB
	f         *os.File
	timeStamp *timestamppb.Timestamp
}

func (b *BoltTestSuite) SetupSuite() {
	l := slog.New(slog.NewTextHandler(io.Discard, nil))
	slog.SetDefault(l)
}

func (b *BoltTestSuite) TearDownTest() {
	os.Remove(b.f.Name())
}
func (b *BoltTestSuite) SetupTest() {
	f, err := os.CreateTemp("", "")
	b.NoError(err)
	b.f = f
	db, err := bolt.Open(f.Name(), 0600, nil)
	b.NoError(err)
	n := timestamppb.Now()
	b.store = &boltStore{
		db: db,
		timeFunc: func() *timestamppb.Timestamp {
			return n
		},
	}
	b.timeStamp = n
	b.db = db
}

func (b *BoltTestSuite) TestCreateTopic_HappyPath() {
	err := b.store.CreateTopic(context.Background(), testTopicName)
	b.NoError(err)
	err = b.db.View(func(tx *bolt.Tx) error {
		// check for bucket
		eb := tx.Bucket(testTopic)
		b.NotNil(eb)

		// check for bucket metadata
		mb := tx.Bucket([]byte(fmt.Sprintf("%s_meta", testTopic)))
		b.NotNil(mb)

		val := mb.Get([]byte("created"))

		tm := time.Time{}
		err := tm.UnmarshalBinary(val)

		ts := timestamppb.New(tm)

		b.NoError(err)
		b.Empty(cmp.Diff(b.timeStamp, ts, protocmp.Transform()))

		// check topics bucket for bucket name
		tb := tx.Bucket(topicsKey)
		b.NotNil(tb)

		v := tb.Get(testTopic)
		b.NotEmpty(v)
		var t time.Time
		err = t.UnmarshalBinary(v)
		b.NoError(err)

		return nil
	})
	b.NoError(err)
}
func (b *BoltTestSuite) TestCreateTopic_Exists() {
	err := b.store.CreateTopic(context.Background(), testTopicName)
	b.NoError(err)

	err = b.store.CreateTopic(context.Background(), testTopicName)
	b.ErrorIs(err, ErrTopicAlreadyExists)
}

func (b *BoltTestSuite) TestCreateTopic_ArchivedTopic() {
	err := b.store.CreateTopic(context.Background(), testTopicName)
	b.NoError(err)

	err = b.store.DeleteTopic(context.Background(), testTopicName, true)
	b.NoError(err)

	err = b.store.CreateTopic(context.Background(), testTopicName)
	b.ErrorIs(err, ErrTopicRecreate)
}

func (b *BoltTestSuite) TestDeleteTopic_Archive() {
	ctx := context.Background()
	err := b.store.CreateTopic(ctx, testTopicName)
	b.NoError(err)

	err = b.store.DeleteTopic(ctx, testTopicName, true)
	b.NoError(err)
	err = b.db.View(func(tx *bolt.Tx) error {
		// check for bucket
		eb := tx.Bucket(testTopic)
		b.Nil(eb)

		// check for bucket metadata
		mb := tx.Bucket([]byte(fmt.Sprintf("%s_meta", testTopic)))
		b.NotNil(mb)

		// check topics bucket for bucket name
		tb := tx.Bucket(topicsKey)
		b.NotNil(tb)
		v := tb.Get(testTopic)
		b.Empty(v)
		return nil
	})
	b.NoError(err)
}
func (b *BoltTestSuite) TestDeleteTopic() {
	ctx := context.Background()
	err := b.store.CreateTopic(ctx, testTopicName)
	b.NoError(err)

	err = b.store.DeleteTopic(ctx, testTopicName, false)
	b.NoError(err)
	err = b.db.View(func(tx *bolt.Tx) error {
		// check for bucket
		eb := tx.Bucket(testTopic)
		b.Nil(eb)

		// check for bucket metadata
		mb := tx.Bucket([]byte(fmt.Sprintf("%s_meta", testTopic)))
		b.Nil(mb)

		// check topics bucket for bucket name
		tb := tx.Bucket(topicsKey)
		b.NotNil(tb)
		v := tb.Get(testTopic)
		b.Empty(v)
		return nil
	})
	b.NoError(err)
}

func (b *BoltTestSuite) TestListTopics() {
	topics, err := b.store.ListTopics(context.Background())
	b.NoError(err)
	b.Empty(topics)
	_ = b.store.CreateTopic(context.Background(), testTopicName)
	topics, err = b.store.ListTopics(context.Background())
	b.NoError(err)
	b.Equal([]string{testTopicName}, topics)

}

func (b *BoltTestSuite) TestAddRecords_InvalidTopic() {
	err := b.store.AddRecords(context.Background(), "invalid", nil)
	b.Error(err)
}

func (b *BoltTestSuite) TestBoltStore() {
	id1 := ulid.Make()
	id2 := ulid.Make()
	id3 := ulid.Make()

	f, err := os.CreateTemp("", "")
	b.NoError(err)

	defer os.Remove(f.Name())
	db, err := bolt.Open(f.Name(), 0600, nil)
	b.NoError(err)
	err = db.Update(func(tx *bolt.Tx) error {
		bu, err := tx.CreateBucket(testTopic)
		if err != nil {
			return err
		}
		_ = bu.Put(id1[:], []byte("one"))
		_ = bu.Put(id2[:], []byte("two"))
		_ = bu.Put(id3[:], []byte("three"))
		return nil
	})
	b.NoError(err)
	var keys [][]byte
	var values [][]byte
	err = db.View(func(tx *bolt.Tx) error {
		bu := tx.Bucket(testTopic)
		c := bu.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			keys = append(keys, k)
			values = append(values, v)
		}
		return nil
	})
	b.Len(keys, 3)
	b.Len(values, 3)
	b.Equal(id1[:], keys[0])
	b.Equal(id2[:], keys[1])
	b.Equal(id3[:], keys[2])
	b.Equal("one", string(values[0]))
	b.Equal("two", string(values[1]))
	b.Equal("three", string(values[2]))
	b.NoError(err)
}

func (b *BoltTestSuite) TestAddRecords_HappyPath() {
	err := b.store.CreateTopic(context.Background(), testTopicName)
	b.NoError(err)

	records := createRecords()

	err = b.store.AddRecords(context.Background(), testTopicName, records...)
	b.NoError(err)

	err = b.db.View(func(tx *bolt.Tx) error {
		tb := tx.Bucket(testTopic)
		b.NotNil(tb)
		c := tb.Cursor()
		i := 0
		for k, v := c.First(); k != nil; k, v = c.Next() {
			r := records[i]
			pid, err := parseID(k)
			b.NoError(err)

			b.Equal(r.Id, pid.String())
			record, err := decode(k, v)
			b.NoError(err)
			b.True(proto.Equal(r, record))
			i++
		}
		mb := tx.Bucket(testTopicMeta)
		b.NotNil(mb)

		val := mb.Get(key("record_count"))
		b.Equal("3", string(val))
		return nil
	})
	b.NoError(err)
}

func (b *BoltTestSuite) TestCommitConsumerPosition() {
	ctx := context.Background()
	err := b.store.CreateTopic(ctx, testTopicName)
	b.NoError(err)

	err = b.store.CommitConsumerPosition(ctx, testTopicName, "testConsumer", "1")
	b.NoError(err)
	err = b.db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket(testTopicMeta)
		b.NotNil(meta)

		val := meta.Get([]byte("testConsumer"))
		b.Equal([]byte("1"), val)

		return nil
	})
	b.NoError(err)
}

func (b *BoltTestSuite) TestCommitConsumerPosition_ArchivedTopic() {
	ctx := context.Background()
	err := b.store.CreateTopic(ctx, testTopicName)
	b.NoError(err)

	err = b.store.DeleteTopic(ctx, testTopicName, true)
	b.NoError(err)

	err = b.store.CommitConsumerPosition(ctx, testTopicName, "testConsumer", "1")
	b.ErrorIs(err, ErrInvalidTopic)
}

func (b *BoltTestSuite) TestCommitConsumerPosition_NonExistentTopic() {
	err := b.store.CommitConsumerPosition(context.Background(), testTopicName, "testConsumer", "1")
	b.ErrorIs(err, ErrMissingBucket)

	err = b.store.CreateTopic(context.Background(), "secondary")
	b.NoError(err)

	err = b.store.CommitConsumerPosition(context.Background(), testTopicName, "testConsumer", "1")
	b.ErrorIs(err, ErrInvalidTopic)
}

func (b *BoltTestSuite) TestGetConsumerPosition() {
	ctx := context.Background()
	err := b.store.CreateTopic(ctx, testTopicName)
	b.NoError(err)

	pos, err := b.store.GetConsumerPosition(ctx, testTopicName, "testConsumer")
	b.NoError(err)
	b.Equal("", pos)

	err = b.store.CommitConsumerPosition(ctx, testTopicName, "testConsumer", "1")
	b.NoError(err)

	pos, err = b.store.GetConsumerPosition(ctx, testTopicName, "testConsumer")
	b.NoError(err)
	b.Equal("1", pos)
}

func (b *BoltTestSuite) TestGetConsumerPosition_InvalidTopic() {
	ctx := context.Background()

	_, err := b.store.GetConsumerPosition(ctx, testTopicName, "testConsumer")
	b.ErrorIs(err, ErrMissingBucket)

	err = b.store.CreateTopic(ctx, "secondary")
	b.NoError(err)

	_, err = b.store.GetConsumerPosition(ctx, testTopicName, "testConsumer")
	b.ErrorIs(err, ErrInvalidTopic)

	err = b.store.CommitConsumerPosition(ctx, "secondary", "testConsumer", "1")
	b.NoError(err)

	err = b.store.DeleteTopic(ctx, "secondary", true)
	b.NoError(err)

	_, err = b.store.GetConsumerPosition(ctx, "secondary", "testConsumer")
	b.ErrorIs(err, ErrInvalidTopic)
}

func createRecords() []*kayakv1.Record {
	return []*kayakv1.Record{
		{
			Topic: "test",
			Id:    ulid.Make().String(),
			Headers: map[string]string{
				"name": "one",
			},
			Payload: []byte("first record"),
		},
		{
			Topic: "test",
			Id:    ulid.Make().String(),
			Headers: map[string]string{
				"name": "two",
			},
			Payload: []byte("second record"),
		},
		{
			Topic: "test",
			Id:    ulid.Make().String(),
			Headers: map[string]string{
				"name": "three",
			},
			Payload: []byte("third record"),
		},
	}
}

func (b *BoltTestSuite) TestGetRecords_AllRecords() {
	ctx := context.Background()
	_ = b.store.CreateTopic(ctx, "test")
	records := createRecords()
	err := b.store.AddRecords(ctx, "test", records...)
	b.NoError(err)
	items, err := b.store.GetRecords(ctx, "test", "", 100)
	b.NoError(err)
	b.Len(items, 3)
}

func (b *BoltTestSuite) TestGetRecords_TwoRecords() {
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

func (b *BoltTestSuite) TestGetRecords_Single() {
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

func (b *BoltTestSuite) TestGetRecords_WithStart() {
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

func TestBoltTestSuite(t *testing.T) {
	suite.Run(t, new(BoltTestSuite))
}
