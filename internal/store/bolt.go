package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/boltdb/bolt"
	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	kayakv1 "github.com/kayak/gen/proto/kayak/v1"
	"github.com/kayak/internal"
)

var (
	topicsKey             = []byte("topic")
	ErrIterFinished       = errors.New("ERR iteration finished successfully")
	ErrInvalidTopic       = errors.New("invalid topic.")
	ErrTopicAlreadyExists = errors.New("topic already exists")
	ErrTopicArchived      = errors.New("topic has been archived")
	ErrTopicRecreate      = errors.New("cannot recreate archived topic")
	ErrMissingBucket      = errors.New("missing topics bucket.")
)

type boltStore struct {
	db       *bolt.DB
	timeFunc func() *timestamppb.Timestamp
}

func (b *boltStore) ListTopics(ctx context.Context) ([]string, error) {
	_, span := otel.GetTracerProvider().Tracer(internal.TracerName).Start(ctx, "list-topics")
	defer span.End()
	var topics []string
	err := b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(topicsKey)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			topics = append(topics, string(k))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return topics, nil
}

// Note: should topics not be re-used. This allows meta to exist after topic is deleted.
func (b *boltStore) CreateTopic(ctx context.Context, name string) error {
	_, span := otel.GetTracerProvider().Tracer(internal.TracerName).Start(ctx, "create-topic")
	defer span.End()
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(topicsKey)
		if err != nil {
			return err
		}
		val, err := b.timeFunc().AsTime().MarshalBinary()
		if err != nil {
			return err
		}

		if err := bucket.Put([]byte(name), val); err != nil {
			span.RecordError(err)
			return err
		}
		if _, err := tx.CreateBucket([]byte(name)); err != nil {
			span.RecordError(err)
			if errors.Is(err, bolt.ErrBucketExists) {
				return ErrTopicAlreadyExists
			}
			return err
		}
		mb, err := tx.CreateBucket([]byte(fmt.Sprintf("%s_meta", name)))
		// CreateBucket is called because it fails if the name already exists as a bucket.
		if err != nil {
			span.RecordError(err)
			if errors.Is(err, bolt.ErrBucketExists) {
				return ErrTopicRecreate
			}
			return err
		}
		if err := mb.Put([]byte("created"), val); err != nil {
			return err
		}

		return nil
	})
}

func (b *boltStore) DeleteTopic(ctx context.Context, name string, archive bool) error {
	topic := []byte(name)
	return b.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(topicsKey)
		if b == nil {
			return ErrMissingBucket
		}
		if err := b.Delete(topic); err != nil {
			slog.ErrorContext(ctx, "could not delete topic from topics bucket", "topic", topic)
			return err
		}
		if err := tx.DeleteBucket(topic); err != nil {
			return ErrInvalidTopic
		}
		if !archive {
			if err := tx.DeleteBucket([]byte(fmt.Sprintf("%s_meta", name))); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *boltStore) GetConsumerPosition(ctx context.Context, topic, consumer string) (string, error) {
	_, span := otel.GetTracerProvider().Tracer(internal.TracerName).Start(ctx, "get-group-position")
	defer span.End()
	position := ""
	err := b.db.View(func(tx *bolt.Tx) error {
		topicsBucket := tx.Bucket(topicsKey)
		if topicsBucket == nil {
			return ErrMissingBucket
		}

		record := topicsBucket.Get([]byte(topic))
		if record == nil {
			return ErrInvalidTopic
		}

		bu := tx.Bucket([]byte(fmt.Sprintf("%s_meta", topic)))
		if bu == nil {
			return ErrInvalidTopic
		}
		position = string(bu.Get([]byte(consumer)))
		return nil
	})
	if err != nil {
		span.RecordError(err)
	}
	return position, err
}

func (b *boltStore) CommitConsumerPosition(ctx context.Context, topic, consumerID, position string) error {
	_, span := otel.GetTracerProvider().Tracer(internal.TracerName).Start(ctx, "commit-group-position")
	defer span.End()
	return b.db.Update(func(tx *bolt.Tx) error {
		topicsBucket := tx.Bucket(topicsKey)
		if topicsBucket == nil {
			return ErrMissingBucket
		}
		record := topicsBucket.Get([]byte(topic))
		if record == nil {
			return ErrInvalidTopic
		}
		bucket := tx.Bucket([]byte(fmt.Sprintf("%s_meta", topic)))
		if bucket == nil {
			return ErrInvalidTopic
		}
		return bucket.Put([]byte(consumerID), []byte(position))
	})
}

func (b *boltStore) AddRecords(ctx context.Context, topic string, records ...*kayakv1.Record) error {
	ctx, span := otel.GetTracerProvider().Tracer(internal.TracerName).Start(ctx, "add-records")
	defer span.End()
	g, _ := errgroup.WithContext(ctx)
	g.Go(func() error {
		return b.db.Batch(func(tx *bolt.Tx) error {
			mb := tx.Bucket(key(fmt.Sprintf("%s_meta", topic)))
			if mb == nil {
				return ErrMissingBucket
			}
			raw := mb.Get(key("record_count"))
			if len(raw) < 1 {
				raw = []byte("0")
			}
			count, err := strconv.Atoi(string(raw))
			if err != nil {
				return err
			}
			count = count + len(records)
			s := strconv.Itoa(count)
			if err := mb.Put(key("record_count"), key(s)); err != nil {
				return err
			}
			return nil
		})
	})
	for _, record := range records {
		r := record
		g.Go(func() error {
			if r == nil {
				return errors.New("invalid record")
			}
			return b.db.Batch(func(tx *bolt.Tx) error {
				bu := tx.Bucket([]byte(r.Topic))
				if bu == nil {
					slog.ErrorContext(ctx, "trying to add record to non-existing topic", "topic", topic)
					return ErrInvalidTopic
				}
				id, _ := ulid.Parse(r.Id)
				data, err := encode(r)
				if err != nil {
					return err
				}

				if err := bu.Put(id[:], data); err != nil {
					return err
				}
				return nil
			})
		})
	}
	return g.Wait()
}

func (b *boltStore) GetRecords(ctx context.Context, topic string, startStr string, limit int) ([]*kayakv1.Record, error) {
	_, span := otel.GetTracerProvider().Tracer(internal.TracerName).Start(ctx, "get-records")
	defer span.End()

	var records []*kayakv1.Record
	var startKey []byte
	var err error
	//var start ulid.ULID
	if startStr != "" {
		start, err := ulid.Parse(startStr)
		if err != nil {
			return nil, err
		}
		startKey = start[:]
	} else {
		startKey = []byte("")
	}
	slog.InfoContext(ctx, "getting records from bolt", "start", startStr)
	err = b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(topic))
		if bucket == nil {
			slog.ErrorContext(ctx, "invalid bucket")
			return errors.New("invalid bucket")
		}
		c := bucket.Cursor()
		counter := 0
		for k, v := c.Seek(startKey); k != nil; k, v = c.Next() {
			if counter >= limit {
				slog.Debug("past limit, done")
				break
			}
			id, err := parseID(k)
			if err != nil {
				return err
			}
			slog.Debug("processing", "key", id)
			if string(k) == string(startKey) {
				slog.Debug("skipping start value", "key", id)
				continue
			}
			record, err := decode(k, v)

			if err != nil {
				return err
			}
			counter++
			records = append(records, record)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "returning records from bolt", "count", len(records))
	return records, nil
}

type StoreStats struct {
	Topics map[string]*kayakv1.TopicMetadata
}

func (b *boltStore) Stats() map[string]*kayakv1.TopicMetadata {
	s := map[string]*kayakv1.TopicMetadata{}
	err := b.db.View(func(tx *bolt.Tx) error {

		bu := tx.Bucket(topicsKey)
		if bu == nil {
			return nil
		}

		c := bu.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			topicBucket := tx.Bucket(v)
			stats := topicBucket.Stats()
			s[string(v)] = &kayakv1.TopicMetadata{
				Name:        string(v),
				RecordCount: int64(stats.KeyN),
			}
		}

		return nil
	})

	if err != nil {
		slog.Error("could not get store stats", "error", err)
	}

	return s
}

func (b *boltStore) Impl() any {
	return b.db
}
func (b *boltStore) Close() {
	b.db.Close()
}
func (b *boltStore) SnapshotItems() <-chan DataItem {
	ch := make(chan DataItem, 1024)
	go func() {
		if err := b.db.View(func(tx *bolt.Tx) error {
			c := tx.Cursor()
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				b.processBucket(tx, k, ch)
			}
			kvi := &KVItem{
				Err: ErrIterFinished,
			}
			ch <- kvi
			return nil
		}); err != nil {
			slog.Error("problem during snapshotitems", "error", err)
		}

	}()
	return ch
}
func (b *boltStore) processBucket(tx *bolt.Tx, bucket []byte, ch chan DataItem) {
	bu := tx.Bucket(bucket)
	c := bu.Cursor()
	keyCount := 0
	for k, v := c.First(); k != nil; k, v = c.Next() {
		kvi := &KVItem{
			Bucket: append([]byte{}, bucket...),
			Key:    append([]byte{}, k...),
			Value:  append([]byte{}, v...),
		}
		ch <- kvi
		keyCount = keyCount + 1
	}

	slog.Info("done with bucket", "bucket", string(bucket), "key_count", keyCount)
}

func NewBoltStore(db *bolt.DB) Store {
	return &boltStore{
		db:       db,
		timeFunc: timestamppb.Now,
	}
}

func parseID(data []byte) (ulid.ULID, error) {
	id := ulid.ULID{}
	err := id.UnmarshalBinary(data)
	return id, err
}

func encode(record *kayakv1.Record) ([]byte, error) {
	return proto.Marshal(record)
}

func decode(key, value []byte) (*kayakv1.Record, error) {
	var record kayakv1.Record
	err := proto.Unmarshal(value, &record)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return &record, nil
	}
	id := ulid.ULID{}
	err = id.UnmarshalBinary(key)
	if err != nil {
		return nil, err
	}
	record.Id = id.String()
	return &record, nil
}

func key(key string) []byte {
	return []byte(key)
}

type KVItem struct {
	Bucket []byte
	Key    []byte
	Value  []byte
	Err    error
}

func (i *KVItem) IsFinished() bool {
	return i.Err == ErrIterFinished
}
