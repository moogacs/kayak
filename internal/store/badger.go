package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	badger "github.com/dgraph-io/badger/v4"
	"google.golang.org/protobuf/proto"

	"google.golang.org/protobuf/types/known/timestamppb"

	kayakv1 "github.com/kayak/gen/proto/kayak/v1"
)

type badgerStore struct {
	db       *badger.DB
	timeFunc func() *timestamppb.Timestamp
}

func MetaKey(meta *kayakv1.TopicMetadata) []byte {
	return key(fmt.Sprintf("topics#%s", meta.Name))
}
func LoadMeta(item *badger.Item, meta *kayakv1.TopicMetadata) error {
	data, err := item.ValueCopy(nil)
	if err != nil {
		return err
	}
	return proto.Unmarshal(data, meta)
}

func (b *badgerStore) CreateTopic(ctx context.Context, name string) error {
	k := key(fmt.Sprintf("topics#%s", name))
	return b.db.Update(func(tx *badger.Txn) error {
		_, err := tx.Get(k)
		// check and see
		if !errors.Is(err, badger.ErrKeyNotFound) {
			fmt.Println("bucket is missing")
			return ErrMissingBucket
		}
		archived, err := b.isTopicArchived(tx, name)
		if err != nil {
			return err
		}
		if archived {
			return ErrTopicArchived
		}

		// create metadata entry
		// add to topics set
		meta := &kayakv1.TopicMetadata{
			Name:        name,
			CreatedAt:   b.timeFunc(),
			RecordCount: 0,
			Consumers:   map[string]string{},
		}
		data, err := proto.Marshal(meta)
		if err != nil {
			return err
		}
		fmt.Println("setting topic", string(k))
		return tx.Set(k, data)
	})
}
func (b *badgerStore) isTopicArchived(tx *badger.Txn, topic string) (bool, error) {
	k := fmt.Sprintf("topics#%s", topic)
	item, err := tx.Get(key(k))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var meta kayakv1.TopicMetadata
	if err := LoadMeta(item, &meta); err != nil {
		return false, err
	}
	return meta.Archived, nil
}

func (b *badgerStore) retrieveTopicMeta(tx *badger.Txn, topic string) (*kayakv1.TopicMetadata, error) {
	metaKey := key(fmt.Sprintf("topics#%s", topic))
	var meta kayakv1.TopicMetadata
	item, err := tx.Get(metaKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, ErrInvalidTopic
	}
	if err != nil {
		return nil, err
	}
	if err := LoadMeta(item, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}
func (b *badgerStore) ArchiveTopic(ctx context.Context, topic string) (err error) {
	err = b.db.DropPrefix(key(fmt.Sprintf("%s#", topic)))
	if err != nil {
		return
	}
	err = b.db.Update(func(tx *badger.Txn) error {
		meta, err := b.retrieveTopicMeta(tx, topic)
		if err != nil {
			return err
		}
		meta.Archived = true
		data, err := proto.Marshal(meta)
		if err != nil {
			return err
		}
		if err := tx.Set(MetaKey(meta), data); err != nil {
			return err
		}
		return nil
	})
	return err
}

func (b *badgerStore) PurgeTopic(ctx context.Context, topic string) (err error) {
	err = b.db.DropPrefix(key(fmt.Sprintf("%s#", topic)))
	if err != nil {
		return
	}
	err = b.db.Update(func(tx *badger.Txn) error {
		return tx.Delete(key(fmt.Sprintf("topics#%s", topic)))
	})
	return
}

func (b *badgerStore) DeleteTopic(ctx context.Context, topic string, archive bool) (err error) {
	if archive {
		return b.ArchiveTopic(ctx, topic)
	}
	return b.PurgeTopic(ctx, topic)
}

func (b *badgerStore) AddRecords(ctx context.Context, topic string, records ...*kayakv1.Record) error {
	return b.db.Update(func(tx *badger.Txn) error {
		meta, err := b.retrieveTopicMeta(tx, topic)
		if err != nil {
			return err
		}
		if meta.Archived {
			return ErrTopicAlreadyExists
		}

		meta.RecordCount += int64(len(records))
		for _, record := range records {
			record.Topic = topic
			recordKey := fmt.Sprintf("%s#%s", topic, record.Id)
			data, err := encode(record)
			if err != nil {
				return err
			}
			if err := tx.Set(key(recordKey), data); err != nil {
				return err
			}
		}
		metaData, err := proto.Marshal(meta)
		if err != nil {
			return err
		}
		if err := tx.Set(MetaKey(meta), metaData); err != nil {
			return err
		}
		return nil
	})
}

func (b *badgerStore) GetRecords(ctx context.Context, topic string, start string, limit int) (records []*kayakv1.Record, err error) {
	err = b.db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte(fmt.Sprintf("%s#", topic))
		startKey := []byte(fmt.Sprintf("%s#%s", topic, start))

		counter := 0
		for it.Seek(startKey); it.ValidForPrefix(prefix); it.Next() {
			if counter >= limit {
				slog.Debug("past limit, exit")
				break
			}
			item := it.Item()
			data, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			record, err := decode(nil, data)
			if err != nil {
				return err
			}
			if record.Id == start {
				slog.Debug("skipping start value", "start", start)
				continue
			}
			records = append(records, record)
			counter++

		}

		return nil
	})
	slog.InfoContext(ctx, "returning records from badger", "count", len(records))
	return
}

func (b *badgerStore) getId(item *badger.Item) string {
	k := string(item.Key())
	key := strings.Split(k, "#")[1]
	return key
}

func (b *badgerStore) ListTopics(ctx context.Context) (topics []string, err error) {
	err = b.db.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := tx.NewIterator(opts)
		defer it.Close()
		prefix := []byte("topics#")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			fmt.Println("got item")
			item := it.Item()
			key := b.getId(item)
			fmt.Println(key)
			topics = append(topics, key)
		}

		return nil
	})
	return
}

func (b *badgerStore) GetConsumerPosition(ctx context.Context, topic, consumer string) (position string, err error) {

	err = b.db.View(func(tx *badger.Txn) error {
		meta, err := b.retrieveTopicMeta(tx, topic)
		if err != nil {
			return err
		}
		position = meta.Consumers[consumer]
		return nil
	})
	return
}

func (b *badgerStore) CommitConsumerPosition(ctx context.Context, topic, consumer, position string) (err error) {
	err = b.db.Update(func(tx *badger.Txn) error {
		item, err := b.retrieveTopicMeta(tx, topic)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrInvalidTopic
		}
		if err != nil {
			return err
		}
		if item.Archived {
			return ErrTopicArchived
		}
		fmt.Println(item)
		if item.Consumers == nil {
			item.Consumers = map[string]string{}
		}
		item.Consumers[consumer] = position

		fmt.Println("about to marsal item", item)
		val, err := proto.Marshal(item)
		if err != nil {
			return err
		}
		if err := tx.Set(MetaKey(item), val); err != nil {
			return err
		}
		return nil
	})
	return
}

func (b *badgerStore) Stats() (results map[string]*kayakv1.TopicMetadata) {
	results = map[string]*kayakv1.TopicMetadata{}
	err := b.db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte("topics#")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			var meta kayakv1.TopicMetadata
			if err := LoadMeta(item, &meta); err != nil {
				return err
			}
			results[string(k)] = &meta
		}
		return nil
	})
	if err != nil {
		slog.Error("error getting stats")
	}
	return
}

func (b *badgerStore) Impl() any {
	return b.db
}

func (b *badgerStore) Close() {
	_ = b.db.Close()
}

func (b *badgerStore) SnapshotItems() <-chan DataItem {
	// loop through topics
	return nil
}

func NewBadger(db *badger.DB) *badgerStore {
	slog.Debug("created new db", "db", db)
	return &badgerStore{
		db:       db,
		timeFunc: timestamppb.Now,
	}
}
