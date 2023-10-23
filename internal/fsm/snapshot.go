package fsm

import (
	"encoding/binary"
	"io"
	"log/slog"

	"github.com/dgraph-io/badger/v4"
	kayakv1 "github.com/kayak/gen/proto/kayak/v1"
	"github.com/kayak/internal/store"
	"google.golang.org/protobuf/proto"

	"github.com/hashicorp/raft"
)

type badgerFsmSnapshot struct {
	db *badger.DB
}

func (fs *badgerFsmSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()
	_, err := fs.db.Backup(sink, 0)
	return err
}
func (fs *badgerFsmSnapshot) Release() {
	slog.Info("releasing fsm badger snapshot")
}

type fsmSnapshot struct {
	store store.Store
}

func (fs *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	slog.Warn("persist call for fsmSnapshot")
	defer sink.Close()
	ch := fs.store.SnapshotItems()
	keyCount := 0
	for {
		dataItem := <-ch
		item := dataItem.(*store.KVItem)
		if item.IsFinished() {
			break
		}
		protoKVItem := &kayakv1.KVItem{
			Key:    item.Key,
			Value:  item.Value,
			Bucket: item.Bucket,
		}
		keyCount = keyCount + 1
		data, err := proto.Marshal(protoKVItem)
		if err != nil {
			return err
		}

		if err := Write(sink, data); err != nil {
			return err
		}
	}

	slog.Warn("done persisting bold", "key_count", keyCount)
	return nil
}
func (f *fsmSnapshot) Release() {
	slog.Info("Releast action in fsmSnapshot")
}

func Write(w io.Writer, msg []byte) error {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(msg)))

	if _, err := w.Write(buf); err != nil {
		return err
	}

	if _, err := w.Write(msg); err != nil {
		return err
	}
	return nil
}
