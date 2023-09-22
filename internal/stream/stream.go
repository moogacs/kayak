package stream

import (
	"io"
	"io/ioutil"
	"sync"

	"github.com/hashicorp/raft"
)

type consumer struct {
	id            string
	accessedIndex int64
}
type StreamTracker struct {
	mtx       sync.RWMutex
	consumers map[string]consumer
}
type snapshot struct {
	consumers map[string]consumer
}

func (st *StreamTracker) Apply(l *raft.Log) interface{} {
	st.mtx.Lock()
	defer st.mtx.Unlock()
	//w := string(l.Data)
	return nil
}

func (f *StreamTracker) Snapshot() (raft.FSMSnapshot, error) {
	// Make sure that any future calls to f.Apply() don't change the snapshot.
	return &snapshot{}, nil
}

func (f *StreamTracker) Restore(r io.ReadCloser) error {
	_, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	//words := strings.Split(string(b), "\n")
	// copy(f.words[:], words)
	return nil
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (s *snapshot) Release() {}
