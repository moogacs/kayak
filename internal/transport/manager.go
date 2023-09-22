package transport

import (
	"errors"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	errCloseErr = errors.New("error closing connections")
)

type Manager struct {
	rpcChan      chan raft.RPC
	localAddress raft.ServerAddress
	dialOptions  []grpc.DialOption

	heartbeatFunc    func(raft.RPC)
	heartbeatFuncMtx sync.Mutex
	heartbeatTimeout time.Duration

	connectionsMtx sync.Mutex
	connections    map[raft.ServerID]*conn
}

func New(localAddress raft.ServerAddress, dialOptions []grpc.DialOption, options ...Option) *Manager {
	dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	m := &Manager{
		localAddress: localAddress,
		dialOptions:  dialOptions,

		rpcChan:     make(chan raft.RPC),
		connections: map[raft.ServerID]*conn{},
	}
	for _, opt := range options {
		opt(m)
	}
	return m
}

func (m *Manager) Service() *service {
	return &service{manager: m}
}

func (m *Manager) Transport() raft.Transport {
	return raftAPI{m}
}

func (m *Manager) Close() error {
	m.connectionsMtx.Lock()
	defer m.connectionsMtx.Unlock()

	err := errCloseErr
	for _, conn := range m.connections {
		// Lock conn.mtx to ensure Dial() is complete
		conn.mtx.Lock()
		closeErr := conn.clientConn.Close()
		if closeErr != nil {
			err = multierror.Append(err, closeErr)
		}
		conn.mtx.Unlock()
	}

	if err != errCloseErr {
		return err
	}

	return nil
}
