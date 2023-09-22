package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	autopilot "github.com/hashicorp/raft-autopilot"
	boltdb "github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/sync/errgroup"

	"github.com/binarymatt/kayak/gen/admin/v1/adminv1connect"
	"github.com/binarymatt/kayak/gen/kayak/v1/kayakv1connect"
	"github.com/binarymatt/kayak/gen/transport/v1/transportv1connect"
	"github.com/binarymatt/kayak/internal/config"
	"github.com/binarymatt/kayak/internal/fsm"
	"github.com/binarymatt/kayak/internal/store"
	"github.com/binarymatt/kayak/internal/transport"
)

var (
	ErrInvalidPeerSet = errors.New("invalid peers")
)

func New(store store.Store, cfg *config.Config) (*service, error) {
	sigs := make(chan os.Signal, 1)
	eventChan := make(chan Event, 1)
	signal.Notify(sigs, syscall.SIGUSR1)
	slog.Info("creating service via New", "addr", cfg.RaftAddress())
	s := &service{
		cfg:        cfg,
		store:      store,
		reloadChan: sigs,
		raftAddr:   raft.ServerAddress(cfg.RaftAddress()),
		peers:      cfg.Peers,
		eventChan:  eventChan,
	}

	return s, nil
}

type service struct {
	store      store.Store
	raft       *raft.Raft
	cfg        *config.Config
	raftAddr   raft.ServerAddress
	manager    *transport.Manager
	reloadChan chan os.Signal
	eventChan  chan Event
	pilot      *autopilot.Autopilot
	serfChan   chan serf.Event
	serfPeers  []*serf.Member
	peers      []string
	sf         *serf.Serf
	ready      atomic.Bool
}

func (s *service) Raft() *raft.Raft {
	return s.raft
}

func (s *service) initAutopilot() {
	integration := NewIntegration(s)
	s.pilot = autopilot.New(s.raft, integration, autopilot.WithUpdateInterval(5*time.Second))
}

func (s *service) autopilotServers() map[raft.ServerID]*autopilot.Server {
	servers := make(map[raft.ServerID]*autopilot.Server)
	for _, member := range s.sf.Members() {
		srv, err := s.autopilotServer(member)
		if err != nil {
			slog.Error("Error building server information", "error", err)
			continue
		}
		servers[srv.ID] = srv
	}
	return servers
}

func (s *service) autopilotServer(m serf.Member) (*autopilot.Server, error) {
	tags := m.Tags
	slog.Debug("building autopilot server", "tags", m.Tags, "status", m.Status.String())
	raftVersion, err := strconv.Atoi(tags["raft_version"])
	if err != nil {
		slog.Error("could not parse raft version", "error", err)
		return nil, err
	}

	server := &autopilot.Server{
		Name:        m.Name,
		ID:          raft.ServerID(tags["server_id"]),
		Address:     raft.ServerAddress(tags["raft_address"]),
		Version:     "",
		RaftVersion: raftVersion,
	}
	switch m.Status {
	case serf.StatusLeft:
		server.NodeStatus = autopilot.NodeLeft
	case serf.StatusAlive, serf.StatusLeaving:
		// we want to treat leaving as alive to prevent autopilot from
		// prematurely removing the node.
		server.NodeStatus = autopilot.NodeAlive
	case serf.StatusFailed:
		server.NodeStatus = autopilot.NodeFailed
	default:
		server.NodeStatus = autopilot.NodeUnknown
	}
	return server, nil
}

func (s *service) initSerf(id string, cfg *raft.Config) error {
	s.serfChan = make(chan serf.Event, 16)
	discardLogger := log.New(io.Discard, "", log.Lshortfile)
	memberConfig := memberlist.DefaultLANConfig()
	memberConfig.BindPort = s.cfg.SerfPort
	memberConfig.AdvertisePort = s.cfg.SerfPort
	memberConfig.Logger = discardLogger

	serfConfig := serf.DefaultConfig()
	serfConfig.NodeName = s.cfg.ServerID
	serfConfig.EventCh = s.serfChan
	serfConfig.MemberlistConfig = memberConfig
	serfConfig.Logger = discardLogger
	raftVersion := strconv.Itoa(int(cfg.ProtocolVersion))
	slog.Warn("raft version", "version", raftVersion)
	serfConfig.Tags = map[string]string{
		"server_id":    id,
		"port":         strconv.Itoa(s.cfg.Port),
		"raft_address": s.cfg.RaftAddress(),
		"raft_version": raftVersion,
	}

	sf, err := serf.Create(serfConfig)
	if err != nil {
		slog.Error("could not create serf", "error", err)
		return err
	}
	s.sf = sf
	_, err = s.sf.Join(s.peers, false)
	if err != nil {
		slog.Error("could not join serf peers", "error", err)
		return err
	}
	slog.Info("serf has been initialized")
	return nil
}
func (s *service) Init() error {

	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(s.cfg.ServerID)

	baseDir := filepath.Join(s.cfg.DataDir, s.cfg.ServerID)

	ldb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "logs.dat"))
	if err != nil {
		return fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "logs.dat"), err)
	}

	sdb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "stable.dat"))
	if err != nil {
		return fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "stable.dat"), err)
	}

	fss, err := raft.NewFileSnapshotStore(baseDir, 3, os.Stderr)
	if err != nil {
		return fmt.Errorf(`error creating snapshot store (%q): %v`, baseDir, err)
	}

	fsm := fsm.NewStore(s.store)
	slog.Warn("fsm initialized")

	manager := transport.New(raft.ServerAddress(s.cfg.RaftAddress()), nil, transport.WithHeartbeatTimeout(30*time.Second))
	tp := manager.Transport()
	s.manager = manager
	c.LogLevel = "warn"
	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, tp)
	if err != nil {
		slog.Error("error setting up raft", "error", err)
		return err
	}
	slog.Warn("done setting up raft")
	s.raft = r

	s.initAutopilot()
	if err := s.initSerf(s.cfg.ServerID, c); err != nil {
		return err
	}

	return nil
}

// Start starts the different goroutines that power kayak.
func (s *service) Start(ctx context.Context) error {
	g := new(errgroup.Group)
	g.Go(func() error { return s.serve(ctx) })
	g.Go(func() error { return s.stats(ctx) })
	g.Go(func() error { return s.handleSerf(ctx) })
	// g.Go(func() error { return s.handleReload(ctx) })
	g.Go(func() error { s.startAutopilot(ctx); return nil })
	err := g.Wait()
	slog.Error("error from service errgroup", "error", err)
	return err
}

// Stop is for cleanup.
func (s *service) Stop(ctx context.Context) {
	close(s.eventChan)
}

func (s *service) startRaft(ctx context.Context) (func(), error) {
	closer := func() {
		<-ctx.Done()
		slog.Warn("context is done starting snapshot")
		if err := s.raft.Snapshot().Error(); err != nil {
			if err != raft.ErrNothingNewToSnapshot {
				slog.Error("error with snapshot", "error", err)
			}
		}
		if err := s.raft.Shutdown().Error(); err != nil {
			slog.Error("error shutting down raft", "error", err)
		}
		slog.Warn("done with shutdown")
	}

	bootstrap := s.cfg.Bootstrap
	slog.Warn("bootstrapping cluser", "bootstrap", bootstrap)
	if bootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(s.cfg.ServerID),
					Address:  s.raftAddr,
				},
			},
		}
		f := s.raft.BootstrapCluster(cfg)
		if f.Error() != nil && f.Error() != raft.ErrCantBootstrap {
			slog.Error("error bootstraping cluster", "error", f.Error())
			return func() {}, fmt.Errorf("raft bootstrap cluster: %w", f.Error())
		}
	}

	return closer, nil
}
func (s *service) SetReady() {
	s.ready.Store(true)
}
func (s *service) IsReady() bool {
	return s.ready.Load()
}
func (s *service) waitForServe(ctx context.Context, until time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			slog.Info("sleeping while waiting for server")
			time.Sleep(500 * time.Millisecond)

		}
		if s.IsReady() {
			return
		}
		slog.Debug("checking time, after select")
		if time.Now().After(until) {
			return
		}
	}
}
func (s *service) stats(ctx context.Context) error {

	tick := time.NewTicker(30 * time.Second)
	for {
		slog.Debug("starting stats loop")
		select {
		case <-ctx.Done():
			slog.Info("context done, shutting down stats loop")
			return nil
		case <-tick.C:
			slog.Debug("tick finished, emitting stats")
			attrs := []slog.Attr{}
			nodeStats := s.raft.Stats()
			for key, value := range nodeStats {
				attrs = append(attrs, slog.Any(key, value))
			}
			slog.Default().LogAttrs(ctx, slog.LevelInfo, "stats", attrs...)
		}
	}
}

func (s *service) startAutopilot(ctx context.Context) {
	until := time.Now().Add(10 * time.Second)
	s.waitForServe(ctx, until)
	slog.Info("starting autopilot")
	s.pilot.Start(ctx)
	<-ctx.Done()
	slog.Info("stopping pilot")
	s.pilot.Stop()
}

func (s *service) serve(ctx context.Context) error {
	slog.Warn("starting serve")
	closer, err := s.startRaft(ctx)
	if err != nil {
		return err
	}
	defer closer()
	path, handler := kayakv1connect.NewKayakServiceHandler(s,
		connect.WithInterceptors(
			otelconnect.NewInterceptor(),
			NewLogInterceptor(),
		),
	)
	raftPath, raftHandler := transportv1connect.NewRaftTransportHandler(s.manager.Service())
	adminPath, adminHandler := adminv1connect.NewAdminServiceHandler(s)
	slog.Info("setting up mux", "path", path, "admin_path", adminPath, "raft_path", raftPath)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	mux.Handle(raftPath, raftHandler)
	mux.Handle(adminPath, adminHandler)
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/ready", s.Ready)
	slog.Info("setting up api service", "advertise", s.cfg.ServiceAddress(), "listen", s.cfg.ListenAddress())
	server := http.Server{
		Addr:    s.cfg.ListenAddress(),
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}

	go func() {
		<-ctx.Done()
		timeoutCtx, cancelTimeout := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelTimeout()

		if err := server.Shutdown(timeoutCtx); err != nil {
			slog.Error("error shutting service", "error", err)
		}
		slog.Warn("done shutting down api server")
	}()

	slog.Warn("api server listen event")
	s.SetReady()
	slog.Info("api server starting")
	return server.ListenAndServe()
}

func (s *service) Ready(w http.ResponseWriter, r *http.Request) {
	slog.Debug("healthcheck!")
	if s.IsReady() {
		_, _ = w.Write([]byte("ok"))
		return
	}
	http.Error(w, "not ready", http.StatusServiceUnavailable)
}

func (s *service) handleSerf(ctx context.Context) error {
	slog.Info("running serf loop", "start", true)
	defer close(s.serfChan)
	for {
		select {
		case <-ctx.Done():
			slog.Info("context is done, shutting down serf event loop")
			return nil
		case ev := <-s.serfChan:
			slog.Info("serf event")
			leader := s.raft.VerifyLeader()
			if memberEvent, ok := ev.(serf.MemberEvent); ok {
				for _, member := range memberEvent.Members {
					eventType := memberEvent.EventType()

					if leader.Error() != nil {
						slog.Warn("not leader skipping serfEvent")
						continue
					}

					if eventType == serf.EventMemberJoin {
						slog.Info("member join event", "member", member)
						if err := s.addPeer(&member); err != nil {
							slog.Error("could not add peer", "error", err)
							return err
						}
					} else if eventType == serf.EventMemberLeave || eventType == serf.EventMemberFailed {
						slog.Info("member leave or failure event", "member", member)
						if err := s.removePeer(member); err != nil {
							slog.Error("could not remove peer", "error", err)
							return err
						}
					}
				}
			}
		}
	}
}

func (s *service) removePeer(member serf.Member) error {
	changed := fmt.Sprintf("%s:%d", member.Addr.String(), member.Port)
	index := -1
	slog.Info("iterating through peers to remove member")
	for i, member := range s.serfPeers {
		changedPeer := fmt.Sprintf("%s:%d", member.Addr.String(), member.Port)
		if changedPeer == changed {
			slog.Info("found matching record", "index", i)
			index = i
			break
		}
	}
	if index == -1 {
		slog.Info("index was never updated, skipping removal", "member", member.Status)
		return nil
	}
	s.serfPeers = append(s.serfPeers[:index], s.serfPeers[index+1:]...)

	serverID := member.Tags["server_id"]
	future := s.raft.RemoveServer(raft.ServerID(serverID), 0, 0)
	return future.Error()
}
func (s *service) addPeer(member *serf.Member) error {
	serverID := member.Tags["server_id"]
	address := member.Tags["raft_address"]
	slog.Info("adding peer", "address", address, "id", serverID)
	s.serfPeers = append(s.serfPeers, member)
	future := s.raft.AddVoter(raft.ServerID(serverID), raft.ServerAddress(address), 0, 0)
	return future.Error()
}
