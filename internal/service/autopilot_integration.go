package service

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/binarymatt/kayak/gen/kayak/v1/kayakv1connect"
	"github.com/hashicorp/raft"
	autopilot "github.com/hashicorp/raft-autopilot"
	"google.golang.org/protobuf/types/known/emptypb"
)

type integration struct {
	service    *service
	config     *autopilot.Config
	removals   map[raft.ServerID]bool
	lock       sync.RWMutex
	serverLock sync.RWMutex
}

func NewIntegration(s *service) *integration {
	config := &autopilot.Config{
		CleanupDeadServers:   true,
		LastContactThreshold: 60 * time.Second,
	}
	return &integration{
		service:  s,
		config:   config,
		removals: make(map[raft.ServerID]bool),
	}
}
func (i *integration) AutopilotConfig() *autopilot.Config {
	return i.config
}
func (i *integration) NotifyState(st *autopilot.State) {
	if st == nil {
		slog.Warn("NotifyState is called, but state is nil", "state", st)
		return
	}
	slog.Debug("NotifyState has been called", slog.Group("state",
		"leader", st.Leader,
		"healthy", st.Healthy,
		"voter_count", len(st.Voters),
		"voters", st.Voters,
		"fault_tolerance", st.FailureTolerance,
		"server_count", len(st.Servers),
	))
}

func (i *integration) FetchServerStats(ctx context.Context, servers map[raft.ServerID]*autopilot.Server) map[raft.ServerID]*autopilot.ServerStats {
	statsResp := map[raft.ServerID]*autopilot.ServerStats{}
	for k, server := range servers {
		client := kayakv1connect.NewKayakServiceClient(http.DefaultClient, string(server.Address))
		resp, err := client.Stats(ctx, connect.NewRequest(&emptypb.Empty{}))
		if err != nil {
			continue
		}
		stats := resp.Msg.Raft
		slog.Info("", "stats", resp.Msg)
		duration := stats["last_contact"]
		lastIndex, err := strconv.ParseUint(stats["last_log_index"], 10, 64)
		if err != nil {
			slog.Error("could not get index", "error", err)
		}
		lastTerm, err := strconv.ParseUint(stats["last_log_term"], 10, 64)
		if err != nil {
			slog.Error("could not get term", "error", err)
		}
		lastContact, err := time.ParseDuration(duration)
		if err != nil {
			slog.Error("", "error", err)
			continue
		}
		statsResp[k] = &autopilot.ServerStats{
			LastContact: lastContact,
			LastIndex:   lastIndex,
			LastTerm:    lastTerm,
		}
	}

	return statsResp
}

func (i *integration) KnownServers() map[raft.ServerID]*autopilot.Server {
	i.serverLock.Lock()
	defer i.serverLock.Unlock()

	return i.service.autopilotServers()
}

func (i *integration) RemoveFailedServer(s *autopilot.Server) {
	go func() {
		defer func() {
			i.lock.Lock()
			delete(i.removals, s.ID)
			i.lock.Unlock()
		}()
		i.lock.Lock()
		if _, ok := i.removals[s.ID]; ok {
			slog.Info("server is in the process of being removed", "id", s.ID)
			i.lock.Unlock()
			return
		}
		i.removals[s.ID] = true
		i.lock.Unlock()

		if future := i.service.raft.RemoveServer(s.ID, 0, 0); future.Error() != nil {
			slog.Error("failed to remove server", "server_id", s.ID, "server_address", s.Address, "server_name", s.Name, "error", future.Error())
			return
		}
	}()
}
