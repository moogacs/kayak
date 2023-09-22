package service

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/bufbuild/protovalidate-go"
	"github.com/hashicorp/raft"
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/types/known/emptypb"

	adminv1 "github.com/binarymatt/kayak/gen/admin/v1"
)

var (
	mtx          sync.Mutex
	operations   = map[string]*future{}
	ErrNotLeader = errors.New("node is not the leader")
)

type future struct {
	f   raft.Future
	mtx sync.Mutex
}

func timeout(ctx context.Context) time.Duration {
	if dl, ok := ctx.Deadline(); ok {
		return time.Until(dl)
	}
	return 0
}

func toFuture(f raft.Future) (*connect.Response[adminv1.Future], error) {
	token := fmt.Sprintf("%x", sha1.Sum([]byte(fmt.Sprintf("%d", rand.Uint64()))))
	mtx.Lock()
	operations[token] = &future{f: f}
	mtx.Unlock()
	return connect.NewResponse(&adminv1.Future{
		OperationToken: token,
	}), nil
}

func (s *service) Await(ctx context.Context, req *connect.Request[adminv1.Future]) (*connect.Response[adminv1.AwaitResponse], error) {
	mtx.Lock()
	f, ok := operations[req.Msg.GetOperationToken()]
	mtx.Unlock()
	if !ok {
		return nil, fmt.Errorf("token %q unknown", req.Msg.GetOperationToken())
	}
	f.mtx.Lock()
	err := f.f.Error()
	f.mtx.Unlock()
	if err != nil {
		return connect.NewResponse(&adminv1.AwaitResponse{
			Error: err.Error(),
		}), nil
	}
	r := &adminv1.AwaitResponse{}
	if ifx, ok := f.f.(raft.IndexFuture); ok {
		r.Index = ifx.Index()
	}
	return connect.NewResponse(r), nil
}

func (s *service) Forget(ctx context.Context, req *connect.Request[adminv1.Future]) (*connect.Response[adminv1.ForgetResponse], error) {
	mtx.Lock()
	delete(operations, req.Msg.GetOperationToken())
	mtx.Unlock()
	return connect.NewResponse(&adminv1.ForgetResponse{}), nil
}

func (s *service) AddNonvoter(ctx context.Context, req *connect.Request[adminv1.AddNonvoterRequest]) (*connect.Response[adminv1.Future], error) {
	return toFuture(
		s.raft.AddNonvoter(
			raft.ServerID(req.Msg.GetId()),
			raft.ServerAddress(req.Msg.GetAddress()),
			req.Msg.GetPreviousIndex(),
			timeout(ctx),
		),
	)
}
func (s *service) AddVoter(ctx context.Context, req *connect.Request[adminv1.AddVoterRequest]) (*connect.Response[adminv1.Future], error) {
	id := req.Msg.GetId()
	if id == "" {
		id = ulid.Make().String()
	}
	return toFuture(
		s.raft.AddVoter(
			raft.ServerID(id),
			raft.ServerAddress(req.Msg.GetAddress()),
			req.Msg.GetPreviousIndex(),
			timeout(ctx),
		),
	)
}
func (s *service) AppliedIndex(ctx context.Context, req *connect.Request[adminv1.AppliedIndexRequest]) (*connect.Response[adminv1.AppliedIndexResponse], error) {
	return connect.NewResponse(&adminv1.AppliedIndexResponse{
		Index: s.raft.AppliedIndex(),
	}), nil
}
func (s *service) DemoteVoter(ctx context.Context, req *connect.Request[adminv1.DemoteVoterRequest]) (*connect.Response[adminv1.Future], error) {
	return toFuture(s.raft.DemoteVoter(raft.ServerID(req.Msg.GetId()), req.Msg.GetPreviousIndex(), timeout(ctx)))
}
func (s *service) GetConfiguration(ctx context.Context, req *connect.Request[adminv1.GetConfigurationRequest]) (*connect.Response[adminv1.GetConfigurationResponse], error) {
	f := s.raft.GetConfiguration()
	if err := f.Error(); err != nil {
		return nil, err
	}
	resp := &adminv1.GetConfigurationResponse{}
	for _, s := range f.Configuration().Servers {
		cs := &adminv1.GetConfigurationResponse_Server{
			Id:      string(s.ID),
			Address: string(s.Address),
		}
		switch s.Suffrage {
		case raft.Voter:
			cs.Suffrage = adminv1.GetConfigurationResponse_Server_VOTER
		case raft.Nonvoter:
			cs.Suffrage = adminv1.GetConfigurationResponse_Server_NONVOTER
		case raft.Staging:
			cs.Suffrage = adminv1.GetConfigurationResponse_Server_STAGING
		default:
			return nil, fmt.Errorf("unknown server suffrage %v for server %q", s.Suffrage, s.ID)
		}
		resp.Servers = append(resp.Servers, cs)
	}
	return connect.NewResponse(resp), nil
}
func (s *service) LastContact(ctx context.Context, req *connect.Request[adminv1.LastContactRequest]) (*connect.Response[adminv1.LastContactResponse], error) {
	t := s.raft.LastContact()
	return connect.NewResponse(&adminv1.LastContactResponse{
		UnixNano: t.UnixNano(),
	}), nil
}
func (s *service) LastIndex(ctx context.Context, req *connect.Request[adminv1.LastIndexRequest]) (*connect.Response[adminv1.LastIndexResponse], error) {
	return connect.NewResponse(&adminv1.LastIndexResponse{
		Index: s.raft.LastIndex(),
	}), nil
}
func (s *service) Leader(ctx context.Context, req *connect.Request[adminv1.LeaderRequest]) (*connect.Response[adminv1.LeaderResponse], error) {
	addr, id := s.raft.LeaderWithID()
	return connect.NewResponse(&adminv1.LeaderResponse{
		Address: string(addr),
		Id:      string(id),
	}), nil
}
func (s *service) LeadershipTransfer(ctx context.Context, req *connect.Request[adminv1.LeadershipTransferRequest]) (*connect.Response[adminv1.Future], error) {
	if s.raft.State() != raft.Leader {
		return nil, connect.NewError(connect.CodeInvalidArgument, ErrNotLeader)
	}
	return toFuture(s.raft.LeadershipTransfer())
}
func (s *service) LeadershipTransferToServer(ctx context.Context, req *connect.Request[adminv1.LeadershipTransferToServerRequest]) (*connect.Response[adminv1.Future], error) {
	return toFuture(s.raft.LeadershipTransferToServer(raft.ServerID(req.Msg.GetId()), raft.ServerAddress(req.Msg.GetAddress())))
}
func (s *service) RemoveServer(ctx context.Context, req *connect.Request[adminv1.RemoveServerRequest]) (*connect.Response[adminv1.Future], error) {
	if s.raft.State() != raft.Leader {
		return nil, connect.NewError(connect.CodeInvalidArgument, ErrNotLeader)
	}
	return toFuture(s.raft.RemoveServer(raft.ServerID(req.Msg.GetId()), req.Msg.GetPreviousIndex(), timeout(ctx)))
}
func (s *service) Shutdown(ctx context.Context, req *connect.Request[adminv1.ShutdownRequest]) (*connect.Response[adminv1.Future], error) {
	return toFuture(s.raft.Shutdown())
}
func (s *service) Snapshot(ctx context.Context, req *connect.Request[adminv1.SnapshotRequest]) (*connect.Response[adminv1.Future], error) {
	return toFuture(s.raft.Snapshot())
}
func (s *service) State(ctx context.Context, req *connect.Request[adminv1.StateRequest]) (*connect.Response[adminv1.StateResponse], error) {
	switch s := s.raft.State(); s {
	case raft.Follower:
		return connect.NewResponse(&adminv1.StateResponse{State: adminv1.StateResponse_FOLLOWER}), nil
	case raft.Candidate:
		return connect.NewResponse(&adminv1.StateResponse{State: adminv1.StateResponse_CANDIDATE}), nil
	case raft.Leader:
		return connect.NewResponse(&adminv1.StateResponse{State: adminv1.StateResponse_LEADER}), nil
	case raft.Shutdown:
		return connect.NewResponse(&adminv1.StateResponse{State: adminv1.StateResponse_SHUTDOWN}), nil
	default:
		return nil, fmt.Errorf("unknown raft state %v", s)
	}
}
func (s *service) VerifyLeader(ctx context.Context, req *connect.Request[adminv1.VerifyLeaderRequest]) (*connect.Response[adminv1.Future], error) {
	return toFuture(s.raft.VerifyLeader())
}

func (s *service) Join(ctx context.Context, req *connect.Request[adminv1.JoinRequest]) (*connect.Response[emptypb.Empty], error) {
	validator, err := protovalidate.New()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if err := validator.Validate(req.Msg); err != nil {
		var valErr *protovalidate.ValidationError
		if ok := errors.As(err, &valErr); ok {
			return nil, connect.NewError(connect.CodeInvalidArgument, valErr)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if _, err := s.sf.Join([]string{req.Msg.GetAddress()}, false); err != nil {
		return nil, connect.NewError(connect.CodeAborted, err)
	}
	return connect.NewResponse(&emptypb.Empty{}), nil
}
