package transport

import (
	"context"
	"io"
	"log/slog"

	"connectrpc.com/connect"
	transportv1 "github.com/binarymatt/kayak/gen/transport/v1"
	"github.com/hashicorp/raft"
)

type service struct {
	manager *Manager
	transportv1.UnsafeRaftTransportServer
}

func isHeartbeat(command interface{}) bool {
	req, ok := command.(*raft.AppendEntriesRequest)
	if !ok {
		return false
	}
	return req.Term != 0 && len(req.RPCHeader.Addr) != 0 && req.PrevLogEntry == 0 && req.PrevLogTerm == 0 && len(req.Entries) == 0 && req.LeaderCommitIndex == 0
}

func (s service) handleRPC(command interface{}, data io.Reader) (interface{}, error) {
	ch := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  command,
		RespChan: ch,
		Reader:   data,
	}
	if isHeartbeat(command) {
		slog.Debug("heartbeating")
		// We can take the fast path and use the heartbeat callback and skip the queue in g.manager.rpcChan.
		s.manager.heartbeatFuncMtx.Lock()
		fn := s.manager.heartbeatFunc
		s.manager.heartbeatFuncMtx.Unlock()
		if fn != nil {
			slog.Debug("calling heartbeat fn")
			fn(rpc)
			goto wait
		}
	}
	s.manager.rpcChan <- rpc
wait:
	resp := <-ch
	if resp.Error != nil {
		slog.Error("resp channel error", "error", resp.Error)
		return nil, resp.Error
	}
	return resp.Response, nil
}
func (s service) AppendEntries(ctx context.Context, req *connect.Request[transportv1.AppendEntriesRequest]) (*connect.Response[transportv1.AppendEntriesResponse], error) {
	slog.Debug("appending entries via raft transport", "leader", req.Msg.Leader, "request_message", req.Msg)
	resp, err := s.handleRPC(decodeAppendEntriesRequest(req.Msg), nil)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(encodeAppendEntriesResponse(resp.(*raft.AppendEntriesResponse))), nil
}

func (s service) RequestVote(ctx context.Context, req *connect.Request[transportv1.RequestVoteRequest]) (*connect.Response[transportv1.RequestVoteResponse], error) {
	slog.Info("request vote")
	resp, err := s.handleRPC(decodeRequestVoteRequest(req.Msg), nil)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(encodeRequestVoteResponse(resp.(*raft.RequestVoteResponse))), nil
}

func (s service) TimeoutNow(ctx context.Context, req *connect.Request[transportv1.TimeoutNowRequest]) (*connect.Response[transportv1.TimeoutNowResponse], error) {
	slog.Info("Timeout Now")
	resp, err := s.handleRPC(decodeTimeoutNowRequest(req.Msg), nil)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(encodeTimeoutNowResponse(resp.(*raft.TimeoutNowResponse))), nil
}

func (s service) InstallSnapshot(ctx context.Context, stream *connect.ClientStream[transportv1.InstallSnapshotRequest]) (*connect.Response[transportv1.InstallSnapshotResponse], error) {
	stream.Receive()
	req := stream.Msg()
	resp, err := s.handleRPC(decodeInstallSnapshotRequest(req), &snapshotStream{stream: stream, buf: req.GetData()})
	if err != nil {
		return nil, connect.NewError(connect.CodeUnknown, err)
	}

	return connect.NewResponse(encodeInstallSnapshotResponse(resp.(*raft.InstallSnapshotResponse))), nil
}

type snapshotStream struct {
	stream *connect.ClientStream[transportv1.InstallSnapshotRequest]

	buf []byte
}

func (s *snapshotStream) Read(b []byte) (int, error) {
	if len(s.buf) > 0 {
		n := copy(b, s.buf)
		s.buf = s.buf[n:]
		return n, nil
	}
	s.stream.Receive()
	req := s.stream.Msg()

	n := copy(b, req.GetData())
	if n < len(req.GetData()) {
		s.buf = req.GetData()[n:]
	}
	return n, nil
}

func (s service) AppendEntriesPipeline(ctx context.Context, stream *connect.BidiStream[transportv1.AppendEntriesRequest, transportv1.AppendEntriesResponse]) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		request, err := stream.Receive()
		if err != nil {
			return err
		}
		resp, err := s.handleRPC(decodeAppendEntriesRequest(request), nil)
		if err != nil {
			return err
		}
		if err := stream.Send(encodeAppendEntriesResponse(resp.(*raft.AppendEntriesResponse))); err != nil {
			return err
		}
	}
}
