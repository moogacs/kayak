package transport

import (
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/kayak/gen/proto/transport/v1"
	transportv1 "github.com/kayak/gen/proto/transport/v1"
)

func encodeAppendEntriesRequest(s *raft.AppendEntriesRequest) *transportv1.AppendEntriesRequest {
	return &transportv1.AppendEntriesRequest{
		RpcHeader:         encodeRPCHeader(s.RPCHeader),
		Term:              s.Term,
		Leader:            s.Leader,
		PrevLogEntry:      s.PrevLogEntry,
		PrevLogTerm:       s.PrevLogTerm,
		Entries:           encodeLogs(s.Entries),
		LeaderCommitIndex: s.LeaderCommitIndex,
	}
}
func encodeRPCHeader(r raft.RPCHeader) *transportv1.RPCHeader {
	return &transportv1.RPCHeader{
		ProtocolVersion: int64(r.ProtocolVersion),
		Id:              r.ID,
		Addr:            r.Addr,
	}
}

func encodeLogs(s []*raft.Log) []*transportv1.Log {
	ret := make([]*transportv1.Log, len(s))
	for i, l := range s {
		ret[i] = encodeLog(l)
	}
	return ret
}

func encodeLog(s *raft.Log) *transportv1.Log {
	return &transportv1.Log{
		Index:      s.Index,
		Term:       s.Term,
		Type:       encodeLogType(s.Type),
		Data:       s.Data,
		Extensions: s.Extensions,
		AppendedAt: timestamppb.New(s.AppendedAt),
	}
}

func encodeLogType(s raft.LogType) transportv1.Log_LogType {
	switch s {
	case raft.LogCommand:
		return transportv1.Log_LOG_COMMAND
	case raft.LogNoop:
		return transportv1.Log_LOG_NOOP
	case raft.LogAddPeerDeprecated:
		return transportv1.Log_LOG_ADD_PEER_DEPRECATED
	case raft.LogRemovePeerDeprecated:
		return transportv1.Log_LOG_REMOVE_PEER_DEPRECATED
	case raft.LogBarrier:
		return transportv1.Log_LOG_BARRIER
	case raft.LogConfiguration:
		return transportv1.Log_LOG_CONFIGURATION
	default:
		panic("invalid LogType")
	}
}

func encodeAppendEntriesResponse(s *raft.AppendEntriesResponse) *pb.AppendEntriesResponse {
	return &pb.AppendEntriesResponse{
		RpcHeader:      encodeRPCHeader(s.RPCHeader),
		Term:           s.Term,
		LastLog:        s.LastLog,
		Success:        s.Success,
		NoRetryBackoff: s.NoRetryBackoff,
	}
}

func encodeRequestVoteRequest(s *raft.RequestVoteRequest) *transportv1.RequestVoteRequest {
	return &transportv1.RequestVoteRequest{
		RpcHeader:          encodeRPCHeader(s.RPCHeader),
		Term:               s.Term,
		Candidate:          s.Candidate,
		LastLogIndex:       s.LastLogIndex,
		LastLogTerm:        s.LastLogTerm,
		LeadershipTransfer: s.LeadershipTransfer,
	}
}

func encodeRequestVoteResponse(s *raft.RequestVoteResponse) *pb.RequestVoteResponse {
	return &pb.RequestVoteResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
		Term:      s.Term,
		Peers:     s.Peers,
		Granted:   s.Granted,
	}
}

func encodeInstallSnapshotRequest(s *raft.InstallSnapshotRequest) *pb.InstallSnapshotRequest {
	return &pb.InstallSnapshotRequest{
		RpcHeader:          encodeRPCHeader(s.RPCHeader),
		SnapshotVersion:    int64(s.SnapshotVersion),
		Term:               s.Term,
		Leader:             s.Leader,
		LastLogIndex:       s.LastLogIndex,
		LastLogTerm:        s.LastLogTerm,
		Peers:              s.Peers,
		Configuration:      s.Configuration,
		ConfigurationIndex: s.ConfigurationIndex,
		Size:               s.Size,
	}
}

func encodeInstallSnapshotResponse(s *raft.InstallSnapshotResponse) *pb.InstallSnapshotResponse {
	return &pb.InstallSnapshotResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
		Term:      s.Term,
		Success:   s.Success,
	}
}

func encodeTimeoutNowRequest(s *raft.TimeoutNowRequest) *pb.TimeoutNowRequest {
	return &pb.TimeoutNowRequest{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
	}
}

func encodeTimeoutNowResponse(s *raft.TimeoutNowResponse) *pb.TimeoutNowResponse {
	return &pb.TimeoutNowResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
	}
}
