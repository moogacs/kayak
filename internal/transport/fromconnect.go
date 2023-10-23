package transport

import (
	"github.com/hashicorp/raft"
	transportv1 "github.com/kayak/gen/proto/transport/v1"
)

func decodeAppendEntriesRequest(m *transportv1.AppendEntriesRequest) *raft.AppendEntriesRequest {
	return &raft.AppendEntriesRequest{
		RPCHeader:         decodeRPCHeader(m.RpcHeader),
		Term:              m.Term,
		Leader:            m.Leader,
		PrevLogEntry:      m.PrevLogEntry,
		PrevLogTerm:       m.PrevLogTerm,
		Entries:           decodeLogs(m.Entries),
		LeaderCommitIndex: m.LeaderCommitIndex,
	}
}

func decodeRPCHeader(m *transportv1.RPCHeader) raft.RPCHeader {
	return raft.RPCHeader{
		ProtocolVersion: raft.ProtocolVersion(m.ProtocolVersion),
		ID:              m.Id,
		Addr:            m.Addr,
	}
}
func decodeLogs(m []*transportv1.Log) []*raft.Log {
	ret := make([]*raft.Log, len(m))
	for i, l := range m {
		ret[i] = decodeLog(l)
	}
	return ret
}

func decodeLog(m *transportv1.Log) *raft.Log {
	return &raft.Log{
		Index:      m.Index,
		Term:       m.Term,
		Type:       decodeLogType(m.Type),
		Data:       m.Data,
		Extensions: m.Extensions,
		AppendedAt: m.AppendedAt.AsTime(),
	}
}

func decodeLogType(m transportv1.Log_LogType) raft.LogType {
	switch m {
	case transportv1.Log_LOG_COMMAND:
		return raft.LogCommand
	case transportv1.Log_LOG_NOOP:
		return raft.LogNoop
	case transportv1.Log_LOG_ADD_PEER_DEPRECATED:
		return raft.LogAddPeerDeprecated
	case transportv1.Log_LOG_REMOVE_PEER_DEPRECATED:
		return raft.LogRemovePeerDeprecated
	case transportv1.Log_LOG_BARRIER:
		return raft.LogBarrier
	case transportv1.Log_LOG_CONFIGURATION:
		return raft.LogConfiguration
	default:
		panic("invalid LogType")
	}
}

func decodeAppendEntriesResponse(m *transportv1.AppendEntriesResponse) *raft.AppendEntriesResponse {
	return &raft.AppendEntriesResponse{
		RPCHeader:      decodeRPCHeader(m.RpcHeader),
		Term:           m.Term,
		LastLog:        m.LastLog,
		Success:        m.Success,
		NoRetryBackoff: m.NoRetryBackoff,
	}
}

func decodeRequestVoteRequest(m *transportv1.RequestVoteRequest) *raft.RequestVoteRequest {
	return &raft.RequestVoteRequest{
		RPCHeader:          decodeRPCHeader(m.RpcHeader),
		Term:               m.Term,
		Candidate:          m.Candidate,
		LastLogIndex:       m.LastLogIndex,
		LastLogTerm:        m.LastLogTerm,
		LeadershipTransfer: m.LeadershipTransfer,
	}
}

func decodeRequestVoteResponse(m *transportv1.RequestVoteResponse) *raft.RequestVoteResponse {
	return &raft.RequestVoteResponse{
		RPCHeader: decodeRPCHeader(m.RpcHeader),
		Term:      m.Term,
		Peers:     m.Peers,
		Granted:   m.Granted,
	}
}

func decodeTimeoutNowRequest(m *transportv1.TimeoutNowRequest) *raft.TimeoutNowRequest {
	return &raft.TimeoutNowRequest{
		RPCHeader: decodeRPCHeader(m.RpcHeader),
	}
}

func decodeTimeoutNowResponse(m *transportv1.TimeoutNowResponse) *raft.TimeoutNowResponse {
	return &raft.TimeoutNowResponse{
		RPCHeader: decodeRPCHeader(m.RpcHeader),
	}
}
func decodeInstallSnapshotResponse(m *transportv1.InstallSnapshotResponse) *raft.InstallSnapshotResponse {
	return &raft.InstallSnapshotResponse{
		RPCHeader: decodeRPCHeader(m.RpcHeader),
		Term:      m.Term,
		Success:   m.Success,
	}
}

func decodeInstallSnapshotRequest(m *transportv1.InstallSnapshotRequest) *raft.InstallSnapshotRequest {
	return &raft.InstallSnapshotRequest{
		RPCHeader:          decodeRPCHeader(m.RpcHeader),
		SnapshotVersion:    raft.SnapshotVersion(m.SnapshotVersion),
		Term:               m.Term,
		Leader:             m.Leader,
		LastLogIndex:       m.LastLogIndex,
		LastLogTerm:        m.LastLogTerm,
		Peers:              m.Peers,
		Configuration:      m.Configuration,
		ConfigurationIndex: m.ConfigurationIndex,
		Size:               m.Size,
	}
}
