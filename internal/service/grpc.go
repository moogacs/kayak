package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"github.com/bufbuild/protovalidate-go"
	"github.com/hashicorp/raft"
	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"

	kayakv1 "github.com/kayak/gen/proto/kayak/v1"
	"github.com/kayak/gen/proto/kayak/v1/kayakv1connect"
)

func (s *service) PutRecords(ctx context.Context, req *connect.Request[kayakv1.PutRecordsRequest]) (*connect.Response[emptypb.Empty], error) {
	slog.Info("put records request")
	v, err := protovalidate.New()
	if err != nil {
		fmt.Println("failed to initialize validator:", err)
	}
	if err := v.Validate(req.Msg); err != nil {
		slog.Error("invalid put request", "error", err)
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	for _, record := range req.Msg.Records {
		id := ulid.Make()
		record.Id = id.String()
		record.Topic = req.Msg.Topic
	}
	command := &kayakv1.Command{
		Payload: &kayakv1.Command_PutRecordsRequest{
			PutRecordsRequest: req.Msg,
		},
	}
	return s.applyCommand(ctx, command)
}

func (s *service) GetRecords(ctx context.Context, req *connect.Request[kayakv1.GetRecordsRequest]) (*connect.Response[kayakv1.GetRecordsResponse], error) {
	limit := int(req.Msg.Limit)
	def := false
	if limit == 0 {
		def = true
		limit = 99
	}
	slog.Info("getting records from store", "default", def, "limit", limit)
	records, err := s.store.GetRecords(ctx, req.Msg.Topic, req.Msg.Start, limit)
	if err != nil {
		slog.Error("error getting records from store", "error", err)
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(&kayakv1.GetRecordsResponse{
		Records: records,
	}), nil
}

func (s *service) FetchRecord(ctx context.Context, req *connect.Request[kayakv1.FetchRecordRequest]) (*connect.Response[kayakv1.FetchRecordsResponse], error) {
	logger := slog.With("consumer", req.Msg.ConsumerId, "topic", req.Msg.Topic, "position", req.Msg.Position)
	logger.Info("fetching records via grpc")

	position := req.Msg.Position
	if req.Msg.ConsumerId != "" {

		logger.Info("getting consumer group position")
		p, err := s.store.GetConsumerPosition(ctx, req.Msg.Topic, req.Msg.ConsumerId)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		slog.Info("retrieved consumer group position from local store", "fetched_position", p)
		position = p
	}
	logger.Info("fetching record")

	records, err := s.store.GetRecords(ctx, req.Msg.Topic, position, 1)
	if err != nil {
		logger.Error("error getting records from store", "error", err)
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	var record *kayakv1.Record
	if len(records) > 0 {
		record = records[0]
	}

	return connect.NewResponse(&kayakv1.FetchRecordsResponse{
		Record: record,
	}), nil
}

func (s *service) StreamRecords(ctx context.Context, req *connect.Request[kayakv1.StreamRecordsRequest], stream *connect.ServerStream[kayakv1.Record]) error {
	validator, err := protovalidate.New()
	if err != nil {
		return err
	}
	if err := validator.Validate(req.Msg); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	startTime := time.Now()
	startPosition := req.Msg.Position
	limit := req.Msg.BatchSize
	if limit == 0 {
		limit = 1
	}
	d := req.Msg.GetTimeout()
	var duration time.Duration
	if d == nil {
		duration = 1 * time.Second
	} else {
		duration = d.AsDuration()
	}
	for {
		records, err := s.store.GetRecords(ctx, req.Msg.Topic, startPosition, int(limit))
		if err != nil {
			return err
		}
		if len(records) > 0 {
			startTime = time.Now()
		}
		for _, record := range records {
			if err := stream.Send(record); err != nil {
				return err
			}
		}
		if startTime.Add(duration).After(time.Now()) {
			return nil
		}

	}
}

func (s *service) CommitRecord(ctx context.Context, req *connect.Request[kayakv1.CommitRecordRequest]) (*connect.Response[emptypb.Empty], error) {
	if err := req.Msg.Validate(); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	command := &kayakv1.Command{
		Payload: &kayakv1.Command_CommitRecordRequest{
			CommitRecordRequest: req.Msg,
		},
	}
	return s.applyCommand(ctx, command)
}
func (s *service) applyCommand(ctx context.Context, command *kayakv1.Command) (*connect.Response[emptypb.Empty], error) {

	if s.raft.State() != raft.Leader {
		leader := fmt.Sprintf("http://%s", s.raft.Leader())
		client := kayakv1connect.NewKayakServiceClient(http.DefaultClient, leader)
		slog.InfoContext(ctx, "applying to leader", "leader", s.raft.Leader())
		return client.Apply(ctx, connect.NewRequest(command))
	}
	data, err := protojson.Marshal(command)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	applyFuture := s.raft.Apply(data, 500*time.Millisecond)
	if err := applyFuture.Error(); err != nil {
		slog.ErrorContext(ctx, "could not apply command to raft", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&emptypb.Empty{}), nil
}

func (s *service) DeleteTopic(ctx context.Context, req *connect.Request[kayakv1.DeleteTopicRequest]) (*connect.Response[emptypb.Empty], error) {
	validator, err := protovalidate.New()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if err := validator.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	command := &kayakv1.Command{
		Payload: &kayakv1.Command_DeleteTopicRequest{
			DeleteTopicRequest: req.Msg,
		},
	}
	return s.applyCommand(ctx, command)
}
func (s *service) CreateTopic(ctx context.Context, req *connect.Request[kayakv1.CreateTopicRequest]) (*connect.Response[emptypb.Empty], error) {
	if err := req.Msg.Validate(); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	slog.InfoContext(ctx, "creating topic", "topic", req.Msg.Name)

	command := &kayakv1.Command{
		Payload: &kayakv1.Command_CreateTopicRequest{
			CreateTopicRequest: req.Msg,
		},
	}
	return s.applyCommand(ctx, command)
}

func (s *service) ListTopics(ctx context.Context, req *connect.Request[kayakv1.ListTopicsRequest]) (*connect.Response[kayakv1.ListTopicsResponse], error) {
	names, err := s.store.ListTopics(ctx)
	if err != nil {
		slog.Error("error with store ListTopics", "error", err)
	}
	var topics []*kayakv1.Topic
	for _, name := range names {
		topics = append(topics, &kayakv1.Topic{
			Name: name,
		})
	}
	return connect.NewResponse(&kayakv1.ListTopicsResponse{Topics: topics}), nil
}

func (s *service) Stats(ctx context.Context, _ *connect.Request[emptypb.Empty]) (*connect.Response[kayakv1.StatsResponse], error) {
	_, span := otel.GetTracerProvider().Tracer("").Start(ctx, "stats-span")
	defer span.End()
	return connect.NewResponse(&kayakv1.StatsResponse{
		Raft:  s.raft.Stats(),
		Store: s.store.Stats(),
	}), nil
}

func (s *service) GetNodeDetails(ctx context.Context, _ *connect.Request[emptypb.Empty]) (*connect.Response[kayakv1.GetNodeDetailsResponse], error) {
	return connect.NewResponse(&kayakv1.GetNodeDetailsResponse{
		Id: s.cfg.ServerID,
	}), nil
}

func (s *service) Apply(ctx context.Context, req *connect.Request[kayakv1.Command]) (*connect.Response[emptypb.Empty], error) {
	_, span := otel.GetTracerProvider().Tracer("").Start(ctx, "apply-span")
	defer span.End()
	if req.Msg.GetCreateTopicRequest() != nil {
		return s.CreateTopic(ctx, connect.NewRequest(req.Msg.GetCreateTopicRequest()))
	}
	if req.Msg.GetPutRecordsRequest() != nil {
		return s.PutRecords(ctx, connect.NewRequest(req.Msg.GetPutRecordsRequest()))
	}
	if req.Msg.GetCommitRecordRequest() != nil {
		return s.CommitRecord(ctx, connect.NewRequest(req.Msg.GetCommitRecordRequest()))
	}
	if req.Msg.GetDeleteTopicRequest() != nil {
		return s.DeleteTopic(ctx, connect.NewRequest(req.Msg.GetDeleteTopicRequest()))
	}

	return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("not valid"))
}
