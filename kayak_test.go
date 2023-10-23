package kayak_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/kayak/client"
	kayakv1 "github.com/kayak/gen/proto/kayak/v1"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

var (
	nodes = []string{
		"http://127.0.0.1:8081",
		"http://127.0.0.1:8082",
		"http://127.0.0.1:8083",
	}
	records = []*kayakv1.Record{
		{Headers: map[string]string{"number": "one"}, Payload: []byte(`{"json":"payload"}`)},
		{Payload: []byte(`{"json":"payload2"}`)},
		{Payload: []byte(`{"json":"payload3"}`)},
		{Payload: []byte("text payload")},
	}

	primaryTopic = "test"
)

type KayakIntegrationTestSuite struct {
	suite.Suite
	client          *client.Client
	secondaryClient *client.Client
}

func (s *KayakIntegrationTestSuite) checkRecord(index int, record *kayakv1.Record) {
	r := s.Require()
	r.NotNil(record)
	r.Equal(records[index].Payload, record.Payload)
	r.Equal(records[index].Headers, record.Headers)
	r.NotEmpty(record.Id)

}

func (s *KayakIntegrationTestSuite) protoEqual(expected, actual proto.Message) {
	s.Empty(cmp.Diff(expected, actual, protocmp.Transform()))
}

func (s *KayakIntegrationTestSuite) SetupSuite() {
	s.client = client.New(
		client.NewConfig(""),
		client.WithAddress(nodes[0]),
		client.WithTopic(primaryTopic),
		client.WithConsumerID("test"),
	)
	s.secondaryClient = client.New(
		client.NewConfig(""),
		client.WithAddress(nodes[1]),
		client.WithTopic(primaryTopic),
		client.WithConsumerID("test"),
	)
}
func (s *KayakIntegrationTestSuite) SetupTest() {
	_ = s.client.CreateTopic(context.Background(), primaryTopic)
}
func (s *KayakIntegrationTestSuite) AfterTest() {
	_ = s.client.DeleteTopic(context.Background(), primaryTopic)
	// s.resetTopic()
}
func (s *KayakIntegrationTestSuite) TestPutRecords() {
	r := s.Require()
	ctx := context.Background()
	err := s.client.PutRecords(ctx, records)
	r.NoError(err)
	items, err := s.client.GetRecords(ctx, primaryTopic, "", 4)
	r.NoError(err)
	r.Len(items, 4)
	// validate ordering
	r.Equal(records[0].Payload, items[0].Payload)
	r.Equal(records[1].Payload, items[1].Payload)
	r.Equal(records[2].Payload, items[2].Payload)
	r.Equal(records[3].Payload, items[3].Payload)
}
func (s *KayakIntegrationTestSuite) TestFetchNoRecords() {
	r := s.Require()
	ctx := context.Background()
	record, err := s.client.FetchRecord(ctx)
	r.NoError(err)
	r.Nil(record)
}

func (s *KayakIntegrationTestSuite) TestFetchRecordNoCommit() {
	r := s.Require()
	ctx := context.Background()
	err := s.client.PutRecords(ctx, records)
	r.NoError(err)

	record, err := s.client.FetchRecord(ctx)
	r.NoError(err)
	r.Equal(records[0].Headers, record.Headers)
	r.Equal(records[0].Payload, record.Payload)

	record2, err := s.client.FetchRecord(ctx)
	r.NoError(err)
	s.protoEqual(record, record2)
}

func (s *KayakIntegrationTestSuite) TestFetchRecords() {

	r := s.Require()
	ctx := context.Background()
	err := s.client.PutRecords(ctx, records)
	r.NoError(err)

	// CASE: ordering
	first, err := s.client.FetchRecord(ctx)
	r.NoError(err)
	s.checkRecord(0, first)

	err = s.client.CommitRecord(ctx, first)
	r.NoError(err)

	second, err := s.client.FetchRecord(ctx)
	r.NoError(err)
	s.checkRecord(1, second)

	err = s.client.CommitRecord(ctx, second)
	r.NoError(err)

	time.Sleep(500 * time.Millisecond)
	// CASE: fetch across different brokers
	third, err := s.secondaryClient.FetchRecord(ctx)
	r.NoError(err)
	s.checkRecord(2, third)

	err = s.secondaryClient.CommitRecord(ctx, third)
	r.NoError(err)

	fourth, err := s.client.FetchRecord(ctx)
	r.NoError(err)
	s.checkRecord(3, fourth)
}
func TestIntegrationTestSuite(t *testing.T) {
	runIntegration := os.Getenv("KAYAK_INTEGRATION_TESTS")
	if runIntegration == "" {
		t.Skip("skipping test in short mode.")
	}
	suite.Run(t, new(KayakIntegrationTestSuite))
}
