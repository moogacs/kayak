package service

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/suite"

	"github.com/kayak/internal/config"
	"github.com/kayak/mocks"
)

type ServiceTestSuite struct {
	suite.Suite
	mockStore *mocks.Store
	service   *service
	cluster   Cluster
}
type Cluster interface {
	Close()
}

func (s *ServiceTestSuite) SetupSuite() {
	l := slog.New(slog.NewTextHandler(io.Discard, nil))
	slog.SetDefault(l)
}

func (s *ServiceTestSuite) TearDownTest() {
	s.cluster.Close()
}
func (s *ServiceTestSuite) SetupTest() {
	hclog.SetDefault(hclog.NewNullLogger())
	conf := raft.DefaultConfig()
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond
	conf.CommitTimeout = 5 * time.Millisecond
	conf.LogOutput = io.Discard
	conf.LogLevel = "ERROR"
	conf.Logger = hclog.NewNullLogger()
	conf.Logger.SetLevel(hclog.Off)

	c := raft.MakeCluster(1, s.T(), conf)
	r := c.Leader()

	//setup service
	store := mocks.NewStore(s.T())
	svc, err := New(store, &config.Config{})
	s.NoError(err)
	svc.raft = r
	s.service = svc
	s.cluster = c
	s.mockStore = store
}

func TestServiceTestSuite(t *testing.T) {
	suite.Run(t, new(ServiceTestSuite))
}

func (s *ServiceTestSuite) TestReady() {
	r := s.Require()
	req := httptest.NewRequest("GET", "http://example.com/ready", nil)
	w := httptest.NewRecorder()
	s.service.Ready(w, req)
	resp := w.Result()
	r.Equal(http.StatusServiceUnavailable, resp.StatusCode)

	s.service.SetReady()
	w2 := httptest.NewRecorder()
	s.service.Ready(w2, req)
	resp2 := w2.Result()
	r.Equal(http.StatusOK, resp2.StatusCode)
}
