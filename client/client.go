package client

import (
	"context"
	"log/slog"
	"net/http"

	"connectrpc.com/connect"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/oklog/ulid/v2"

	kayakv1 "github.com/kayak/gen/proto/kayak/v1"
	"github.com/kayak/gen/proto/kayak/v1/kayakv1connect"
)

type Config struct {
	Address    string
	ConsumerID string
	HTTPClient *http.Client
	ID         string
	Topic      string
}
type CfgOption func(*Config)

func NewConfig(id string) *Config {
	if id == "" {
		id = ulid.Make().String()
	}

	return &Config{
		ID:         id,
		HTTPClient: retryablehttp.NewClient().StandardClient(),
	}
}

func WithAddress(address string) CfgOption {
	return func(c *Config) {
		c.Address = address
	}
}

func WithTopic(topic string) CfgOption {
	return func(c *Config) {
		c.Topic = topic
	}
}

func WithConsumerID(consumer string) CfgOption {
	return func(c *Config) {
		c.ConsumerID = consumer
	}
}

func WithHTTPClient(client *http.Client) CfgOption {
	return func(c *Config) {
		c.HTTPClient = client
	}
}

type Client struct {
	cfg    *Config
	client kayakv1connect.KayakServiceClient
}

func (c *Client) PutRecords(ctx context.Context, records []*kayakv1.Record) error {
	topic := c.cfg.Topic
	_, err := c.client.PutRecords(ctx, connect.NewRequest(&kayakv1.PutRecordsRequest{
		Topic:   topic,
		Records: records,
	}))
	return err
}

func (c *Client) FetchRecord(ctx context.Context) (*kayakv1.Record, error) {
	req := connect.NewRequest(&kayakv1.FetchRecordRequest{
		Topic:      c.cfg.Topic,
		ConsumerId: c.cfg.ConsumerID,
	})
	slog.Info("kayak client fetch", "topic", c.cfg.Topic, "consumer", c.cfg.ConsumerID)
	resp, err := c.client.FetchRecord(ctx, req)
	return resp.Msg.GetRecord(), err
}

func (c *Client) CommitRecord(ctx context.Context, record *kayakv1.Record) error {
	_, err := c.client.CommitRecord(ctx, connect.NewRequest(&kayakv1.CommitRecordRequest{
		Topic:      c.cfg.Topic,
		ConsumerId: c.cfg.ConsumerID,
		Position:   record.GetId(),
	}))
	return err
}

func (c *Client) GetRecords(ctx context.Context, topic string, start string, limit int32) ([]*kayakv1.Record, error) {
	req := connect.NewRequest(&kayakv1.GetRecordsRequest{
		Topic: topic,
		Start: start,
		Limit: limit,
	})
	resp, err := c.client.GetRecords(ctx, req)
	return resp.Msg.GetRecords(), err
}
func (c *Client) CreateTopic(ctx context.Context, topic string) error {
	_, err := c.client.CreateTopic(ctx, connect.NewRequest(&kayakv1.CreateTopicRequest{
		Name: topic,
	}))
	return err
}
func (c *Client) DeleteTopic(ctx context.Context, topic string) error {
	req := connect.NewRequest(&kayakv1.DeleteTopicRequest{
		Topic: topic,
	})
	_, err := c.client.DeleteTopic(ctx, req)
	return err
}

func New(cfg *Config, options ...CfgOption) *Client {
	for _, f := range options {
		f(cfg)
	}
	c := kayakv1connect.NewKayakServiceClient(cfg.HTTPClient, cfg.Address)
	return &Client{
		client: c,
		cfg:    cfg,
	}
}
