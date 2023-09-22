package config

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Path      string   `yaml:"path"`
	ServerID  string   `yaml:"id"`
	Host      string   `yaml:"host"`
	Port      int      `yaml:"port"`
	DataDir   string   `yaml:"dir"`
	Bootstrap bool     `yaml:"bootstrap"`
	Peers     []string `yaml:"peers"`
	SerfPort  int      `yaml:"serf_port"`
	// MemberBindPort      int      `yaml:"member_bind_port"`
	// MemberAdvertisePort int      `yaml:"member_advertise_port"`
	// MemberAdvertiseAddr string   `yaml:"member_advertise_addr"`
	// MemberPeers         []string `yaml:"member_peers"`
	// AdvertisePort int    `yaml:"advertise_port"`
	// RaftBind      string   `yaml:"raft_bind"`
}

func (c *Config) RaftAddress() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

func (c *Config) ServiceAddress() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

func (c *Config) ListenAddress() string {
	return fmt.Sprintf("0.0.0.0:%d", c.Port)
}

func (c *Config) DataPath() string {
	return filepath.Join(c.DataDir, c.ServerID, "data.db")
}

func New(cctx *cli.Context) *Config {
	return &Config{
		Path:      cctx.String("config"),
		ServerID:  cctx.String("id"),
		Host:      cctx.String("host"),
		Port:      cctx.Int("port"),
		DataDir:   cctx.String("dir"),
		Peers:     cctx.StringSlice("peers"),
		Bootstrap: cctx.Bool("bootstrap"),
		SerfPort:  cctx.Int("serf_port"),
	}
}

func Parse(path string) (*Config, error) {
	slog.Info("parsing config", "path", path)
	data, err := os.ReadFile(path)
	if err != nil {
		slog.Error("could not read config file", "error", err)
		return nil, err
	}
	cg := Config{}
	err = yaml.Unmarshal(data, &cg)
	if err != nil {
		return nil, err
	}

	return &cg, nil
}
