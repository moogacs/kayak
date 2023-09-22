package cli

import (
	"errors"
	"fmt"
	"log/slog"

	"connectrpc.com/connect"
	adminv1 "github.com/binarymatt/kayak/gen/admin/v1"
	nomad "github.com/hashicorp/nomad/api"
	"github.com/urfave/cli/v2"
)

var (
	ErrMultipleLeaders = errors.New("multiple leaders found")
	ErrMissingLeader   = errors.New("no leader found")
)

type khost struct {
	id   string
	host string
}

// SetupCluster gets the leader and then joins other nodes to the leader.
func SetupCluster(cctx *cli.Context) error {
	host := cctx.String("nomad_address")

	kayakHosts := []khost{}
	var leader string
	nc, err := nomad.NewClient(&nomad.Config{
		Address: host,
	})
	if err != nil {
		return err
	}

	allocs, _, err := nc.Jobs().Allocations("kayak", false, nil)

	if err != nil {
		return err
	}
	slog.Info("setting up cluster", "size", len(allocs))
	for _, alloc := range allocs {
		a, _, _ := nc.Allocations().Info(alloc.ID, nil)
		ports := a.AllocatedResources.Shared.Ports

		slog.Info(alloc.Name, "id", alloc.ID, "status", a.DeploymentStatus)
		for _, port := range ports {
			if port.Label == "grpc" {
				slog.Info("", "host", port.HostIP, "port", port.Value)
				node := fmt.Sprintf("%s:%d", port.HostIP, port.Value)
				kayakHosts = append(kayakHosts, khost{id: alloc.ID, host: node})
				ac := buildAdminClient(fmt.Sprintf("http://%s", node))
				resp, err := ac.Leader(cctx.Context, connect.NewRequest(&adminv1.LeaderRequest{}))
				if err != nil {
					return err
				}
				if resp.Msg.Address != "" {
					if leader != "" {
						slog.Info("multiple leaders found")
						return ErrMultipleLeaders
					}
					leader = node
				}
			}
		}
	}

	if leader == "" {
		return ErrMissingLeader
	}
	ac := buildAdminClient(fmt.Sprintf("http://%s", leader))
	for _, host := range kayakHosts {
		if host.host == leader {
			continue
		}

		_, err := ac.AddVoter(cctx.Context, connect.NewRequest(&adminv1.AddVoterRequest{
			Id:      host.id,
			Address: host.host,
		}))
		if err != nil {
			return err
		}

		slog.Info("added voter", "host", host.host, "id", host.id)
	}

	return nil
}
