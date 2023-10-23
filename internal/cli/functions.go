package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"connectrpc.com/connect"
	"github.com/urfave/cli/v2"
	"google.golang.org/protobuf/types/known/emptypb"

	adminv1 "github.com/kayak/gen/proto/admin/v1"
)

func AdminLeader(cctx *cli.Context) error {
	client := buildAdminClient(cctx.String("host"))
	resp, err := client.Leader(cctx.Context, connect.NewRequest(&adminv1.LeaderRequest{}))
	if err != nil {
		return err
	}
	slog.Info("admin leader", "address", resp.Msg.Address, "id", resp.Msg.Id)
	return nil
}

func AddVoter(cctx *cli.Context) error {

	nodeId := cctx.String("id")
	address := cctx.String("address")
	client := buildAdminClient(cctx.String("host"))
	f, err := client.AddVoter(cctx.Context, connect.NewRequest(&adminv1.AddVoterRequest{
		Id:      nodeId,
		Address: address,
	}))
	if err != nil {
		return err
	}
	resp, err := client.Await(cctx.Context, connect.NewRequest(f.Msg))
	if err != nil {
		return err
	}
	slog.Info("add voter response", "index", resp.Msg.Index, "error", resp.Msg.Error)
	if _, err := client.Forget(cctx.Context, connect.NewRequest(f.Msg)); err != nil {
		return err
	}
	return nil
}

func RemoveServer(cctx *cli.Context) error {
	client := buildAdminClient(cctx.String("host"))

	f, err := client.RemoveServer(cctx.Context, connect.NewRequest(&adminv1.RemoveServerRequest{
		Id: cctx.String("id"),
	}))
	if err != nil {
		return err
	}
	resp, err := client.Await(cctx.Context, connect.NewRequest(f.Msg))
	if err != nil {
		return err
	}
	if resp.Msg.Error != "" {
		return errors.New(resp.Msg.Error)
	}

	if _, err := client.Forget(cctx.Context, connect.NewRequest(f.Msg)); err != nil {
		return err
	}
	slog.Info("server removed", "server", cctx.String("host"))
	return nil
}

func GetConfiguration(cctx *cli.Context) error {
	client := buildAdminClient(cctx.String("host"))

	resp, err := client.GetConfiguration(cctx.Context, connect.NewRequest(&adminv1.GetConfigurationRequest{}))
	if err != nil {
		return err
	}
	val, err := json.MarshalIndent(resp.Msg, "", "    ")
	//val, err := json.Marshal(resp.Msg)
	if err != nil {
		return err
	}
	fmt.Println(string(val))
	return nil
}

func Stats(cCtx *cli.Context) error {
	client := buildClient(cCtx.String("host"))
	//resp, err := client.Ring(cCtx.Context, connect.NewRequest(&emptypb.Empty{}))
	resp, err := client.Stats(cCtx.Context, connect.NewRequest(&emptypb.Empty{}))
	if err != nil {
		return err
	}
	val, err := json.MarshalIndent(resp.Msg, "", "    ")
	if err != nil {
		return err
	}
	fmt.Println(string(val))
	return nil
}
