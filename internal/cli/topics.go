package cli

import (
	"fmt"

	"connectrpc.com/connect"
	"github.com/urfave/cli/v2"

	kayakv1 "github.com/binarymatt/kayak/gen/kayak/v1"
)

func CreateTopic(ctx *cli.Context) error {
	client := buildClient(ctx.String("host"))
	_, err := client.CreateTopic(ctx.Context, connect.NewRequest(&kayakv1.CreateTopicRequest{
		Name: ctx.String("name"),
	}))
	return err
}
func ListTopics(cCtx *cli.Context) error {
	client := buildClient(cCtx.String("host"))
	resp, err := client.ListTopics(cCtx.Context, connect.NewRequest(&kayakv1.ListTopicsRequest{}))
	if err != nil {
		return err
	}
	for _, topic := range resp.Msg.Topics {
		fmt.Println(topic.GetName())
	}
	return nil
}
