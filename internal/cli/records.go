package cli

import (
	"fmt"

	"connectrpc.com/connect"
	"github.com/oklog/ulid/v2"
	"github.com/urfave/cli/v2"

	kayakv1 "github.com/binarymatt/kayak/gen/kayak/v1"
)

func CreateRecords(cctx *cli.Context) error {
	records := []*kayakv1.Record{
		{
			Id:      ulid.Make().String(),
			Topic:   cctx.String("topic"),
			Payload: []byte(cctx.String("data")),
		},
	}
	client := buildClient(cctx.String("host"))
	_, err := client.PutRecords(cctx.Context, connect.NewRequest(&kayakv1.PutRecordsRequest{
		Topic:   cctx.String("topic"),
		Records: records,
	}))
	return err
}
func ListRecords(cCtx *cli.Context) error {
	client := buildClient(cCtx.String("host"))
	resp, err := client.GetRecords(cCtx.Context, connect.NewRequest(&kayakv1.GetRecordsRequest{
		Topic: cCtx.String("topic"),
	}))
	if err != nil {
		return err
	}
	for _, record := range resp.Msg.Records {
		fmt.Println(record)
	}
	return nil
}
