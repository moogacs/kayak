module github.com/kayak/client

go 1.21

replace github.com/kayak => ../.

require (
	connectrpc.com/connect v1.11.1
	github.com/hashicorp/go-retryablehttp v0.7.4
	github.com/kayak v0.0.0-00010101000000-000000000000
	github.com/oklog/ulid/v2 v2.1.0
	github.com/stretchr/testify v1.8.4
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.30.0-20230530223247-ca37dc8895db.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/net v0.12.0 // indirect
	golang.org/x/sys v0.11.0 // indirect
	golang.org/x/text v0.11.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230803162519-f966b187b2e5 // indirect
	google.golang.org/grpc v1.57.0 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
