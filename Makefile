.PHONY: proto
proto:
	buf generate

.PHONY: mocks
mocks:
	mockery --all --with-expecter=true --dir=./internal/store

.PHONY: test
test:
	go test -v ./...

.PHONY: local_cluster
local_cluster:
	docker compose up

.PHONY: integration_test
integration_test:
	docker compose down
	earthly +docker
	docker compose up -d
	env KAYAK_INTEGRATION_TESTS=true go test -v ./kayak_test.go
	docker compose down

