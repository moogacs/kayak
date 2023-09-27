# Kayak
A raft based stream service.

## Description
Inspired by kafka, but targeting simplicity.

Kayak is an streaming server built on top of [Raft](https://raft.github.io/), using the go library implementation from [Hashicorp](https://github.com/hashicorp/raft).

TODO: GRPC/Connect description for api.

## Running Kayak
TODO: describe docker compose. (Earthly or raw Dockerfile)

### Failures

### Request Forwarding.
If a node receives a write request and is not the leader, it will forward on the request to the current leader.
