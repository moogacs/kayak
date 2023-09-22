VERSION 0.7
FROM golang:1.21-alpine3.18
WORKDIR /go-workdir

deps:
    COPY go.mod go.sum ./
    RUN go mod download
    SAVE ARTIFACT go.mod AS LOCAL go.mod
    SAVE ARTIFACT go.sum AS LOCAL go.sum

test:
    FROM +deps
    COPY --dir * ./
    RUN go test ./...

build:
    FROM +deps
    COPY --dir * ./
    RUN	go build -o output/kayak ./cmd/kayak/main.go
    SAVE ARTIFACT output/kayak

docker:
    FROM alpine
    ARG EARTHLY_TARGET_TAG
    ARG EARTHLY_GIT_SHORT_HASH
    ARG TAG=$EARTHLY_GIT_SHORT_HASH
    COPY +build/kayak .
    ENTRYPOINT ["/kayak"]
    SAVE IMAGE kayak:latest
    SAVE IMAGE kayak:$TAG
