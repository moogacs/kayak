# Use the Go image to build our application.
FROM golang:1.21-alpine3.18 as builder

# Copy the present working directory to our source directory in Docker.
# Change the current directory in Docker to our source directory.
COPY . /src/kayak
WORKDIR /src/kayak
RUN go mod download

RUN	go build -o /usr/local/bin/kayak ./cmd/kayak/main.go


# This starts our final image; based on alpine to make it small.
FROM alpine AS kayak

# Copy executable from builder.
COPY --from=builder /usr/local/bin/kayak /usr/local/bin/kayak

RUN apk add bash

# Create data directory (although this will likely be mounted too)
RUN mkdir -p /data

ENTRYPOINT ["/usr/local/bin/kayak"]
