# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a CodeCrafters challenge to implement a Kafka broker in Go. The broker listens on TCP port 9092 and implements the Kafka wire protocol (ApiVersions, Fetch, and other APIs as stages progress).

## Build & Run

```bash
# Compile
go build -o /tmp/codecrafters-build-kafka-go app/*.go

# Run locally (compiles + runs)
./your_program.sh

# Run tests
go test -v -timeout 10s ./app/...

# Submit to CodeCrafters for automated testing
codecrafters submit
```

## Architecture

The broker logic is split across five files under `app/`:

- **`app/main.go`** — `Server` struct, `NewServer()`, `Run()` accept loop, `handleRequest()` per-connection request loop, `writeResponse()`
- **`app/request.go`** — `Request` struct, `parseRequest()` binary decoder; `ProduceRequest` types, `parseProduceRequest()`, `readCompactString()`, `readCompactBytes()`
- **`app/response.go`** — `Encoder` interface, `HeaderResponse`, `ApiVersionsResponse`, `ProduceResponse` types, `NewResponse()` factory, `createApiVersionsResponse()`, `createApiProduceResponse()`
- **`app/metadata.go`** — `ClusterMetadata`, `TopicMetadata`, `PartitionMetadata` types; `readClusterMetadata()` parses the KRaft `__cluster_metadata-0` log segment; `validateTopicPartition()` checks existence
- **`app/*_test.go`** — unit and integration tests, one file per source file

**Module**: `github.com/codecrafters-io/kafka-starter-go` (Go 1.26, no external dependencies)  
**Port**: Always binds to `0.0.0.0:9092`

### Kafka Wire Protocol Conventions

Request format (big-endian):
```
[4 bytes] MessageSize  (int32)  — total bytes that follow
[2 bytes] API Key      (int16)
[2 bytes] API Version  (int16)
[4 bytes] Correlation ID (int32)
[2 bytes] ClientID length (int16, -1 = null)
[N bytes] ClientID content
[1 byte]  TagBuffer    (int8)
[...]     API-specific body
```

Response format:
```
[4 bytes] MessageSize  (int32)
[4 bytes] Correlation ID (int32)
[...]     API-specific payload
```

- All integers: big-endian
- Error codes: int16, 0 = no error
- Strings: int16 length prefix + UTF-8 bytes (or -1 for null)
- Compact arrays (flexible version encoding): int8 length = actual count + 1

### Implemented APIs

| API Key | Name        | Versions | Notes                                                       |
|---------|-------------|----------|-------------------------------------------------------------|
| 18      | ApiVersions | 0–4      | Error code 35 (UNSUPPORTED_VERSION) if version outside 0–4 |
| 0       | Produce     | 11       | Validates topic/partition via KRaft metadata log; error code 3 (UNKNOWN_TOPIC_OR_PARTITION) for invalid, 0 for valid |
| *       | (others)    | —        | Returns `HeaderResponse` with only CorrelationID            |

### KRaft Cluster Metadata Log

The Produce handler reads the KRaft metadata log at:
```
/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log
```
It parses RecordBatches, extracting `TOPIC_RECORD` (type 2) and `PARTITION_RECORD` (type 3) entries to build a `ClusterMetadata` in memory. If the file is absent, all partitions are treated as invalid.

### Key Design Patterns

- **`Encoder` interface** — strategy pattern; `HeaderResponse`, `ApiVersionsResponse`, and `ProduceResponse` each implement `Encode() []byte`
- **`NewResponse()` factory** — switches on `RequestAPIKey` to select the correct response type
- **Keep-alive loop** in `handleRequest()` — reads requests in a loop until EOF, so one TCP connection handles multiple sequential requests

## Pre-commit Hooks

Hooks run `go fmt`, `go vet`, `go build`, and `go test` automatically on every commit.

```bash
# Install pre-commit (once)
pip3 install pre-commit

# Install hooks into .git/hooks/
pre-commit install

# Run manually against all files
pre-commit run --all-files
```

Config: `.pre-commit-config.yaml`

## CodeCrafters Infrastructure
- `.codecrafters/compile.sh` — how CodeCrafters CI compiles the project
- `.codecrafters/run.sh` — how CodeCrafters CI runs the project
- `codecrafters.yml` — sets Go version (`go-1.26`) and debug flag
