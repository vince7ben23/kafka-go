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
- **`app/request.go`** — `Request` struct, `parseRequest()` binary decoder; `ProduceRequest`/`ProduceTopicData`/`ProducePartitionData` types, `parseProduceRequest()`, `readCompactString()`, `readCompactBytes()`
- **`app/response.go`** — `Encoder` interface, `MessageSize` type, `HeaderResponse`/`ApiVersionsResponse`/`ProduceResponse`/`ProduceTopicResponse`/`ProducePartitionResponse` types; `NewResponse()` factory, `createApiVersionsResponse()`, `createApiProduceResponse()`, `buildPartitionResponse()`; helpers `writeUvarint()`, `writeCompactString()`, `writePartitionLog()`, `toCompactArrayLen()`
- **`app/metadata.go`** — `ClusterMetadata`, `TopicMetadata`, `PartitionMetadata` types; `kafkaLogBaseDir` and `clusterMetadataLogPath` constants; `readClusterMetadata()` parses the KRaft `__cluster_metadata-0` log segment; `validateTopicPartition()` checks topic+partition existence; `parseMetadataLog()`, `readRecordValue()`, `parseMetadataRecord()`, `parseTopicRecord()`, `parsePartitionRecord()` for decoding the log format
- **`app/testhelpers_test.go`** — shared test utilities: `mustBinaryWrite()`, `mustBinaryRead()`, `writeRequestHeader()`
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
| 18      | ApiVersions | 0–4      | Error code 35 (UNSUPPORTED_VERSION) if version outside 0–4; response advertises API 18 (v0–4) and API 0 (v0–11) |
| 0       | Produce     | 11       | Validates topic/partition via KRaft metadata log; error code 3 (UNKNOWN_TOPIC_OR_PARTITION) for unknown, error code 5 (LEADER_NOT_AVAILABLE) on storage write failure, 0 for success; accepted RecordBatches are appended to the topic-partition log file |
| 1       | Fetch       | 16       | Parses the request topics; each requested topic_id is resolved against KRaft cluster metadata — a known topic_id yields error code 0 and, per partition, the raw RecordBatch bytes read from that topic-partition's log file (`readPartitionLog`; a missing log file means empty records); an unknown topic_id yields error code 100 (UNKNOWN_TOPIC_ID). Top-level error_code and throttle_time_ms are 0 (response header v1) |
| *       | (others)    | —        | Returns `HeaderResponse` with only CorrelationID            |

### Log Files on Disk

```
/tmp/kraft-combined-logs/
├── __cluster_metadata-0/
│   └── 00000000000000000000.log   ← cluster metadata log
└── <topic>-<partition>/
    └── 00000000000000000000.log   ← topic data log
```

#### `__cluster_metadata-0/00000000000000000000.log`

Records the current state of the cluster: which topics exist and which partitions each topic has. The broker reads this file once at startup (in `NewServer()`) to build a `ClusterMetadata` in memory that is cached for the broker's lifetime; the Produce handler consults that in-memory cache rather than re-reading the log per request. It extracts `TOPIC_RECORD` (type 2) and `PARTITION_RECORD` (type 3) entries. If the file is absent, all topic/partition combinations are treated as invalid (error code 3).

> **Known limitation:** the metadata cache is a static snapshot taken at startup and is never refreshed. Topics/partitions created (or removed) in the log *after* the broker starts are invisible, and if the log is missing at startup every topic stays invalid for the broker's lifetime. This is acceptable under the challenge's assumption that metadata is fixed before the broker boots; a real broker would tail the log and update the cache (guarded by a lock, since connection goroutines read it concurrently).

#### `<topic>-<partition>/00000000000000000000.log` (e.g. `test-topic-0/`)

The actual message data for one topic-partition. The Produce handler appends RecordBatches here after the topic/partition is confirmed valid against cluster metadata.

### Produce Response Wire Format

`ProduceResponse.Encode()` uses **response header v1** (correlation_id + tag_buffer) — one extra tag_buffer byte after the correlation ID that is absent in older header versions. Each partition entry includes:
- `record_errors`: empty COMPACT_ARRAY (`0x01`)
- `error_message`: null COMPACT_NULLABLE_STRING (`0x00`)

These two fields are part of the Produce v11 flexible-version schema and must be present even on success.

### KRaft Metadata Log Format

Each entry in `__cluster_metadata-0/00000000000000000000.log` is a `RecordBatch`. The record value bytes begin with:
- `FrameVersion` (int8)
- `RecordType` (int8): `2` = `TOPIC_RECORD`, `3` = `PARTITION_RECORD`
- `Version` (int8)

`TOPIC_RECORD` carries a COMPACT_STRING topic name + 16-byte UUID.  
`PARTITION_RECORD` carries a 4-byte partition ID + 16-byte topic UUID.  
`validateTopicPartition()` resolves topic name → UUID → partition ID to confirm the requested combination exists.

### Key Design Patterns

- **`Encoder` interface** — strategy pattern; `HeaderResponse`, `ApiVersionsResponse`, and `ProduceResponse` each implement `Encode() []byte`
- **`NewResponse()` factory** — switches on `RequestAPIKey` to select the correct response type
- **Keep-alive loop** in `handleRequest()` — reads requests in a loop until EOF, so one TCP connection handles multiple sequential requests
- **`buildPartitionResponse()`** — centralises the per-partition logic: validate → write log → return response; storage errors are mapped to error code 5 rather than crashing the handler

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
