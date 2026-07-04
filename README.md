# Current Implementation

The broker is split across five source files:

- `app/main.go` — server setup, connection accept loop, per-connection request handling
- `app/request.go` — Kafka request header parsing; `ProduceRequest` decoder (`parseProduceRequest`, `readCompactString`, `readCompactBytes`)
- `app/response.go` — response encoding; `Encoder` interface with `ApiVersionsResponse`, `ProduceResponse`, and a `HeaderResponse` fallback; `writePartitionLog` persists RecordBatches to disk
- `app/metadata.go` — KRaft cluster metadata log reader (`__cluster_metadata-0` segment); `validateTopicPartition` resolves topic name → UUID → partition ID

**ApiVersions (API Key 18, versions 0–4)** is fully implemented. Requests with an unsupported version receive error code 35 (`UNSUPPORTED_VERSION`). The response advertises support for ApiVersions (v0–4) and Produce (v0–11). The server maintains keep-alive connections and handles multiple sequential requests per TCP connection.

**Produce (API Key 0, version 11)** is implemented. The broker reads the KRaft cluster metadata log to validate each requested topic/partition. Valid partitions receive error code 0 and have their RecordBatch appended to the topic-partition data log; unknown topics or partitions receive error code 3 (`UNKNOWN_TOPIC_OR_PARTITION`); storage write failures receive error code 5 (`LEADER_NOT_AVAILABLE`).

Log files on disk:

```
/tmp/kraft-combined-logs/
├── __cluster_metadata-0/
│   └── 00000000000000000000.log   ← which topics and partitions currently exist
└── <topic>-<partition>/
    └── 00000000000000000000.log   ← actual message data for that topic-partition
```

To run tests locally:

```sh
go test -v -timeout 10s ./app/...
```

To run the broker:

```sh
./your_program.sh
```

To submit to CodeCrafters:

```sh
codecrafters submit
```

# Pre-commit Hooks

Hooks run `go fmt`, `go vet`, `go build`, and `go test` automatically on every commit.

```sh
# Install pre-commit (once)
pip3 install pre-commit

# Install hooks into .git/hooks/
pre-commit install

# Run manually against all files
pre-commit run --all-files
```
