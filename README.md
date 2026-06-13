# Current Implementation

The broker is split across five source files:

- `app/main.go` — server setup, connection accept loop, per-connection request handling
- `app/request.go` — Kafka request header parsing and Produce request body decoder
- `app/response.go` — response encoding; `Encoder` interface with `ApiVersionsResponse`, `ProduceResponse`, and a `HeaderResponse` fallback
- `app/metadata.go` — KRaft cluster metadata log reader (`__cluster_metadata-0` segment)

**ApiVersions (API Key 18, versions 0–4)** is fully implemented. Requests with an unsupported version receive error code 35 (`UNSUPPORTED_VERSION`). The server maintains keep-alive connections and handles multiple sequential requests per TCP connection.

**Produce (API Key 0, version 11)** is implemented. The broker reads the KRaft cluster metadata log at `/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log` to validate each requested topic/partition. Valid partitions receive error code 0; unknown topics or partitions receive error code 3 (`UNKNOWN_TOPIC_OR_PARTITION`).

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
