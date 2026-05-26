# Current Implementation

The broker is split across three source files:

- `app/main.go` — server setup, connection accept loop, per-connection request handling
- `app/request.go` — Kafka request header parsing (API key, version, correlation ID, ClientID, tag buffer)
- `app/response.go` — response encoding; `Encoder` interface with `ApiVersionsResponse` and a base `Response` fallback

**ApiVersions (API Key 18, versions 0–4)** is fully implemented. Requests with an unsupported version receive error code 35 (`UNSUPPORTED_VERSION`). The server maintains keep-alive connections and handles multiple sequential requests per TCP connection.

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
