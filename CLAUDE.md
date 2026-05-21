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

# Submit to CodeCrafters for automated testing
codecrafters submit
```

## Architecture

- **Entry point**: `app/main.go` — all broker logic lives here (or in additional files under `app/`)
- **Module**: `github.com/codecrafters-io/kafka-starter-go` (Go 1.26, no external dependencies)
- **Protocol**: Kafka wire protocol over TCP — binary encoding with big-endian integers, length-prefixed messages
- **Port**: Always binds to `0.0.0.0:9092`

### Kafka Wire Protocol Conventions
- Request header: 4-byte message length (int32), 2-byte API key, 2-byte API version, 4-byte correlation ID
- Response header: 4-byte message length, 4-byte correlation ID
- Error codes: int16, 0 = no error
- Strings: int16 length prefix + UTF-8 bytes (or -1 for null)
- Arrays: int32 count prefix + elements

## CodeCrafters Infrastructure
- `.codecrafters/compile.sh` — how CodeCrafters CI compiles the project
- `.codecrafters/run.sh` — how CodeCrafters CI runs the project
- `codecrafters.yml` — sets Go version and debug flag
