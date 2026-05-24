package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
)

func writeRequestHeader(w io.Writer, msgSize int32, apiKey int16, apiVersion int16, correlationID int32) {
	binary.Write(w, binary.BigEndian, msgSize)
	binary.Write(w, binary.BigEndian, apiKey)
	binary.Write(w, binary.BigEndian, apiVersion)
	binary.Write(w, binary.BigEndian, correlationID)
}

func TestParseRequestHeader(t *testing.T) {
	tests := []struct {
		name          string
		msgSize       int32
		apiKey        int16
		apiVersion    int16
		correlationID int32
	}{
		{"ApiVersions request", 10, 18, 4, 42},
		{"zero correlation ID", 8, 1, 0, 0},
		{"max int32 correlation ID", 12, 18, 4, 2147483647},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer server.Close()

			go func() {
				writeRequestHeader(client, tt.msgSize, tt.apiKey, tt.apiVersion, tt.correlationID)
				client.Close()
			}()

			h, err := parseRequestHeader(server)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if h.MessageSize != tt.msgSize {
				t.Errorf("MessageSize: got %d, want %d", h.MessageSize, tt.msgSize)
			}
			if h.RequestAPIKey != tt.apiKey {
				t.Errorf("RequestAPIKey: got %d, want %d", h.RequestAPIKey, tt.apiKey)
			}
			if h.RequestAPIVersion != tt.apiVersion {
				t.Errorf("RequestAPIVersion: got %d, want %d", h.RequestAPIVersion, tt.apiVersion)
			}
			if h.CorrelationID != tt.correlationID {
				t.Errorf("CorrelationID: got %d, want %d", h.CorrelationID, tt.correlationID)
			}
		})
	}
}

func TestParseRequestHeaderTruncated(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	go func() {
		// Write only 2 bytes instead of the full 12-byte header
		binary.Write(client, binary.BigEndian, int16(99))
		client.Close()
	}()

	_, err := parseRequestHeader(server)
	if err == nil {
		t.Fatal("expected error for truncated header, got nil")
	}
}

func TestHandleRequest(t *testing.T) {
	tests := []struct {
		name          string
		correlationID int32
	}{
		{"correlation ID 99", 99},
		{"correlation ID 0", 0},
		{"correlation ID max", 2147483647},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()

			go handleRequest(server)

			writeRequestHeader(client, 10, 18, 4, tt.correlationID)

			var resp Response
			if err := binary.Read(client, binary.BigEndian, &resp); err != nil {
				t.Fatalf("read response: %v", err)
			}
			if resp.CorrelationID != tt.correlationID {
				t.Errorf("CorrelationID: got %d, want %d", resp.CorrelationID, tt.correlationID)
			}
		})
	}
}

func TestResponseBinaryEncoding(t *testing.T) {
	resp := &Response{
		MessageSize:   4,
		CorrelationID: 12345,
	}
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, resp); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// MessageSize=4 → 0x00000004, CorrelationID=12345 → 0x00003039
	want := []byte{0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x30, 0x39}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("encoding: got %#v, want %#v", buf.Bytes(), want)
	}
}
