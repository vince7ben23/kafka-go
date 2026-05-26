package main

import (
	"encoding/binary"
	"net"
	"testing"
)

func TestParseRequest(t *testing.T) {
	tests := []struct {
		name          string
		msgSize       int32
		apiKey        int16
		apiVersion    int16
		correlationID int32
	}{
		{"ApiVersions request", 14, 18, 4, 42},
		{"zero correlation ID", 14, 1, 0, 0},
		{"max int32 correlation ID", 14, 18, 4, 2147483647},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer server.Close()

			go func() {
				defer client.Close()
				writeRequestHeader(client, tt.msgSize, tt.apiKey, tt.apiVersion, tt.correlationID)
			}()

			h, err := parseRequest(server)
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

func TestParseRequestTruncated(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	go func() {
		defer client.Close()
		binary.Write(client, binary.BigEndian, int16(99))
	}()

	_, err := parseRequest(server)
	if err == nil {
		t.Fatal("expected error for truncated header, got nil")
	}
}
