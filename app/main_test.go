package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
)

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

			writeRequestHeader(client, 14, APIKeyApiVersions, 4, tt.correlationID)

			var msgSize int32
			if err := binary.Read(client, binary.BigEndian, &msgSize); err != nil {
				t.Fatalf("read message size: %v", err)
			}
			returnedData := make([]byte, msgSize)
			if _, err := io.ReadFull(client, returnedData); err != nil {
				t.Fatalf("read response bytes: %v", err)
			}
			reader := bytes.NewReader(returnedData)
			var resp ApiVersionsResponse
			mustBinaryRead(t, reader, binary.BigEndian, &resp.CorrelationID)
			mustBinaryRead(t, reader, binary.BigEndian, &resp.ErrorCode)
			// Kafka compact array: length field = actual count + 1 (flexible version encoding)
			var apiArrayLength int8
			mustBinaryRead(t, reader, binary.BigEndian, &apiArrayLength)
			resp.APIVersions = make([]ApiVersion, int(apiArrayLength)-1)
			for i := range resp.APIVersions {
				mustBinaryRead(t, reader, binary.BigEndian, &resp.APIVersions[i])
			}
			mustBinaryRead(t, reader, binary.BigEndian, &resp.ThrottleTime)
			mustBinaryRead(t, reader, binary.BigEndian, &resp.TagBuffer)
			if resp.CorrelationID != tt.correlationID {
				t.Errorf("CorrelationID: got %d, want %d", resp.CorrelationID, tt.correlationID)
			}
			if resp.ErrorCode != 0 {
				t.Errorf("ErrorCode: got %d, want 0", resp.ErrorCode)
			}
		})
	}
}
