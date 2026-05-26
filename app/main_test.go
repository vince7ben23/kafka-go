package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
)

func mustBinaryWrite(w io.Writer, order binary.ByteOrder, data any) {
	if err := binary.Write(w, order, data); err != nil {
		panic(err)
	}
}

func mustBinaryRead(t *testing.T, r io.Reader, order binary.ByteOrder, data any) {
	t.Helper()
	if err := binary.Read(r, order, data); err != nil {
		t.Fatalf("read response field: %v", err)
	}
}

// writeRequestHeader writes a complete minimal Kafka request:
// msgSize(4) apiKey(2) apiVersion(2) correlationID(4) clientIDLength(2=-1 null)
// tagBuffer(1=0) body(3 bytes: empty compact clientID + softwareVersion + tagBuffer)
// msgSize should be 14 (2+2+4+2+1+1+1+1).
func writeRequestHeader(w io.Writer, msgSize int32, apiKey int16, apiVersion int16, correlationID int32) {
	mustBinaryWrite(w, binary.BigEndian, msgSize)
	mustBinaryWrite(w, binary.BigEndian, apiKey)
	mustBinaryWrite(w, binary.BigEndian, apiVersion)
	mustBinaryWrite(w, binary.BigEndian, correlationID)
	mustBinaryWrite(w, binary.BigEndian, int16(-1)) // null ClientID
	mustBinaryWrite(w, binary.BigEndian, int8(0))   // TagBuffer (empty)
	mustBinaryWrite(w, binary.BigEndian, int8(1))   // BodyClientIDLength (empty compact string)
	mustBinaryWrite(w, binary.BigEndian, int8(1))   // BodySoftwareVersionLength (empty compact string)
	mustBinaryWrite(w, binary.BigEndian, int8(0))   // BodyTagBuffer (empty)
}

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
		// Write only 2 bytes instead of the full 12-byte header
		binary.Write(client, binary.BigEndian, int16(99))
		client.Close()
	}()

	_, err := parseRequest(server)
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

			writeRequestHeader(client, 14, 18, 4, tt.correlationID)

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
			mustBinaryRead(t, reader, binary.BigEndian, &resp.APIArrayLength)
			// Kafka compact array: length field = actual count + 1 (flexible version encoding)
			resp.APIVersions = make([]ApiVersion, int(resp.APIArrayLength)-1)
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

func TestNewResponse(t *testing.T) {
	t.Run("ApiVersions valid version returns ApiVersionsResponse with no error", func(t *testing.T) {
		req := &Request{RequestAPIKey: APIKeyApiVersions, RequestAPIVersion: 4, CorrelationID: 42}
		resp, ok := NewResponse(req).(*ApiVersionsResponse)
		if !ok {
			t.Fatal("expected *ApiVersionsResponse")
		}
		if resp.CorrelationID != 42 {
			t.Errorf("CorrelationID: got %d, want 42", resp.CorrelationID)
		}
		if resp.ErrorCode != 0 {
			t.Errorf("ErrorCode: got %d, want 0", resp.ErrorCode)
		}
	})

	t.Run("ApiVersions unsupported version returns error code 35", func(t *testing.T) {
		req := &Request{RequestAPIKey: APIKeyApiVersions, RequestAPIVersion: 99, CorrelationID: 7}
		resp, ok := NewResponse(req).(*ApiVersionsResponse)
		if !ok {
			t.Fatal("expected *ApiVersionsResponse")
		}
		if resp.ErrorCode != 35 {
			t.Errorf("ErrorCode: got %d, want 35", resp.ErrorCode)
		}
	})

	t.Run("unknown API key returns base Response", func(t *testing.T) {
		req := &Request{RequestAPIKey: 99, CorrelationID: 5}
		resp, ok := NewResponse(req).(*Response)
		if !ok {
			t.Fatal("expected *Response")
		}
		if resp.CorrelationID != 5 {
			t.Errorf("CorrelationID: got %d, want 5", resp.CorrelationID)
		}
	})
}

func TestApiVersionsResponseBinaryEncoding(t *testing.T) {
	resp := &ApiVersionsResponse{
		Response:  Response{CorrelationID: 12345},
		ErrorCode: 0,
	}
	got := resp.Encode()
	// CorrelationID=12345 → 0x00003039, ErrorCode=0 → 0x0000
	want := []byte{0x00, 0x00, 0x30, 0x39, 0x00, 0x00}
	if !bytes.Equal(got[:len(want)], want) {
		t.Errorf("encoding prefix: got %#v, want %#v", got[:len(want)], want)
	}
}
