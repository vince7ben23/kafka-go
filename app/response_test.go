package main

import (
	"bytes"
	"testing"
)

func TestNewResponse(t *testing.T) {
	t.Run("ApiVersions valid version returns ApiVersionsResponse with no error", func(t *testing.T) {
		req := &Request{RequestAPIKey: APIKeyApiVersions, RequestAPIVersion: 4, CorrelationID: 42}
		resp, err := NewResponse(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		avResp, ok := resp.(*ApiVersionsResponse)
		if !ok {
			t.Fatal("expected *ApiVersionsResponse")
		}
		if avResp.CorrelationID != 42 {
			t.Errorf("CorrelationID: got %d, want 42", avResp.CorrelationID)
		}
		if avResp.ErrorCode != 0 {
			t.Errorf("ErrorCode: got %d, want 0", avResp.ErrorCode)
		}
	})

	t.Run("ApiVersions unsupported version returns error code 35", func(t *testing.T) {
		req := &Request{RequestAPIKey: APIKeyApiVersions, RequestAPIVersion: 99, CorrelationID: 7}
		resp, err := NewResponse(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		avResp, ok := resp.(*ApiVersionsResponse)
		if !ok {
			t.Fatal("expected *ApiVersionsResponse")
		}
		if avResp.ErrorCode != 35 {
			t.Errorf("ErrorCode: got %d, want 35", avResp.ErrorCode)
		}
	})

	t.Run("unknown API key returns base Response", func(t *testing.T) {
		req := &Request{RequestAPIKey: 99, CorrelationID: 5}
		resp, err := NewResponse(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		baseResp, ok := resp.(*Response)
		if !ok {
			t.Fatal("expected *Response")
		}
		if baseResp.CorrelationID != 5 {
			t.Errorf("CorrelationID: got %d, want 5", baseResp.CorrelationID)
		}
	})
}

func TestApiVersionsResponseBinaryEncoding(t *testing.T) {
	resp := &ApiVersionsResponse{
		Response:       Response{CorrelationID: 12345},
		ErrorCode:      0,
		APIArrayLength: 2,
		APIVersions: []ApiVersion{
			{APIKey: APIKeyApiVersions, MinVersion: 0, MaxVersion: 4, TagBuffer: 0},
		},
		ThrottleTime: 0,
		TagBuffer:    0,
	}
	got := resp.Encode()
	// Encoded layout (19 bytes, big-endian):
	// [0:4]   CorrelationID=12345 → 0x00 0x00 0x30 0x39
	// [4:6]   ErrorCode=0         → 0x00 0x00
	// [6]     APIArrayLength=2    → 0x02  (compact array: 1 entry + 1)
	// [7:9]   APIKey=18           → 0x00 0x12
	// [9:11]  MinVersion=0        → 0x00 0x00
	// [11:13] MaxVersion=4        → 0x00 0x04
	// [13]    ApiVersion TagBuffer→ 0x00
	// [14:18] ThrottleTime=0      → 0x00 0x00 0x00 0x00
	// [18]    TagBuffer=0         → 0x00
	want := []byte{0x00, 0x00, 0x30, 0x39, 0x00, 0x00, 0x02, 0x00, 0x12, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	if !bytes.Equal(got, want) {
		t.Errorf("encoding: got %#v, want %#v", got, want)
	}
}
