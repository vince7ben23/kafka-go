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
		baseResp, ok := resp.(*HeaderResponse)
		if !ok {
			t.Fatal("expected *HeaderResponse")
		}
		if baseResp.CorrelationID != 5 {
			t.Errorf("CorrelationID: got %d, want 5", baseResp.CorrelationID)
		}
	})
}

func TestNewResponseProduce(t *testing.T) {
	body := buildMinimalProduceBody("test-topic", []int32{99})
	req := &Request{RequestAPIKey: APIKeyApiProduce, RequestAPIVersion: 11, CorrelationID: 7, Body: body}
	resp, err := NewResponse(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pr, ok := resp.(*ProduceResponse)
	if !ok {
		t.Fatal("expected *ProduceResponse")
	}
	if pr.CorrelationID != 7 {
		t.Errorf("CorrelationID: got %d, want 7", pr.CorrelationID)
	}
	if len(pr.Topics) != 1 {
		t.Fatalf("Topics len: got %d, want 1", len(pr.Topics))
	}
	if pr.Topics[0].Name != "test-topic" {
		t.Errorf("topic name: got %q, want %q", pr.Topics[0].Name, "test-topic")
	}
	if len(pr.Topics[0].Partitions) != 1 {
		t.Fatalf("Partitions len: got %d, want 1", len(pr.Topics[0].Partitions))
	}
	if pr.Topics[0].Partitions[0].Index != 99 {
		t.Errorf("partition index: got %d, want 99", pr.Topics[0].Partitions[0].Index)
	}
}

func TestProduceResponseBinaryEncoding(t *testing.T) {
	pr := &ProduceResponse{
		HeaderResponse:  HeaderResponse{CorrelationID: 7},
		HeaderTagBuffer: 0,
		ThrottleTimeMs:  0,
		Topics: []ProduceTopicResponse{
			{
				Name: "test-topic",
				Partitions: []ProducePartitionResponse{
					{Index: 99, ErrorCode: 3, BaseOffset: -1, TagBuffer: 0},
				},
				TagBuffer: 0,
			},
		},
		TagBuffer: 0,
	}
	got := pr.Encode()
	// Encoded layout (57 bytes):
	// [0:4]   CorrelationID=7          → 0x00 0x00 0x00 0x07
	// [4]     HeaderTagBuffer=0        → 0x00
	// [5]     topics count+1=2         → 0x02
	// [6]     topic name len+1=11      → 0x0B
	// [7:17]  "test-topic"             → 10 bytes
	// [17]    partitions count+1=2     → 0x02
	// [18:22] Index=99                 → 0x00 0x00 0x00 0x63
	// [22:24] ErrorCode=3              → 0x00 0x03
	// [24:32] BaseOffset=-1            → 0xFF x8
	// [32:40] LogAppendTime=0          → 0x00 x8
	// [40:48] LogStartOffset=0         → 0x00 x8
	// [48]    record_errors (empty)    → 0x01
	// [49]    error_message (null)     → 0x00
	// [50]    partition TagBuffer      → 0x00
	// [51]    topic TagBuffer          → 0x00
	// [52:56] ThrottleTimeMs=0         → 0x00 0x00 0x00 0x00
	// [56]    body TagBuffer           → 0x00
	want := []byte{
		0x00, 0x00, 0x00, 0x07, // CorrelationID
		0x00,                                                   // HeaderTagBuffer
		0x02,                                                   // topics count+1
		0x0B, 't', 'e', 's', 't', '-', 't', 'o', 'p', 'i', 'c', // topic name
		0x02,                   // partitions count+1
		0x00, 0x00, 0x00, 0x63, // Index=99
		0x00, 0x03, // ErrorCode=3
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // BaseOffset=-1
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // LogAppendTime=0
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // LogStartOffset=0
		0x01,                   // record_errors: empty COMPACT_ARRAY
		0x00,                   // error_message: null
		0x00,                   // partition TagBuffer
		0x00,                   // topic TagBuffer
		0x00, 0x00, 0x00, 0x00, // ThrottleTimeMs=0
		0x00, // body TagBuffer
	}
	if !bytes.Equal(got, want) {
		t.Errorf("encoding mismatch:\ngot  %#v\nwant %#v", got, want)
	}
}

func TestApiVersionsResponseBinaryEncoding(t *testing.T) {
	req := &Request{RequestAPIKey: APIKeyApiVersions, RequestAPIVersion: 4, CorrelationID: 12345}
	enc, err := NewResponse(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := enc.Encode()
	// Encoded layout (26 bytes, big-endian):
	// [0:4]   CorrelationID=12345 → 0x00 0x00 0x30 0x39
	// [4:6]   ErrorCode=0         → 0x00 0x00
	// [6]     APIArrayLength=3    → 0x03  (compact array: 2 entries + 1)
	// [7:9]   APIKey=18 (ApiVersions) → 0x00 0x12
	// [9:11]  MinVersion=0        → 0x00 0x00
	// [11:13] MaxVersion=4        → 0x00 0x04
	// [13]    TagBuffer           → 0x00
	// [14:16] APIKey=0 (ApiProduce) → 0x00 0x00
	// [16:18] MinVersion=0        → 0x00 0x00
	// [18:20] MaxVersion=11       → 0x00 0x0B
	// [20]    TagBuffer           → 0x00
	// [21:25] ThrottleTime=0      → 0x00 0x00 0x00 0x00
	// [25]    TagBuffer=0         → 0x00
	want := []byte{
		0x00, 0x00, 0x30, 0x39, // CorrelationID
		0x00, 0x00, // ErrorCode
		0x03,                                     // APIArrayLength (compact: 2+1)
		0x00, 0x12, 0x00, 0x00, 0x00, 0x04, 0x00, // ApiVersions entry
		0x00, 0x00, 0x00, 0x00, 0x00, 0x0B, 0x00, // ApiProduce entry
		0x00, 0x00, 0x00, 0x00, // ThrottleTime
		0x00, // TagBuffer
	}
	if !bytes.Equal(got, want) {
		t.Errorf("encoding mismatch:\ngot  %#v\nwant %#v", got, want)
	}
}
