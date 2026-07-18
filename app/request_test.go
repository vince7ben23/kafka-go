package main

import (
	"encoding/binary"
	"net"
	"testing"
)

// buildMinimalProduceBody builds a Produce v11 request body with null records,
// for use in unit tests only. acks=-1, timeout_ms=1500.
func buildMinimalProduceBody(topicName string, partitionIndexes []int32) []byte {
	var body []byte
	body = append(body, 0x00)                   // transactional_id: null
	body = append(body, 0xFF, 0xFF)             // acks = -1
	body = append(body, 0x00, 0x00, 0x05, 0xDC) // timeout_ms = 1500

	body = append(body, putUvarint(2)...) // 1 topic (count+1)
	body = append(body, putUvarint(uint64(len(topicName)+1))...)
	body = append(body, []byte(topicName)...)

	body = append(body, putUvarint(uint64(len(partitionIndexes)+1))...)
	for _, idx := range partitionIndexes {
		body = append(body, putInt32BE(idx)...)
		body = append(body, 0x00) // records: null
		body = append(body, 0x00) // partition TAG_BUFFER
	}
	body = append(body, 0x00) // topic TAG_BUFFER
	return body
}

func TestParseProduceRequest(t *testing.T) {
	t.Run("single topic single partition", func(t *testing.T) {
		body := buildMinimalProduceBody("test-topic", []int32{99})
		pr, err := parseProduceRequest(body)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pr.TransactionalID != "" {
			t.Errorf("TransactionalID: got %q, want empty", pr.TransactionalID)
		}
		if pr.Acks != -1 {
			t.Errorf("Acks: got %d, want -1", pr.Acks)
		}
		if pr.TimeoutMs != 1500 {
			t.Errorf("TimeoutMs: got %d, want 1500", pr.TimeoutMs)
		}
		if len(pr.TopicData) != 1 {
			t.Fatalf("TopicData len: got %d, want 1", len(pr.TopicData))
		}
		if pr.TopicData[0].Name != "test-topic" {
			t.Errorf("topic name: got %q, want %q", pr.TopicData[0].Name, "test-topic")
		}
		if len(pr.TopicData[0].PartitionData) != 1 {
			t.Fatalf("PartitionData len: got %d, want 1", len(pr.TopicData[0].PartitionData))
		}
		if pr.TopicData[0].PartitionData[0].Index != 99 {
			t.Errorf("partition index: got %d, want 99", pr.TopicData[0].PartitionData[0].Index)
		}
	})

	t.Run("single topic multiple partitions", func(t *testing.T) {
		body := buildMinimalProduceBody("my-topic", []int32{0, 1, 2})
		pr, err := parseProduceRequest(body)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pr.TopicData[0].Name != "my-topic" {
			t.Errorf("topic name: got %q, want %q", pr.TopicData[0].Name, "my-topic")
		}
		if len(pr.TopicData[0].PartitionData) != 3 {
			t.Fatalf("PartitionData len: got %d, want 3", len(pr.TopicData[0].PartitionData))
		}
		for i, want := range []int32{0, 1, 2} {
			if got := pr.TopicData[0].PartitionData[i].Index; got != want {
				t.Errorf("PartitionData[%d].Index: got %d, want %d", i, got, want)
			}
		}
	})

	t.Run("truncated body returns error", func(t *testing.T) {
		_, err := parseProduceRequest([]byte{0x00, 0xFF})
		if err == nil {
			t.Fatal("expected error for truncated body, got nil")
		}
	})
}

// buildDescribeTopicPartitionsBody builds a DescribeTopicPartitions v0 request body
// carrying the given topic names, for use in unit tests only.
func buildDescribeTopicPartitionsBody(topicNames ...string) []byte {
	var body []byte
	body = append(body, putUvarint(uint64(len(topicNames)+1))...) // topics count+1
	for _, name := range topicNames {
		body = append(body, putUvarint(uint64(len(name)+1))...)
		body = append(body, []byte(name)...)
		body = append(body, 0x00) // topic TAG_BUFFER
	}
	body = append(body, 0x00, 0x00, 0x00, 0x64) // response_partition_limit = 100
	body = append(body, 0xFF)                   // cursor: null
	body = append(body, 0x00)                   // request TAG_BUFFER
	return body
}

func TestParseDescribeTopicPartitionsRequest(t *testing.T) {
	t.Run("single topic", func(t *testing.T) {
		body := buildDescribeTopicPartitionsBody("unknown-topic")
		dr, err := parseDescribeTopicPartitionsRequest(body)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(dr.TopicNames) != 1 {
			t.Fatalf("TopicNames len: got %d, want 1", len(dr.TopicNames))
		}
		if dr.TopicNames[0] != "unknown-topic" {
			t.Errorf("topic name: got %q, want %q", dr.TopicNames[0], "unknown-topic")
		}
	})

	t.Run("multiple topics", func(t *testing.T) {
		body := buildDescribeTopicPartitionsBody("foo", "bar")
		dr, err := parseDescribeTopicPartitionsRequest(body)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []string{"foo", "bar"}
		if len(dr.TopicNames) != len(want) {
			t.Fatalf("TopicNames len: got %d, want %d", len(dr.TopicNames), len(want))
		}
		for i, w := range want {
			if dr.TopicNames[i] != w {
				t.Errorf("TopicNames[%d]: got %q, want %q", i, dr.TopicNames[i], w)
			}
		}
	})

	t.Run("empty topics array", func(t *testing.T) {
		body := buildDescribeTopicPartitionsBody()
		dr, err := parseDescribeTopicPartitionsRequest(body)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(dr.TopicNames) != 0 {
			t.Errorf("TopicNames len: got %d, want 0", len(dr.TopicNames))
		}
	})
}

// buildFetchBody builds a Fetch v16 request body carrying one topic with the
// given topic_id and partition indexes, for use in unit tests only. The leading
// scalar fields are filled with arbitrary valid values.
func buildFetchBody(topicID [16]byte, partitionIndexes []int32) []byte {
	var body []byte
	body = append(body, 0x00, 0x00, 0x01, 0xF4) // max_wait_ms = 500
	body = append(body, 0x00, 0x00, 0x00, 0x01) // min_bytes = 1
	body = append(body, 0x00, 0x10, 0x00, 0x00) // max_bytes = 1048576
	body = append(body, 0x00)                   // isolation_level = 0
	body = append(body, 0x00, 0x00, 0x00, 0x00) // session_id = 0
	body = append(body, 0xFF, 0xFF, 0xFF, 0xFF) // session_epoch = -1

	body = append(body, putUvarint(2)...) // 1 topic (count+1)
	body = append(body, topicID[:]...)
	body = append(body, putUvarint(uint64(len(partitionIndexes)+1))...)
	for _, idx := range partitionIndexes {
		body = append(body, putInt32BE(idx)...)     // partition
		body = append(body, 0xFF, 0xFF, 0xFF, 0xFF) // current_leader_epoch = -1
		body = append(body, 0, 0, 0, 0, 0, 0, 0, 0) // fetch_offset = 0
		body = append(body, 0xFF, 0xFF, 0xFF, 0xFF) // last_fetched_epoch = -1
		body = append(body, 0, 0, 0, 0, 0, 0, 0, 0) // log_start_offset = 0
		body = append(body, 0x00, 0x10, 0x00, 0x00) // partition_max_bytes
		body = append(body, 0x00)                   // partition TAG_BUFFER
	}
	body = append(body, 0x00) // topic TAG_BUFFER
	body = append(body, 0x01) // forgotten_topics_data: empty COMPACT_ARRAY
	body = append(body, 0x01) // rack_id: empty COMPACT_STRING
	body = append(body, 0x00) // request TAG_BUFFER
	return body
}

func TestParseFetchRequest(t *testing.T) {
	topicID := unknownTopicID

	t.Run("single topic single partition", func(t *testing.T) {
		body := buildFetchBody(topicID, []int32{0})
		fr, err := parseFetchRequest(body)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(fr.Topics) != 1 {
			t.Fatalf("Topics len: got %d, want 1", len(fr.Topics))
		}
		if fr.Topics[0].TopicID != topicID {
			t.Errorf("TopicID: got %v, want %v", fr.Topics[0].TopicID, topicID)
		}
		if len(fr.Topics[0].Partitions) != 1 {
			t.Fatalf("Partitions len: got %d, want 1", len(fr.Topics[0].Partitions))
		}
		if fr.Topics[0].Partitions[0].Partition != 0 {
			t.Errorf("Partition: got %d, want 0", fr.Topics[0].Partitions[0].Partition)
		}
	})

	t.Run("multiple partitions", func(t *testing.T) {
		body := buildFetchBody(topicID, []int32{0, 1, 2})
		fr, err := parseFetchRequest(body)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(fr.Topics[0].Partitions) != 3 {
			t.Fatalf("Partitions len: got %d, want 3", len(fr.Topics[0].Partitions))
		}
		for i, want := range []int32{0, 1, 2} {
			if got := fr.Topics[0].Partitions[i].Partition; got != want {
				t.Errorf("Partitions[%d].Partition: got %d, want %d", i, got, want)
			}
		}
	})

	t.Run("truncated body returns error", func(t *testing.T) {
		_, err := parseFetchRequest([]byte{0x00, 0x00})
		if err == nil {
			t.Fatal("expected error for truncated body, got nil")
		}
	})
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
		defer client.Close()
		binary.Write(client, binary.BigEndian, int16(99))
	}()

	_, err := parseRequest(server)
	if err == nil {
		t.Fatal("expected error for truncated header, got nil")
	}
}
