package main

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// buildMetadataLogBytes constructs a minimal __cluster_metadata log binary containing
// one RecordBatch with one TOPIC_RECORD and one PARTITION_RECORD.
func buildMetadataLogBytes(topicName string, topicID [16]byte, partitionID int32) []byte {
	records := buildTopicRecord(topicName, topicID)
	records = append(records, buildPartitionRecord(partitionID, topicID)...)

	return buildRecordBatch(records, 2)
}

// buildRecordBatch wraps pre-encoded records in a RecordBatch envelope.
func buildRecordBatch(encodedRecords []byte, recordCount int32) []byte {
	// RecordBatch body (after BaseOffset and BatchLength):
	// PartitionLeaderEpoch(4) + Magic(1) + CRC(4) + Attributes(2) +
	// LastOffsetDelta(4) + BaseTimestamp(8) + MaxTimestamp(8) +
	// ProducerID(8) + ProducerEpoch(2) + BaseSequence(4) + RecordCount(4) + records
	var body bytes.Buffer
	mustBinaryWrite(&body, binary.BigEndian, int32(0))  // PartitionLeaderEpoch
	mustBinaryWrite(&body, binary.BigEndian, int8(2))   // Magic
	mustBinaryWrite(&body, binary.BigEndian, int32(0))  // CRC (not validated in tests)
	mustBinaryWrite(&body, binary.BigEndian, int16(0))  // Attributes
	mustBinaryWrite(&body, binary.BigEndian, int32(0))  // LastOffsetDelta
	mustBinaryWrite(&body, binary.BigEndian, int64(0))  // BaseTimestamp
	mustBinaryWrite(&body, binary.BigEndian, int64(0))  // MaxTimestamp
	mustBinaryWrite(&body, binary.BigEndian, int64(-1)) // ProducerID
	mustBinaryWrite(&body, binary.BigEndian, int16(-1)) // ProducerEpoch
	mustBinaryWrite(&body, binary.BigEndian, int32(-1)) // BaseSequence
	mustBinaryWrite(&body, binary.BigEndian, recordCount)
	body.Write(encodedRecords)

	var batch bytes.Buffer
	mustBinaryWrite(&batch, binary.BigEndian, int64(0))          // BaseOffset
	mustBinaryWrite(&batch, binary.BigEndian, int32(body.Len())) // BatchLength
	batch.Write(body.Bytes())
	return batch.Bytes()
}

// encodeRecord wraps a value payload as a Kafka Record (signed varints).
func encodeRecord(value []byte) []byte {
	var rec bytes.Buffer
	// Attributes: int8
	rec.WriteByte(0)
	// TimestampDelta: signed varint = 0
	rec.WriteByte(0)
	// OffsetDelta: signed varint = 0
	rec.WriteByte(0)
	// KeyLength: signed varint = -1 (null)
	tmp := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(tmp, -1)
	rec.Write(tmp[:n])
	// ValueLength
	n = binary.PutVarint(tmp, int64(len(value)))
	rec.Write(tmp[:n])
	rec.Write(value)
	// Headers count: uvarint = 0
	rec.WriteByte(0)

	// Prefix with signed varint length of the record body
	var out bytes.Buffer
	n = binary.PutVarint(tmp, int64(rec.Len()))
	out.Write(tmp[:n])
	out.Write(rec.Bytes())
	return out.Bytes()
}

func buildTopicRecord(name string, topicID [16]byte) []byte {
	var val bytes.Buffer
	val.WriteByte(0) // FrameVersion
	val.WriteByte(2) // RecordType = TOPIC_RECORD
	val.WriteByte(0) // Version
	// Compact string: uvarint(len+1) + bytes
	tmp := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(tmp, uint64(len(name)+1))
	val.Write(tmp[:n])
	val.WriteString(name)
	val.Write(topicID[:])
	val.WriteByte(0) // TagBuffer
	return encodeRecord(val.Bytes())
}

func buildPartitionRecord(partitionID int32, topicID [16]byte) []byte {
	var val bytes.Buffer
	val.WriteByte(0) // FrameVersion
	val.WriteByte(3) // RecordType = PARTITION_RECORD
	val.WriteByte(0) // Version
	mustBinaryWrite(&val, binary.BigEndian, partitionID)
	val.Write(topicID[:])
	writeTestCompactInt32Array(&val, []int32{1})      // replicas
	writeTestCompactInt32Array(&val, []int32{1})      // isr
	writeTestCompactInt32Array(&val, nil)             // removing_replicas
	writeTestCompactInt32Array(&val, nil)             // adding_replicas
	mustBinaryWrite(&val, binary.BigEndian, int32(1)) // leader
	mustBinaryWrite(&val, binary.BigEndian, int32(0)) // leader_epoch
	// Remaining fields (partition_epoch, directories, tag_buffer) are not read
	// by parsePartitionRecord, so they are omitted here.
	return encodeRecord(val.Bytes())
}

// writeTestCompactInt32Array encodes a COMPACT_ARRAY of int32: uvarint(len+1)
// followed by each element as a big-endian int32 (nil encodes an empty array).
func writeTestCompactInt32Array(buf *bytes.Buffer, vs []int32) {
	tmp := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(tmp, uint64(len(vs)+1))
	buf.Write(tmp[:n])
	for _, v := range vs {
		mustBinaryWrite(buf, binary.BigEndian, v)
	}
}

func TestParseRecordBatches(t *testing.T) {
	var topicID [16]byte
	topicID[15] = 0x01
	data := buildMetadataLogBytes("test-topic", topicID, 0)

	meta := &ClusterMetadata{}
	r := bytes.NewReader(data)
	if err := parseRecordBatches(r, meta); err != nil {
		t.Fatalf("parseRecordBatches: %v", err)
	}
	if len(meta.Topics) != 1 {
		t.Fatalf("Topics: got %d, want 1", len(meta.Topics))
	}
	if meta.Topics[0].Name != "test-topic" {
		t.Errorf("topic name: got %q, want %q", meta.Topics[0].Name, "test-topic")
	}
	if meta.Topics[0].TopicID != topicID {
		t.Errorf("topic ID mismatch")
	}
	if len(meta.Partitions) != 1 {
		t.Fatalf("Partitions: got %d, want 1", len(meta.Partitions))
	}
	if meta.Partitions[0].PartitionID != 0 {
		t.Errorf("partition ID: got %d, want 0", meta.Partitions[0].PartitionID)
	}
	if meta.Partitions[0].TopicID != topicID {
		t.Errorf("partition topicID mismatch")
	}
}

func TestValidateTopicPartition(t *testing.T) {
	var topicID [16]byte
	topicID[15] = 0x42
	meta := &ClusterMetadata{
		Topics:     []TopicMetadata{{Name: "my-topic", TopicID: topicID}},
		Partitions: []PartitionMetadata{{PartitionID: 0, TopicID: topicID}},
	}

	if !meta.validateTopicPartition("my-topic", 0) {
		t.Error("expected true for existing topic+partition")
	}
	if meta.validateTopicPartition("no-topic", 0) {
		t.Error("expected false for missing topic")
	}
	if meta.validateTopicPartition("my-topic", 99) {
		t.Error("expected false for missing partition")
	}
}

func TestReadClusterMetadata(t *testing.T) {
	var topicID [16]byte
	topicID[15] = 0x05
	data := buildMetadataLogBytes("foo", topicID, 2)

	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "00000000000000000000.log")
	if err := os.WriteFile(logPath, data, 0600); err != nil {
		t.Fatalf("write temp log: %v", err)
	}

	meta, err := readClusterMetadata(logPath)
	if err != nil {
		t.Fatalf("readClusterMetadata: %v", err)
	}
	if !meta.validateTopicPartition("foo", 2) {
		t.Error("expected to find topic 'foo' partition 2")
	}
}

func TestReadClusterMetadataFileNotFound(t *testing.T) {
	_, err := readClusterMetadata("/tmp/nonexistent/log.log")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}
