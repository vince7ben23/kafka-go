// gen_test_logs generates three __cluster_metadata log fixtures under
// /tmp/kraft-combined-logs/__cluster_metadata-0/:
//
//   valid_topic_valid_partition.log     — TOPIC_RECORD + matching PARTITION_RECORD
//   valid_topic_invalid_partition.log   — TOPIC_RECORD only (no matching partition)
//   invalid_topic_invalid_partition.log — empty batch (no records)
package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
)

const outDir = "/tmp/kraft-combined-logs/__cluster_metadata-0"

// testTopicID is the UUID used for "test-topic" across all fixtures.
var testTopicID = [16]byte{
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
}

// otherTopicID is a different UUID to make an invalid cross-reference.
var otherTopicID = [16]byte{
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
}

func main() {
	if err := os.MkdirAll(outDir, 0755); err != nil {
		log.Fatalf("mkdir: %v", err)
	}

	// 1. valid topic + valid partition
	data1 := buildBatch(
		encodeTopicRecord("test-topic", testTopicID),
		encodePartitionRecord(0, testTopicID),
	)
	write(outDir+"/valid_topic_valid_partition.log", data1)

	// 2. valid topic + invalid partition (partition references a different topicID)
	data2 := buildBatch(
		encodeTopicRecord("test-topic", testTopicID),
		encodePartitionRecord(0, otherTopicID),
	)
	write(outDir+"/valid_topic_invalid_partition.log", data2)

	// 3. invalid topic + invalid partition (no records)
	data3 := buildBatch()
	write(outDir+"/invalid_topic_invalid_partition.log", data3)

	log.Printf("written to %s", outDir)
}

func write(path string, data []byte) {
	if err := os.WriteFile(path, data, 0644); err != nil {
		log.Fatalf("write %s: %v", path, err)
	}
	log.Printf("  %s (%d bytes)", path, len(data))
}

// buildBatch wraps zero or more pre-encoded Record payloads into one RecordBatch.
func buildBatch(records ...[]byte) []byte {
	var recs bytes.Buffer
	for _, r := range records {
		recs.Write(r)
	}

	var body bytes.Buffer
	mustWrite(&body, int32(0))  // PartitionLeaderEpoch
	mustWrite(&body, int8(2))   // Magic
	mustWrite(&body, int32(0))  // CRC (not verified)
	mustWrite(&body, int16(0))  // Attributes
	mustWrite(&body, int32(0))  // LastOffsetDelta
	mustWrite(&body, int64(0))  // BaseTimestamp
	mustWrite(&body, int64(0))  // MaxTimestamp
	mustWrite(&body, int64(-1)) // ProducerID
	mustWrite(&body, int16(-1)) // ProducerEpoch
	mustWrite(&body, int32(-1)) // BaseSequence
	mustWrite(&body, int32(len(records)))
	body.Write(recs.Bytes())

	var out bytes.Buffer
	mustWrite(&out, int64(0))           // BaseOffset
	mustWrite(&out, int32(body.Len()))  // BatchLength
	out.Write(body.Bytes())
	return out.Bytes()
}

// encodeRecord wraps a value byte slice as a Kafka Record with signed-varint framing.
func encodeRecord(value []byte) []byte {
	var rec bytes.Buffer
	rec.WriteByte(0) // Attributes
	rec.WriteByte(0) // TimestampDelta varint(0)
	rec.WriteByte(0) // OffsetDelta varint(0)
	writeVarint(&rec, -1)              // KeyLength = null
	writeVarint(&rec, int64(len(value))) // ValueLength
	rec.Write(value)
	rec.WriteByte(0) // Headers count uvarint(0)

	var out bytes.Buffer
	writeVarint(&out, int64(rec.Len()))
	out.Write(rec.Bytes())
	return out.Bytes()
}

func encodeTopicRecord(name string, topicID [16]byte) []byte {
	var val bytes.Buffer
	val.WriteByte(0) // FrameVersion
	val.WriteByte(2) // RecordType = TOPIC_RECORD
	val.WriteByte(0) // Version
	writeCompactString(&val, name)
	val.Write(topicID[:])
	val.WriteByte(0) // TagBuffer
	return encodeRecord(val.Bytes())
}

func encodePartitionRecord(partitionID int32, topicID [16]byte) []byte {
	var val bytes.Buffer
	val.WriteByte(0) // FrameVersion
	val.WriteByte(3) // RecordType = PARTITION_RECORD
	val.WriteByte(0) // Version
	mustWrite(&val, partitionID)
	val.Write(topicID[:])
	return encodeRecord(val.Bytes())
}

func mustWrite(w *bytes.Buffer, v any) {
	if err := binary.Write(w, binary.BigEndian, v); err != nil {
		panic(err)
	}
}

func writeVarint(w *bytes.Buffer, v int64) {
	tmp := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(tmp, v)
	w.Write(tmp[:n])
}

func writeCompactString(w *bytes.Buffer, s string) {
	tmp := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(tmp, uint64(len(s)+1))
	w.Write(tmp[:n])
	w.WriteString(s)
}
