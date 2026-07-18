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
	"flag"
	"log"
	"os"
)

const (
	baseLogDir  = "/tmp/kraft-combined-logs"
	metadataDir = baseLogDir + "/__cluster_metadata-0"
	// partitionDataDir is where the broker reads a topic-partition's message log
	// (see readPartitionLog in app/response.go). test-topic partition 0.
	partitionDataDir = baseLogDir + "/test-topic-0"
)

// singleMessageValue is the payload of the lone message in the
// fetch-single-message data log; its exact bytes are irrelevant to the broker,
// which returns the whole RecordBatch verbatim.
var singleMessageValue = []byte("Hello, Kafka Fetch!")

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

// fixture is a __cluster_metadata log candidate; copy the chosen file to
// 00000000000000000000.log before starting the broker.
type fixture struct {
	caseName string
	fileName string
	data     []byte
}

// partitionLog is a topic-partition message log written straight to the broker's
// canonical segment path (no rename needed), so readPartitionLog reads it as-is.
type partitionLog struct {
	caseName string
	dir      string
	data     []byte
}

const partitionSegmentName = "00000000000000000000.log"

func main() {
	caseFlag := flag.String("case", "all", "which fixture to generate: valid | invalid-partition | invalid-topic | fetch-empty | fetch-single-message | all")
	flag.Parse()

	// Metadata fixtures — all written to metadataDir under their own names.
	metadataFixtures := []fixture{
		{
			"valid",
			"valid_topic_valid_partition.log",
			buildBatch(encodeTopicRecord("test-topic", testTopicID), encodePartitionRecord(0, testTopicID)),
		},
		{
			"invalid-partition",
			"valid_topic_invalid_partition.log",
			buildBatch(encodeTopicRecord("test-topic", testTopicID), encodePartitionRecord(0, otherTopicID)),
		},
		{
			"invalid-topic",
			"invalid_topic_invalid_partition.log",
			buildBatch(),
		},
		{
			// Topic exists in metadata but has no PARTITION_RECORD and no message
			// log — the "Fetch for a topic with no messages" case. Fetch resolves
			// testTopicID via hasTopicID and replies error code 0 with empty records.
			"fetch-empty",
			"fetch_empty_topic.log",
			buildBatch(encodeTopicRecord("test-topic", testTopicID)),
		},
		{
			// The "Fetch for a topic with a single message on disk" case: metadata
			// side makes the topic known; the message itself lives in the
			// partitionLogs entry below.
			"fetch-single-message",
			"fetch_single_message_meta.log",
			buildBatch(encodeTopicRecord("test-topic", testTopicID)),
		},
	}

	// Partition message logs — written straight to <topic>-<partition>/ under the
	// canonical segment name, ready for the broker to read.
	partitionLogs := []partitionLog{
		{
			"fetch-single-message",
			partitionDataDir,
			buildBatch(encodeRecord(singleMessageValue)),
		},
	}

	written := 0
	for _, f := range metadataFixtures {
		if *caseFlag == "all" || *caseFlag == f.caseName {
			write(metadataDir, f.fileName, f.data)
			written++
		}
	}
	for _, p := range partitionLogs {
		if *caseFlag == "all" || *caseFlag == p.caseName {
			write(p.dir, partitionSegmentName, p.data)
			written++
		}
	}
	if written == 0 {
		log.Fatalf("unknown -case %q: must be valid | invalid-partition | invalid-topic | fetch-empty | fetch-single-message | all", *caseFlag)
	}
}

func write(dir, fileName string, data []byte) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatalf("mkdir %s: %v", dir, err)
	}
	path := dir + "/" + fileName
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
	// Replica/ISR/leader fields read by parsePartitionRecord (app/metadata.go).
	writeCompactInt32Array(&val, []int32{1}) // replicas
	writeCompactInt32Array(&val, []int32{1}) // isr
	writeCompactInt32Array(&val, nil)        // removing_replicas
	writeCompactInt32Array(&val, nil)        // adding_replicas
	mustWrite(&val, int32(1))                // leader
	mustWrite(&val, int32(0))                // leader_epoch
	mustWrite(&val, int32(0))                // partition_epoch
	val.WriteByte(0)                         // TagBuffer
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

// writeCompactInt32Array encodes a COMPACT_ARRAY of int32: uvarint(len+1)
// followed by each element as a big-endian int32 (nil encodes an empty array).
func writeCompactInt32Array(w *bytes.Buffer, vs []int32) {
	tmp := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(tmp, uint64(len(vs)+1))
	w.Write(tmp[:n])
	for _, v := range vs {
		mustWrite(w, v)
	}
}
