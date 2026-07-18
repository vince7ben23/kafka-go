package main

import (
	"encoding/binary"
	"io"
	"testing"
)

// unknownTopicID is the canonical 16-byte topic_id used across Fetch tests for a
// topic that does not exist in cluster metadata (expected to yield error code 100).
// It matches the bytes sent by test_script/test_fetch_unknown_topic.sh.
var unknownTopicID = [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}

// putUvarint encodes v as a Kafka uvarint (used for COMPACT_ARRAY/STRING lengths).
func putUvarint(v uint64) []byte {
	tmp := make([]byte, binary.MaxVarintLen64)
	return tmp[:binary.PutUvarint(tmp, v)]
}

// putInt32BE encodes v as a big-endian int32.
func putInt32BE(v int32) []byte {
	return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}

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
