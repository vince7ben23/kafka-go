package main

import (
	"encoding/binary"
	"io"
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
