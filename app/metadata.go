package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

var kafkaLogBaseDir = "/tmp/kraft-combined-logs"

const (
	recordTypeTopicRecord     = int8(2)
	recordTypePartitionRecord = int8(3)
	clusterMetadataLogPath    = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
)

type TopicMetadata struct {
	Name    string
	TopicID [16]byte
}

type PartitionMetadata struct {
	PartitionID int32
	TopicID     [16]byte
}

type ClusterMetadata struct {
	Topics     []TopicMetadata
	Partitions []PartitionMetadata
}

// findTopicID resolves a topic name to its UUID. It returns ok=false when the
// metadata is absent (nil receiver) or the topic does not exist.
func (m *ClusterMetadata) findTopicID(topicName string) ([16]byte, bool) {
	if m == nil {
		return [16]byte{}, false
	}
	for _, t := range m.Topics {
		if t.Name == topicName {
			return t.TopicID, true
		}
	}
	return [16]byte{}, false
}

func (m *ClusterMetadata) validateTopicPartition(topicName string, partitionID int32) bool {
	topicID, ok := m.findTopicID(topicName)
	if !ok {
		return false
	}
	for _, p := range m.Partitions {
		if p.PartitionID == partitionID && p.TopicID == topicID {
			return true
		}
	}
	return false
}

func readClusterMetadata(path string) (*ClusterMetadata, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("readClusterMetadata: %w", err)
	}
	meta := &ClusterMetadata{}
	r := bytes.NewReader(data)
	if err := parseRecordBatches(r, meta); err != nil {
		return nil, fmt.Errorf("readClusterMetadata: %w", err)
	}
	return meta, nil
}

func parseRecordBatches(r *bytes.Reader, meta *ClusterMetadata) error {
	for r.Len() > 0 {
		var baseOffset int64
		if err := binary.Read(r, binary.BigEndian, &baseOffset); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read base_offset: %w", err)
		}
		var batchLength int32
		if err := binary.Read(r, binary.BigEndian, &batchLength); err != nil {
			return fmt.Errorf("read batch_length: %w", err)
		}
		batchData := make([]byte, batchLength)
		if _, err := io.ReadFull(r, batchData); err != nil {
			return fmt.Errorf("read batch data: %w", err)
		}
		br := bytes.NewReader(batchData)

		// RecordBatch header: PartitionLeaderEpoch(4) + Magic(1) + CRC(4) +
		// Attributes(2) + LastOffsetDelta(4) + BaseTimestamp(8) + MaxTimestamp(8) +
		// ProducerID(8) + ProducerEpoch(2) + BaseSequence(4) = 45 bytes
		header := make([]byte, 45)
		if _, err := io.ReadFull(br, header); err != nil {
			return fmt.Errorf("read batch header: %w", err)
		}

		var recordCount int32
		if err := binary.Read(br, binary.BigEndian, &recordCount); err != nil {
			return fmt.Errorf("read record_count: %w", err)
		}

		for i := 0; i < int(recordCount); i++ {
			value, err := readRecordValue(br)
			if err != nil {
				return fmt.Errorf("record %d: %w", i, err)
			}
			if value != nil {
				if err := parseMetadataRecord(value, meta); err != nil {
					return fmt.Errorf("record %d metadata: %w", i, err)
				}
			}
		}
	}
	return nil
}

// readRecordValue reads one Record and returns its value bytes.
func readRecordValue(r *bytes.Reader) ([]byte, error) {
	// Length: signed varint (zigzag)
	length, err := binary.ReadVarint(r)
	if err != nil {
		return nil, fmt.Errorf("read length: %w", err)
	}
	if length <= 0 {
		return nil, nil
	}

	// Attributes: int8
	if _, err := r.ReadByte(); err != nil {
		return nil, fmt.Errorf("read attributes: %w", err)
	}
	// TimestampDelta: signed varint
	if _, err := binary.ReadVarint(r); err != nil {
		return nil, fmt.Errorf("read timestamp_delta: %w", err)
	}
	// OffsetDelta: signed varint
	if _, err := binary.ReadVarint(r); err != nil {
		return nil, fmt.Errorf("read offset_delta: %w", err)
	}
	// KeyLength: signed varint (-1 = null)
	keyLength, err := binary.ReadVarint(r)
	if err != nil {
		return nil, fmt.Errorf("read key_length: %w", err)
	}
	if keyLength > 0 {
		if _, err := r.Seek(keyLength, io.SeekCurrent); err != nil {
			return nil, fmt.Errorf("skip key: %w", err)
		}
	}
	// ValueLength: signed varint (-1 = null)
	valueLength, err := binary.ReadVarint(r)
	if err != nil {
		return nil, fmt.Errorf("read value_length: %w", err)
	}
	var value []byte
	if valueLength > 0 {
		value = make([]byte, valueLength)
		if _, err := io.ReadFull(r, value); err != nil {
			return nil, fmt.Errorf("read value: %w", err)
		}
	}
	// Headers: uvarint count, then key/value pairs
	headerCount, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("read headers count: %w", err)
	}
	for i := 0; i < int(headerCount); i++ {
		hkLen, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("read header[%d] key length: %w", i, err)
		}
		if hkLen > 0 {
			if _, err := r.Seek(int64(hkLen), io.SeekCurrent); err != nil {
				return nil, fmt.Errorf("skip header[%d] key: %w", i, err)
			}
		}
		hvLen, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("read header[%d] value length: %w", i, err)
		}
		if hvLen > 0 {
			if _, err := r.Seek(int64(hvLen), io.SeekCurrent); err != nil {
				return nil, fmt.Errorf("skip header[%d] value: %w", i, err)
			}
		}
	}
	return value, nil
}

func parseMetadataRecord(value []byte, meta *ClusterMetadata) error {
	r := bytes.NewReader(value)
	// FrameVersion: int8
	if _, err := r.ReadByte(); err != nil {
		return nil
	}
	// RecordType: int8
	recordTypeByte, err := r.ReadByte()
	if err != nil {
		return nil
	}
	recordType := int8(recordTypeByte)
	// Version: int8
	if _, err := r.ReadByte(); err != nil {
		return nil
	}

	switch recordType {
	case recordTypeTopicRecord:
		return parseTopicRecord(r, meta)
	case recordTypePartitionRecord:
		return parsePartitionRecord(r, meta)
	}
	return nil
}

func parseTopicRecord(r *bytes.Reader, meta *ClusterMetadata) error {
	name, err := readCompactString(r)
	if err != nil {
		return fmt.Errorf("TOPIC_RECORD name: %w", err)
	}
	var topicID [16]byte
	if _, err := io.ReadFull(r, topicID[:]); err != nil {
		return fmt.Errorf("TOPIC_RECORD topic_id: %w", err)
	}
	meta.Topics = append(meta.Topics, TopicMetadata{Name: name, TopicID: topicID})
	return nil
}

func parsePartitionRecord(r *bytes.Reader, meta *ClusterMetadata) error {
	var partitionID int32
	if err := binary.Read(r, binary.BigEndian, &partitionID); err != nil {
		return fmt.Errorf("PARTITION_RECORD partition_id: %w", err)
	}
	var topicID [16]byte
	if _, err := io.ReadFull(r, topicID[:]); err != nil {
		return fmt.Errorf("PARTITION_RECORD topic_id: %w", err)
	}
	meta.Partitions = append(meta.Partitions, PartitionMetadata{PartitionID: partitionID, TopicID: topicID})
	return nil
}
