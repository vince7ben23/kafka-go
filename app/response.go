package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

const APIKeyApiProduce = int16(0)
const APIKeyApiVersions = int16(18)
const APIKeyDescribeTopicPartitions = int16(75)

type MessageSize int32

type Encoder interface {
	Encode() []byte
}

type HeaderResponse struct {
	CorrelationID int32
}

func (r *HeaderResponse) Encode() []byte {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.BigEndian, r.CorrelationID)
	return buf.Bytes()
}

type ApiVersionsResponse struct {
	HeaderResponse
	ErrorCode      int16
	APIArrayLength int8
	APIVersions    []ApiVersion
	ThrottleTime   int32
	TagBuffer      int8
}

func (r *ApiVersionsResponse) Encode() []byte {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.BigEndian, r.CorrelationID)
	_ = binary.Write(buf, binary.BigEndian, r.ErrorCode)
	_ = binary.Write(buf, binary.BigEndian, r.APIArrayLength)
	for _, v := range r.APIVersions {
		_ = binary.Write(buf, binary.BigEndian, v)
	}
	_ = binary.Write(buf, binary.BigEndian, r.ThrottleTime)
	_ = binary.Write(buf, binary.BigEndian, r.TagBuffer)
	return buf.Bytes()
}

type ApiVersion struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
	TagBuffer  int8
}

// ProducePartitionResponse holds per-partition fields echoed back to the client.
type ProducePartitionResponse struct {
	Index          int32
	ErrorCode      int16
	BaseOffset     int64
	LogAppendTime  int64
	LogStartOffset int64
	TagBuffer      int8
}

// ProduceTopicResponse holds per-topic fields echoed back to the client.
type ProduceTopicResponse struct {
	Name       string
	Partitions []ProducePartitionResponse
	TagBuffer  int8
}

// ProduceResponse encodes a Produce API v11 response (flexible version, response header v1).
type ProduceResponse struct {
	HeaderResponse
	HeaderTagBuffer int8
	Topics          []ProduceTopicResponse
	ThrottleTimeMs  int32
	TagBuffer       int8
}

func writeUvarint(buf *bytes.Buffer, v uint64) {
	b := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(b, v)
	buf.Write(b[:n])
}

func writePartitionLog(baseDir, topicName string, partitionIdx int32, records []byte) error {
	dir := filepath.Join(baseDir, fmt.Sprintf("%s-%d", topicName, partitionIdx))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("writePartitionLog mkdir: %w", err)
	}
	logPath := filepath.Join(dir, "00000000000000000000.log")
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("writePartitionLog open: %w", err)
	}
	defer f.Close()
	if _, err := f.Write(records); err != nil {
		return fmt.Errorf("writePartitionLog write: %w", err)
	}
	return nil
}

func writeCompactString(buf *bytes.Buffer, s string) {
	writeUvarint(buf, uint64(len(s)+1))
	buf.WriteString(s)
}

func (r *ProduceResponse) Encode() []byte {
	buf := new(bytes.Buffer)

	// Response header v1: correlation_id + tag_buffer
	_ = binary.Write(buf, binary.BigEndian, r.CorrelationID)
	_ = binary.Write(buf, binary.BigEndian, r.HeaderTagBuffer)

	// responses: COMPACT_ARRAY (count+1)
	writeUvarint(buf, uint64(len(r.Topics)+1))
	for _, topic := range r.Topics {
		writeCompactString(buf, topic.Name)

		// partition_responses: COMPACT_ARRAY (count+1)
		writeUvarint(buf, uint64(len(topic.Partitions)+1))
		for _, part := range topic.Partitions {
			_ = binary.Write(buf, binary.BigEndian, part.Index)
			_ = binary.Write(buf, binary.BigEndian, part.ErrorCode)
			_ = binary.Write(buf, binary.BigEndian, part.BaseOffset)
			_ = binary.Write(buf, binary.BigEndian, part.LogAppendTime)
			_ = binary.Write(buf, binary.BigEndian, part.LogStartOffset)
			buf.WriteByte(0x01) // record_errors: empty COMPACT_ARRAY
			buf.WriteByte(0x00) // error_message: null COMPACT_NULLABLE_STRING
			_ = binary.Write(buf, binary.BigEndian, part.TagBuffer)
		}
		_ = binary.Write(buf, binary.BigEndian, topic.TagBuffer)
	}
	_ = binary.Write(buf, binary.BigEndian, r.ThrottleTimeMs)
	_ = binary.Write(buf, binary.BigEndian, r.TagBuffer)

	return buf.Bytes()
}

func NewResponse(req *Request) (Encoder, error) {
	switch req.RequestAPIKey {
	case APIKeyApiVersions:
		return createApiVersionsResponse(req)
	case APIKeyApiProduce:
		return createApiProduceResponse(req)
	default:
		return &HeaderResponse{CorrelationID: req.CorrelationID}, nil
	}
}

func createApiVersionsResponse(req *Request) (*ApiVersionsResponse, error) {
	errorCode := int16(0)
	if req.RequestAPIVersion < 0 || req.RequestAPIVersion > 4 {
		errorCode = 35
	}
	// Kafka compact array: length field = actual count + 1 (flexible version encoding)
	apiVersions := []ApiVersion{
		{APIKey: APIKeyApiVersions, MinVersion: 0, MaxVersion: 4, TagBuffer: 0},
		{APIKey: APIKeyApiProduce, MinVersion: 0, MaxVersion: 11, TagBuffer: 0},
		{APIKey: APIKeyDescribeTopicPartitions, MinVersion: 0, MaxVersion: 0, TagBuffer: 0},
	}

	apiArrayLength, err := toCompactArrayLen(len(apiVersions))
	if err != nil {
		return nil, err
	}
	return &ApiVersionsResponse{
		HeaderResponse: HeaderResponse{CorrelationID: req.CorrelationID},
		ErrorCode:      errorCode,
		APIArrayLength: apiArrayLength,
		APIVersions:    apiVersions,
	}, nil
}

func toCompactArrayLen(n int) (int8, error) {
	if n+1 > 127 {
		return 0, fmt.Errorf("toCompactArrayLen: array too large: %d", n)
	}
	return int8(n + 1), nil
}

func buildPartitionResponse(meta *ClusterMetadata, topicName string, part ProducePartitionData) ProducePartitionResponse {
	if meta == nil || !meta.validateTopicPartition(topicName, part.Index) {
		return ProducePartitionResponse{
			Index: part.Index, ErrorCode: 3,
			BaseOffset: -1, LogAppendTime: -1, LogStartOffset: -1,
		}
	}
	if err := writePartitionLog(kafkaLogBaseDir, topicName, part.Index, part.Records); err != nil {
		// storage write failed — return a well-formed error instead of a bare HeaderResponse
		return ProducePartitionResponse{
			Index: part.Index, ErrorCode: 5, // LEADER_NOT_AVAILABLE as proxy for storage error
			BaseOffset: -1, LogAppendTime: -1, LogStartOffset: -1,
		}
	}
	return ProducePartitionResponse{
		Index: part.Index, ErrorCode: 0,
		BaseOffset: 0, LogAppendTime: -1, LogStartOffset: 0,
	}
}

func createApiProduceResponse(req *Request) (*ProduceResponse, error) {
	pr, err := parseProduceRequest(req.Body)
	if err != nil {
		return nil, fmt.Errorf("createApiProduceResponse: %w", err)
	}

	meta, _ := readClusterMetadata(clusterMetadataLogPath)

	resp := &ProduceResponse{HeaderResponse: HeaderResponse{CorrelationID: req.CorrelationID}}
	for _, topic := range pr.TopicData {
		topicResp := ProduceTopicResponse{Name: topic.Name}
		for _, part := range topic.PartitionData {
			topicResp.Partitions = append(topicResp.Partitions, buildPartitionResponse(meta, topic.Name, part))
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp, nil
}
