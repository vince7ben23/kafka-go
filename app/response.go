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
	ErrorCode    int16
	APIVersions  []ApiVersion
	ThrottleTime int32
	TagBuffer    int8
}

func (r *ApiVersionsResponse) Encode() []byte {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.BigEndian, r.CorrelationID)
	_ = binary.Write(buf, binary.BigEndian, r.ErrorCode)
	writeCompactArrayLen(buf, len(r.APIVersions))
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

// DescribeTopicPartitionsTopicResponse holds per-topic metadata echoed back to the client.
type DescribeTopicPartitionsTopicResponse struct {
	ErrorCode  int16
	Name       string
	TopicID    [16]byte // all zeros for an unknown topic
	IsInternal bool
}

// DescribeTopicPartitionsResponse encodes a DescribeTopicPartitions API v0 response
// (flexible version, response header v1).
type DescribeTopicPartitionsResponse struct {
	HeaderResponse
	HeaderTagBuffer int8
	ThrottleTimeMs  int32
	Topics          []DescribeTopicPartitionsTopicResponse
}

func (r *DescribeTopicPartitionsResponse) Encode() []byte {
	buf := new(bytes.Buffer)

	// Response header v1: correlation_id + tag_buffer
	_ = binary.Write(buf, binary.BigEndian, r.CorrelationID)
	_ = binary.Write(buf, binary.BigEndian, r.HeaderTagBuffer)

	_ = binary.Write(buf, binary.BigEndian, r.ThrottleTimeMs)

	// topics: COMPACT_ARRAY (count+1)
	writeCompactArrayLen(buf, len(r.Topics))
	for _, topic := range r.Topics {
		_ = binary.Write(buf, binary.BigEndian, topic.ErrorCode)
		writeCompactString(buf, topic.Name)
		buf.Write(topic.TopicID[:]) // topic_id: 16-byte UUID
		if topic.IsInternal {       // is_internal: BOOLEAN
			buf.WriteByte(0x01)
		} else {
			buf.WriteByte(0x00)
		}
		writeCompactArrayLen(buf, 0)                      // partitions: empty COMPACT_ARRAY
		_ = binary.Write(buf, binary.BigEndian, int32(0)) // topic_authorized_operations
		buf.WriteByte(0x00)                               // topic tag_buffer
	}

	buf.WriteByte(0xff) // next_cursor: null (NULLABLE, -1)
	buf.WriteByte(0x00) // top-level tag_buffer

	return buf.Bytes()
}

func writeUvarint(buf *bytes.Buffer, v uint64) {
	b := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(b, v)
	buf.Write(b[:n])
}

// writeCompactArrayLen writes a Kafka flexible-version COMPACT_ARRAY length,
// encoded as the actual element count + 1 (0 = null array).
func writeCompactArrayLen(buf *bytes.Buffer, n int) {
	writeUvarint(buf, uint64(n+1))
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
	writeCompactArrayLen(buf, len(r.Topics))
	for _, topic := range r.Topics {
		writeCompactString(buf, topic.Name)

		// partition_responses: COMPACT_ARRAY (count+1)
		writeCompactArrayLen(buf, len(topic.Partitions))
		for _, part := range topic.Partitions {
			_ = binary.Write(buf, binary.BigEndian, part.Index)
			_ = binary.Write(buf, binary.BigEndian, part.ErrorCode)
			_ = binary.Write(buf, binary.BigEndian, part.BaseOffset)
			_ = binary.Write(buf, binary.BigEndian, part.LogAppendTime)
			_ = binary.Write(buf, binary.BigEndian, part.LogStartOffset)
			writeCompactArrayLen(buf, 0) // record_errors: empty COMPACT_ARRAY
			buf.WriteByte(0x00)          // error_message: null COMPACT_NULLABLE_STRING
			_ = binary.Write(buf, binary.BigEndian, part.TagBuffer)
		}
		_ = binary.Write(buf, binary.BigEndian, topic.TagBuffer)
	}
	_ = binary.Write(buf, binary.BigEndian, r.ThrottleTimeMs)
	_ = binary.Write(buf, binary.BigEndian, r.TagBuffer)

	return buf.Bytes()
}

func NewResponse(req *Request, meta *ClusterMetadata) (Encoder, error) {
	switch req.RequestAPIKey {
	case APIKeyApiVersions:
		return createApiVersionsResponse(req)
	case APIKeyApiProduce:
		return createApiProduceResponse(req, meta)
	case APIKeyDescribeTopicPartitions:
		return createDescribeTopicPartitionsResponse(req, meta)
	default:
		return &HeaderResponse{CorrelationID: req.CorrelationID}, nil
	}
}

func createApiVersionsResponse(req *Request) (*ApiVersionsResponse, error) {
	errorCode := int16(0)
	if req.RequestAPIVersion < 0 || req.RequestAPIVersion > 4 {
		errorCode = 35
	}
	apiVersions := []ApiVersion{
		{APIKey: APIKeyApiVersions, MinVersion: 0, MaxVersion: 4, TagBuffer: 0},
		{APIKey: APIKeyApiProduce, MinVersion: 0, MaxVersion: 11, TagBuffer: 0},
		{APIKey: APIKeyDescribeTopicPartitions, MinVersion: 0, MaxVersion: 0, TagBuffer: 0},
	}

	return &ApiVersionsResponse{
		HeaderResponse: HeaderResponse{CorrelationID: req.CorrelationID},
		ErrorCode:      errorCode,
		APIVersions:    apiVersions,
	}, nil
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

func createApiProduceResponse(req *Request, meta *ClusterMetadata) (*ProduceResponse, error) {
	pr, err := parseProduceRequest(req.Body)
	if err != nil {
		return nil, fmt.Errorf("createApiProduceResponse: %w", err)
	}

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

func createDescribeTopicPartitionsResponse(req *Request, meta *ClusterMetadata) (*DescribeTopicPartitionsResponse, error) {
	dr, err := parseDescribeTopicPartitionsRequest(req.Body)
	if err != nil {
		return nil, fmt.Errorf("createDescribeTopicPartitionsResponse: %w", err)
	}

	resp := &DescribeTopicPartitionsResponse{
		HeaderResponse: HeaderResponse{CorrelationID: req.CorrelationID},
	}
	for _, name := range dr.TopicNames {
		topicResp := DescribeTopicPartitionsTopicResponse{Name: name}
		// Resolve the topic through the shared cluster metadata. A known topic
		// echoes back its real UUID with error code 0; an unknown topic (or no
		// metadata at all) yields error code 3 with a zero UUID.
		// Note: partition listing for known topics is a later stage.
		if topicID, ok := meta.findTopicID(name); ok {
			topicResp.TopicID = topicID
		} else {
			topicResp.ErrorCode = 3
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp, nil
}
