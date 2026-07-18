package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

// maxMessageSize caps a single request's declared size to guard the message
// buffer allocation, matching Kafka's default socket.request.max.bytes (100 MiB).
const maxMessageSize = 100 * 1024 * 1024

type Request struct {
	MessageSize       int32
	RequestAPIKey     int16
	RequestAPIVersion int16
	CorrelationID     int32
	ClientIDLength    int16
	ClientIDContent   string
	TagBuffer         int8
	Body              []byte // remaining bytes after header; parsed by each API's handler
}

type ProducePartitionData struct {
	Index   int32
	Records []byte
}

type ProduceTopicData struct {
	Name          string
	PartitionData []ProducePartitionData
}

type ProduceRequest struct {
	TransactionalID string
	Acks            int16
	TimeoutMs       int32
	TopicData       []ProduceTopicData
}

type DescribeTopicPartitionsRequest struct {
	TopicNames []string
}

type FetchPartitionData struct {
	Partition int32
}

type FetchTopicData struct {
	TopicID    [16]byte
	Partitions []FetchPartitionData
}

type FetchRequest struct {
	Topics []FetchTopicData
}

// fetchRequestHeader is the fixed-size scalar prefix of a Fetch v16 body.
// Decoded as a whole so the field names document the wire layout; none of
// these values are used by this stage.
type fetchRequestHeader struct {
	MaxWaitMs      int32
	MinBytes       int32
	MaxBytes       int32
	IsolationLevel int8
	SessionID      int32
	SessionEpoch   int32
}

// fetchPartitionFields is the fixed-size portion of a Fetch v16 partition entry
// (before its TAG_BUFFER). Only Partition is retained; the rest are decoded to
// advance past them.
type fetchPartitionFields struct {
	Partition          int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedEpoch   int32
	LogStartOffset     int64
	PartitionMaxBytes  int32
}

func parseRequest(conn net.Conn) (*Request, error) {
	req := &Request{}

	if err := binary.Read(conn, binary.BigEndian, &req.MessageSize); err != nil {
		return nil, fmt.Errorf("read message_size: %w", err)
	}
	// Guard make([]byte, ...) against a malformed size: negative values panic
	// makeslice, and absurdly large ones would try to allocate gigabytes. Kafka
	// caps this at socket.request.max.bytes (100 MiB by default).
	if req.MessageSize <= 0 || req.MessageSize > maxMessageSize {
		return nil, fmt.Errorf("invalid message_size %d (must be 1..%d)", req.MessageSize, maxMessageSize)
	}

	msgBuf := make([]byte, req.MessageSize)
	if _, err := io.ReadFull(conn, msgBuf); err != nil {
		return nil, fmt.Errorf("read message body: %w", err)
	}
	r := bytes.NewReader(msgBuf)

	if err := binary.Read(r, binary.BigEndian, &req.RequestAPIKey); err != nil {
		return nil, fmt.Errorf("read api_key: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &req.RequestAPIVersion); err != nil {
		return nil, fmt.Errorf("read api_version: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &req.CorrelationID); err != nil {
		return nil, fmt.Errorf("read correlation_id: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &req.ClientIDLength); err != nil {
		return nil, fmt.Errorf("read client_id_length: %w", err)
	}
	if req.ClientIDLength > 0 {
		buf := make([]byte, req.ClientIDLength)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, fmt.Errorf("read client_id_content: %w", err)
		}
		req.ClientIDContent = string(buf)
	}
	if err := binary.Read(r, binary.BigEndian, &req.TagBuffer); err != nil {
		return nil, fmt.Errorf("read tag_buffer: %w", err)
	}
	req.Body, _ = io.ReadAll(r)

	return req, nil
}

// readCompactString reads a Kafka COMPACT_NULLABLE_STRING (uvarint length+1, then bytes).
func readCompactString(r *bytes.Reader) (string, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return "", fmt.Errorf("read compact string length: %w", err)
	}
	if n == 0 {
		return "", nil
	}
	buf := make([]byte, n-1)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", fmt.Errorf("read compact string body: %w", err)
	}
	return string(buf), nil
}

// readCompactBytes reads a Kafka COMPACT_NULLABLE_BYTES (uvarint length+1, then bytes).
func readCompactBytes(r *bytes.Reader) ([]byte, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("read compact bytes length: %w", err)
	}
	if n == 0 {
		return nil, nil
	}
	buf := make([]byte, n-1)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("read compact bytes body: %w", err)
	}
	return buf, nil
}

// parseProduceRequest decodes a Produce request v11 body.
func parseProduceRequest(body []byte) (*ProduceRequest, error) {
	r := bytes.NewReader(body)
	pr := &ProduceRequest{}

	var err error
	pr.TransactionalID, err = readCompactString(r)
	if err != nil {
		return nil, fmt.Errorf("produce: transactional_id: %w", err)
	}

	if err := binary.Read(r, binary.BigEndian, &pr.Acks); err != nil {
		return nil, fmt.Errorf("produce: acks: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &pr.TimeoutMs); err != nil {
		return nil, fmt.Errorf("produce: timeout_ms: %w", err)
	}

	topicCount, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("produce: topic count: %w", err)
	}
	for i := 0; i < int(topicCount)-1; i++ {
		var topic ProduceTopicData

		topic.Name, err = readCompactString(r)
		if err != nil {
			return nil, fmt.Errorf("produce: topic name: %w", err)
		}

		partCount, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("produce: partition count: %w", err)
		}
		for j := 0; j < int(partCount)-1; j++ {
			var part ProducePartitionData

			if err := binary.Read(r, binary.BigEndian, &part.Index); err != nil {
				return nil, fmt.Errorf("produce: partition index: %w", err)
			}
			part.Records, err = readCompactBytes(r)
			if err != nil {
				return nil, fmt.Errorf("produce: records: %w", err)
			}
			// TAG_BUFFER (empty tagged fields)
			if _, err := binary.ReadUvarint(r); err != nil {
				return nil, fmt.Errorf("produce: partition tag_buffer: %w", err)
			}
			topic.PartitionData = append(topic.PartitionData, part)
		}
		// TAG_BUFFER for topic
		if _, err := binary.ReadUvarint(r); err != nil {
			return nil, fmt.Errorf("produce: topic tag_buffer: %w", err)
		}
		pr.TopicData = append(pr.TopicData, topic)
	}

	return pr, nil
}

// parseFetchRequest decodes a Fetch request v16 (flexible) body, extracting the
// requested topic IDs and partition indexes. The leading scalar fields and each
// partition's non-index fields are decoded only to advance the reader; the
// trailing forgotten_topics_data, rack_id, and top-level tag_buffer are not
// needed for this stage.
func parseFetchRequest(body []byte) (*FetchRequest, error) {
	r := bytes.NewReader(body)
	fr := &FetchRequest{}

	// Advance past the fixed-size scalar prefix; its layout is documented by
	// fetchRequestHeader, whose size drives the skip (no magic number).
	if _, err := io.CopyN(io.Discard, r, int64(binary.Size(fetchRequestHeader{}))); err != nil {
		return nil, fmt.Errorf("fetch: header fields: %w", err)
	}

	// topics: COMPACT_ARRAY, encoded length = actual count + 1.
	topicCount, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("fetch: topic count: %w", err)
	}
	for i := 0; i < int(topicCount)-1; i++ {
		var topic FetchTopicData

		if _, err := io.ReadFull(r, topic.TopicID[:]); err != nil {
			return nil, fmt.Errorf("fetch: topic_id: %w", err)
		}

		partCount, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("fetch: partition count: %w", err)
		}
		for j := 0; j < int(partCount)-1; j++ {
			var fields fetchPartitionFields
			if err := binary.Read(r, binary.BigEndian, &fields); err != nil {
				return nil, fmt.Errorf("fetch: partition fields: %w", err)
			}
			// partition TAG_BUFFER
			if _, err := binary.ReadUvarint(r); err != nil {
				return nil, fmt.Errorf("fetch: partition tag_buffer: %w", err)
			}
			topic.Partitions = append(topic.Partitions, FetchPartitionData{Partition: fields.Partition})
		}
		// topic TAG_BUFFER
		if _, err := binary.ReadUvarint(r); err != nil {
			return nil, fmt.Errorf("fetch: topic tag_buffer: %w", err)
		}
		fr.Topics = append(fr.Topics, topic)
	}

	return fr, nil
}

// parseDescribeTopicPartitionsRequest decodes a DescribeTopicPartitions request v0 body,
// extracting the requested topic names. Fields after the topics array
// (response_partition_limit, cursor, request tag_buffer) are not needed for this stage.
func parseDescribeTopicPartitionsRequest(body []byte) (*DescribeTopicPartitionsRequest, error) {
	r := bytes.NewReader(body)
	dr := &DescribeTopicPartitionsRequest{}

	// topics: COMPACT_ARRAY, encoded length = actual count + 1.
	// 0 = null array, 1 = empty array; neither carries any topic entry.
	topicCount, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("describe: topic count: %w", err)
	}
	for i := 0; i < int(topicCount)-1; i++ {
		name, err := readCompactString(r)
		if err != nil {
			return nil, fmt.Errorf("describe: topic name: %w", err)
		}
		// TAG_BUFFER for topic
		if _, err := binary.ReadUvarint(r); err != nil {
			return nil, fmt.Errorf("describe: topic tag_buffer: %w", err)
		}
		dr.TopicNames = append(dr.TopicNames, name)
	}

	return dr, nil
}
