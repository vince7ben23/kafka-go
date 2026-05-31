package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

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

func parseRequest(conn net.Conn) (*Request, error) {
	req := &Request{}

	if err := binary.Read(conn, binary.BigEndian, &req.MessageSize); err != nil {
		return nil, fmt.Errorf("read message_size: %w", err)
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
