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
