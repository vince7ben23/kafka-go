package main

import (
	"encoding/binary"
	"fmt"
	"net"
)

type Request struct {
	MessageSize       int32
	RequestAPIKey     int16
	RequestAPIVersion int16
	CorrelationID     int32
}

func parseRequest(conn net.Conn) (*Request, error) {
	req := &Request{}

	if err := binary.Read(conn, binary.BigEndian, &req.MessageSize); err != nil {
		return nil, fmt.Errorf("read message_size: %w", err)
	}
	if err := binary.Read(conn, binary.BigEndian, &req.RequestAPIKey); err != nil {
		return nil, fmt.Errorf("read api_key: %w", err)
	}
	if err := binary.Read(conn, binary.BigEndian, &req.RequestAPIVersion); err != nil {
		return nil, fmt.Errorf("read api_version: %w", err)
	}
	if err := binary.Read(conn, binary.BigEndian, &req.CorrelationID); err != nil {
		return nil, fmt.Errorf("read correlation_id: %w", err)
	}
	return req, nil
}
