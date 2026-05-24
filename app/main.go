package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
)

type Server struct {
	Listener net.Listener
}

func NewServer() (*Server, error) {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		return nil, fmt.Errorf("NewServer: %w", err)
	}
	return &Server{Listener: l}, nil
}

func (s *Server) Run() error {
	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			return fmt.Errorf("Run: accept: %w", err)
		}
		go handleRequest(conn)
	}
}

type RequestHeader struct {
	MessageSize        int32
	RequestAPIKey      int16
	RequestAPIVersion  int16
	CorrelationID      int32
}

type Response struct {
	MessageSize   int32
	CorrelationID int32
}

func parseRequestHeader(conn net.Conn) (*RequestHeader, error) {
	h := &RequestHeader{}

	if err := binary.Read(conn, binary.BigEndian, &h.MessageSize); err != nil {
		return nil, fmt.Errorf("read message_size: %w", err)
	}
	if err := binary.Read(conn, binary.BigEndian, &h.RequestAPIKey); err != nil {
		return nil, fmt.Errorf("read api_key: %w", err)
	}
	if err := binary.Read(conn, binary.BigEndian, &h.RequestAPIVersion); err != nil {
		return nil, fmt.Errorf("read api_version: %w", err)
	}
	if err := binary.Read(conn, binary.BigEndian, &h.CorrelationID); err != nil {
		return nil, fmt.Errorf("read correlation_id: %w", err)
	}
	return h, nil
}

func handleRequest(conn net.Conn) {
	defer conn.Close()

	reqHeader, err := parseRequestHeader(conn)
	if err != nil {
		log.Printf("handleRequest: parse header: %v", err)
		return
	}
	log.Printf("apiKey=%d apiVersion=%d correlationID=%d",
		reqHeader.RequestAPIKey, reqHeader.RequestAPIVersion, reqHeader.CorrelationID)

	resp := &Response{
		CorrelationID: reqHeader.CorrelationID,
	}
	if err := binary.Write(conn, binary.BigEndian, resp); err != nil {
		log.Printf("handleRequest: write response: %v", err)
		return
	}
}

func main() {
	server, err := NewServer()
	if err != nil {
		log.Fatal(err)
	}
	if err = server.Run(); err != nil {
		log.Fatal(err)
	}
}
