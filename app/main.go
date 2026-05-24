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

type Request struct {
	MessageSize       int32
	RequestAPIKey     int16
	RequestAPIVersion int16
	CorrelationID     int32
}

type Response struct {
	MessageSize   int32
	CorrelationID int32
	ErrorCode     int16
}

func NewResponse(req *Request) *Response {
	resp := &Response{
		CorrelationID: req.CorrelationID,
	}
	if req.RequestAPIKey == 18 {
		if req.RequestAPIVersion >= 0 && req.RequestAPIVersion <= 4 {
			resp.ErrorCode = 0
		} else {
			resp.ErrorCode = 35
		}
	}
	return resp
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

func handleRequest(conn net.Conn) {
	defer conn.Close()

	req, err := parseRequest(conn)
	if err != nil {
		log.Printf("handleRequest: parse request: %v", err)
		return
	}
	log.Printf("request: %+v\n", req)

	resp := NewResponse(req)
	log.Printf("response: %+v\n", resp)

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
