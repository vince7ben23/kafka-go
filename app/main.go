package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
		log.Printf("Accepted TCP connection from %s\n", conn.RemoteAddr())
		go handleRequest(conn)
	}
}

func writeResponse(conn net.Conn, resp Encoder) error {
	data := resp.Encode()
	msgSize := MessageSize(len(data))
	log.Printf("Message size: %d bytes, response: %+v\n", msgSize, resp)
	if err := binary.Write(conn, binary.BigEndian, msgSize); err != nil {
		return fmt.Errorf("writeResponse: write message size: %w", err)
	}
	_, err := conn.Write(data)
	if err != nil {
		return fmt.Errorf("writeResponse: write: %w", err)
	}
	return nil
}

func handleRequest(conn net.Conn) {
	defer conn.Close()
	for {
		req, err := parseRequest(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			log.Printf("handleRequest: parse request: %v", err)
			return
		}
		log.Printf("request: %+v\n", req)

		resp, err := NewResponse(req)
		if err != nil {
			log.Printf("handleRequest: new response: %v", err)
			_ = writeResponse(conn, &HeaderResponse{CorrelationID: req.CorrelationID})
			return
		}

		if err := writeResponse(conn, resp); err != nil {
			log.Printf("handleRequest: write response: %v", err)
			return
		}
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
