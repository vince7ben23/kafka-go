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

func handleRequest(conn net.Conn) {
	defer conn.Close()

	response := make([]byte, 8)
	binary.BigEndian.PutUint64(response, 7)
	if _, err := conn.Write(response); err != nil {
		log.Printf("handleRequest: write: %v", err)
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
