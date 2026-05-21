package main

import (
	"encoding/binary"
	"log"
	"net"
)

type Server struct {
	Listener net.Listener
}

// Go style constructor
func NewServer() (*Server, error) {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		return nil, err
	}
	server := &Server{
		Listener: l,
	}
	return server, nil
}

func (s *Server) Run() error {
	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			return err
		}
		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	defer conn.Close()
	// reader := bufio.NewReadWriter(conn)
	response := make([]byte, 8)
	binary.BigEndian.PutUint64(response, 7)
	conn.Write(response)
}

func main() {

	server, err := NewServer()
	if err != nil {
		log.Fatal("Failed to create server: ", err)
	}
	if err = server.Run(); err != nil {
		log.Fatal("Failed to run server: ", err)
	}
}
