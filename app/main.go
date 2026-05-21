package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func handleRequest(conn net.Conn) {
	defer conn.Close()
	// reader := bufio.NewReadWriter(conn)
	response := make([]byte, 8)
	binary.BigEndian.PutUint64(response, 7)
	conn.Write(response)
}

func main() {

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	handleRequest(conn)
}
