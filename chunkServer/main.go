package main

import (
	"fmt"
	"log"
	"net"
)

func main() {
	// Start chunk server
	listener, err := net.Listen("tcp", ":8081")
	if err != nil {
		log.Fatalf("Failed to start chunk server: %v", err)
	}
	defer listener.Close()

	fmt.Println("Chunk server listening on :8081")

	// Register with master server
	if err := registerWithMaster(); err != nil {
		log.Fatalf("Failed to register with master server: %v", err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		go handleConnection(conn)
	}
}

func registerWithMaster() error {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		return fmt.Errorf("failed to connect to master server: %v", err)
	}
	defer conn.Close()

	// TODO: Implement registration protocol
	return nil
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	
	// Get client address
	remoteAddr := conn.RemoteAddr().String()
	fmt.Printf("New connection from: %s\n", remoteAddr)

	// TODO: Implement chunk server logic
}
