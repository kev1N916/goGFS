package main

import (
	"fmt"
	"log"
	"net"
)

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	defer listener.Close()

	fmt.Println("Server listening on port 8080...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		go handleConnection(conn)
	}
	
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Get client address
	remoteAddr := conn.RemoteAddr().String()
	fmt.Printf("New connection from: %s\n", remoteAddr)

	// Simple echo server functionality
	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			log.Printf("Connection closed from %s: %v\n", remoteAddr, err)
			return
		}

		// Echo the received data back to the client
		_, err = conn.Write(buffer[:n])
		if err != nil {
			log.Printf("Failed to write to connection: %v", err)
			return
		}
	}
}
