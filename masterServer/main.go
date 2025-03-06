package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/involk-secure-1609/goGFS/constants"
)

// Master represents the master server that manages files and chunk handlers
type Master struct {
	port         string
	fileMap      map[string][]int64 // maps file names to array of chunkIds
	chunkHandler map[int64][]string // maps chunkIds to the chunkServers which store those chunks
	mu           sync.Mutex          // mutex for safe concurrent access to maps
}

// NewMaster creates and initializes a new Master instance
func NewMaster(port string) *Master {
	return &Master{
		port:         port,
		fileMap:      make(map[string][]int64),
		chunkHandler: make(map[int64][]string),
	}
}

// Main initializes and starts the master server
func main() {
	masterPort := "8080"
	// Initialize master server
	master := NewMaster(masterPort)
	log.Printf("Master server listening on :%s", master.port)
	// Start master server
	listener, err := net.Listen("tcp", ":"+masterPort)
	if err != nil {
		log.Fatalf("Failed to start master server: %v", err)
	}
	defer listener.Close()

	fmt.Println("Master server listening on :8080")

	// Main loop to accept connections from clients
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		go master.handleConnection(conn)
	}
}

func (master *Master)handleConnection(conn net.Conn) {
	defer conn.Close()
	
	
	for {
		// Read the request type (1 byte)
		requestType := make([]byte, 1)
		n, err := conn.Read(requestType)
		if err != nil {
			if err == io.EOF {
				log.Println("Connection closed by client")
			} else {
				log.Println("Error reading request type:", err)
			}
			return
		}
		if n == 0 {
			continue // No data read, try again
		}
		
		// Read the request length (2 bytes)
		requestLength := make([]byte, 2)
		_, err = io.ReadFull(conn, requestLength)
		if err != nil {
			log.Println("Error reading request length:", err)
			return
		}
		
		// Get the length as uint16
		length := binary.LittleEndian.Uint16(requestLength)
		
		// Read the request body
		requestBodyBytes := make([]byte, length)
		_, err = io.ReadFull(conn, requestBodyBytes)
		if err != nil {
			log.Println("Error reading request body:", err)
			return
		}
		
		// Process the request based on type
		switch int(requestType[0]) {
		case constants.MasterReadRequestType:
			log.Println("Received MasterReadRequestType")
			// Process read request
			master.handleMasterReadRequest(conn, requestBodyBytes)
			
		case constants.MasterWriteRequestType:
			log.Println("Received MasterWriteRequestType")
			// Process write request
			master.handleMasterWriteRequest(conn, requestBodyBytes)
			
		case constants.MasterChunkServerHandshakeType:
			log.Println("Received MasterChunkServerHandshakeType")
			// Process handshake
			master.handleMasterHandshake(conn, requestBodyBytes)
		case constants.MasterChunkServerHeartbeatType:
			log.Println("Received MasterChunkServerHeartbeatType")
			// Process heartbeat
			master.handleMasterHeartbeat(conn, requestBodyBytes)
			
		default:
			log.Println("Received unknown request type:", requestType[0])
		}
	}
}