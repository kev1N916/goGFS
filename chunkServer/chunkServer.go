package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"log"
	"net"
	"os"

	"github.com/involk-secure-1609/goGFS/constants"
)

type ChunkServer struct {
	masterPort string
	masterConnection net.Conn
	port string
	chunkIds []int64
}

func NewChunkServer(port string) *ChunkServer {
	return &ChunkServer{
		port: port,
	}
}

func (chunkServer *ChunkServer) start(){
	// Start chunk server
	listener, err := net.Listen("tcp", ":8081")
	if err != nil {
		log.Panicf("Failed to start chunk server: %v", err)
	}
	defer listener.Close()

	log.Println("Chunk server listening on :8081")

	// Register with master server
	if err := chunkServer.registerWithMaster(); err != nil {
		log.Panicf("Failed to register with master server: %v", err)
	}

	go chunkServer.handleConnection(chunkServer.masterConnection)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		go chunkServer.handleConnection(conn)
	}
}

func (chunkServer *ChunkServer) initiateHandshake() error {
	handshakeBody:=constants.MasterChunkServerHandshake{
		ChunkIds:chunkServer.chunkIds,
	}
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	encoder.Encode(handshakeBody)

	lengthOfHandshake := len(buf.Bytes())
	handshakeBytes:=make([]byte,0)
	handshakeBytes=append(handshakeBytes,byte(constants.MasterChunkServerHandshakeType))
	handshakeBytes = binary.LittleEndian.AppendUint16(handshakeBytes, uint16(lengthOfHandshake))
	handshakeBytes = append(handshakeBytes, buf.Bytes()...)
	
	_,err:=chunkServer.masterConnection.Write(handshakeBytes)
	if err!=nil{
		return err;
	}
	return nil
}
func (chunkServer *ChunkServer) registerWithMaster() error {
	conn, err := net.Dial("tcp",chunkServer.masterPort)
	if err != nil {
		return err
	}
	chunkServer.masterConnection=conn
	defer chunkServer.masterConnection.Close()

	err=chunkServer.initiateHandshake()
	if err!=nil{
		return err
	}
	return nil
}


func (chunkServer *ChunkServer) loadChunks() {
	// Open the file
	file, err := os.Open("chunkIds.txt")
	if err != nil {
		log.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// Read numberOfChunks (4 bytes)
	var numberOfChunks uint32
	err = binary.Read(file, binary.LittleEndian, &numberOfChunks)
	if err != nil {
		log.Println("Error reading numberOfChunks:", err)
		return
	}

	log.Println("Number of chunks:", numberOfChunks)

	// Read chunk IDs (each 64-bit = 8 bytes)
	chunkIds := make([]int64, numberOfChunks)
	for i := uint32(0); i < numberOfChunks; i++ {
		err = binary.Read(file, binary.LittleEndian, &chunkIds[i])
		if err != nil {
			log.Println("Error reading chunk ID:", err)
			return
		}
	}
	chunkServer.chunkIds=chunkIds

	// Print loaded chunk IDs
	log.Println("Loaded chunk IDs:", chunkIds)
}

func (chunkServer *ChunkServer) handleClientReadRequest(){

}

func (chunkServer *ChunkServer) handleClientWriteRequest(){

}

func (chunkServer *ChunkServer) handleMasterHandshakeResponse(){

}

func (chunkServer *ChunkServer) handleMasterHeartbeatResponse(){

}


func (chunkServer *ChunkServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		// Read the request type (1 byte)
		messageType := make([]byte, 1)
		n, err := conn.Read(messageType)
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
		messageLength := make([]byte, 2)
		_, err = io.ReadFull(conn, messageLength)
		if err != nil {
			log.Println("Error reading request length:", err)
			return
		}
		
		// Get the length as uint16
		length := binary.LittleEndian.Uint16(messageLength)
		
		// Read the request body
		requestBodyBytes := make([]byte, length)
		_, err = io.ReadFull(conn, requestBodyBytes)
		if err != nil {
			log.Println("Error reading request body:", err)
			return
		}
		
	
		switch constants.MessageType(messageType[0]) {
		case constants.ClientChunkServerReadRequestType:
			chunkServer.handleClientReadRequest()
		case constants.ClientChunkServerWriteRequestType:
			chunkServer.handleClientWriteRequest()
		case constants.MasterChunkServerHandshakeResponseType:
			chunkServer.handleMasterHandshakeResponse()
		case constants.MasterChunkServerHeartbeatResponseType:
			chunkServer.handleMasterHeartbeatResponse()
		default:
			log.Println("Received unknown request type:", messageType[0])
		}
	}
}
