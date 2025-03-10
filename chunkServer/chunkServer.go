package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"

	"github.com/involk-secure-1609/goGFS/constants"
)

type ChunkServer struct {
	chunkServerMu sync.Mutex
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

	err=chunkServer.handleMasterHandshakeResponse()
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

func (chunkServer *ChunkServer) handleClientReadRequest(conn net.Conn,requestBodyBytes []byte){
	var request constants.ClientChunkServerReadRequest
	requestReader := bytes.NewReader(requestBodyBytes)
	decoder := gob.NewDecoder(requestReader)
	err := decoder.Decode(&request)
	if err != nil {
		log.Println("Encoding failed:", err)
		return
	}

	chunkServer.writeClientReadResponse(conn,request)

}

// func (chunkServer *ChunkServer) readChunk(request constants.ClientChunkServerReadRequest) ([]byte,error){

// 	file, err := os.Open(strconv.FormatInt(request.ChunkHandle, 10)+".chunk")
// 	if err != nil {
// 		return nil,err
// 	}
// 	defer file.Close()
// 	 // Get file stat
// 	 fileInfo, _ := file.Stat()

// 	 // Send the file size
// 	 sizeBuf := make([]byte, 8)
// 	 binary.LittleEndian.PutUint64(sizeBuf, uint64(fileInfo.Size()))
// 	 _, err = conn.Write(sizeBuf)
// 	 if err != nil {
// 		 return err
// 	 }
 
// 	 // Send the file contents
// 	 _, err = io.Copy(conn, file)
// 	 return err

// }
 
func (chunkServer *ChunkServer) writeClientReadResponse(conn net.Conn,request constants.ClientChunkServerReadRequest){

	file, err := os.Open(strconv.FormatInt(request.ChunkHandle, 10)+".chunk")
	if err != nil {
		return 
	}
	defer file.Close()
	 // Get file stat
	 fileInfo, _ := file.Stat()

	 // Send the file size
	 sizeBuf := make([]byte, 8)
	 binary.LittleEndian.PutUint64(sizeBuf, uint64(fileInfo.Size()))
	 _, err = conn.Write(sizeBuf)
	 if err != nil {
		 return 
	 }
 
	 // Send the file contents
	 _, err = io.Copy(conn, file)
	 if err!=nil{
		return
	 }

}

func (chunkServer *ChunkServer) handleClientWriteRequest(conn net.Conn,requestBodyBytes []byte){

}

func (chunkServer *ChunkServer) handleMasterHandshakeResponse()error{
	messageType,_,err:=readMessage(chunkServer.masterConnection)
	if err!=nil{
		log.Println(err)
		return err
	}	
	
	if (constants.MasterChunkServerHandshakeResponseType) != messageType {
	return err
}
return nil
}

func (chunkServer *ChunkServer) handleMasterHeartbeatResponse(conn net.Conn,requestBodyBytes []byte){

}

func readMessage(conn net.Conn) (constants.MessageType,[]byte,error){
	messageType := make([]byte, 1)
	n, err := conn.Read(messageType)
	if err != nil {
		if err==io.EOF { 
			log.Println("Connection closed by client during message type read") 
			return constants.MessageType(0), nil, err
		}
		log.Printf("Error reading request type: %v", err) 
		return constants.MessageType(0), nil, ErrReadMessageType     
	}
	if n == 0 {
		log.Println("No data read for message type, connection might be closing") // Log this case
		return constants.MessageType(0), nil, io.ErrUnexpectedEOF // Or return a more appropriate error, maybe retry logic in caller?
	}

	// Read the request length (2 bytes)
	messageLength := make([]byte, 2)
	_, err = io.ReadFull(conn, messageLength)
	if err != nil {
		log.Printf("Error reading request length: %v", err)
		return constants.MessageType(0), nil, ErrReadMessageLength
	}

	// Get the length as uint16
	length := binary.LittleEndian.Uint16(messageLength)

	// Read the request body
	requestBodyBytes := make([]byte, length)
	_, err = io.ReadFull(conn, requestBodyBytes)
	if err != nil {
		log.Printf("Error reading request body (length %d): %v", length, err)
		return constants.MessageType(0), nil, ErrReadMessageBody
	}

	return constants.MessageType(messageType[0]), requestBodyBytes, nil
}

func (chunkServer *ChunkServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		messageType,messageBody,err:=readMessage(conn)
		if err!=nil{
			log.Println(err)
			return
		}	

		switch messageType {
		case constants.ClientChunkServerReadRequestType:
			chunkServer.handleClientReadRequest(conn,messageBody)
		case constants.ClientChunkServerWriteRequestType:
			chunkServer.handleClientWriteRequest(conn,messageBody)
		case constants.MasterChunkServerHeartbeatResponseType:
			chunkServer.handleMasterHeartbeatResponse(conn,messageBody)
		default:
			log.Println("Received unknown request type:", messageType)
		}
	}
}
