package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/involk-secure-1609/goGFS/constants"
	lrucache "github.com/involk-secure-1609/goGFS/lruCache"
)

type CommitRequest struct {
	conn          net.Conn
	commitRequest constants.PrimaryChunkCommitRequest
}
type ChunkServer struct {
	commitRequestChannel chan CommitRequest
	lruCache             *lrucache.LRUBufferCache
	chunkServerMu        sync.Mutex
	masterPort           string
	masterConnection     net.Conn
	port                 string
	chunkIds             []int64
}

func NewChunkServer(port string) *ChunkServer {

	chunkServer := &ChunkServer{
		commitRequestChannel: make(chan CommitRequest),
		lruCache:             lrucache.NewLRUBufferCache(100),
		port:                 port,
	}

	go chunkServer.startCommitRequestHandler()
	return chunkServer
}

func (chunkServer *ChunkServer) startCommitRequestHandler() {
	const batchDuration = 3 * time.Second
	const maxBatchSize = 100

	for {
		// Use a slice to accumulate the commit requests
		pendingCommits := make([]CommitRequest, 0, maxBatchSize)

		// Set up a timer for batching
		timer := time.NewTimer(batchDuration)

		// Accumulate commit requests until either:
		// 1. The batch duration expires
		// 2. We hit the max batch size
		batchComplete := false

		for !batchComplete && len(pendingCommits) < maxBatchSize {
			select {
			case req, ok := <-chunkServer.commitRequestChannel:
				if !ok {
					// Channel was closed, exit the goroutine
					return
				}
				pendingCommits = append(pendingCommits, req)

			case <-timer.C:
				// Timer expired, process the batch
				batchComplete = true
			}
		}

		// If timer hasn't fired yet, stop it to avoid leaks
		if !batchComplete {
			timer.Stop()
		}

		// Skip processing if no requests were accumulated
		if len(pendingCommits) == 0 {
			continue
		}

		// Process the batch of commit requests
		chunkServer.processCommitBatch(pendingCommits)
	}
}

// processCommitBatch handles a batch of commit requests
func (chunkServer *ChunkServer) processCommitBatch(requests []CommitRequest) {
	chunkServer.chunkServerMu.Lock()

	log.Printf("Processing batch of %d commit requests", len(requests))

	// Group requests by chunk ID for more efficient processing
	chunkBatches := make(map[int64][]CommitRequest)
	for _, req := range requests {
		chunkBatches[req.commitRequest.ChunkHandle] = append(chunkBatches[req.commitRequest.ChunkHandle], req)
	}
	chunkServer.chunkServerMu.Unlock()

	for key,value:=range(chunkBatches){
		go chunkServer.handleChunkCommit(key,value)
	}
}


func (chunkServer *ChunkServer) handleChunkCommit(chunkHandle int64,requests []CommitRequest) error{
	
	file, err := os.OpenFile(strconv.FormatInt(chunkHandle, 10) + ".chunk",os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}

	chunkServer.chunkServerMu.Lock()
	defer chunkServer.chunkServerMu.Unlock()
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	errorOnPrimary:=false
	for _,value:=range(requests){
		err=chunkServer.mutateChunk(file,value.commitRequest.MutationId)
		if err!=nil{
			errorOnPrimary=true
			break
		}
	}
	if errorOnPrimary{

	}

	mutationOrder:=make([]int64,0)
	for _,value:=range(requests){
		mutationOrder = append(mutationOrder, value.commitRequest.MutationId)
	}

	interChunkServerCommitRequest:=constants.InterChunkServerCommitRequest{
		ChunkHandle: chunkHandle,
		ChunkOffset: offset,
		MutationOrder: mutationOrder,
	}
	errorOnSecondary:=false

	for _,value:=range(requests){
		err:=chunkServer.writeInterChunkServerCommitRequest(value.conn,interChunkServerCommitRequest)
		if err!=nil{
			errorOnSecondary=true
		}
	} 

	if errorOnSecondary{

	}

	return nil


}


func (chunkServer *ChunkServer) start() {
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
	handshakeBody := constants.MasterChunkServerHandshake{
		ChunkIds: chunkServer.chunkIds,
	}
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	encoder.Encode(handshakeBody)

	lengthOfHandshake := len(buf.Bytes())
	handshakeBytes := make([]byte, 0)
	handshakeBytes = append(handshakeBytes, byte(constants.MasterChunkServerHandshakeType))
	handshakeBytes = binary.LittleEndian.AppendUint16(handshakeBytes, uint16(lengthOfHandshake))
	handshakeBytes = append(handshakeBytes, buf.Bytes()...)

	_, err := chunkServer.masterConnection.Write(handshakeBytes)
	if err != nil {
		return err
	}

	err = chunkServer.handleMasterHandshakeResponse()
	if err != nil {
		return err
	}

	return nil
}
func (chunkServer *ChunkServer) registerWithMaster() error {
	conn, err := net.Dial("tcp", chunkServer.masterPort)
	if err != nil {
		return err
	}
	chunkServer.masterConnection = conn
	defer chunkServer.masterConnection.Close()

	err = chunkServer.initiateHandshake()
	if err != nil {
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
	chunkServer.chunkIds = chunkIds

	// Print loaded chunk IDs
	log.Println("Loaded chunk IDs:", chunkIds)
}

func (chunkServer *ChunkServer) handleClientReadRequest(conn net.Conn, requestBodyBytes []byte) {
	var request constants.ClientChunkServerReadRequest
	requestReader := bytes.NewReader(requestBodyBytes)
	decoder := gob.NewDecoder(requestReader)
	err := decoder.Decode(&request)
	if err != nil {
		log.Println("Encoding failed:", err)
		return
	}

	chunkServer.writeClientReadResponse(conn, request)

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

func (chunkServer *ChunkServer) writeClientReadResponse(conn net.Conn, request constants.ClientChunkServerReadRequest) {

	file, err := os.Open(strconv.FormatInt(request.ChunkHandle, 10) + ".chunk")
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
	if err != nil {
		return
	}

}

func (chunkServer *ChunkServer) writeChunkToCache(req constants.ClientChunkServerWriteRequest) error {
	chunkServer.lruCache.Put(req.MutationId, req.Data)
	return nil
}

func (chunkServer *ChunkServer) mutateChunk(file *os.File,mutationId int64) (error) {

	data, present := chunkServer.lruCache.Get(mutationId)
	if !present {
		return errors.New("data not present in lru cache")
	}

	_, err := io.Copy(file,bytes.NewReader(data))
	if err != nil {
		return err
	}
	return nil

}
// func (chunkServer *ChunkServer) handleClientCommitRequest(conn net.Conn, requestBodyBytes []byte) {
// 	var req constants.PrimaryChunkCommitRequest
// 	decoder := gob.NewDecoder(bytes.NewReader(requestBodyBytes))
// 	err := decoder.Decode(&req)
// 	if err != nil {
// 		return
// 	}

// 	offset, err := chunkServer.mutateChunk(req)
// 	if err != nil {
// 		return
// 	}

// 	interChunkServerCommitRequest := constants.InterChunkServerCommitRequest{
// 		ChunkHandle:   req.ChunkHandle,
// 		MutationOrder: make([]int64, 0),
// 		ChunkOffset:   offset,
// 	}

// 	for i := range req.SecondaryServers {

// 	}

// }

func (chunkServer *ChunkServer) writeInterChunkServerCommitRequest(conn net.Conn,req constants.InterChunkServerCommitRequest) error {

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(req)
	if err != nil {
		return err
	}

	lengthOfRequest := len(buf.Bytes())
	requestBytes := make([]byte, 0)
	requestBytes = append(requestBytes, byte(constants.InterChunkServerCommitRequestType))
	requestBytes = binary.LittleEndian.AppendUint16(requestBytes, uint16(lengthOfRequest))
	requestBytes = append(requestBytes, buf.Bytes()...)	

	_,err=conn.Write(requestBytes)
	if err!=nil{
		return err
	}

	err=chunkServer.handleInterChunkCommitResponse(conn)
	if err!=nil{
		return err
	}
	return nil
}

func (chunkServer *ChunkServer) handleInterChunkCommitResponse(conn net.Conn) error{

	messageType,messageBytes,err:=readMessage(conn)
	if err!=nil{
		return err
	}

	if messageType!=constants.InterChunkServerCommitResponseType{
		return err
	}

	var response constants.InterChunkServerCommitResponse
	responseReader := bytes.NewReader(messageBytes)
	decoder := gob.NewDecoder(responseReader)
	err = decoder.Decode(&response)
	if err!=nil{
		return errors.New("error on one of the chunk Servers")
	}
	if !response.Status{
		return errors.New("error on one of the chunk Servers")
	}
	return nil

}

func (chunkServer *ChunkServer) handleInterChunkServerCommitRequest(chunkServers []string) {


}

func (chunkServer *ChunkServer) writeClientWriteResponse(conn net.Conn) error {

	writeResponse := constants.ClientChunkServerWriteResponse{
		Status: true,
	}

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	encoder.Encode(writeResponse)

	lengthOfResponse := len(buf.Bytes())
	responseBytes := make([]byte, 0)
	responseBytes = append(responseBytes, byte(constants.ClientChunkServerWriteResponseType))
	responseBytes = binary.LittleEndian.AppendUint16(responseBytes, uint16(lengthOfResponse))
	responseBytes = append(responseBytes, buf.Bytes()...)

	_, err := conn.Write(responseBytes)
	if err != nil {
		return err
	}

	return nil
}
func (chunkServer *ChunkServer) handleClientWriteRequest(conn net.Conn, requestBodyBytes []byte) {
	var req constants.ClientChunkServerWriteRequest
	decoder := gob.NewDecoder(bytes.NewReader(requestBodyBytes))
	err := decoder.Decode(&req)
	if err != nil {
		return
	}

	err = chunkServer.writeChunkToCache(req)
	if err != nil {
		return
	}

	err = chunkServer.writeClientWriteResponse(conn)
	if err != nil {
		return
	}
}

func (chunkServer *ChunkServer) handleMasterHandshakeResponse() error {
	messageType, _, err := readMessage(chunkServer.masterConnection)
	if err != nil {
		log.Println(err)
		return err
	}

	if (constants.MasterChunkServerHandshakeResponseType) != messageType {
		return err
	}
	return nil
}

func (chunkServer *ChunkServer) handleMasterHeartbeatResponse(conn net.Conn, requestBodyBytes []byte) {

}

func readMessage(conn net.Conn) (constants.MessageType, []byte, error) {
	messageType := make([]byte, 1)
	n, err := conn.Read(messageType)
	if err != nil {
		if err == io.EOF {
			log.Println("Connection closed by client during message type read")
			return constants.MessageType(0), nil, err
		}
		log.Printf("Error reading request type: %v", err)
		return constants.MessageType(0), nil, ErrReadMessageType
	}
	if n == 0 {
		log.Println("No data read for message type, connection might be closing") // Log this case
		return constants.MessageType(0), nil, io.ErrUnexpectedEOF                 // Or return a more appropriate error, maybe retry logic in caller?
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
		messageType, messageBody, err := readMessage(conn)
		if err != nil {
			log.Println(err)
			return
		}

		switch messageType {
		case constants.PrimaryChunkCommitRequestType:
			// chunkServer.
		case constants.ClientChunkServerReadRequestType:
			chunkServer.handleClientReadRequest(conn, messageBody)
		case constants.ClientChunkServerWriteRequestType:
			chunkServer.handleClientWriteRequest(conn, messageBody)
		case constants.MasterChunkServerHeartbeatResponseType:
			chunkServer.handleMasterHeartbeatResponse(conn, messageBody)
		default:
			log.Println("Received unknown request type:", messageType)
		}
	}
}
