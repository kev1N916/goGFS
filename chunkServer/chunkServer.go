package chunkserver

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/involk-secure-1609/goGFS/common"
	"github.com/involk-secure-1609/goGFS/helper"
	lrucache "github.com/involk-secure-1609/goGFS/lruCache"
)

type CommitRequest struct {
	conn          net.Conn
	commitRequest common.PrimaryChunkCommitRequest
}

type CommitResponse struct {
	conn           net.Conn
	commitResponse common.PrimaryChunkCommitResponse
}
type ChunkServer struct {
	leaseUsage map[int64] time.Time
	commitRequestChannel chan CommitRequest
	lruCache             *lrucache.LRUBufferCache
	mu        sync.Mutex
	masterPort           string
	masterConnection     net.Conn
	port                 string
	chunkIds             []int64
	leaseGrants map[int64]*LeaseGrant
}

type LeaseGrant struct{
	chunkHandle int64 
	granted bool
	grantTime time.Time
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

/* ChunkServer->ChunkServer Request */
func (chunkServer *ChunkServer) writeCommitRequestToSingleServer(chunkServerPort string, requestBytes []byte) error {
	maxRetries := 5
	initialBackoff := 100 * time.Millisecond
	maxBackoff := 5 * time.Second

	for attempt := range maxRetries {
		// Try to send the data
		err := func() error {
			// Establish connection to the chunk server
			conn, err := net.Dial("tcp", chunkServerPort)
			if err != nil {
				return errors.New("failed to connect to chunk server")
			}
			defer conn.Close()
			_, err = conn.Write(requestBytes)
			if err != nil {
				return err
			}

			err = chunkServer.handleInterChunkCommitResponse(conn)
			if err != nil {
				return err
			}

			return nil
		}()

		// If successful, return nil
		if err == nil {
			return nil
		}

		// If this was the last attempt, return the error
		if attempt == maxRetries-1 {
			return fmt.Errorf("failed to write to chunk server after %d attempts: %w", maxRetries, err)
		}

		// Calculate backoff with exponential increase and jitter
		backoff := min(initialBackoff*time.Duration(1<<uint(attempt)), maxBackoff)

		// Add jitter (±25% of backoff)
		jitter := time.Duration(rand.Int64N(int64(backoff) / 2))
		if rand.IntN(2) == 0 {
			backoff -= jitter
		} else {
			backoff += jitter
		}

		log.Printf("Write to chunk server %s failed (attempt %d/%d): %v. Retrying in %v",
			chunkServerPort, attempt+1, maxRetries, err, backoff)

		time.Sleep(backoff)
	}

	// This should never be reached due to the return in the last iteration of the loop
	return fmt.Errorf("failed to write to chunk server after %d attempts", maxRetries)
}

/* ChunkServer->Client Response */
func (chunkServer *ChunkServer) writePrimaryChunkCommitResponse(response CommitResponse) error {

	responseBytes, err := helper.EncodeMessage(common.PrimaryChunkCommitResponseType, response.commitResponse)
	if err != nil {
		return err
	}
	_,err= response.conn.Write(responseBytes)
	if err!=nil{
		return err
	}
	return nil	
}

/* ChunkServer->ChunkServer Request */
func (chunkServer *ChunkServer) writeInterChunkServerCommitRequest(secondaryServers []string, req common.InterChunkServerCommitRequest) error {

	requestBytes, err := helper.EncodeMessage(common.InterChunkServerCommitRequestType, req)
	if err != nil {
		return err
	}

	for _, serverPort := range secondaryServers {
		err = chunkServer.writeCommitRequestToSingleServer(serverPort, requestBytes)
		if err != nil {
			return err
		}
	}

	return nil
}

/* ChunkServer->ChunkServer  */
func (chunkServer *ChunkServer) handleInterChunkCommitResponse(conn net.Conn) error {

	messageType, messageBytes, err := helper.ReadMessage(conn)
	if err != nil {
		return err
	}

	if messageType != common.InterChunkServerCommitResponseType {
		return err
	}

	response, err := helper.DecodeMessage[common.InterChunkServerCommitResponse](messageBytes)
	if err != nil {
		return err
	}

	if !response.Status {
		return errors.New("error on one of the chunk Servers")
	}
	return nil

}

/* ChunkServer->ChunkServer Request */
func (chunkServer *ChunkServer) handleInterChunkServerCommitRequest(conn net.Conn, requestBodyBytes []byte) error{

	response := common.InterChunkServerCommitResponse{
		Status: true,
	}
	request, err := helper.DecodeMessage[common.InterChunkServerCommitRequest](requestBodyBytes)
	if err != nil {
		response.Status=false
		responseBytes, _ := helper.EncodeMessage(common.InterChunkServerCommitResponseType, response)
		_, err = conn.Write(responseBytes)
		if err != nil {
			return err
		}
		return nil
	}

	chunkServer.mu.Lock()
	defer chunkServer.mu.Unlock()
	file, err := os.OpenFile(strconv.FormatInt(request.ChunkHandle, 10)+".chunk", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		response.Status=false
		responseBytes, _ := helper.EncodeMessage(common.InterChunkServerCommitResponseType, response)
		_, err = conn.Write(responseBytes)
		if err != nil {
			return err
		}
		return nil
	}
	chunkOffset:=request.ChunkOffset
	for _, mutationId := range request.MutationOrder {
		amountWritten, err := chunkServer.mutateChunk(file, mutationId, chunkOffset)
		if err != nil {
			response.Status=false
			responseBytes, _ := helper.EncodeMessage(common.InterChunkServerCommitResponseType, response)
			_, err = conn.Write(responseBytes)
			if err != nil {
				return err
			}
			return nil
		}
		chunkOffset+=amountWritten
	}

	requestBytes, _ := helper.EncodeMessage(common.InterChunkServerCommitResponseType, response)

	_, err = conn.Write(requestBytes)
	if err != nil {
		return err
	}
	return nil
}

func(chunkServer *ChunkServer) handleChunkExceedsMaxSize(requests []CommitRequest) error{
	response:=common.PrimaryChunkCommitResponse{
		Offset: -1,
		Status: true,
		ErrorMessage: "chunk full try again with new Chunk",
	}

	for _, value := range requests {		
		go chunkServer.writePrimaryChunkCommitResponse(CommitResponse{conn: value.conn,commitResponse: response})
	}

	return nil
}

/* Client->ChunkServer Request */
func (chunkServer *ChunkServer) handleChunkPrimaryCommit(chunkHandle int64, requests []CommitRequest) error {
	// response := common.PrimaryChunkCommitResponse{
	// 	Offset: 0,
	// 	Status: true,
	// }

	totalCommitSize:=0
	for _,commit:=range(requests){
		totalCommitSize+=commit.commitRequest.SizeOfData
	}
	secondaryServers := requests[0].commitRequest.SecondaryServers
	chunk, err := os.OpenFile(strconv.FormatInt(chunkHandle, 10)+".chunk", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}

	chunkServer.mu.Lock()
	defer chunkServer.mu.Unlock()
	chunkInfo, err := chunk.Stat()
	if err != nil {
		return err
	}

	chunkOffset := chunkInfo.Size()

	if(chunkOffset+int64(totalCommitSize)>common.ChunkSize){
		return chunkServer.handleChunkExceedsMaxSize(requests)
	}
	successfullWrites := make([]CommitResponse, 0)
	unsucessfullWrites := make([]CommitResponse, 0)


	mutationOrder := make([]int64, 0)
	
	for _, value := range requests {
		amountWritten, err := chunkServer.mutateChunk(chunk, value.commitRequest.MutationId, chunkOffset)
		if err != nil {
			// errorOnPrimary = true
			unsucessfullWrites = append(unsucessfullWrites, CommitResponse{
				conn: value.conn,
				commitResponse: common.PrimaryChunkCommitResponse{
					Offset: -1,
					Status: false,
					ErrorMessage: "failed to write data",
				},
			})
			continue
		}
		successfullWrites = append(successfullWrites, CommitResponse{
			conn: value.conn,
			commitResponse: common.PrimaryChunkCommitResponse{
				Offset: chunkOffset,
				Status: true,
				ErrorMessage: "data has successfully been written",
			},
		})
		mutationOrder = append(mutationOrder, value.commitRequest.MutationId)
		chunkOffset += amountWritten
	}

	chunkServer.sendLeaseExtensionRequest(chunkHandle)
	if len(mutationOrder) > 0 {
		interChunkServerCommitRequest := common.InterChunkServerCommitRequest{
			ChunkHandle:   chunkHandle,
			ChunkOffset:   chunkOffset,
			MutationOrder: mutationOrder,
		}

		err := chunkServer.writeInterChunkServerCommitRequest(secondaryServers, interChunkServerCommitRequest)
		if err != nil {
			for _, value := range successfullWrites {
				value.commitResponse.Status = false
				value.commitResponse.ErrorMessage="failed to write data"
				value.commitResponse.Offset=-1
			}
		}
	}

	for _, value := range successfullWrites {
		go chunkServer.writePrimaryChunkCommitResponse(value)
	}

	for _, value := range unsucessfullWrites {
		go chunkServer.writePrimaryChunkCommitResponse(value)
	}

	return nil

}


func (chunkServer *ChunkServer) sendLeaseExtensionRequest(chunkHandle int64){

	request:=common.MasterChunkServerLeaseRequest{
		ChunkHandle: chunkHandle,
	}
	requestBytes,_:=helper.EncodeMessage(common.MasterChunkServerLeaseRequestType,request)

	chunkServer.masterConnection.Write(requestBytes)

}
/* Client->ChunkServer */
func (chunkServer *ChunkServer) handleClientReadRequest(conn net.Conn, requestBodyBytes []byte) error {

	request, err := helper.DecodeMessage[common.ClientChunkServerReadRequest](requestBodyBytes)
	if err != nil {
		return err
	}
	err = chunkServer.writeClientReadResponse(conn, *request)
	if err != nil {
		return err
	}
	return nil
}
/* ChunkServer->Client */
func (chunkServer *ChunkServer) writeClientReadResponse(conn net.Conn, request common.ClientChunkServerReadRequest) error {
	var chunkPresent byte
	chunkPresent=1
	chunk, err := os.OpenFile(strconv.FormatInt(request.ChunkHandle, 10) + ".chunk",os.O_RDONLY,0600)
	if err != nil {
		chunkPresent=0
		return err
	}
	defer chunk.Close()

	fileInfo, err := chunk.Stat()
	if err != nil {
		return err
	}
	if (fileInfo.Size()==0){
		chunkPresent=0
	}

	_,err=conn.Write([]byte{chunkPresent})
	if err!=nil{
		return err
	}
	if(chunkPresent==0){
		return nil
	}

	sizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBuf, uint32(fileInfo.Size()))
	_, err = conn.Write(sizeBuf)
	if err != nil {
		return err
	}

	_, err = io.Copy(conn, chunk)
	if err != nil {
		return err
	}
	return nil
}

/* ChunkServer->Client */
func (chunkServer *ChunkServer) writeClientWriteResponse(conn net.Conn,writeResponse common.ClientChunkServerWriteResponse) error {

	responseBytes, err := helper.EncodeMessage(common.ClientChunkServerWriteResponseType, writeResponse)
	if err != nil {
		return err
	}

	_, err = conn.Write(responseBytes)
	if err != nil {
		return err
	}

	return nil
}

/* Client->ChunkServer */
func (chunkServer *ChunkServer) handleClientWriteRequest(conn net.Conn, requestBodyBytes []byte) error {

	req, err := helper.DecodeMessage[common.ClientChunkServerWriteRequest](requestBodyBytes)
	if err != nil {
		return err
	}

	
	// Read file size
	sizeBuf := make([]byte, 8)
	_, err = io.ReadFull(conn, sizeBuf)
	if err != nil {
		return err
	}

	chunkSize := binary.LittleEndian.Uint64(sizeBuf)
	log.Printf("Receiving file of size: %d bytes\n", chunkSize)

	chunkData := make([]byte, chunkSize)

	// Read file contents from connection into the byte array
	_, err = io.ReadFull(conn, chunkData)
	if err != nil {
		return err
	}
	writeResponse:=common.ClientChunkServerWriteResponse{
			Status: true,
			ErrorMessage: "",
		}
	err = chunkServer.writeChunkToCache(req.MutationId, chunkData)
	if err != nil {
		writeResponse.ErrorMessage="error while writing chunk on chunkServer"
		writeResponse.Status=false
	}

	err = chunkServer.writeClientWriteResponse(conn,writeResponse)
	if err != nil {
		return err
	}
	return nil
}

/* Master->ChunkServer */
func (chunkServer *ChunkServer) handleMasterHeartbeat( requestBodyBytes []byte) error {

	return nil
}

/*
The master grants a chunklease to one of the repli
cas, which we call the primary. The primary picks a serial
order for all mutations to the chunk. All replicas follow this
order when applying mutations. Thus, the global mutation
order is defined first by the lease grant order chosen by the
master, and within a lease by the serial numbers assigned
by the primary.
The lease mechanism is designed to minimize management overhead at the master. A lease has an initial timeout
of 60 seconds. However, as long as the chunk is being 
mutated, the primary can request and typically receive extensions from the master indefinitely.
These extension requests and grants are piggybacked on the HeartBeat messages regularly
exchanged between the master and all chunkservers.
The master may sometimes try to revoke a lease before it expires (e.g., when the master wants to disable mutations
on a file that is being renamed).
*/

func (chunkServer *ChunkServer) leaseRequestHandler(){
	leaseRequests:=make([]int64,0)
	for _,lease:=range(chunkServer.leaseGrants){
		if(time.Now().Unix()-chunkServer.leaseUsage[lease.chunkHandle].Unix()<60){
			leaseRequests=append(leaseRequests, lease.chunkHandle)
		}
	}

	heartbeatRequest:=common.MasterChunkServerHeartbeat{
		LeaseExtensionRequests: leaseRequests,
	}

	heartBeatBytes,_:=helper.EncodeMessage(common.MasterChunkServerHeartbeatType,heartbeatRequest)
	chunkServer.masterConnection.Write(heartBeatBytes)
}
 
func (chunkServer *ChunkServer) handleMasterHeartbeatResponse(messageBody []byte){
	heartBeatResponse,_:=helper.DecodeMessage[common.MasterChunkServerHeartbeatResponse](messageBody)
	chunkServer.mu.Lock()
	defer chunkServer.mu.Unlock()
	for _,lease:=range(heartBeatResponse.LeaseGrants){
		chunkServer.leaseGrants[lease].grantTime=time.Now()
	}
}
func (chunkServer *ChunkServer) handleMasterLeaseRequest(requestBodyBytes []byte) error {
	leaseRequest,err:=helper.DecodeMessage[common.MasterChunkServerLeaseRequest](requestBodyBytes)
	if err!=nil{
		return err
	}
	chunkServer.mu.Lock()
	defer chunkServer.mu.Unlock()
	chunkServer.leaseGrants[leaseRequest.ChunkHandle]=&LeaseGrant{
		chunkHandle:leaseRequest.ChunkHandle ,
		granted: true,
		grantTime: time.Now(),
	}
	return nil
}



/* ChunkServer->Master */
func (chunkServer *ChunkServer) handleMasterHandshakeResponse() error {
	messageType, _, err := helper.ReadMessage(chunkServer.masterConnection)
	if err != nil {
		log.Println(err)
		return err
	}

	if (common.MasterChunkServerHandshakeResponseType) != messageType {
		return err
	}
	return nil
}
/* ChunkServer->Master */
func (chunkServer *ChunkServer) initiateHandshake() error {
	handshakeBody := common.MasterChunkServerHandshake{
		ChunkIds: chunkServer.chunkIds,
	}

	handshakeBytes, err := helper.EncodeMessage(common.MasterChunkServerHandshakeType, handshakeBody)
	if err != nil {
		return err
	}
	_, err = chunkServer.masterConnection.Write(handshakeBytes)
	if err != nil {
		return err
	}

	err = chunkServer.handleMasterHandshakeResponse()
	if err != nil {
		return err
	}

	return nil
}

/* ChunkServer->Master */
func (chunkServer *ChunkServer) registerWithMaster() error {
	conn, err := net.Dial("tcp", chunkServer.masterPort)
	if err != nil {
		return err
	}
	chunkServer.masterConnection = conn

	err = chunkServer.initiateHandshake()
	if err != nil {
		return err
	}
	return nil
}

func (chunkServer *ChunkServer) handlePrimaryChunkCommitRequest(conn net.Conn, requestBodyBytes []byte)error {
	req, err := helper.DecodeMessage[common.PrimaryChunkCommitRequest](requestBodyBytes)
	if err != nil {
		return err
	}

	// isPrimary:=chunkServer.checkIfPrimary(req.ChunkHandle)
	// if !isPrimary{
	// 	response:=common.PrimaryChunkCommitResponse{
	// 		Offset: -1,
	// 		Status: false,
	// 	}

	// 	err=chunkServer.writePrimaryChunkCommitResponse(CommitResponse{conn: conn,commitResponse: response})
	// 	if err!=nil{
	// 		return  err
	// 	}
	// 	return nil
	// }
	commitRequest := CommitRequest{
		conn:          conn,
		commitRequest: *req,
	}

	// chunkServer.leaseUsage[req.ChunkHandle]=time.Now()

	chunkServer.commitRequestChannel <- commitRequest
	return nil
}
func (chunkServer *ChunkServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		messageType, messageBody, err := helper.ReadMessage(conn)
		if err != nil {
			log.Println(err)
			return
		}

		switch messageType {
		case common.PrimaryChunkCommitRequestType:
			err = helper.AddTimeoutForTheConnection(conn, 5*time.Second)
			if err != nil {
				return
			}
			err=chunkServer.handlePrimaryChunkCommitRequest(conn, messageBody)
			if err!=nil{
				return 
			}
		case common.InterChunkServerCommitRequestType:
			err =  helper.AddTimeoutForTheConnection(conn, 30*time.Second)
			if err != nil {
				return
			}
			err=chunkServer.handleInterChunkServerCommitRequest(conn, messageBody)
			if err != nil {
				return
			}
		case common.ClientChunkServerReadRequestType:
			err =  helper.AddTimeoutForTheConnection(conn, 5*time.Second)
			if err != nil {
				return
			}
			err = chunkServer.handleClientReadRequest(conn, messageBody)
			if err != nil {
				return
			}
		case common.ClientChunkServerWriteRequestType:
			err =  helper.AddTimeoutForTheConnection(conn, 30*time.Second)
			if err != nil {
				return
			}
			err = chunkServer.handleClientWriteRequest(conn, messageBody)
			if err != nil {
				return
			}
		case common.MasterChunkServerHeartbeatType:
			err=chunkServer.handleMasterHeartbeat( messageBody)
			if err!=nil{
				return
			}
		case common.MasterChunkServerHeartbeatResponseType:
			chunkServer.handleMasterHeartbeatResponse(messageBody)
			// if err!=nil{
			// 	return
			// }
		case common.MasterChunkServerLeaseRequestType:
			err=chunkServer.handleMasterLeaseRequest(messageBody)
			if err!=nil{
				return 
			}
		default:
			log.Println("Received unknown request type:", messageType)
		}
	}
}


func (chunkServer *ChunkServer) Start() {
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
