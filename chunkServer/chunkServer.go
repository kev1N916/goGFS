package chunkserver

import (
	"errors"
	"hash/crc32"
	"log"
	"math/rand/v2"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/involk-secure-1609/goGFS/common"
	"github.com/involk-secure-1609/goGFS/helper"
	lrucache "github.com/involk-secure-1609/goGFS/lruCache"
)

type CommitRequest struct {
	conn          net.Conn
	commitRequest *common.PrimaryChunkCommitRequest
}

type CommitResponse struct {
	conn           net.Conn
	commitResponse *common.PrimaryChunkCommitResponse
}

type LeaseGrant struct {
	ChunkHandle int64
	granted     bool
	GrantTime   time.Time
}

type InterChunkServerResponseHandler struct {
	responseChannel chan int
	wg              sync.WaitGroup
}

type ChunkCheckSum struct {
	crcTable *crc32.Table
}

//	type Chunk struct {
//		mu            *sync.Mutex
//		versionNumber atomic.Int64
//	}
type ChunkServer struct {
	CheckSums                  map[int64][]byte
	checkSummer                *ChunkCheckSum
	shutDownChan               chan struct{}
	InTestMode                 bool
	HeartbeatRequestsReceived  atomic.Int32
	HeartbeatResponsesReceived atomic.Int32
	logger                     *common.DefaultLog
	listener                   net.Listener
	ChunkDirectory             string
	chunkManager               map[int64]*sync.Mutex
	commitRequestChannel       chan CommitRequest
	LruCache                   *lrucache.LRUBufferCache
	mu                         sync.RWMutex
	MasterPort                 string
	MasterConnection           net.Conn
	address                    *net.TCPAddr
	ChunkHandles               []int64
	ChunkVersionNumbers        map[int64]int64
	LeaseGrants                map[int64]*LeaseGrant
}

// tested
func NewChunkServer(chunkDirectory string, masterPort string) *ChunkServer {

	checkSummer := &ChunkCheckSum{
		crcTable: crc32.MakeTable(crc32.Castagnoli),
	}
	logger := common.DefaultLogger(common.ERROR)
	chunkServer := &ChunkServer{
		ChunkVersionNumbers:  make(map[int64]int64),
		CheckSums:            make(map[int64][]byte),
		checkSummer:          checkSummer,
		logger:               logger,
		shutDownChan:         make(chan struct{}, 1),
		MasterPort:           masterPort,
		ChunkHandles:         make([]int64, 0),
		LeaseGrants:          make(map[int64]*LeaseGrant),
		mu:                   sync.RWMutex{},
		ChunkDirectory:       chunkDirectory,
		chunkManager:         make(map[int64]*sync.Mutex),
		commitRequestChannel: make(chan CommitRequest),
		LruCache:             lrucache.NewLRUBufferCache(1000),
	}

	return chunkServer
}

/*
Stale Replica Detection

 After a sequence of successful mutations, the mutated file
 region is guaranteed to be defined and contain the data writ
ten by the last mutation. GFS achieves this by (a) applying
 mutations to a chunkin the same order on all its replicas
 (Section 3.1), and (b) using chunkversion numbers to detect
 any replica that has become stale because it has missed mu
tations while its chunkserver was down (Section 4.5). Stale
 replicas will never be involved in a mutation or given to
 clients asking the master for chunk locations. They are
 garbage collected at the earliest opportunity.


Chunk replicas may become stale if a chunkserver fails and misses mutations to the chunk while it is down. For
each chunk, the master maintains a chunk version number to distinguish between up-to-date and stale replicas.
Whenever the master grants a new lease on a chunk, it increases the chunkversion number and informs the up-to
date replicas.
The master and these replicas all record the new version number in their persistent state.
This occurs before any client is notified and therefore before it can start writing to the chunk.
If another replica is currently unavailable, its chunkversion number will not be advanced.
The master will detect that this chunkserver has a stale replica when the chunkserver restarts
and reports its set of chunks and their associated version numbers. If the master sees a
version number greater than the one in its records, the master assumes that it failed when
granting the lease and so takes the higher version to be up-to-date.
The master removes stale replicas in its regular garbage collection. Before that,
it effectively considers a stale replica not to exist at all when it replies to client requests for chunk information.
As another safeguard, the master includes the chunkversion number when it informs clients which chunkserver
holds a lease on a chunk or when it instructs a chunkserver to read the chunk from another chunkserver
in a cloning operation.
The client or the chunkserver verifies the version number when it performs the operation so that
it is always accessing up-to-date data.

master ->gets a write request -> before granting a new lease->does a increase chunk version number operation
->writes the op to log-> notifies the chosen chunk servers with up to date replicas of the new version number->
replies to the client with the primary chunk server and secondary chunk servers
*/

/* ChunkServer->ChunkServer Request */
// Responsible for sending the commit request to a chunkServer. Uses exponential backoff with jitter.
func (chunkServer *ChunkServer) writeCommitRequestToSingleServer(responseHandler *InterChunkServerResponseHandler, chunkServerPort string, requestBytes []byte) {
	defer responseHandler.wg.Done()
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
			// if the write is successfull we send a 1 into the channel
			responseHandler.responseChannel <- 1
			return
		}

		// If this was the last attempt, return the error
		if attempt == maxRetries-1 {
			responseHandler.responseChannel <- 0
			return
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

	// if the write is unsuccessfull we send a 0 into the channel
	responseHandler.responseChannel <- 0
}

/* ChunkServer->Client Response */
// Writes response back to the client after it receives a Primary Commit request
func (chunkServer *ChunkServer) writePrimaryChunkCommitResponse(response CommitResponse) error {

	responseBytes, err := helper.EncodeMessage(common.PrimaryChunkCommitResponseType, response.commitResponse)
	if err != nil {
		return err
	}
	_, err = response.conn.Write(responseBytes)
	if err != nil {
		return err
	}
	return nil
}

/* ChunkServer->ChunkServer Request */
// This function is responsible for sending inter chunk server commit requests which are exchanged between
// the primary chunkServer and the secondary chunkServers after the primary ChunkServer has finished applying the
// mutation on its own chunk.
func (chunkServer *ChunkServer) writeInterChunkServerCommitRequest(secondaryServers []string, req common.InterChunkServerCommitRequest) error {

	requestBytes, err := helper.EncodeMessage(common.InterChunkServerCommitRequestType, req)
	if err != nil {
		return err
	}

	// intitialize the responseHandler so that we can send concurrent requests
	responseHandler := InterChunkServerResponseHandler{}
	responseHandler.responseChannel = make(chan int, len(secondaryServers))
	responseHandler.wg.Add(len(secondaryServers))

	// send the interchunkCommitRequests concurrently
	for _, serverPort := range secondaryServers {
		go chunkServer.writeCommitRequestToSingleServer(&responseHandler, serverPort, requestBytes)
	}

	// wait for the requests to complete
	responseHandler.wg.Wait()
	close(responseHandler.responseChannel)

	// if any of the requests fail the response channel will contain a 0
	for i := range responseHandler.responseChannel {
		if i == 0 {
			return errors.New("inter chunk server commit request failed")
		}
	}

	return nil
}

/* ChunkServer->ChunkServer  */
// This function is called after another chunkServer has received an InterChunkServerCommitRequest
// and replies to it. The mutation on the chunkServer could fail for a large number of reasons(the write on the
// chunk could fail etc), and so if any error occurs we return the error.
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
		return errors.New(response.ErrorMessage)
	}
	return nil

}

/* ChunkServer->ChunkServer Request */
// This function is triggered when the the chunkServer receives an InterChunkCommitRequest message.
// The message would contain a list of mutationIds which the chunkServer has to apply on the chunk specified in
// the request. The request would also specify the offset where the mutations have to start from.
// If errors happen anywhere we will send a response back to the primary chunkServer which indicates
// the write has failed.
func (chunkServer *ChunkServer) handleInterChunkServerCommitRequest(conn net.Conn, requestBodyBytes []byte) error {

	response := common.InterChunkServerCommitResponse{
		Status:       true,
		ErrorMessage: "",
	}

	errorFunc := func() error {
		response.Status = false
		response.ErrorMessage = "error on secondary chunkServers, please try again"
		responseBytes, _ := helper.EncodeMessage(common.InterChunkServerCommitResponseType, response)
		_, err := conn.Write(responseBytes) // write the response back to the primary
		if err != nil {
			return err
		}
		return nil
	}

	// if there is any error during the ecoding we returna an error to the primary saying
	// that the write has failed.
	request, err := helper.DecodeMessage[common.InterChunkServerCommitRequest](requestBodyBytes)
	if err != nil {
	}

	chunk, err := chunkServer.openChunk(request.ChunkHandle)
	if err != nil {
		// if there is an error in opening the file then we again return an error to the primary
		return errorFunc()
	}

	chunkBuffer, isVerified, err := chunkServer.verifyChunkCheckSum(chunk, request.ChunkHandle)
	if err != nil || !isVerified {
		return errorFunc()
	}

	lastChunkBuffer := chunkServer.getLastPartOfChunk(chunkBuffer)

	_, present := chunkServer.getCheckSum(request.ChunkHandle)
	if !present {
		return errorFunc()
	}

	// checkSumBuffer := bytes.NewBuffer(checkSumData)
	// Get the offset from which we have to write to the chunk
	// On an atomic append all the primary and secondary chunk servers have to append at the same offset
	chunkOffset := request.ChunkOffset
	for _, mutationId := range request.MutationOrder {
		// apply the mutations in the same sequence the primary did
		// if there is any error anywhere then we will return an error to the primary
		amountWritten, err := chunkServer.mutateChunk(
			chunk, request.ChunkHandle,
			mutationId,
			chunkOffset,
			lastChunkBuffer)
		if err != nil {
			return errorFunc()
		}
		chunkOffset += amountWritten
	}

	chunk.Close()

	// send the successfull response back to the primary
	requestBytes, _ := helper.EncodeMessage(common.InterChunkServerCommitResponseType, response)

	_, err = conn.Write(requestBytes)
	if err != nil {
		return err
	}
	return nil
}

// Client->ChunkServer Request
// The primary first applies the mutations on its own chunk and then
// forwards the write request to all secondary replicas. Each secondary replica applies
// mutations in the same serial number order assigned by the primary.
// The secondaries all reply to the primary indicating that they have completed the operation.
// A file region is <consistent> if all clients will
// always see the same data, regardless of which replicas they
// read from. A region is <defined> after a file data mutation if it
// is consistent and clients will see what the mutation writes in
// its entirety. When a mutation succeeds without interference
// from concurrent writers, the affected region is defined (and
// by implication consistent): all clients will always see what
// the mutation has written. Concurrent successful mutations
// leave the region undefined but consistent: all clients see the
// same data, but it may not reflect what any one mutation
// has written.
func (chunkServer *ChunkServer) handleChunkPrimaryCommit(chunkHandle int64, requests []CommitRequest) {

	secondaryServers := requests[0].commitRequest.SecondaryServers

	log.Println("HANDLING THE PRIMARY COMMIT FOR ", chunkHandle)

	chunkMutex, exists := chunkServer.getChunkMutex(chunkHandle)

	if !exists {
		chunkMutex = chunkServer.setChunkMutex(chunkHandle)
	}

	chunkMutex.Lock()
	defer chunkMutex.Unlock()

	if !chunkServer.checkIfPrimary(chunkHandle) {

		log.Println(chunkServer.address.String() + " is not primary, so connection is going to time out")
		// i can just return in this case because the connection is going to close anyways
		// after 20 seconds
		return
	}

	chunk, err := chunkServer.openChunk(chunkHandle)
	if err != nil {

		log.Println("error while opening the chunk")
		// if there is an error in opening the chunk then we again return an error to the primary
		return
	}

	// chunkInfo, err := chunk.Stat()
	// if err != nil {
	// 	return
	// }

	// chunkOffset := chunkInfo.Size()

	// returns a buffer containing the entire chunk data
	chunkBuffer, isVerified, err := chunkServer.verifyChunkCheckSum(chunk, chunkHandle)
	if err != nil || !isVerified {
		log.Println("error while verifying the chunk checksum")
		return
	}

	// this will be the offset which we will send to all the secondary chunkServers
	// they have talked about the atomic record append in the paper
	// In a record append, however, the client specifies only the data. GFS
	// appends it to the file at least once atomically (i.e., as one continuous sequence of bytes)
	// at an offset of GFS’s choosing and returns that offset to the client.
	// We need to keep track of the offset which is present at the beginning of the mutations
	// and return it to the client.
	initialChunkOffset := int64(chunkBuffer.Len())

	chunkOffset := (initialChunkOffset)

	_, present := chunkServer.getCheckSum(chunkHandle)
	if !present {
		return
	}

	// checkSumBuffer := bytes.NewBuffer(checkSumData)

	// to keep track of which writes succeeded/failed on the primary itself
	successfullPrimaryWrites := make([]CommitResponse, 0)
	unsucessfullPrimaryWrites := make([]CommitResponse, 0)

	mutationOrder := make([]int64, 0)

	lastPartOfChunk := chunkServer.getLastPartOfChunk(chunkBuffer)

	// apply the mutations on the primary chunkServer
	for _, value := range requests {

		// we return back the amount of data written so that we can increase our offset
		// for the subsequent requests
		amountWritten, err := chunkServer.mutateChunk(
			chunk, chunkHandle,
			value.commitRequest.MutationId,
			chunkOffset,
			lastPartOfChunk)
		if err != nil {
			// either an error occurs because the write has actually failed
			// or an error occurs due to our check on if the append is going to exceed the max
			// chunk size, either way we add it to the list of unsuccessfull requests
			response := CommitResponse{
				conn: value.conn,
				commitResponse: &common.PrimaryChunkCommitResponse{
					Offset:       -1,
					Status:       false,
					ErrorMessage: "failed to write data on primary chunk server",
				},
			}
			if err == common.ErrChunkFull {
				log.Println("CHUNK SERVER HAS GOT ", err)
				response.commitResponse.ErrorMessage = common.ErrChunkFull.Error()
			}
			unsucessfullPrimaryWrites = append(unsucessfullPrimaryWrites, response)
			continue
		}

		// if the mutation is successfull on the primary then we append it to the successfull
		// writes list
		successfullPrimaryWrites = append(successfullPrimaryWrites, CommitResponse{
			conn: value.conn,
			commitResponse: &common.PrimaryChunkCommitResponse{
				Offset:       chunkOffset,
				Status:       true,
				ErrorMessage: "data has successfully been written",
			},
		})
		// we also append the mutationId as we need to send all the successfull mutations to our
		// secondary servers
		mutationOrder = append(mutationOrder, value.commitRequest.MutationId)
		chunkOffset += amountWritten
	}
	chunk.Close()

	// we send a leaseExtension request to the master server, this is because the lease may expire
	// during the mutation ie. when we are sending the interChunkServerCommitRequests, and i guess this
	// tries to prevent it from happening.
	chunkServer.sendLeaseExtensionRequest(chunkHandle)
	if len(mutationOrder) > 0 {
		interChunkServerCommitRequest := common.InterChunkServerCommitRequest{
			ChunkHandle:   chunkHandle,
			ChunkOffset:   initialChunkOffset,
			MutationOrder: mutationOrder,
		}
		/* The primary replies to the client. Any errors encountered at any of the replicas are reported
		to the client. In case of errors, the write may have succeeded at the
		primary and an arbitrary subset of the secondary replicas. (If it had failed at the primary, it would not
		have been assigned a serial number and forwarded).The client request is considered to have failed, and the
		modified region is left in an inconsistent state. Our client code handles such errors by retrying the failed
		mutation. It will make a few attempts at steps (3) through (7) before falling back
		to a retry from the beginning of the write

		Basically if our interChunkCommitRequests fail at any of the chunkServers we will have to start from the
		beginning of the step where we push our data to all the chunkServers. So if we fail on any chunkServer
		we return an error to all our active clients.
		*/
		err := chunkServer.writeInterChunkServerCommitRequest(secondaryServers, interChunkServerCommitRequest)
		if err != nil {
			for _, value := range successfullPrimaryWrites {
				value.commitResponse.Status = false
				value.commitResponse.ErrorMessage = "failed to write data on secondary chunk server "
				value.commitResponse.Offset = -1
			}
		}
	}

	// respond to the clients for both successfull and unsuccessfull writes
	for _, value := range successfullPrimaryWrites {
		go chunkServer.writePrimaryChunkCommitResponse(value)
	}

	for _, value := range unsucessfullPrimaryWrites {
		go chunkServer.writePrimaryChunkCommitResponse(value)
	}
}

// Sends lease extension requests to the master.
// This function is called in the middle of the commit request to the primary chunk server, we dont want the
// primary's lease to expire during the write so this acts like a safeguard.
func (chunkServer *ChunkServer) sendLeaseExtensionRequest(chunkHandle int64) {

	request := common.MasterChunkServerLeaseRequest{
		Server:      chunkServer.address.String(),
		ChunkHandle: chunkHandle,
	}
	requestBytes, _ := helper.EncodeMessage(common.MasterChunkServerLeaseRequestType, request)

	chunkServer.MasterConnection.Write(requestBytes)
}

/* Client->ChunkServer */
// Handles the client read request
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
// The chunk server handles a read from the client by first checking to see if the file exists or not
// If the file does not exist, then it returns an error to the client.
func (chunkServer *ChunkServer) writeClientReadResponse(conn net.Conn, request common.ClientChunkServerReadRequest) error {

	// fileName := chunkServer.translateChunkHandleToFileName(request.ChunkHandle,false)
	return chunkServer.transferFile(conn, request.ChunkHandle)
}

/* ChunkServer->ChunkServer */
func (chunkServer *ChunkServer) handleInterChunkServerCloneRequest(conn net.Conn, messageBody []byte) error {

	request, err := helper.DecodeMessage[common.InterChunkServerCloneRequest](messageBody)
	if err != nil {
		return err
	}
	// if we have an error during the opening of the file we return an error
	// fileName := chunkServer.translateChunkHandleToFileName(request.ChunkHandle,false)
	return chunkServer.transferFile(conn, request.ChunkHandle)
}

/* ChunkServer->Client */
// Writes a response to the client write request
func (chunkServer *ChunkServer) writeClientWriteResponse(conn net.Conn, writeResponse common.ClientChunkServerWriteResponse) error {

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

// Client->ChunkServer
// This writeRequest is sent from the client to the chunkServer when it wants to push data into the chunkServer.
// It is not a commit request and on receiving this request, the chunkServer does not persist anything to its disk,
// or it does not actually mutate the chunk, it simply stores the data in an LRU cache till it receives the commit request
// from the primary chunkServer.
func (chunkServer *ChunkServer) handleClientWriteRequest(conn net.Conn, requestBodyBytes []byte) error {

	// decodes the message
	req, err := helper.DecodeMessage[common.ClientChunkServerWriteRequest](requestBodyBytes)
	if err != nil {
		return err
	}

	writeResponse := common.ClientChunkServerWriteResponse{
		Status:       true,
		ErrorMessage: "",
	}
	// store that data into the chunkServers LRU cache
	err = chunkServer.writeChunkToCache(req.MutationId, req.ChunkData)
	if err != nil { // there should not be any error her ig
		writeResponse.ErrorMessage = "error while writing chunk on chunkServer"
		writeResponse.Status = false
	}

	// writes a response to the client which acknowledges that the data has been received and stored in the LRU cache
	err = chunkServer.writeClientWriteResponse(conn, writeResponse)
	if err != nil {
		return err
	}
	return nil
}

/* Master->ChunkServer */
func (chunkServer *ChunkServer) handleMasterHeartbeatRequest(requestBodyBytes []byte) error {
	heartBeatRequest, err := helper.DecodeMessage[common.MasterToChunkServerHeartbeatRequest](requestBodyBytes)
	if err != nil {
		return err
	}

	if heartBeatRequest.Heartbeat != "HEARTBEAT" {
		return errors.New("heartbeat message is not correct")
	}

	if chunkServer.InTestMode {
		chunkServer.HeartbeatRequestsReceived.Add(1)
	}
	chunkServer.mu.Lock()

	chunks := make([]common.Chunk, 0)

	for _, chunkHandle := range chunkServer.ChunkHandles {
		chunkVersionNumber, present := chunkServer.ChunkVersionNumbers[chunkHandle]
		if !present {
			chunks = append(chunks, common.Chunk{ChunkHandle: chunkHandle, ChunkVersion: 0})
		} else {
			chunks = append(chunks, common.Chunk{ChunkHandle: chunkHandle, ChunkVersion: chunkVersionNumber})
		}
	}

	heartBeatResponse := common.ChunkServerToMasterHeartbeatResponse{
		ChunksPresent: chunks,
	}

	chunkServer.mu.Unlock()
	responseBodyBytes, err := helper.EncodeMessage(common.ChunkServerToMasterHeartbeatResponseType, heartBeatResponse)
	if err != nil {
		return err
	}
	_, err = chunkServer.MasterConnection.Write(responseBodyBytes)
	if err != nil {
		return err
	}
	return nil
}

// func (chunkServer *ChunkServer) writeMasterHeartbeatResponse(responseBodyBytes []byte) error {

// 	return nil
// }

func (chunkServer *ChunkServer) handleMasterHeartbeatResponse(messageBody []byte) {

	// log.Println("chunkServer " + chunkServer.ChunkDirectory + " received heartbeat response from master with chunks to delete")
	heartBeatResponse, _ := helper.DecodeMessage[common.MasterToChunkServerHeartbeatResponse](messageBody)
	for _, chunkHandle := range heartBeatResponse.ChunksToBeDeleted {
		log.Println("chunk ", chunkHandle, " to be deleted")
		go chunkServer.deleteChunk(chunkHandle)
	}

	if chunkServer.InTestMode {
		chunkServer.HeartbeatResponsesReceived.Add(1)
	}
}

/*
The master grants a chunklease to one of the replicas, which we call the primary. The primary picks a serial
order for all mutations to the chunk. All replicas follow this order when applying mutations.
Thus, the global mutation order is defined first by the lease grant order chosen by the
master, and within a lease by the serial numbers assigned by the primary.
The lease mechanism is designed to minimize management overhead at the master. A lease has an initial timeout
of 60 seconds. However, as long as the chunk is being
mutated, the primary can request and typically receive extensions from the master indefinitely.
These extension requests and grants are piggybacked on the HeartBeat messages regularly
exchanged between the master and all chunkservers.
The master may sometimes try to revoke a lease before it expires (e.g., when the master wants to disable mutations
on a file that is being renamed).
*/
func (chunkServer *ChunkServer) handleMasterLeaseRequest(requestBodyBytes []byte) error {
	leaseRequest, err := helper.DecodeMessage[common.MasterChunkServerLeaseRequest](requestBodyBytes)
	if err != nil {
		return err
	}
	chunkServer.mu.Lock()
	defer chunkServer.mu.Unlock()
	chunkServer.LeaseGrants[leaseRequest.ChunkHandle] = &LeaseGrant{
		ChunkHandle: leaseRequest.ChunkHandle,
		granted:     true,
		GrantTime:   time.Now(),
	}
	return nil
}

/* ChunkServer->Master */
// This is just a standard response from the master which acknowledges that it has
// received the handshake request.
func (chunkServer *ChunkServer) handleMasterHandshakeResponse() error {
	messageType, _, err := helper.ReadMessage(chunkServer.MasterConnection)
	if err != nil {
		log.Println("ERROR ", err)
		return err
	}

	if messageType != common.MasterChunkServerHandshakeResponseType {
		log.Println("ERROR ", err)
		return err
	}
	return nil
}

// tested
/* ChunkServer->Master */
// We send all the ChunkHandles which are present on the chunk server to the master
func (chunkServer *ChunkServer) initiateHandshake() error {
	log.Println("logging before handshake" + chunkServer.address.String())
	handshakeBody := common.MasterChunkServerHandshakeRequest{
		ChunkHandles: chunkServer.ChunkHandles,
		Port:         chunkServer.address.String(),
	}

	handshakeBytes, err := helper.EncodeMessage(common.MasterChunkServerHandshakeRequestType, handshakeBody)
	if err != nil {
		return err
	}
	_, err = chunkServer.MasterConnection.Write(handshakeBytes)
	if err != nil {
		return err
	}

	err = chunkServer.handleMasterHandshakeResponse()
	if err != nil {
		return err
	}

	return nil
}

// tessted
/* ChunkServer->Master */
// The initial handshake which is exchanged between the chunkServer and the master
// the chunkServer informs the master about all the chunks that reside on its server,
// this way the master does not have to keep track of which chunk servers contain which chunks.
// The chunk server itself serves as a source of truth regarding which chunks are present on it.
func (chunkServer *ChunkServer) registerWithMaster() error {
	conn, err := net.Dial("tcp", chunkServer.MasterPort)
	if err != nil {
		return err
	}
	chunkServer.MasterConnection = conn

	err = chunkServer.initiateHandshake()
	if err != nil {
		return err
	}
	return nil
}

// When the primary ChunkServer receives this commit request it doesnt mutatte the chunk immediately.
// Instead it buffers the write and then after a certain amount of time, tries to apply multiple mutations
// at a time to the chunk.
// I am trying to do this in accordance with the GFS paper section 3.1 -> 4th point where they say
// Once all the replicas have acknowledged receiving the data, the client sends a write request to the primary.
// The request identifies the data pushed earlier to all of
// the replicas. The primary assigns consecutive serial numbers to all the mutations it receives, possibly from
// multiple clients, which provides the necessary serial ization. It applies the mutation to its own local state
// in serial number order. Since we can receive mutations from multiple clients for the same chunk, it makes
// sense to accumulate multiple mutation requests and apply them in one go.
func (chunkServer *ChunkServer) handlePrimaryChunkCommitRequest(conn net.Conn, requestBodyBytes []byte) error {
	req, err := helper.DecodeMessage[common.PrimaryChunkCommitRequest](requestBodyBytes)
	if err != nil {
		return err
	}

	// intially I was keeping a check to see whether the current chunkServer is still the primary server
	// associated with this chunk. However i dont think this is necessary as the master will be doing the check.
	// But I probably will have to change this logic.

	// isPrimary:=chunkServer.checkIfPrimary(req.ChunkHandle)
	// if !isPrimary{
	// 	response:=common.PrimaryChunkCommitResponse{
	// 		Offset: -1,
	// 		Status: false,
	// 	}
	commitRequest := CommitRequest{
		conn:          conn,
		commitRequest: req,
	}

	// chunkServer.leaseUsage[req.ChunkHandle]=time.Now()

	// sends the request into the commitRequest buffer/channel
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
			err = helper.AddTimeoutForTheConnection(conn, 10*time.Second)
			if err != nil {
				return
			}
			err = chunkServer.handlePrimaryChunkCommitRequest(conn, messageBody)
			if err != nil {
				return
			}
		case common.InterChunkServerCommitRequestType:
			err = helper.AddTimeoutForTheConnection(conn, 180*time.Second)
			if err != nil {
				return
			}
			err = chunkServer.handleInterChunkServerCommitRequest(conn, messageBody)
			if err != nil {
				return
			}
		case common.InterChunkServerCloneRequestType:
			err = helper.AddTimeoutForTheConnection(conn, 180*time.Second)
			if err != nil {
				return
			}
			err = chunkServer.handleInterChunkServerCloneRequest(conn, messageBody)
			if err != nil {
				return
			}
		case common.ClientChunkServerReadRequestType:
			err = helper.AddTimeoutForTheConnection(conn, 25*time.Second)
			if err != nil {
				return
			}
			err = chunkServer.handleClientReadRequest(conn, messageBody)
			if err != nil {
				return
			}
		case common.ClientChunkServerWriteRequestType:
			err = helper.AddTimeoutForTheConnection(conn, 180*time.Second)
			if err != nil {
				return
			}
			err = chunkServer.handleClientWriteRequest(conn, messageBody)
			if err != nil {
				return
			}
		case common.MasterToChunkServerHeartbeatRequestType:
			err = chunkServer.handleMasterHeartbeatRequest(messageBody)
			if err != nil {
				return
			}
		case common.MasterToChunkServerHeartbeatResponseType:
			chunkServer.handleMasterHeartbeatResponse(messageBody)
			// if err!=nil{
			// 	return
			// }
		case common.MasterChunkServerIncreaseVersionNumberRequestType:
			chunkServer.handleMasterIncreaseVersionNumberRequest(messageBody)
		case common.MasterChunkServerLeaseRequestType:
			err = chunkServer.handleMasterLeaseRequest(messageBody)
			if err != nil {
				chunkServer.logger.Errorf(err.Error())
			}
		case common.MasterChunkServerCloneRequestType:
			err = chunkServer.handleMasterCloneRequest(messageBody)
			if err != nil {
				chunkServer.logger.Errorf(err.Error())
			}
		default:
			log.Println("Received unknown request type:", messageType)
		}
	}
}

func (chunkServer *ChunkServer) Shutdown() error {
	chunkServer.mu.Lock()
	defer chunkServer.mu.Unlock()
	chunkServer.listener.Close()
	return nil
}

// tested
func (chunkServer *ChunkServer) Start() (string, error) {

	err := chunkServer.loadChunks()
	if err != nil {
		return "", err
		// log.Panicf("Failed to start chunk server: %v", err)
	}
	listener, err := net.Listen("tcp", "")
	if err != nil {
		return "", err
	}
	chunkServer.listener = listener
	// log.Println(" chunkServer " + chunkServer.ChunkDirectory + " started on port " + listener.Addr().String())
	addr := listener.Addr().(*net.TCPAddr)
	chunkServer.address = addr

	// Register with master server
	// log.Println(" trying to register with master")
	if err := chunkServer.registerWithMaster(); err != nil {
		// log.Println("registration with master failed")
		log.Println(err)
		return "", err // log.Panicf("Failed to register with master server: %v", err)
	}
	// log.Println(" successfully registered with master")
	go chunkServer.startCommitRequestHandler()
	go chunkServer.handleConnection(chunkServer.MasterConnection)
	startWG := sync.WaitGroup{}
	startWG.Add(1)
	// Main loop to accept connections from clients
	go func() {
		startWG.Done()
		defer listener.Close()
		for {
			conn, err := listener.Accept()
			if err != nil {
				// Check if the listener was closed
				if opErr, ok := err.(*net.OpError); ok {
					// Check if it's a "use of closed" error
					if strings.Contains(opErr.Error(), "use of closed") {
						return
					}
				}
				log.Printf("Failed to accept connection: %v", err)
				continue
			}
			go chunkServer.handleConnection(conn)

		}
	}()
	startWG.Wait()
	// log.Println(" chunkServer " + chunkServer.ChunkDirectory + " waiting to accept connections")
	return addr.String(), nil
}
