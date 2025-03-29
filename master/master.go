package master

import (
	"container/heap"
	"log"
	"net"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/involk-secure-1609/goGFS/common"
	"github.com/involk-secure-1609/goGFS/helper"
)

// Master represents the master server that manages files and chunk handlers
type Master struct {
	inTestMode             bool
	listener               net.Listener
	ChunkServerConnections []*ChunkServerConnection
	opLogger               *OpLogger
	LastCheckpointTime     time.Time
	LastLogSwitchTime      time.Time
	ServerList             *ServerList
	idGenerator            *snowflake.Node
	Port                   string
	LeaseGrants            map[int64]*Lease
	FileMap                map[string][]Chunk // maps file names to array of chunkIds
	ChunkHandles           []int64      // contains all the chunkHandles of all the non-deleted files
	ChunkServerHandler     map[int64][]string // maps chunkIds to the chunkServers which store those chunks
	mu                     sync.Mutex
}

// each Chunk contains its ChunkHandle and its size
// currently we are not using the ChunkSize for anything
type Chunk struct {
	ChunkHandle int64
	ChunkSize   int64
}

type ChunkServerConnection struct {
	Port string
	Conn net.Conn
}

// The lease struct is used to designate the primary ChunkServer during write Requests
// the lease is granted only for a certain period of time
// chunkServers can extend their leases by sending extension requests on heartbeat messages
// regularly exchanged with the masterServer
type Lease struct {
	server    string
	grantTime time.Time
}

// NewMaster creates and initializes a new Master instance
func NewMaster(port string,inTestMode bool) (*Master, error) {
	node, err := snowflake.NewNode(1)
	if err != nil {
		return nil, err
	}
	pq := &ServerList{}
	heap.Init(pq)
	master := &Master{
		inTestMode: inTestMode,
		ChunkServerConnections: make([]*ChunkServerConnection, 0),
		// currentOpLog: opLogFile,
		ServerList:   pq,
		idGenerator:  node,
		Port:         port,
		FileMap:      make(map[string][]Chunk),
		ChunkHandles: make([]int64, 0),
		ChunkServerHandler: make(map[int64][]string),
		LeaseGrants:  make(map[int64]*Lease),
	}
	opLogger, err := NewOpLogger(master)
	if err != nil {
		return nil, err
	}
	master.opLogger = opLogger
	return master, nil
}

// writes the response back to the client for a read request
func (master *Master) writeMasterReadResponse(conn net.Conn, readResponse common.ClientMasterReadResponse) error {

	// serializes the readResponse struct into bytes
	readResponseInBytes, err := helper.EncodeMessage(common.ClientMasterReadResponseType, readResponse)
	if err != nil {
		return err
	}
	// writes it to the client
	_, err = conn.Write(readResponseInBytes)
	if err != nil {
		return err
	}
	return nil
}

// the master handles the readRequest from the client by replying with the chunkHandle and the chunkServers
// if the master does not have any metadata on the file, it replies with an error Message
func (master *Master) handleClientMasterReadRequest(conn net.Conn, requestBodyBytes []byte) error {
	request, err := helper.DecodeMessage[common.ClientMasterReadRequest](requestBodyBytes)
	if err != nil {
		log.Println("Decoding failed:", err)
		return err
	}
	readResponse := common.ClientMasterReadResponse{
		ErrorMessage: "file not found",
		ChunkHandle:  -1,
		ChunkServers: make([]string, 0),
	}
	chunk, chunkServers, err := master.getMetadataForFile(request.Filename, request.ChunkIndex)
	if err != nil {
		readResponse.ErrorMessage = err.Error()
		err = master.writeMasterReadResponse(conn, readResponse)
		return err
	}
	readResponse.ChunkHandle = chunk.ChunkHandle
	readResponse.ChunkServers = chunkServers
	readResponse.ErrorMessage = ""
	err=master.writeMasterReadResponse(conn, readResponse)
	if err!=nil{
		return err
	}
	return nil
}

// writes the response back to the client for a delete request
func (master *Master) writeMasterDeleteResponse(conn net.Conn, response common.ClientMasterDeleteResponse) error {

	responseBytes, err := helper.EncodeMessage(common.ClientMasterDeleteResponseType, response)
	if err != nil {
		return err
	}

	_, err = conn.Write(responseBytes)
	if err != nil {
		return err
	}
	return nil
}

// Handles a client delete request
// During a delete request we just delete the metadata about the file from the master.
// We first delete the file temporarily by renaming it to a alternate name.
// If this temporary file is in our system for more than a certain amount of time then
// we will delete the file permanenetly.
// Deletion from the master is primarily a change in metadata operation.
func (master *Master) handleClientMasterDeleteRequest(conn net.Conn, requestBodyBytes []byte) error {
	request, err := helper.DecodeMessage[common.ClientMasterDeleteRequest](requestBodyBytes)
	if err != nil {
		log.Println("Decoding failed:", err)
		return err
	}

	deleteResponse := common.ClientMasterDeleteResponse{
		Status: true,
	}
	err = master.deleteFile(request.Filename)
	if err != nil {
		log.Println("deleting the file failed:", err)
		deleteResponse.Status = false
	}
	err = master.writeMasterDeleteResponse(conn, deleteResponse)
	if err != nil {
		return err
	}
	return nil

}

// the client write request only consists of the file to which the client wants to mutate.
// the master , if the file does not have any chunks associated with it , we create a new one and
// log this creation, after that we assign secondary chunk servers for this chunk. Then we choose
// primary and secondary servers from these chunk Servers.
func (master *Master) handleClientMasterWriteRequest(conn net.Conn, requestBodyBytes []byte) error {
	request, err := helper.DecodeMessage[common.ClientMasterWriteRequest](requestBodyBytes)
	if err != nil {
		log.Println("Decoding failed:", err)
		return err
	}
	writeResponse := common.ClientMasterWriteResponse{
		ChunkHandle: -1,
		// a mutationId is generated so that during the commit request we can mantain an order
		// between concurrent requests
		MutationId:            -1,
		PrimaryChunkServer:    "",
		SecondaryChunkServers: make([]string, 0),
		ErrorMessage:          "error during write request on master ",
	}
	chunkHandle, primaryServer, secondaryServers, err := master.handleChunkCreation(request.Filename)
	if err == nil {
		writeResponse = common.ClientMasterWriteResponse{
			ChunkHandle:           chunkHandle,
			MutationId:            master.idGenerator.Generate().Int64(),
			PrimaryChunkServer:    primaryServer,
			SecondaryChunkServers: secondaryServers,
			ErrorMessage:          "",
		}
	}
	// write a response back to the client
	err = master.writeClientMasterWriteResponse(conn, writeResponse)
	if err != nil {
		return err
	}
	return nil
}

// writes the response back to the client for a write request
func (master *Master) writeClientMasterWriteResponse(conn net.Conn, writeResponse common.ClientMasterWriteResponse) error {

	writeResponseInBytes, err := helper.EncodeMessage(common.ClientMasterWriteResponseType, writeResponse)
	if err != nil {
		return err
	}
	_, err = conn.Write(writeResponseInBytes)
	if err != nil {
		return err
	}
	return nil
}

// writes the response back to the chunkServer for a handshake
func (master *Master) writeHandshakeResponse(conn net.Conn, handshakeResponse common.MasterChunkServerHandshakeResponse) error {
	// handshakeResponse := common.MasterChunkServerHandshakeResponse{
	// 	Message: "Handshake successful",
	// }
	handshakeResponseInBytes, err := helper.EncodeMessage(common.MasterChunkServerHandshakeResponseType, handshakeResponse)
	if err != nil {
		return err
	}
	_, err = conn.Write(handshakeResponseInBytes)
	if err != nil {
		return err
	}
	return nil
}

/*
The master periodically communicates with each chunkserver in HeartBeat
messages to give it instructions and collect its state.
The master can keep itself up-to-date thereafter because it controls all chunk placement and
monitors chunkserver status with regular HeartBeat messages
The primary chunkServer can request and typically receive lease extensions from the master indeﬁnitely.
These extension requests and grants are piggybacked on the HeartBeat messages
regularly exchanged between the master and all chunkservers.
In a regular scan of the chunk namespace, the master identiﬁes orphaned chunks (i.e., those not reachable from any ﬁle)
and erases the metadata for those chunks. In a HeartBeat message regularly exchanged with the master,
each chunkserver reports a subset of the chunks it has, and the master replies with the identity of all chunks
that are no longer present in the master’s metadata. The chunkserver
is free to delete its replicas of such chunks.
*/
// func (master *Master) writeHeartbeat(conn net.Conn, chunksToBeDeleted []int64) {
// 	heartbeatResponse := common.MasterChunkServerHeartbeatResponse{
// 		ChunksToBeDeleted: chunksToBeDeleted,
// 	}

// 	heartbeatResponseInBytes, err := helper.EncodeMessage(common.MessageType(common.MasterChunkServerHeartbeatResponseType), heartbeatResponse)
// 	if err != nil {
// 		return
// 	}

// 	conn.Write(heartbeatResponseInBytes)

// }

func (master *Master) isChunkDeleted(chunkHandle int64) bool{

	for _,val:=range(master.ChunkHandles){
		if(val==chunkHandle){
			return false
		}
	}
	return true
}
func (master *Master) handleChunkServerHeartbeatResponse(conn net.Conn, requestBodyBytes []byte) error {
	heartBeatResponse, err := helper.DecodeMessage[common.ChunkServerToMasterHeartbeatResponse](requestBodyBytes)
	if err != nil {
		return err
	}
	master.mu.Lock()
	chunksToBeDeleted := make([]int64, 0)
	for _, chunkHandle := range heartBeatResponse.ChunksPresent {
		isChunkDeleted:=master.isChunkDeleted(chunkHandle)
		if isChunkDeleted {
			chunksToBeDeleted = append(chunksToBeDeleted, chunkHandle)
		}
	}
	master.mu.Unlock()

	heartbeatResponse := common.MasterToChunkServerHeartbeatResponse{
		ChunksToBeDeleted: chunksToBeDeleted,
		ErrorMessage:      "",
	}
	return master.writeHeartbeatResponse(conn, heartbeatResponse)

}

// writes the response back to the chunk server for a heartbeat request
func (master *Master) writeHeartbeatResponse(conn net.Conn, response common.MasterToChunkServerHeartbeatResponse) error {

	heartbeatResponse, err := helper.EncodeMessage(common.MessageType(common.MasterToChunkServerHeartbeatResponseType), response)
	if err != nil {
		log.Println("Encoding failed:", err)
		return err
	}
	_, err = conn.Write(heartbeatResponse)
	if err != nil {
		return err
	}
	return nil
}

// Handles the initial chunkServer master handshake request
func (master *Master) handleChunkServerMasterHandshake(conn net.Conn, requestBodyBytes []byte) error {
	handshake, err := helper.DecodeMessage[common.MasterChunkServerHandshakeRequest](requestBodyBytes)
	if err != nil {
		log.Println("Encoding failed:", err)
		return err
	}
	master.mu.Lock()
	for _, chunkId := range handshake.ChunkHandles {
		master.ChunkServerHandler[chunkId] = append(master.ChunkServerHandler[chunkId], handshake.Port)
	}
	handshakeResponse := common.MasterChunkServerHandshakeResponse{
		Message: "Handshake successful",
	}
	err = master.writeHandshakeResponse(conn, handshakeResponse)
	if err != nil {
		return err
	}

	chunkServerConnection := &ChunkServerConnection{Port: handshake.Port, Conn: conn}
	master.ChunkServerConnections = append(master.ChunkServerConnections,chunkServerConnection)
	master.mu.Unlock()

	if (!master.inTestMode){
	go master.startChunkServerHeartbeat(chunkServerConnection)
	}
	return nil
}

func (master *Master) startChunkServerHeartbeat(chunkServerConnection *ChunkServerConnection){

	ticker:=time.NewTicker(45*time.Second)
	for{
		select{
		case <-ticker.C:
			err:=master.SendHeartBeatToChunkServer(chunkServerConnection)
			log.Println(err)
			ticker.Reset(45*time.Second)
		default:
		}
	}
}

func (master *Master) SendHeartBeatToChunkServer(chunkServerConnection *ChunkServerConnection) error{
	heartBeatRequest := common.MasterToChunkServerHeartbeatRequest{
		Heartbeat: "HEARTBEAT",
	}

	heartbeatRequestBytes, err := helper.EncodeMessage(common.MasterToChunkServerHeartbeatRequestType, heartBeatRequest)
	if err != nil {
		return err
	}

	_, err = chunkServerConnection.Conn.Write(heartbeatRequestBytes)
	if err != nil {
		return err
	}
	return nil
}

func (master *Master) Start() error {
	err := master.recover()
	if err != nil {
		return err
		// log.Panicf("master failed to recover correctly")
	}
	// Start master server
	listener, err := net.Listen("tcp", ":"+master.Port)
	if err != nil {
		return err
		// log.Fatalf("Failed to start master server: %v", err)
	}
	master.listener = listener
	

	go master.startBackgroundCheckpoint()
	log.Println("Master server listening on :8080")
	startWG := sync.WaitGroup{}
	startWG.Add(1)
	// Main loop to accept connections from clients
	go func() {
		startWG.Done()
		defer master.listener.Close()
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("Failed to accept connection: %v", err)
				continue
			}
			go master.handleConnection(conn)
		}
	}()
	startWG.Wait()
	log.Println("connection accepting loop for master started succesfully")
	return nil
}

// func (master *Master) writeCreateNewChunkResponse(conn net.Conn,messageBytes []byte)error{
// 	newChunkRequest, err := helper.DecodeMessage[common.ClientMasterCreateNewChunkRequest](messageBytes)
// 	if err != nil {
// 		return err
// 	}

// 	err=master.createNewChunk(newChunkRequest.Filename)
// 	if err!=nil{
// 		return err
// 	}
// 	return nil
// }

// Handles the creation of a new chunk for the specified file, this is sent to the master by the client
// when the client receives the error that the chunk is going to overflow during writes
func (master *Master) handleCreateNewChunkRequest(conn net.Conn, messageBytes []byte) error {
	newChunkRequest, err := helper.DecodeMessage[common.ClientMasterCreateNewChunkRequest](messageBytes)
	if err != nil {
		return err
	}

	err = master.createNewChunk(newChunkRequest.Filename)
	if err != nil {
		return err
	}
	return nil
}
func (master *Master) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		messageType, messageBytes, err := helper.ReadMessage(conn)
		if err != nil {
			return
		}

		// Process the request based on type
		switch messageType {
		case common.ClientMasterCreateNewChunkRequestType:
			log.Println("Received ClientMasterNewChunkRequestType")
			// Process read request
			err = helper.AddTimeoutForTheConnection(conn, 20*time.Second)
			if err != nil {
				return
			}
			err = master.handleCreateNewChunkRequest(conn, messageBytes)
			if err != nil {
				return
			}
		case common.ClientMasterReadRequestType:
			log.Println("Received MasterReadRequestType")
			// Process read request
			err = helper.AddTimeoutForTheConnection(conn, 20*time.Second)
			if err != nil {
				return
			}
			err = master.handleClientMasterReadRequest(conn, messageBytes)
			if err != nil {
				return
			}
		case common.ClientMasterWriteRequestType:
			log.Println("Received MasterWriteRequestType")
			// Process read request
			err = helper.AddTimeoutForTheConnection(conn, 30*time.Second)
			if err != nil {
				return
			}

			// Process write request
			err = master.handleClientMasterWriteRequest(conn, messageBytes)
			if err != nil {
				return
			}
		case common.ClientMasterDeleteRequestType:
			log.Println("Received MasterChunkServerHeartbeatType")
			err = helper.AddTimeoutForTheConnection(conn, 30*time.Second)
			if err != nil {
				return
			}
			err = master.handleClientMasterDeleteRequest(conn, messageBytes)
			if err != nil {
				return
			}
		case common.MasterChunkServerHandshakeRequestType:
			log.Println("Received MasterChunkServerHandshakeType")
			// Process handshake
			err = master.handleChunkServerMasterHandshake(conn, messageBytes)
			if err != nil {
				return
			}
		case common.ChunkServerToMasterHeartbeatResponseType:
			log.Println("Received MasterChunkServerHeartbeatType")
			// Process heartbeat
			err = master.handleChunkServerHeartbeatResponse(conn, messageBytes)
			if err != nil {
				return
			}
		case common.MasterChunkServerLeaseRequestType:
			log.Println("Received MasterChunkServerLeaseRequestType")
			err = master.handleMasterLeaseRequest(conn, messageBytes)
			if err != nil {
				return
			}
		default:
			log.Println("Received unknown request type:", messageType)
		}
	}
}
