package master

import (
	"container/heap"
	"errors"
	"io/fs"

	// "io/fs"
	"log"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/involk-secure-1609/goGFS/common"
	"github.com/involk-secure-1609/goGFS/helper"
)

// Master represents the master server that manages files and chunk handlers
type Master struct {
	MasterDirectory           string
	inTestMode                bool
	shutDownChan              chan struct{}
	Listener                  net.Listener
	ChunkServerConnections    []*ChunkServerConnection
	opLogger                  *OpLogger
	LastCheckpointTime        time.Time
	VersionNumberSynchronizer map[string]*IncreaseVersionNumberSynchronizer
	LastLogSwitchTime         time.Time
	ServerList                *ServerList
	idGenerator               *snowflake.Node
	Port                      string
	LeaseGrants               map[int64]*Lease
	FileMap                   map[string][]int64        // maps file names to array of chunkIds
	ChunkHandles              map[int64]*Chunk          // contains all the chunkHandles of all the non-deleted files
	ChunkServerHandler        map[int64]map[string]bool // maps chunkIds to the chunkServer Ports which store those chunks
	mu                        sync.RWMutex
}

// each Chunk contains its ChunkHandle and its size
// currently we are not using the ChunkSize for anything
type Chunk struct {
	ChunkHandle  int64
	ChunkVersion int64
}

type IncreaseVersionNumberSynchronizer struct {
	ErrorChan chan bool
}
type ChunkServerConnection struct {
	Port      string
	Conn      net.Conn
	Wg        sync.WaitGroup
	ErrorChan chan int
}

// The lease struct is used to designate the primary ChunkServer during write Requests
// the lease is granted only for a certain period of time
// chunkServers can extend their leases by sending extension requests on heartbeat messages
// regularly exchanged with the masterServer
type Lease struct {
	Server    string
	GrantTime time.Time
}

// NewMaster creates and initializes a new Master instance
func NewMaster(inTestMode bool) (*Master, error) {
	node, err := snowflake.NewNode(1)
	if err != nil {
		return nil, err
	}

	log.Println("intialized snowflake ")
	pq := &ServerList{}
	heap.Init(pq)

	master := &Master{
		MasterDirectory:        "masterDir",
		inTestMode:             inTestMode,
		ChunkServerConnections: make([]*ChunkServerConnection, 0),
		// currentOpLog: opLogFile,
		ServerList:                pq,
		idGenerator:               node,
		FileMap:                   make(map[string][]int64),
		ChunkHandles:              make(map[int64]*Chunk),
		ChunkServerHandler:        make(map[int64]map[string]bool),
		LeaseGrants:               make(map[int64]*Lease),
		VersionNumberSynchronizer: make(map[string]*IncreaseVersionNumberSynchronizer),
	}

	_, err = os.ReadDir(master.MasterDirectory)
	if err != nil {
		log.Println("this is being logged", err)
		var pathErr *fs.PathError // Note the pointer type
		if !errors.As(err, &pathErr) {
			// This means it IS a path error

			log.Println("this is also being logged", err)
			return nil, err
		}
		err = os.Mkdir(master.MasterDirectory, 0755)
		if err!=nil{
			return nil,err
		}
	}

	log.Println("initializing opLogger")
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
		ChunkVersion: -1,
		ChunkServers: nil,
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
	readResponse.ChunkVersion = chunk.ChunkVersion
	err = master.writeMasterReadResponse(conn, readResponse)
	if err != nil {
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
		SecondaryChunkServers: nil,
		ErrorMessage:          "error during write request on master ",
	}

	// if an error occurs while we write to the opLog then a chunkHandle will not be created
	// and this will return an error, if that happens then we shudnt be assigning chunkServers
	chunk, err := master.handleChunkCreation(request.Filename)
	// primaryServer,secondaryServers,err:=master.assignChunkServers(chunkHandle)
	if err == nil {
		primaryServer, secondaryServers, preExistingLeaseExists, err := master.assignChunkServers(chunk.ChunkHandle)
		if err == nil {
			if !master.inTestMode {
				servers := slices.Concat([]string{primaryServer}, secondaryServers)
				err = master.sendIncreaseVersionNumberToChunkServers(chunk, servers)
				if err == nil {
					leaseErr := master.grantLeaseToPrimaryServer(primaryServer, chunk.ChunkHandle)
					if leaseErr == nil {
						writeResponse = common.ClientMasterWriteResponse{
							ChunkHandle:           chunk.ChunkHandle,
							MutationId:            master.idGenerator.Generate().Int64(),
							PrimaryChunkServer:    primaryServer,
							SecondaryChunkServers: secondaryServers,
							ErrorMessage:          "",
						}
					}
				}
			}
			if !preExistingLeaseExists {
				master.mu.Lock()
				newLease := &Lease{}
				newLease.GrantTime = time.Now()
				newLease.Server = primaryServer
				master.LeaseGrants[chunk.ChunkHandle] = newLease
				master.mu.Unlock()
			}
		} else {
			writeResponse.ErrorMessage = "no chunk servers available"
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

func (master *Master) isChunkDeleted(chunkHandle int64) bool {

	for _, val := range master.ChunkHandles {
		if chunkHandle == val.ChunkHandle {
			return false
		}
	}
	return true
}

func (master *Master) verifyChunkVersion(chunk common.Chunk) bool {
	return false
}
func (master *Master) handleChunkServerHeartbeatResponse(connection *ChunkServerConnection, requestBodyBytes []byte) error {
	heartBeatResponse, err := helper.DecodeMessage[common.ChunkServerToMasterHeartbeatResponse](requestBodyBytes)
	if err != nil {
		return err
	}
	master.mu.Lock()
	chunksToBeDeleted := make([]int64, 0)
	for _, chunk := range heartBeatResponse.ChunksPresent {
		isChunkDeleted := master.isChunkDeleted(chunk.ChunkHandle)
		isChunkVersionOutdated := master.verifyChunkVersion(chunk)
		if isChunkDeleted {
			chunksToBeDeleted = append(chunksToBeDeleted, chunk.ChunkHandle)
		}
		if isChunkVersionOutdated {
			delete(master.ChunkServerHandler[chunk.ChunkHandle], connection.Port)
		} else {
			master.ChunkServerHandler[chunk.ChunkHandle][connection.Port] = true
		}
	}
	master.mu.Unlock()

	heartbeatResponse := common.MasterToChunkServerHeartbeatResponse{
		ChunksToBeDeleted: chunksToBeDeleted,
		ErrorMessage:      "",
	}
	return master.writeHeartbeatResponse(connection.Conn, heartbeatResponse)

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
func (master *Master) handleChunkServerMasterHandshake(connection *ChunkServerConnection, requestBodyBytes []byte) error {
	handshake, err := helper.DecodeMessage[common.MasterChunkServerHandshakeRequest](requestBodyBytes)
	if err != nil {
		log.Println("Encoding failed:", err)
		return err
	}
	master.mu.Lock()

	// We create a ChunkServerConnection object for the chunkServer and append it to the list of connections
	// The ChunkServerConnection object contains the port and the connection to the chunkServer
	connection.Port = handshake.Port
	master.ChunkServerConnections = append(master.ChunkServerConnections, connection)

	// the chunkServer includes all the chunkHandles it has in its handshake
	// the master appends the port of the chunkServer in the mapping from chunkHandle to chunkServers
	for _, chunkHandle := range handshake.ChunkHandles {

		_,present:=master.ChunkServerHandler[chunkHandle]
		if !present{
			master.ChunkServerHandler[chunkHandle]=make(map[string]bool)
		}
		master.ChunkServerHandler[chunkHandle][handshake.Port] = true
	}

	// Add the Server to the heap of ChunkServers
	server := &Server{
		Server:         handshake.Port,
		NumberOfChunks: len(handshake.ChunkHandles),
	}

	// First check if a server with the same port is present which can happen if the server disconnects to the master
	// and then connects again after sometime
	for _, server := range *master.ServerList {
		if server.Server == handshake.Port {
			heap.Remove(master.ServerList, server.index)
		}
	}
	// insert the server into the heap
	heap.Push(master.ServerList, server)
	master.mu.Unlock()

	// send a successfull handshake response to the Master
	handshakeResponse := common.MasterChunkServerHandshakeResponse{
		Message: "Handshake successful",
	}
	err = master.writeHandshakeResponse(connection.Conn, handshakeResponse)
	if err != nil {
		return err
	}

	if !master.inTestMode {
		go master.startChunkServerHeartbeat(connection)
	}
	return nil
}

func (master *Master) startChunkServerHeartbeat(chunkServerConnection *ChunkServerConnection) {

	// ticker := time.NewTicker(45 * time.Second)
	for {
		// select {
		// case <-ticker.C:
		time.Sleep(45 * time.Second)
		err := master.SendHeartBeatToChunkServer(chunkServerConnection)
		if err != nil {
			master.handleChunkServerFailure(chunkServerConnection)
		}
	}
}

func (master *Master) SendHeartBeatToChunkServer(chunkServerConnection *ChunkServerConnection) error {
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

func (master *Master) Shutdown() error {
	master.mu.Lock()
	defer master.mu.Unlock()
	master.Listener.Close()
	err := master.opLogger.currentOpLog.Sync()
	if err != nil {
		return err
	}
	return master.opLogger.currentOpLog.Close()
}
func (master *Master) Start() error {
	err := master.recover()
	if err != nil {
		return err
		// log.Panicf("master failed to recover correctly")
	}
	// Start master server
	listener, err := net.Listen("tcp", "")

	// net.ListenConfig
	if err != nil {
		return err
		// log.Fatalf("Failed to start master server: %v", err)
	}
	master.Listener = listener

	go master.startBackgroundCheckpoint()
	// log.Println("Master server listening on :",listener.Addr().String())
	startWG := sync.WaitGroup{}
	startWG.Add(1)
	// Main loop to accept connections from clients
	go func() {
		startWG.Done()
		defer master.Listener.Close()
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
			chunkServerConnection := &ChunkServerConnection{
				Conn: conn,
				Wg:   sync.WaitGroup{},
			}
			go master.handleConnection(chunkServerConnection)
		}
	}()
	startWG.Wait()
	// log.Println("connection accepting loop for master started succesfully")
	return nil
}

// Handles the creation of a new chunk for the specified file, this is sent to the master by the client
// when the client receives the error that the chunk is going to overflow during writes
func (master *Master) handleCreateNewChunkRequest(conn net.Conn, messageBytes []byte) error {
	newChunkRequest, err := helper.DecodeMessage[common.ClientMasterCreateNewChunkRequest](messageBytes)
	if err != nil {
		return err
	}
	response := common.ClientMasterCreateNewChunkResponse{
		Status: true,
	}
	err = master.createNewChunk(newChunkRequest.Filename, newChunkRequest.LastChunkHandle)
	if err != nil {
		return err
	}

	responseBytes, err := helper.EncodeMessage(common.ClientMasterCreateNewChunkResponseType, response)
	if err != nil {
		return err
	}

	_, err = conn.Write(responseBytes)
	return err
}
func (master *Master) handleConnection(connection *ChunkServerConnection) {
	conn := connection.Conn
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
			err = master.handleChunkServerMasterHandshake(connection, messageBytes)
			if err != nil {
				return
			}
		case common.MasterChunkServerIncreaseVersionNumberResponseType:
			log.Println("Received MasterChunkServerIncreaseVersionNumberResponseType")
			err = master.handleChunkServerIncreaseVersionNumberResponse(messageBytes)
			if err != nil {
				return
			}
		case common.ChunkServerToMasterHeartbeatResponseType:
			log.Println("Received MasterChunkServerHeartbeatType")
			// Process heartbeat
			err = master.handleChunkServerHeartbeatResponse(connection, messageBytes)
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
