package master

import (
	"errors"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/involk-secure-1609/goGFS/common"
	"github.com/involk-secure-1609/goGFS/helper"
)

// Master represents the master server that manages files and chunk handlers
type Master struct {
	chunkServerConnections []ChunkServerConnection
	lastCheckpointTime     time.Time
	lastLogSwitchTime      time.Time
	currentOpLog           *os.File
	serverList             ServerList
	idGenerator            *snowflake.Node
	port                   string
	leaseGrants            map[int64]string
	fileMap                map[string][]Chunk // maps file names to array of chunkIds
	chunkHandler           map[int64][]string // maps chunkIds to the chunkServers which store those chunks
	mu                     sync.Mutex
	opLogMu                sync.Mutex
}
type Chunk struct {
	ChunkHandle int64
	ChunkSize   int64
}

type ChunkServerConnection struct {
	port string
	conn net.Conn
}

// NewMaster creates and initializes a new Master instance
func NewMaster(port string) (*Master, error) {
	node, _ := snowflake.NewNode(1)
	opLogFile, err := os.OpenFile("opLog.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &Master{
		currentOpLog: opLogFile,
		serverList:   make(ServerList, 0),
		idGenerator:  node,
		port:         port,
		fileMap:      make(map[string][]Chunk),
		chunkHandler: make(map[int64][]string),
	}, nil
}
func (master *Master) writeMasterReadResponse(conn net.Conn, readResponse common.ClientMasterReadResponse) error {
	// readResponse := common.ClientMasterReadResponse{
	// 	ChunkHandle:  chunkHandle,
	// 	ChunkServers: chunkServers,
	// }
	readResponseInBytes, err := helper.EncodeMessage(common.ClientMasterReadResponseType, readResponse)
	if err != nil {
		return err
	}
	_, err = conn.Write(readResponseInBytes)
	if err != nil {
		return err
	}
	return nil
}

func (master *Master) handleMasterReadRequest(conn net.Conn, requestBodyBytes []byte) error {
	request, err := helper.DecodeMessage[common.ClientMasterReadRequest](requestBodyBytes)
	if err != nil {
		log.Println("Decoding failed:", err)
		return err
	}
	readResponse := common.ClientMasterReadResponse{
		Error:        "file not found",
		ChunkHandle:  -1,
		ChunkServers: make([]string, 0),
	}
	fileInfo, err := os.Stat(request.Filename)
	if err != nil {
		master.writeMasterReadResponse(conn, readResponse)
		return err
	}
	chunkOffset := fileInfo.Size() / common.ChunkSize
	master.mu.Lock()
	chunk := master.fileMap[request.Filename][chunkOffset]
	chunkServers := master.chunkHandler[chunk.ChunkHandle]
	master.mu.Unlock()
	readResponse.ChunkHandle = chunk.ChunkHandle
	readResponse.ChunkServers = chunkServers
	readResponse.Error = ""
	master.writeMasterReadResponse(conn, readResponse)
	return nil
}

func (master *Master) findChunkServerConnection(server string) net.Conn{
	for _,connection :=range(master.chunkServerConnections){
		if (connection.port==server){
			return connection.conn
		}
	}

	return nil
}
func (master *Master) grantLeaseToPrimaryServer(primaryServer string, chunkHandle int64) error {
	conn := master.findChunkServerConnection(primaryServer)
	if conn == nil {
		return errors.New("connection to chunk Server does not exist")
	}

	grantLeaseRequest := common.MasterChunkServerLeaseRequest{
		ChunkHandle: chunkHandle,
	}

	messageBytes, err := helper.EncodeMessage(common.MasterChunkServerLeaseRequestType, grantLeaseRequest)
	if err != nil {
		return err
	}
	_, err = conn.Write(messageBytes)
	if err != nil {
		return nil
	}
	return nil
}

func(master *Master) deleteFile(fileName string) error{
	master.mu.Lock()
	chunks, ok := master.fileMap[fileName]
	if !ok{
		master.mu.Unlock()
		return errors.New("file does not exist")
	}
	delete(master.fileMap,fileName)
	newFileName:=fileName+"/"+time.Now().String()+"/"+".deleted"
	master.fileMap[newFileName] = chunks
	master.mu.Unlock()
	op:=Operation{
		Type:        common.ClientMasterDeleteRequestType,
		File:        fileName,
		ChunkHandle: -1,
		NewName: newFileName,
	}
	err:=master.writeToOpLog(op)
	if err!=nil{
		return err
	}
	return nil
}

func (master *Master) handleMasterDeleteRequest(conn net.Conn, requestBodyBytes []byte) error {
	request, err := helper.DecodeMessage[common.ClientMasterDeleteRequest](requestBodyBytes)
	if err != nil {
		log.Println("Decoding failed:", err)
		return err
	}

	err=master.deleteFile(request.Filename)
	if err!=nil{
		return err
	}

	return nil

}

func (master *Master) handleMasterWriteRequest(conn net.Conn, requestBodyBytes []byte) error {
	request, err := helper.DecodeMessage[common.ClientMasterWriteRequest](requestBodyBytes)
	if err != nil {
		log.Println("Decoding failed:", err)
		return err
	}

	// opsToLog := make([], 0)
	op := Operation{
		Type:        common.ClientMasterWriteRequestType,
		File:        request.Filename,
		ChunkHandle: -1,
	}
	master.mu.Lock()
	_, fileAlreadyExists := master.fileMap[request.Filename]
	if !fileAlreadyExists {
		master.fileMap[request.Filename] = make([]Chunk, 0)
		chunk := Chunk{
			ChunkHandle: master.generateNewChunkId(),
			ChunkSize:   0,
		}
		op.ChunkHandle = chunk.ChunkHandle
		master.fileMap[request.Filename] = append(master.fileMap[request.Filename], chunk)
	}
	chunkHandle := master.fileMap[request.Filename][len(master.fileMap[request.Filename])-1].ChunkHandle
	_, chunkServerExists := master.chunkHandler[chunkHandle]
	// var servers := master.chooseSecondaryServers()
	var servers []string
	if !chunkServerExists {
		master.chunkHandler[chunkHandle] = 	master.chooseSecondaryServers()
	}
	servers=master.chunkHandler[chunkHandle]
	master.mu.Unlock()
	primaryServer, secondaryServers := master.choosePrimaryAndSecondary(servers)
	err = master.grantLeaseToPrimaryServer(primaryServer, chunkHandle)
	if err != nil {
		return err
	}
	writeResponse := common.ClientMasterWriteResponse{
		ChunkHandle:           chunkHandle,
		MutationId:            master.idGenerator.Generate().Int64(),
		PrimaryChunkServer:    primaryServer,
		SecondaryChunkServers: secondaryServers,
	}
	if op.ChunkHandle != -1 {
		err = master.writeToOpLog(op)
		if err != nil {
			return err
		}
	}
	err = master.writeMasterWriteResponse(conn, writeResponse)
	if err != nil {
		return err
	}
	return nil
}

func (master *Master) writeMasterWriteResponse(conn net.Conn, writeResponse common.ClientMasterWriteResponse) error {

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
func (master *Master) writeHeartbeat(conn net.Conn, chunksToBeDeleted []int64) {
	heartbeatResponse := common.MasterChunkServerHeartbeatResponse{
		ChunksToBeDeleted: chunksToBeDeleted,
	}

	heartbeatResponseInBytes, err := helper.EncodeMessage(common.MessageType(common.MasterChunkServerHeartbeatResponseType), heartbeatResponse)
	if err != nil {
		return
	}

	conn.Write(heartbeatResponseInBytes)

}
func (master *Master) handleChunkServerHeartbeatResponse(conn net.Conn, requestBodyBytes []byte) {
	heartBeatResponse, err := helper.DecodeMessage[common.MasterChunkServerHeartbeat](requestBodyBytes)
	if err != nil {
		return
	}
	chunksToBeDeleted := make([]int64, 0)
	for _, chunkId := range heartBeatResponse.ChunkIds {
		_, presentOnMaster := master.chunkHandler[chunkId]
		if !presentOnMaster {
			chunksToBeDeleted = append(chunksToBeDeleted, chunkId)
		}
	}

	for _, leaseRequest := range heartBeatResponse.LeaseExtensionRequests {
		master.leaseGrants[leaseRequest] = conn.RemoteAddr().String()
	}

	// master.writeHeartbeatResponse(conn, chunksToBeDeleted)

}

func (master *Master) handleMasterHandshake(conn net.Conn, requestBodyBytes []byte) error {
	handshake, err := helper.DecodeMessage[common.MasterChunkServerHandshake](requestBodyBytes)
	if err != nil {
		log.Println("Encoding failed:", err)
		return err
	}
	master.mu.Lock()
	for _, chunkId := range handshake.ChunkIds {
		master.chunkHandler[chunkId] = append(master.chunkHandler[chunkId], conn.RemoteAddr().String())
	}
	master.mu.Unlock()
	handshakeResponse := common.MasterChunkServerHandshakeResponse{
		Message: "Handshake successful",
	}
	err = master.writeHandshakeResponse(conn, handshakeResponse)
	if err != nil {
		return err
	}
	master.chunkServerConnections = append(master.chunkServerConnections, ChunkServerConnection{port: conn.RemoteAddr().String(), conn: conn})
	return nil
}

func (master *Master) Start() error {
	// Start master server
	listener, err := net.Listen("tcp", ":"+master.port)
	if err != nil {
		log.Fatalf("Failed to start master server: %v", err)
	}
	defer listener.Close()

	log.Println("Master server listening on :8080")

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

func (master *Master) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		messageType, messageBytes, err := helper.ReadMessage(conn)
		if err != nil {
			return
		}

		// Process the request based on type
		switch messageType {
		case common.ClientMasterReadRequestType:
			log.Println("Received MasterReadRequestType")
			// Process read request
			err = helper.AddTimeoutForTheConnection(conn, 20*time.Second)
			if err != nil {
				return
			}
			err = master.handleMasterReadRequest(conn, messageBytes)
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
			err = master.handleMasterWriteRequest(conn, messageBytes)
			if err != nil {
				return
			}
		case common.ClientMasterDeleteRequestType:
			log.Println("Received MasterChunkServerHeartbeatType")
			err = helper.AddTimeoutForTheConnection(conn, 30*time.Second)
			if err != nil {
				return
			}
			// Process heartbeat
			master.handleMasterDeleteRequest(conn, messageBytes)
		case common.MasterChunkServerHandshakeType:
			log.Println("Received MasterChunkServerHandshakeType")
			// Process handshake
			err = master.handleMasterHandshake(conn, messageBytes)
			if err != nil {
				return
			}
		case common.MasterChunkServerHeartbeatResponseType:
			log.Println("Received MasterChunkServerHeartbeatType")
			// Process heartbeat
			master.handleChunkServerHeartbeatResponse(conn, messageBytes)
		default:
			log.Println("Received unknown request type:", messageType)
		}
	}
}