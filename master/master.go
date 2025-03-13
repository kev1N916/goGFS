package master

import (
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
	lastCheckpointTime time.Time
	lastLogSwitchTime  time.Time
	currentOpLog       *os.File
	serverList         ServerList
	idGenerator        *snowflake.Node
	port               string
	leaseGrants        map[int64]string
	fileMap            map[string][]Chunk // maps file names to array of chunkIds
	chunkHandler       map[int64][]string // maps chunkIds to the chunkServers which store those chunks
	mu                 sync.Mutex
	opLogMu            sync.Mutex
}
type Chunk struct {
	ChunkHandle int64
	ChunkSize   int64
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

func (master *Master) handleMasterWriteRequest(conn net.Conn, requestBodyBytes []byte) error {
	request, err := helper.DecodeMessage[common.ClientMasterWriteRequest](requestBodyBytes)
	if err != nil {
		log.Println("Decoding failed:", err)
		return err
	}

	opsToLog := make([]FileChunkMapping, 0)
	master.mu.Lock()
	_, fileAlreadyExists := master.fileMap[request.Filename]
	if !fileAlreadyExists {
		master.fileMap[request.Filename] = make([]Chunk, 0)
		chunk := Chunk{
			ChunkHandle: master.generateNewChunkId(),
			ChunkSize:   0,
		}
		opsToLog = append(opsToLog, FileChunkMapping{
			File:        request.Filename,
			ChunkHandle: chunk.ChunkHandle,
		})
		master.fileMap[request.Filename] = append(master.fileMap[request.Filename], chunk)
	}
	chunkHandle := master.fileMap[request.Filename][len(master.fileMap[request.Filename])-1].ChunkHandle
	_, chunkServerExists := master.chunkHandler[chunkHandle]
	servers := master.chooseSecondaryServers()
	if !chunkServerExists {
		master.chunkHandler[chunkHandle] = servers
	}
	master.mu.Unlock()
	primaryServer, secondaryServers := master.choosePrimaryAndSecondary(servers)
	writeResponse := common.ClientMasterWriteResponse{
		ChunkHandle:           chunkHandle,
		MutationId:            master.idGenerator.Generate().Int64(),
		PrimaryChunkServer:    primaryServer,
		SecondaryChunkServers: secondaryServers,
	}
	err = master.writeToOpLog(opsToLog)
	if err != nil {
		return err
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
func (master *Master) writeHeartbeatResponse(conn net.Conn, chunksToBeDeleted []int64) {
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

	master.writeHeartbeatResponse(conn, chunksToBeDeleted)

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
		case common.MasterChunkServerHandshakeType:
			log.Println("Received MasterChunkServerHandshakeType")
			// Process handshake
			err=master.handleMasterHandshake(conn, messageBytes)
			if err!=nil{
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
