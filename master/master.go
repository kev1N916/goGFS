package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/involk-secure-1609/goGFS/constants"
)

// Master represents the master server that manages files and chunk handlers
type Master struct {
	lastCheckpointTime time.Time
	lastLogSwitchTime time.Time
	currentOpLog *os.File
	serverList ServerList
	idGenerator  *snowflake.Node
	port         string
	leaseGrants  map[int64]string
	fileMap      map[string][]Chunk // maps file names to array of chunkIds
	chunkHandler map[int64][]string // maps chunkIds to the chunkServers which store those chunks
	mu           sync.Mutex          
	opLogMu   sync.Mutex
}
type Chunk struct{
	ChunkHandle int64
	ChunkSize int64
}
// NewMaster creates and initializes a new Master instance
func NewMaster(port string) (*Master,error) {
	node,_:=snowflake.NewNode(1)
	opLogFile, err := os.OpenFile("opLog.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return nil,err
    }
	return &Master{
		currentOpLog: opLogFile,
		serverList: make(ServerList,0),
		idGenerator:  node,
		port:         port,
		fileMap:      make(map[string][]Chunk),
		chunkHandler: make(map[int64][]string),
	},nil
}
func (master *Master) writeMasterReadResponse(conn net.Conn,chunkServers []string,chunkHandle int64){
	handshakeResponse:=constants.ClientMasterReadResponse{
		ChunkHandle: chunkHandle,
		ChunkServers: chunkServers,
	}
	// Encode using gob
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(handshakeResponse)
	if err != nil {
		log.Println("Encoding failed:", err)
		return
	}
	responseBytes := buf.Bytes()
	lengthOfRequest:=len(responseBytes)

	readResponseInBytes := make([]byte, 0)
	readResponseInBytes = append(readResponseInBytes, byte(constants.ClientMasterReadResponseType))
	binary.LittleEndian.AppendUint16(readResponseInBytes,uint16(lengthOfRequest))
	readResponseInBytes=append(readResponseInBytes,responseBytes...)
	
	conn.Write(readResponseInBytes)

}

func (master *Master) handleMasterReadRequest(conn net.Conn,requestBodyBytes []byte){
	var response constants.ClientMasterReadRequest
	responseReader := bytes.NewReader(requestBodyBytes)
	decoder := gob.NewDecoder(responseReader)
	err := decoder.Decode(&response)
	if err != nil {
		log.Println("Encoding failed:", err)
		return
	}


	fileInfo, err := os.Stat(response.Filename)
	if err != nil {
		log.Println("Error getting file info:", err)
		return
	}

	log.Println("File Name:", fileInfo.Name())
	log.Println("Size (bytes):", fileInfo.Size())
	chunkOffset:=fileInfo.Size()/constants.ChunkSize
	chunk:=master.fileMap[response.Filename][chunkOffset]
	chunkServers:=master.chunkHandler[chunk.ChunkHandle]
	master.writeMasterReadResponse(conn,chunkServers,chunk.ChunkHandle)
}

func (master *Master) chooseSecondaryServers()[]string{
	servers:=make([]string,0)
	for i:=range 3 {
		server := master.serverList[i]
		servers=append(servers,server.server)
		master.serverList.update(server, server.NumberOfChunks+1)
	}

	return servers
}

func (master *Master) generateNewChunkId() int64{
	id:=master.idGenerator.Generate()
	return id.Int64()
}

func (master *Master) handleMasterWriteRequest(conn net.Conn,requestBodyBytes []byte){
	var response constants.ClientMasterWriteRequest
	responseReader := bytes.NewReader(requestBodyBytes)
	decoder := gob.NewDecoder(responseReader)
	err := decoder.Decode(&response)
	if err != nil {
		log.Println("Encoding failed:", err)
		return
	}

	opsToLog:=make([]FileChunkMapping,0)
	master.mu.Lock()
	_,fileAlreadyExists:=master.fileMap[response.Filename]
	if !fileAlreadyExists{
		master.fileMap[response.Filename]=make([]Chunk,0)
		chunk:=Chunk{
			ChunkHandle: master.generateNewChunkId(),
			ChunkSize: 0,
		}
		opsToLog=append(opsToLog,FileChunkMapping{
			File: response.Filename,
			ChunkHandle: chunk.ChunkHandle,
		})
		master.fileMap[response.Filename]=append(master.fileMap[response.Filename],chunk)
	}
	chunkHandle:=master.fileMap[response.Filename][len(master.fileMap[response.Filename])-1].ChunkHandle
	_,chunkServerExists:=master.chunkHandler[chunkHandle]
	if !chunkServerExists{
		master.chunkHandler[chunkHandle]=master.chooseSecondaryServers()
	}
	master.mu.Unlock()
	err=master.writeToOpLog(opsToLog)
	if err!=nil{
		return
	}
	master.writeMasterWriteResponse(conn,chunkHandle)
}

func (master *Master) writeMasterWriteResponse(conn net.Conn,chunkHandle int64){
	writeResponse:=constants.ClientMasterWriteResponse{
		ChunkHandle: chunkHandle,
		MutationId: master.idGenerator.Generate().Int64(),
		PrimaryChunkServer: master.chunkHandler[chunkHandle][0],
		SecondaryChunkServers: master.chunkHandler[chunkHandle][1:],
	}
	// Encode using gob
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(writeResponse)
	if err != nil {
		log.Println("Encoding failed:", err)
		return
	}

	responseBytes := buf.Bytes()
	lengthOfRequest:=len(responseBytes)

	writeResponseInBytes := make([]byte, 0)
	writeResponseInBytes = append(writeResponseInBytes, byte(constants.ClientMasterWriteResponseType))
	binary.LittleEndian.AppendUint16(writeResponseInBytes,uint16(lengthOfRequest))
	writeResponseInBytes=append(writeResponseInBytes,responseBytes...)
	
	conn.Write(writeResponseInBytes)
}

func (master *Master) writeHandshakeResponse(conn net.Conn){
	handshakeResponse:=constants.MasterChunkServerHandshakeResponse{
		Message: "Handshake successful",
	}
	// Encode using gob
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(handshakeResponse)
	if err != nil {
		log.Println("Encoding failed:", err)
		return
	}
	responseBytes := buf.Bytes()
	lengthOfRequest:=len(responseBytes)

	handshakeResponseInBytes := make([]byte, 0)
	handshakeResponseInBytes = append(handshakeResponseInBytes, byte(constants.MasterChunkServerHandshakeResponseType))
	binary.LittleEndian.AppendUint16(handshakeResponseInBytes,uint16(lengthOfRequest))
	handshakeResponseInBytes=append(handshakeResponseInBytes,responseBytes...)
	
	conn.Write(handshakeResponseInBytes)

}
func (master *Master) writeHeartbeatResponse(conn net.Conn,chunksToBeDeleted[]int64){
	heartbeatResponse := constants.MasterChunkServerHeartbeatResponse{
		ChunksToBeDeleted: chunksToBeDeleted,
	}

	// Encode using gob
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(heartbeatResponse)
	if err != nil {
		log.Println("Encoding failed:", err)
		return
	}

	responseBytes := buf.Bytes()
	lengthOfRequest := len(responseBytes)

	// Construct final response
	heartbeatResponseInBytes := make([]byte, 0)
	heartbeatResponseInBytes = append(heartbeatResponseInBytes, byte(constants.MasterChunkServerHeartbeatResponseType))
	heartbeatResponseInBytes = binary.LittleEndian.AppendUint16(heartbeatResponseInBytes, uint16(lengthOfRequest))
	heartbeatResponseInBytes = append(heartbeatResponseInBytes, responseBytes...)
	
	conn.Write(heartbeatResponseInBytes)

}
func (master *Master) handleMasterHeartbeat(conn net.Conn,requestBodyBytes []byte){
	var response constants.MasterChunkServerHeartbeat
	responseReader := bytes.NewReader(requestBodyBytes)
	decoder := gob.NewDecoder(responseReader)
	err := decoder.Decode(&response)
	if err != nil {
		log.Println("Encoding failed:", err)
		return
	}
	chunksToBeDeleted:=make([]int64,0)
	for _,chunkId :=range(response.ChunkIds){
		_,presentOnMaster:=master.chunkHandler[chunkId];
		if !presentOnMaster{
			chunksToBeDeleted = append(chunksToBeDeleted, chunkId)
		}
	}

	for _,leaseRequest:=range(response.LeaseExtensionRequests){
		master.leaseGrants[leaseRequest]=conn.RemoteAddr().String()
	}

	master.writeHeartbeatResponse(conn,chunksToBeDeleted)

}

func (master *Master) handleMasterHandshake(conn net.Conn,requestBodyBytes []byte){
	var response constants.MasterChunkServerHandshake
	responseReader := bytes.NewReader(requestBodyBytes)
	decoder:=gob.NewDecoder(responseReader)
	err := decoder.Decode(&response)
	if err != nil {
		log.Println("Encoding failed:", err)
		return
	}
	for _,chunkId :=range(response.ChunkIds){
		master.chunkHandler[chunkId]=append(master.chunkHandler[chunkId],conn.RemoteAddr().String())
	}

	master.writeHandshakeResponse(conn)

}