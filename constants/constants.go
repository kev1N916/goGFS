package constants

type MessageType = int

const (
	ChunkSize = 400 // Chunk Size in bytes
	// Client-ChunkServer Read Messages

	ClientChunkServerReadRequestType = iota
	ClientChunkServerReadResponseType

	// Client-ChunkServer Write Messages
	ClientChunkServerWriteRequestType
	ClientChunkServerWriteResponseType

	// Client-MasterServer Read Messages
	ClientMasterReadRequestType
	ClientMasterReadResponseType

	// Client-MasterServer Write Messages
	ClientMasterWriteRequestType
	ClientMasterWriteResponseType

	// ChunkServer-MasterServer Messages
	MasterChunkServerHandshakeType
	MasterChunkServerHeartbeatType
	MasterChunkServerHandshakeResponseType
	MasterChunkServerHeartbeatResponseType

	PrimaryChunkCommitRequestType
    PrimaryChunkCommitResponseType

	InterChunkServerCommitRequestType
	InterChunkServerCommitResponseType
)

type MasterChunkServerHandshake struct {
	ChunkIds []int64
}

type MasterChunkServerHandshakeResponse struct {
	Message string
}

type MasterChunkServerHeartbeat struct {
	Port                   string
	ChunkIds               []int64
	LeaseExtensionRequests []int64
}

type MasterChunkServerHeartbeatResponse struct {
	ChunksToBeDeleted []int64
}
type ClientMasterReadRequest struct {
	Filename string // Capitalized field name
	Offset   int    // Capitalized field name
}

type UnserializedResponse struct {
	MessageType       MessageType // Capitalized field name
	ResponseBodyBytes []byte      // Capitalized field name
}

type ClientMasterReadResponse struct {
	ChunkHandle  int64    // Capitalized field name
	ChunkServers []string // Capitalized field name
}

type ClientMasterWriteRequest struct {
	Filename string // Capitalized field name
}

type ClientMasterWriteResponse struct {
	ChunkHandle           int64
	MutationId            int64
	PrimaryChunkServer    string   // Capitalized field name
	SecondaryChunkServers []string // Capitalized field name
}

type ClientChunkServerReadRequest struct {
	ChunkHandle int64 // Capitalized field name
	OffsetStart int64 // Capitalized field name
	OffsetEnd   int64 // Capitalized field name
}

type PrimaryChunkCommitRequest struct {
	ChunkHandle int64 // ID of the chunk to commit
	MutationId  int64 // ID of the mutation
    SecondaryServers []string // List of secondary servers
}

type PrimaryChunkCommitResponse struct {
	Status bool // 1 if the message was received succefully
}

type ClientChunkServerReadResponse struct {
	ResponseLength int32 // Capitalized field name
}

type ClientChunkServerWriteResponse struct {
	Status bool // 1 if the message was received succefully
}

type InterChunkServerCommitRequest struct{
	ChunkHandle int64
	ChunkOffset int64
	MutationOrder []int64
}


type InterChunkServerCommitResponse struct{
	Status bool
}
type ClientChunkServerWriteRequest struct {
	MutationId int64
	ChunkHandle int64  // Capitalized field name
	Data    []byte // Capitalized field name
}
