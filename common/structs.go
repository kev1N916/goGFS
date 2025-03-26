package common

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

	ClientMasterDeleteRequestType
	ClientMasterDeleteResponseType
	ClientMasterCreateNewChunkRequestType
	ClientMasterCreateNewChunkResponseType

	// ChunkServer-MasterServer Messages
	MasterChunkServerHandshakeType
	MasterChunkServerHeartbeatType
	MasterChunkServerHandshakeResponseType
	MasterChunkServerHeartbeatResponseType
	MasterChunkServerLeaseRequestType

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
	ChunkIds               []int64
	LeaseExtensionRequests []int64
}

type MasterChunkServerHeartbeatResponse struct {
	ChunksToBeDeleted []int64
	LeaseGrants []int64
	ErrorMessage string
}

type MasterChunkServerLeaseRequest struct {
	ChunkHandle int64
}
type ClientMasterReadRequest struct {
	Filename string // Capitalized field name
	Offset   int    // Capitalized field name
}

type ClientMasterCreateNewChunkRequest struct{
	Filename string
}

type ClientMasterCreateNewChunkResponse struct{
	Filename string
}


// type UnserializedResponse struct {
// 	MessageType       MessageType // Capitalized field name
// 	ResponseBodyBytes []byte      // Capitalized field name
// }

type ClientMasterReadResponse struct {
	ChunkHandle  int64    // Capitalized field name
	ChunkServers []string // Capitalized field name
	ErrorMessage string
}

type ClientMasterWriteRequest struct {
	Filename string // Capitalized field name
}

type ClientMasterDeleteRequest struct {
	Filename string // Capitalized field name
}

type ClientMasterDeleteResponse struct {
	Status bool // Capitalized field name
	ErrorMessage string

}

type ClientMasterWriteResponse struct {
	ChunkHandle           int64
	MutationId            int64
	PrimaryChunkServer    string   // Capitalized field name
	SecondaryChunkServers []string // Capitalized field name
	ErrorMessage string
}

type ClientChunkServerReadRequest struct {
	ChunkHandle int64 // Capitalized field name
	// OffsetStart int64 // Capitalized field name
	// OffsetEnd   int64 // Capitalized field name
}

type PrimaryChunkCommitRequest struct {
	ChunkHandle int64 // ID of the chunk to commit
	MutationId  int64 // ID of the mutation
    SecondaryServers []string // List of secondary servers
	SizeOfData int
}

type PrimaryChunkCommitResponse struct {
	Offset int64
	Status bool // 1 if the commit was succeffuly
	ErrorMessage string
}

type ClientChunkServerReadResponse struct {
	ResponseLength int32 // Capitalized field name
	ErrorMessage string
}

type ClientChunkServerWriteResponse struct {
	Status bool // 1 if the message was received succefully
	ErrorMessage string

}

type InterChunkServerCommitRequest struct{
	ChunkHandle int64
	ChunkOffset int64
	MutationOrder []int64
}


type InterChunkServerCommitResponse struct{
	Status bool
	ErrorMessage string
}
type ClientChunkServerWriteRequest struct {
	MutationId int64
	ChunkHandle int64  // Capitalized field name
}
