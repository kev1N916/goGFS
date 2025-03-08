package constants

type MessageType= int

const (

    ChunkSize = 400  // Chunk Size in bytes
	// Client-ChunkServer Read Messages
    ClientChunkServerReadRequestType  = iota
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
)

type MasterChunkServerHandshake struct{
	ChunkIds []int64
}

type MasterChunkServerHandshakeResponse struct{
	Message string
}

type MasterChunkServerHeartbeat struct{
	Port string
	ChunkIds []int64
    LeaseExtensionRequests []int64
}

type MasterChunkServerHeartbeatResponse struct{
	ChunksToBeDeleted []int64
}
type ClientMasterReadRequest struct {
    Filename    string      // Capitalized field name
    Offset      int         // Capitalized field name
}

type UnserializedResponse struct{
    MessageType     MessageType // Capitalized field name
    ResponseBodyBytes []byte      // Capitalized field name
}

type  ClientMasterReadResponse struct{
    ChunkHandle int64    // Capitalized field name
    ChunkServers  []string // Capitalized field name
}

type  ClientMasterWriteRequest struct {
    Filename    string      // Capitalized field name
    LengthOfData        int64      // Capitalized field name
}

type  ClientMasterWriteResponse struct {
    PrimaryChunkServer   string   // Capitalized field name
    SecondaryChunkServers []string // Capitalized field name
}

type ClientChunkServerReadRequest struct{
    ChunkHandle int64 // Capitalized field name
    OffsetStart int64 // Capitalized field name
    OffsetEnd   int64 // Capitalized field name
}

type  ClientChunkServerReadResponse struct{
    ResponseLength int32 // Capitalized field name
}

type ClientChunkServerWriteResponse struct{
    // Fields can be added here, if any, and they should also be capitalized
}

type ClientChunkServerWriteRequest struct{
    ChunkId    int64      // Capitalized field name
    Data        []byte      // Capitalized field name
}