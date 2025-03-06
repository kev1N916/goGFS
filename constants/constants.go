package constants

type MessageType= int

const (

	// Client-ChunkServer Read Messages
    ChunkServerReadRequestType  = iota
    ChunkServerReadResponseType

	// Client-ChunkServer Write Messages
    ChunkServerWriteRequestType
	ChunkServerWriteResponseType

	// Client-MasterServer Read Messages
	MasterReadRequestType
	MasterReadResponseType

	// Client-MasterServer Write Messages
	MasterWriteRequestType
	MasterWriteResponseType

	// ChunkServer-MasterServer Messages
	MasterChunkServerHandshakeType
	MasterChunkServerHeartbeatType
	MasterChunkServerHandshakeResponseType
	MasterChunkServerHeartbeatResponseType
)

type MasterChunkServerHandshake struct{
	Port string
	ChunkIds []int64
}

type MasterChunkServerHandshakeResponse struct{
	Message string
}

type MasterChunkServerHeartbeat struct{
	Port string
	ChunkIds []int64
}
type MasterReadRequest struct {
    MessageType MessageType // Capitalized field name
    Filename    string      // Capitalized field name
    Offset      int         // Capitalized field name
}

type UnserializedResponse struct{
    MessageType     MessageType // Capitalized field name
    ResponseBodyBytes []byte      // Capitalized field name
}

type MasterReadResponse struct{
    ChunkHandle int64    // Capitalized field name
    ChunkServers  []string // Capitalized field name
}

type MasterWriteRequest struct {
    MessageType MessageType // Capitalized field name
    Filename    string      // Capitalized field name
    Offset      int         // Capitalized field name
    Data        []byte      // Capitalized field name
}

type MasterWriteResponse struct {
    PrimaryChunkServer   string   // Capitalized field name
    SecondaryChunkServer []string // Capitalized field name
}

type ChunkServerReadRequest struct{
    ChunkHandle int64 // Capitalized field name
    OffsetStart int64 // Capitalized field name
    OffsetEnd   int64 // Capitalized field name
}

type ChunkServerReadResponse struct{
    ResponseLength int32 // Capitalized field name
}

type ChunkServerWriteResponse struct{
    // Fields can be added here, if any, and they should also be capitalized
}

type ChunkServerWriteRequest struct{
    // Fields can be added here, if any, and they should also be capitalized
}