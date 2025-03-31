package common

import "errors"

// Define custom error types for better error identification
var (
	ErrReadMessage = errors.New("error reading message")

	ErrEncodeMessage = errors.New("error while encoding the message into bytes")

	ErrWriteConnection = errors.New("error while writing into a connection")

	ErrReadConnection = errors.New("error while reading from a connection")

	ErrReadFromChunkServer = errors.New("error because we were unable to read a chunk from any of the given chunk servers")

	ErrIntendedResponseType = errors.New("error because response received is not of intended type")

	ErrDecodeMessage = errors.New("error while decoding bytes into a message struct")

	ErrDialServer = errors.New("error while trying to establish tcp connection with a server using dial")

	ErrWriteToMasterServer = errors.New("failed during write at -> WriteToMasterServer")

	ErrDeleteFromMaster = errors.New("delete operation from master unsuccessful")

	ErrCreateNewChunk     = errors.New("failed to create new chunk")
	ErrWriteToChunkServer = errors.New("error because we were unable to write the chunk to any chunkServer")
	ErrOnChunkServer      = errors.New("error on one of the chunk Servers")
	ErrConnectionClosed   = errors.New("connection closed by client") // Or a more specific error type if needed
	ErrChunkFull          = errors.New("chunk full, try again with new Chunk")
	ErrBadMagic           = errors.New("bad magic ")
)
