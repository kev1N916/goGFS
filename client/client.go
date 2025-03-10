package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/involk-secure-1609/goGFS/constants"
)

type Client struct {
	clientMu sync.Mutex
	clientPort         string
	masterServer string
	chunkCache   map[string][]string
}

func NewClient(masterServer string) *Client {
	return &Client{
		masterServer: masterServer,
	}
}

func (client *Client) readGenericResponse(conn net.Conn) (*constants.UnserializedResponse, error) {
	responseTypeBytes := make([]byte, 1)
	bytesReadType, readTypeErr := conn.Read(responseTypeBytes)
	if readTypeErr != nil {
		return nil, fmt.Errorf("failed to read response type: %w", readTypeErr) // Wrap error
	}
	
	log.Printf("Bytes read for response type: %d", bytesReadType)
	responseMessageType := int(responseTypeBytes[0])
	responseLengthBytes := make([]byte, 4)
	_, readLengthErr := conn.Read(responseLengthBytes)
	if readLengthErr != nil {
		return nil, fmt.Errorf("failed to read response length: %w", readLengthErr) // Wrap error
	}
	
	responseLength := binary.LittleEndian.Uint16(responseLengthBytes)
	responseBodyBytes := make([]byte, responseLength)
	bytesReadBody, readBodyErr := conn.Read(responseBodyBytes)
	if readBodyErr != nil {
		return nil, fmt.Errorf("failed to read response body: %w", readBodyErr) // Wrap error
	}
	if bytesReadBody != int(responseLength) {
		return nil, fmt.Errorf("expected to read %d bytes for response body, but read %d bytes", responseLength, bytesReadBody) // More informative error
	}

	return &constants.UnserializedResponse{MessageType: constants.MessageType(responseMessageType), ResponseBodyBytes: responseBodyBytes}, nil
}
// func serializeChunkServerReadRequest(chunkServerReadRequest *constants.ClientChunkServerReadRequest) ([]byte, error) {
// 	readRequestInBytes := make([]byte, 0)
// 	readRequestInBytes = append(readRequestInBytes, byte(constants.ClientChunkServerReadRequestType))
// 	var buf bytes.Buffer
// 	err := binary.Write(&buf, binary.LittleEndian, chunkServerReadRequest) // Choose endianness
// 	if err != nil {
// 		return nil, err
// 	}
// 	requestBytes := buf.Bytes()
// 	lengthOfRequest := len(requestBytes)
// 	binary.LittleEndian.AppendUint16(readRequestInBytes, uint16(lengthOfRequest))
// 	readRequestInBytes = append(readRequestInBytes, requestBytes...)
// 	return readRequestInBytes, nil
// }

// func serializeMasterReadRequest(masterReadRequest *constants.ClientMasterReadRequest) ([]byte, error) {
// 	readRequestInBytes := make([]byte, 0)
// 	readRequestInBytes = append(readRequestInBytes, byte(constants.ClientMasterReadRequestType))
// 	var buf bytes.Buffer
// 	err := binary.Write(&buf, binary.LittleEndian, masterReadRequest) // Choose endianness
// 	if err != nil {
// 		return nil, err
// 	}
// 	requestBytes := buf.Bytes()
// 	lengthOfRequest := len(requestBytes)
// 	binary.LittleEndian.AppendUint16(readRequestInBytes, uint16(lengthOfRequest))
// 	readRequestInBytes = append(readRequestInBytes, requestBytes...)
// 	return readRequestInBytes, nil
// }

// SerializeMessage serializes any message type into bytes
func serializeMessage(msgType constants.MessageType, msg interface{}) ([]byte, error) {
    // Initialize byte slice with message type
    serialized := make([]byte, 1)
    serialized[0] = byte(msgType)
    
    // Serialize the message struct
    var buf bytes.Buffer
    err := gob.NewEncoder(&buf).Encode(msg)
    if err != nil {
        return nil, fmt.Errorf("encode error: %w", err)
    }
    
    // Get serialized bytes
    msgBytes := buf.Bytes()
    
    // Append length of message (using 4 bytes to support larger messages)
    lengthBytes := make([]byte, 4)
    binary.LittleEndian.PutUint32(lengthBytes, uint32(len(msgBytes)))
    serialized = append(serialized, lengthBytes...)
    
    // Append message data
    serialized = append(serialized, msgBytes...)
    
    return serialized, nil
}

func (client *Client) readFromMasterServer(readRequest constants.ClientMasterReadRequest) (*constants.ClientMasterReadResponse, error) {

	conn, dialErr := net.Dial("tcp", client.masterServer)
	if dialErr != nil {
		return nil, fmt.Errorf("failed to dial master server: %w", dialErr) // Wrap error for more context
	}
	defer conn.Close() // Ensure connection is closed when function exits

	requestBytes, serializeErr := serializeMessage(constants.ClientMasterReadRequestType,readRequest)
	if serializeErr != nil {
		return nil, fmt.Errorf("failed to serialize read request: %w", serializeErr) // Wrap error
	}

	bytesWritten, writeErr := conn.Write(requestBytes)
	if writeErr != nil {
		return nil, fmt.Errorf("failed to write read request to connection: %w", writeErr) // Wrap error
	}
	log.Printf("Bytes written for request: %d", bytesWritten) // More descriptive log

	unserializedResponse, err := client.readGenericResponse(conn)
	if err != nil {
		return nil, err
	}
	// To reconstruct (deserialize):
	var response constants.ClientMasterReadResponse
	responseReader := bytes.NewReader(unserializedResponse.ResponseBodyBytes)
	deserializeErr := binary.Read(responseReader, binary.LittleEndian, &response)
	if deserializeErr != nil {
		return nil, fmt.Errorf("failed to deserialize read response: %w", deserializeErr) // Wrap error
	}
	fmt.Printf("Reconstructed Response Struct: %+v\n", response) // Renamed variable in Printf

	return &response, nil

}

func (client *Client) cacheChunkServers(fileName string, readResponse *constants.ClientMasterReadResponse) {
	client.chunkCache[fileName] = readResponse.ChunkServers
}

func (client *Client) readFromChunkServer(offsetStart int64, offsetEnd int64, readResponse *constants.ClientMasterReadResponse) error {
	for _, chunkServer := range readResponse.ChunkServers {
		conn, dialErr := net.Dial("tcp", chunkServer)
		if dialErr != nil {
			log.Printf("failed to dial chunk server: %v", dialErr) // Log error
			continue
		}
		defer conn.Close() // Ensure connection is closed when function exits

		chunkServerReadRequest := constants.ClientChunkServerReadRequest{
			ChunkHandle: readResponse.ChunkHandle,
			OffsetStart: offsetStart,
			OffsetEnd:   offsetEnd,
		}

		requestBytes, serializeErr := serializeMessage(constants.ClientChunkServerReadRequestType,chunkServerReadRequest)

		if serializeErr != nil {
			continue
			// return fmt.Errorf("failed to serialize read request: %w", serializeErr) // Wrap error
		}

		bytesWritten, writeErr := conn.Write(requestBytes)
		if writeErr != nil {
			continue 
			// return fmt.Errorf("failed to write read request to connection: %w", writeErr) // Wrap error
		}
		log.Printf("Bytes written for request: %d", bytesWritten) // More descriptive log

		// Read file size
		sizeBuf := make([]byte, 8)
		_, err := io.ReadFull(conn, sizeBuf)
		if err != nil {
			continue
			// fmt.Println("Error reading file size:", err)
			// return err
		}

		fileSize := binary.LittleEndian.Uint64(sizeBuf)
		fmt.Printf("Receiving file of size: %d bytes\n", fileSize)
		// Create a buffer to hold the file data

		fileData := make([]byte, fileSize)

		// Read file contents from connection into the byte array
		bytesReceived, err := io.ReadFull(conn, fileData)
		if err != nil {
			continue
			// fmt.Println("Error receiving file data:", err)
			// return nil, err
		}

		fmt.Printf("File received successfully. %d bytes read\n", bytesReceived)

		// Now, fileData contains the entire file as a byte array
		return nil

	}

	return fmt.Errorf("failed to read from any chunk server")
}

func (client *Client) writeFromMasterServer(request constants.ClientMasterWriteRequest) (*constants.ClientMasterWriteResponse ,error){

	conn, err := net.Dial("tcp", client.masterServer)
	if err != nil {
		return nil,err
	}
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	encoder.Encode(request)

	_,err=conn.Write(buf.Bytes())
	if err!=nil{
		return nil,err
	}

	userializedResponse,err:=client.readGenericResponse(conn)
	if err!=nil{
		return nil,err
	}

	decoder:=gob.NewDecoder(bytes.NewReader(userializedResponse.ResponseBodyBytes))
	var response constants.ClientMasterWriteResponse
	err=decoder.Decode(&response)
	if err!=nil{
		return nil,err
	}


	return &response,err
}
func (client *Client) read(filename string, offset int) error {

	masterReadRequest := constants.ClientMasterReadRequest{
		Filename: filename,
		Offset:   offset,
	}
	readResponse, err := client.readFromMasterServer(masterReadRequest)
	if err != nil {
		return err
	}

	client.cacheChunkServers(filename, readResponse)

	err = client.readFromChunkServer(0, 80, readResponse)
	if err != nil {
		return err
	}
	return nil
}

func (client *Client) writeChunkToSingleServer(chunkServerPort string, data []byte) error {
    maxRetries := 5
    initialBackoff := 100 * time.Millisecond
    maxBackoff := 5 * time.Second
    
    for attempt := range maxRetries {
        // Try to send the data
        err := func() error {
            // Establish connection to the chunk server
            conn, err := net.Dial("tcp", chunkServerPort)
            if err != nil {
                return fmt.Errorf("failed to connect to chunk server %s: %w", chunkServerPort, err)
            }
            defer conn.Close()
            
            // Send data size first (8 bytes, little endian)
            sizeBuf := make([]byte, 8)
            binary.LittleEndian.PutUint64(sizeBuf, uint64(len(data)))
            if _, err = conn.Write(sizeBuf); err != nil {
                return fmt.Errorf("failed to send data size: %w", err)
            }
            
            // Send the actual data
            _, err = io.Copy(conn, bytes.NewReader(data))
            if err != nil {
                return fmt.Errorf("failed to send data: %w", err)
            }
            
            // Wait for acknowledgment (optional, implement based on your protocol)
            // For example, read a success/failure response
            respBuf := make([]byte, 1)
            if _, err = conn.Read(respBuf); err != nil {
                return fmt.Errorf("failed to receive acknowledgment: %w", err)
            }
            
            if respBuf[0] != 1 { // Assuming 1 means success
                return fmt.Errorf("server returned error code: %d", respBuf[0])
            }
            
            return nil
        }()
        
        // If successful, return nil
        if err == nil {
            return nil
        }
        
        // If this was the last attempt, return the error
        if attempt == maxRetries-1 {
            return fmt.Errorf("failed to write to chunk server after %d attempts: %w", maxRetries, err)
        }
        
        // Calculate backoff with exponential increase and jitter
        backoff := min(initialBackoff * time.Duration(1<<uint(attempt)), maxBackoff)
        
        // Add jitter (±25% of backoff)
        jitter := time.Duration(rand.Int64N(int64(backoff) / 2))
        if rand.IntN(2) == 0 {
            backoff -= jitter
        } else {
            backoff += jitter
        }
        
        log.Printf("Write to chunk server %s failed (attempt %d/%d): %v. Retrying in %v", 
            chunkServerPort, attempt+1, maxRetries, err, backoff)
        
        time.Sleep(backoff)
    }
    
    // This should never be reached due to the return in the last iteration of the loop
    return fmt.Errorf("failed to write to chunk server after %d attempts", maxRetries)
}
func (client *Client) replicateChunkToAllServers(writeResponse constants.ClientMasterWriteResponse,data []byte) error{

	err:=client.writeChunkToSingleServer(writeResponse.PrimaryChunkServer,data)
	if err!=nil{
		return err
	}

	for i:=range(writeResponse.SecondaryChunkServers){
		err=client.writeChunkToSingleServer(writeResponse.SecondaryChunkServers[i],data)
		if err!=nil{
			return err
		}
	}

	return nil

}

func (client *Client) commitChunkToPrimary(port string) error{
	conn, err := net.Dial("tcp", port)
	if err != nil {
		return err
	}
	defer conn.Close()
	
	var bytes bytes.Buffer
	encoder := gob.NewEncoder(&bytes)
	err = encoder.Encode(constants.PrimaryChunkCommitRequest{

	})
	// Send commit message
	_, err = conn.Write([]byte{1})
	if err != nil {
		return err
	}
	
	// Read acknowledgment
	ack := make([]byte, 1)
	_, err = conn.Read(ack)
	if err != nil {
		return err
	}
	
	if ack[0] != 1 {
		return fmt.Errorf("commit failed with error code %d", ack[0])
	}
	
	return nil

}
func (client *Client) write(filename string,data []byte) error {

	masterWriteRequest := constants.ClientMasterWriteRequest{
		Filename: filename,
	}

	writeResponse,err:=client.writeFromMasterServer(masterWriteRequest)
	if err != nil {
		return err
	}

	err=client.replicateChunkToAllServers(*writeResponse,data);
	if err!=nil{
		return err
	}

	err=client.commitChunkToPrimary(writeResponse.PrimaryChunkServer)
	if err!=nil{
		return err
	}



	
	return nil
}
