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

	"github.com/involk-secure-1609/goGFS/common"
	"github.com/involk-secure-1609/goGFS/helper"
)

type Client struct {
	clientMu sync.Mutex
	masterServer string
	chunkCache   map[string][]string
}

func NewClient(masterServer string) *Client {
	return &Client{
		masterServer: masterServer,
	}
}

// SerializeMessage serializes any message type into bytes
func serializeMessage(msgType common.MessageType, msg any) ([]byte, error) {
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

func (client *Client) readFromMasterServer(readRequest common.ClientMasterReadRequest) (*common.ClientMasterReadResponse, error) {

	conn, dialErr := net.Dial("tcp", client.masterServer)
	if dialErr != nil {
		return nil, fmt.Errorf("failed to dial master server: %w", dialErr) // Wrap error for more context
	}
	defer conn.Close() // Ensure connection is closed when function exits

	requestBytes, serializeErr := serializeMessage(common.ClientMasterReadRequestType,readRequest)
	if serializeErr != nil {
		return nil, fmt.Errorf("failed to serialize read request: %w", serializeErr) // Wrap error
	}

	bytesWritten, writeErr := conn.Write(requestBytes)
	if writeErr != nil {
		return nil, fmt.Errorf("failed to write read request to connection: %w", writeErr) // Wrap error
	}
	log.Printf("Bytes written for request: %d", bytesWritten) // More descriptive log

	messageType,messageBody,err:=helper.ReadMessage(conn)
	if err!=nil{
		return nil,err
	}
	if messageType!=common.ClientMasterReadResponseType{
		return nil,fmt.Errorf("expected response type %d but got %d",common.PrimaryChunkCommitResponseType,messageType)
	}
	// To reconstruct (deserialize):
	var response common.ClientMasterReadResponse
	responseReader := bytes.NewReader(messageBody)
	deserializeErr := binary.Read(responseReader, binary.LittleEndian, &response)
	if deserializeErr != nil {
		return nil, fmt.Errorf("failed to deserialize read response: %w", deserializeErr) // Wrap error
	}
	fmt.Printf("Reconstructed Response Struct: %+v\n", response) // Renamed variable in Printf

	return &response, nil

}

func (client *Client) cacheChunkServers(fileName string, readResponse *common.ClientMasterReadResponse) {
	client.clientMu.Lock()
	defer client.clientMu.Unlock()
	client.chunkCache[fileName] = readResponse.ChunkServers
}

func (client *Client) readFromChunkServer(offsetStart int64, offsetEnd int64, readResponse *common.ClientMasterReadResponse) error {
	for _, chunkServer := range readResponse.ChunkServers {
		conn, dialErr := net.Dial("tcp", chunkServer)
		if dialErr != nil {
			log.Printf("failed to dial chunk server: %v", dialErr) // Log error
			continue
		}
		defer conn.Close() // Ensure connection is closed when function exits

		chunkServerReadRequest := common.ClientChunkServerReadRequest{
			ChunkHandle: readResponse.ChunkHandle,
			OffsetStart: offsetStart,
			OffsetEnd:   offsetEnd,
		}

		requestBytes, serializeErr := serializeMessage(common.ClientChunkServerReadRequestType,chunkServerReadRequest)

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

func (client *Client) writeToMasterServer(request common.ClientMasterWriteRequest) (*common.ClientMasterWriteResponse ,error){

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

	messageType,messageBody,err:=helper.ReadMessage(conn)
	if err!=nil{
		return nil,err
	}
	if messageType!=common.ClientMasterWriteResponseType{
		return nil,fmt.Errorf("expected response type %d but got %d",common.PrimaryChunkCommitResponseType,messageType)
	}

	decoder:=gob.NewDecoder(bytes.NewReader(messageBody))
	var response common.ClientMasterWriteResponse
	err=decoder.Decode(&response)
	if err!=nil{
		return nil,err
	}


	return &response,err
}
func (client *Client) Read(filename string, offset int) error {

	masterReadRequest := common.ClientMasterReadRequest{
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
func (client *Client) replicateChunkToAllServers(writeResponse common.ClientMasterWriteResponse,data []byte) error{

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

func (client *Client) sendWriteRequestToPrimary(port string,writeRequestToPrimary common.PrimaryChunkCommitRequest) error{
	conn, err := net.Dial("tcp", port)
	if err != nil {
		return err
	}
	defer conn.Close()
	
	messageBytes,err:=serializeMessage(common.PrimaryChunkCommitRequestType,writeRequestToPrimary)
	if err!=nil{
		return err
	}
	_,err=conn.Write(messageBytes)
	if err!=nil{
		return err
	}

	messageType,messageBytes,err:=helper.ReadMessage(conn)
	if err!=nil{
		return err
	}
	if messageType!=common.PrimaryChunkCommitResponseType{
		return fmt.Errorf("expected response type %d but got %d",common.PrimaryChunkCommitResponseType,messageType)
	}
	var responseBody common.PrimaryChunkCommitResponse
	decoder:=gob.NewDecoder(bytes.NewReader(messageBytes))
	err=decoder.Decode(&responseBody)
	if err!=nil{
		return err
	}
	if !responseBody.Status{
		return fmt.Errorf("primary chunk commit failed")
	}
	return nil

}
func (client *Client) Write(filename string,data []byte) error {

	masterWriteRequest := common.ClientMasterWriteRequest{
		Filename: filename,
	}

	writeResponse,err:=client.writeToMasterServer(masterWriteRequest)
	if err != nil {
		return err
	}

	err=client.replicateChunkToAllServers(*writeResponse,data);
	if err!=nil{
		return err
	}

	writeRequestToPrimary := common.PrimaryChunkCommitRequest{
		ChunkHandle: writeResponse.ChunkHandle,
		MutationId: writeResponse.MutationId,
		SecondaryServers: writeResponse.SecondaryChunkServers,
	}
	err=client.sendWriteRequestToPrimary(writeResponse.PrimaryChunkServer,writeRequestToPrimary)
	if err!=nil{
		return err
	}
	
	return nil
}
