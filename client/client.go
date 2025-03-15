package client

import (
	"bytes"
	"encoding/binary"
	"errors"
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
	chunkCache   map[int64][]string
}

func NewClient(masterServer string) *Client {
	return &Client{
		masterServer: masterServer,
	}
}


/* CLIENT READ OPERATIONS */

/* Client->Master Operation*/
func (client *Client) readFromMasterServer(readRequest common.ClientMasterReadRequest) (*common.ClientMasterReadResponse, error) {

	conn, dialErr := net.Dial("tcp", client.masterServer)
	if dialErr != nil {
		return nil, fmt.Errorf("failed to dial master server: %w", dialErr) 
	}
	defer conn.Close() 

	requestBytes, serializeErr := helper.EncodeMessage(common.ClientMasterReadRequestType,readRequest)
	if serializeErr != nil {
		return nil, fmt.Errorf("failed to serialize read request: %w", serializeErr) // Wrap error
	}

	_, writeErr := conn.Write(requestBytes)
	if writeErr != nil {
		return nil, fmt.Errorf("failed to write read request to connection: %w", writeErr) // Wrap error
	}

	messageType,messageBody,err:=helper.ReadMessage(conn)
	if err!=nil{
		return nil,err
	}
	if messageType!=common.ClientMasterReadResponseType{
		return nil,fmt.Errorf("expected response type %d but got %d",common.PrimaryChunkCommitResponseType,messageType)
	}

	response,err:=helper.DecodeMessage[common.ClientMasterReadResponse](messageBody)
	if err!=nil{
		return nil,err
	}
	return response, nil

}

/* Client->ChunkServer Operation*/
func (client *Client) readFromChunkServer(readResponse *common.ClientMasterReadResponse) ([]byte,error) {
	for _, chunkServer := range readResponse.ChunkServers {
		conn, dialErr := net.Dial("tcp", chunkServer)
		if dialErr != nil {
			log.Printf("failed to dial chunk server: %v", dialErr) 
			continue
		}
		defer conn.Close() 

		chunkServerReadRequest := common.ClientChunkServerReadRequest{
			ChunkHandle: readResponse.ChunkHandle,
			// OffsetStart: offsetStart,
			// OffsetEnd:   offsetEnd,
		}

		requestBytes, serializeErr := helper.EncodeMessage(common.ClientChunkServerReadRequestType,chunkServerReadRequest)

		if serializeErr != nil {
			return nil,errors.New("error during encoding of message")
		}

		bytesWritten, writeErr := conn.Write(requestBytes)
		if writeErr != nil {
			return nil,writeErr
		}
		log.Printf("Bytes written for request: %d", bytesWritten) // More descriptive log

		// Read file size
		sizeBuf := make([]byte, 8)
		_, err := io.ReadFull(conn, sizeBuf)
		if err != nil {
			return nil,err
		}

		fileSize := binary.LittleEndian.Uint64(sizeBuf)
		log.Printf("Receiving file of size: %d bytes\n", fileSize)

		fileData := make([]byte, fileSize)

		// Read file contents from connection into the byte array
		bytesReceived, err := io.ReadFull(conn, fileData)
		if err != nil {
			return nil,err
		}

		log.Printf("File received successfully. %d bytes read\n", bytesReceived)

		return fileData,nil

	}

	return nil,fmt.Errorf("failed to read from any chunk server")
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

	if(readResponse.ErrorMessage!=""){
		return errors.New(readResponse.ErrorMessage)
	}
	client.cacheChunkServers(readResponse.ChunkHandle, readResponse)

	chunkData,err := client.readFromChunkServer(readResponse)
	if err != nil {
		return err
	}
	log.Print(chunkData)
	return nil
}


/* CLIENT WRITE OPERATIONS */


/* Client->Master Operation*/
func (client *Client) writeToMasterServer(request common.ClientMasterWriteRequest) (*common.ClientMasterWriteResponse ,error){

	conn, err := net.Dial("tcp", client.masterServer)
	if err != nil {
		return nil,err
	}
	defer conn.Close()
	// var buf bytes.Buffer
	// encoder := gob.NewEncoder(&buf)
	// encoder.Encode(request)

	requestBytes,err:=helper.EncodeMessage(common.ClientMasterWriteRequestType,request)
	if err!=nil{
		return nil,err
	}
	_,err=conn.Write(requestBytes)
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

	// decoder:=gob.NewDecoder(bytes.NewReader(messageBody))
	// var response common.ClientMasterWriteResponse
	// err=decoder.Decode(&response)

	response,err:=helper.DecodeMessage[common.ClientMasterWriteResponse](messageBody)
	if err!=nil{
		return nil,err
	}

	return response,err
}

/* Client->ChunkServer Operation*/
func (client *Client) writeChunkToSingleServer(chunkServerPort string,request common.ClientChunkServerWriteRequest,data []byte) error {
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
			requestBytes,err:=helper.EncodeMessage(common.ClientChunkServerWriteRequestType,request)
			if err!=nil{
				return err
			}
			_,err=conn.Write(requestBytes)
			if err!=nil{
				return err
			}
            // Send the actual data
            _, err = io.Copy(conn, bytes.NewReader(data))
            if err != nil {
                return fmt.Errorf("failed to send data: %w", err)
            }

			messageType,messageBytes,err:=helper.ReadMessage(conn)
			if err!=nil{
				return errors.New("error on one of the chunk Servers")
			}
			if messageType!=common.ClientChunkServerWriteResponseType{
				return errors.New("error on one of the chunk Servers")
			}

			response,err:=helper.DecodeMessage[common.ClientChunkServerWriteResponse](messageBytes)
			if err!=nil{
				return errors.New("error on one of the chunk Servers")
			}
			if !response.Status{
				return errors.New("error on one of the chunk Servers")
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

/* Client->ChunkServer Operation*/
func (client *Client) sendWriteRequestToPrimary(port string,writeRequestToPrimary common.PrimaryChunkCommitRequest) error{
	conn, err := net.Dial("tcp", port)
	if err != nil {
		return err
	}
	defer conn.Close()
	
	// messageBytes,err:=serializeMessage(common.PrimaryChunkCommitRequestType,writeRequestToPrimary)
	// if err!=nil{
	// 	return err
	// }
	messageBytes,err:=helper.EncodeMessage(common.PrimaryChunkCommitRequestType,writeRequestToPrimary)
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
	// var responseBody common.PrimaryChunkCommitResponse
	// decoder:=gob.NewDecoder(bytes.NewReader(messageBytes))
	// err=decoder.Decode(&responseBody)
	responseBody,err:=helper.DecodeMessage[common.PrimaryChunkCommitResponse](messageBytes)
	if err!=nil{
		return err
	}

	if !responseBody.Status{
		return errors.New(responseBody.ErrorMessage)
	}
	// if responseBody.Status && responseBody.ErrorMessage=="chunk full try again with new Chunk"{

	// }
	return nil

}

func (client *Client) createNewChunkForFile(filename string)error{

	newChunkRequest:=common.ClientMasterCreateNewChunkRequest{
		Filename: filename,
	}

	requestBytes,_:=helper.EncodeMessage(common.ClientMasterCreateNewChunkRequestType,newChunkRequest)
	conn, err := net.Dial("tcp", client.masterServer)
	if err != nil {
		return err
	}
	defer conn.Close()
	_,err=conn.Write(requestBytes)
	if err!=nil{
		return err
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
		SizeOfData:int(len(data)),
	}
	err=client.sendWriteRequestToPrimary(writeResponse.PrimaryChunkServer,writeRequestToPrimary)
	if err!=nil{
		if(err.Error()=="chunk full try again with new Chunk"){
			err=client.createNewChunkForFile(filename)
			if err!=nil{
				return err
			}
			err=client.Write(filename,data)
			if err!=nil{
				return err
			}
		}
		return err
	}
	
	return nil
}

/* Client->ChunkServer Operation*/
func (client *Client) replicateChunkToAllServers(writeResponse common.ClientMasterWriteResponse,data []byte) error{

	request:=common.ClientChunkServerWriteRequest{
		ChunkHandle: writeResponse.ChunkHandle,
		MutationId: writeResponse.MutationId,
	}
	err:=client.writeChunkToSingleServer(writeResponse.PrimaryChunkServer,request,data)
	if err!=nil{
		return err
	}

	for i:=range(writeResponse.SecondaryChunkServers){
		err=client.writeChunkToSingleServer(writeResponse.SecondaryChunkServers[i],request,data)
		if err!=nil{
			return err
		}
	}

	return nil

}

/* Client Delete Operations */


func (client *Client) sendDeleteRequestToMaster(deleteRequest common.ClientMasterDeleteRequest) error{
	conn, dialErr := net.Dial("tcp", client.masterServer)
	if dialErr != nil {
		return errors.New("failed to dial master server") 
	}
	defer conn.Close() 

	requestBytes, serializeErr := helper.EncodeMessage(common.ClientMasterDeleteRequestType,deleteRequest)
	if serializeErr != nil {
		return errors.New("failed to serialize read request") // Wrap error
	}

	_, writeErr := conn.Write(requestBytes)
	if writeErr != nil {
		return errors.New("failed to write read request to connection") // Wrap error
	}

	messageType,messageBody,err:=helper.ReadMessage(conn)
	if err!=nil{
		return err
	}
	if messageType!=common.ClientMasterDeleteResponseType{
		return fmt.Errorf("expected response type %d but got %d",common.PrimaryChunkCommitResponseType,messageType)
	}

	response,err:=helper.DecodeMessage[common.ClientMasterDeleteResponse](messageBody)
	if err!=nil{
		return err
	}
	if !response.Status{
		return errors.New("delete operation unsuccessful")
	}
	return nil
}

// func (client *Client) readDeleteResposnse
func(client *Client) Delete(fileName string) error{

	deleteRequest:=common.ClientMasterDeleteRequest{
		Filename: fileName,
	}

	err:=client.sendDeleteRequestToMaster(deleteRequest)
	if err!=nil{
		return err
	}
	return nil
}