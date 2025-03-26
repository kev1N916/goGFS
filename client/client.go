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

// the client struct
type Client struct {
	clientMu sync.Mutex
	masterServer string  // contains the masterServer port
	chunkCache   map[int64][]string  // the chunk cache which is a chunkId->chunkServer mapping
}

func NewClient(masterServer string) *Client {
	return &Client{
		masterServer: masterServer,
	}
}


/* CLIENT READ OPERATIONS */

/* Client->Master Operation*/
/*
During a client read from the master , we specify the fileName and the fileOffset and we get back 
the chunkHandle and the chunkServers which contain this chunkHandle.
Since the paper has not specified whether the connection between and client and the master is persistent,
I have decided to make it a temporary connection.This makes more sense as clients would not be regularly in contact with the master,
thesy would have majority of their messaging with the chunkServers and so keeping a persistent connection would be a waste.
*/
func (client *Client) readFromMasterServer(readRequest common.ClientMasterReadRequest) (*common.ClientMasterReadResponse, error) {

	// connect to the master
	conn, dialErr := net.Dial("tcp", client.masterServer)
	if dialErr != nil {
		return nil, fmt.Errorf("failed to dial master server: %w", dialErr) 
	}
	defer conn.Close() 

	// serialize the request
	requestBytes, serializeErr := helper.EncodeMessage(common.ClientMasterReadRequestType,readRequest)
	if serializeErr != nil {
		return nil, fmt.Errorf("failed to serialize read request: %w", serializeErr) // Wrap error
	}

	// write the request
	_, writeErr := conn.Write(requestBytes)
	if writeErr != nil {
		return nil, fmt.Errorf("failed to write read request to connection: %w", writeErr) // Wrap error
	}

	// read the response 
	messageType,messageBody,err:=helper.ReadMessage(conn)
	if err!=nil{
		return nil,err
	}

	// if the response is not of intended type return an error
	if messageType!=common.ClientMasterReadResponseType{
		return nil,fmt.Errorf("expected response type %d but got %d",common.PrimaryChunkCommitResponseType,messageType)
	}

	// decode the bytes to a struct
	response,err:=helper.DecodeMessage[common.ClientMasterReadResponse](messageBody)
	if err!=nil{
		return nil,err
	}
	return response, nil

}

/* Client->ChunkServer Operation*/
// The client to chunkServer read request is pretty straightforward,
// In my implemenation we iterate through the list of chunkServers which we have received in our 
// response from the master and then issue readRequests to the chunkServers.
// When the request succeeds we break from our loop.
// 
func (client *Client) readFromChunkServer(chunkHandle int64,chunkServers []string) ([]byte,error) {
	// iterate through all the chunkServers
	for _, chunkServer := range chunkServers {

		// initiate a connection with the chunkServer
		conn, dialErr := net.Dial("tcp", chunkServer)
		if dialErr != nil {
			log.Printf("failed to dial chunk server: %v", dialErr) 
			continue
		}
		defer conn.Close() // close the connection after the request ends

		// In our request we only send the chunkHandle , we could send both the chunkHandle 
		// and also the start and end offsets which we have to read but in our implementation
		// the chunkSize isnt really that large so reading the entire chunk into the memory 
		// should not be a problem. However if the chunkSize is larger then this would cause problems obviously.
		// the ChunkSize according to the official GFS paper is 64mb 
		// but that is too large for a toy implementation.
		chunkServerReadRequest := common.ClientChunkServerReadRequest{
			ChunkHandle: chunkHandle,
		}

		// we serialize the message 
		requestBytes, serializeErr := helper.EncodeMessage(common.ClientChunkServerReadRequestType,chunkServerReadRequest)

		if serializeErr != nil {
			return nil,errors.New("error during encoding of message")
		}

		// send the message 
		_, writeErr := conn.Write(requestBytes)
		if writeErr != nil {
			// return nil,writeErr
			continue
		}

		// The chunkServer responds by first sending a single byte which indicates whether the chunkIs present or 
		// not on its server , if chunkPresent=1 then its is present otherwise it is not
		chunkPresent := make([]byte, 1)
		_, err :=conn.Read(chunkPresent)
		if err != nil {
			// return nil,err
			continue
		}
		if (chunkPresent[0]==0){
			continue 
		}
		// Read file size-> 32 bit number 
		sizeBuf := make([]byte, 4)
		_, err = conn.Read(sizeBuf)
		if err != nil {
			continue
		}

		fileSize := binary.LittleEndian.Uint32(sizeBuf)
		log.Printf("Receiving file of size: %d bytes\n", fileSize)

		fileData := make([]byte, fileSize)

		// Read file contents from connection into the byte array
		// we use io.ReadFull as it is more efficient in copying large files
		bytesReceived, err := io.ReadFull(conn, fileData)
		if err != nil {
			// return nil,err
			continue
		}

		log.Printf("File received successfully. %d bytes read\n", bytesReceived)

		return fileData,nil

	}

	return nil,fmt.Errorf("failed to read from any chunk server")
}

// During a client Read the file name and the offset in the file is specified,
// the client first requests metadata information from the master,
// the master response includes the chunkId to which this offset belongs and the chunkServers which hold this chunkId
// The client then caches this response and further reads for these chunks no longer require an interaction with
// the master. The client then requests any of the chunkServers (most likely the closest) for the data
// Since in this implementation there is no difference in terms of distance between the client and the chunkServers,
// we can send our request to any of the servers.
func (client *Client) Read(filename string, offset int) error {

	// formulates the request
	masterReadRequest := common.ClientMasterReadRequest{
		Filename: filename,
		Offset:   offset,
	}

	// sends the request to the master
	readResponse, err := client.readFromMasterServer(masterReadRequest)
	if err != nil {
		return err
	}

	// if the master replies with an error message then something has gone wrong
	if(readResponse.ErrorMessage!=""){
		return errors.New(readResponse.ErrorMessage)
	}

	// cache the chunkServers which are present in the response
	client.cacheChunkServers(readResponse.ChunkHandle, readResponse)

	// read from any of the chunkServers 
	chunkData,err := client.readFromChunkServer(readResponse.ChunkHandle,readResponse.ChunkServers)
	if err != nil {
		return err
	}
	log.Print(chunkData)
	return nil
}


/* CLIENT WRITE OPERATIONS */


// Client->Master Operation
// The client first sends the write request to the master.
// the master replies with the latest ChunkHandle of the file, the primary chunk server ,
//  the secondary chunkServers, a MutationId and a optional error message.
// The MutationId is used to provide a serial order to multiple writes/mutations in case there are multiple writes.
func (client *Client) writeToMasterServer(request common.ClientMasterWriteRequest) (*common.ClientMasterWriteResponse ,error){

	// connect to the master
	conn, err := net.Dial("tcp", client.masterServer)
	if err != nil {
		return nil,err
	}
	defer conn.Close()
	// var buf bytes.Buffer
	// encoder := gob.NewEncoder(&buf)
	// encoder.Encode(request)

	// encode the request
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

	// if the response is not of intended type reuturn an error
	if messageType!=common.ClientMasterWriteResponseType{
		return nil,fmt.Errorf("expected response type %d but got %d",common.PrimaryChunkCommitResponseType,messageType)
	}

	// decoder:=gob.NewDecoder(bytes.NewReader(messageBody))
	// var response common.ClientMasterWriteResponse
	// err=decoder.Decode(&response)

	// decode the response 
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

// Writes in gfs take place in the below order->
// the client first sends a request to the master which includes the chunkHandle and the master replies
// with the primary and secondary servers. The client caches this data for future mutations. It needs
// to contact the master again only when the primary  becomes unreachable or replies that it no longer holds
// a lease. The client then pushes the data which needs to be written to the primary and secondary servers.
// The chunkServers hold this data which the client has just sent them in an LRU cache. It doesnt actually write the 
// data to the chunk yet. After all the chunkServers acknowledge that they have received the data, the client sends
// a commit request to the primary which commits the data on its own server on if this is successfull 
// sends commit requests to all the other secondary servers. If any of these commit requests fail the client will be notified
// and will have to try again.

// In our implementaion of writes, the client just specifies which file it wants to write to,
// the master replies with the latest chunkHandle and also the primary and seconday servers of this chunk
// 
func (client *Client) Write(filename string,data []byte) error {

	// formulate the request
	masterWriteRequest := common.ClientMasterWriteRequest{
		Filename: filename,
	}

	// send the request to the master
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