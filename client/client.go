package client

import (
	"encoding/binary"
	"errors"
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
	logger         *common.DefaultLog
	WriteOffsets   map[int64][]int64
	clientMu       sync.Mutex
	masterServer   string // contains the masterServer port
	readChunkCache map[ReadChunkCacheKey]ReadChunkCacheValue
	// writeChunkCache map[WriteChunkCacheKey]WriteChunkCacheValue
}

type ReadChunkCacheKey struct {
	FileName   string
	ChunkIndex int
}

type ReadChunkCacheValue struct {
	readResponse *common.ClientMasterReadResponse
	lastRead     time.Time
}

// type WriteChunkCacheKey struct {
// }

// type WriteChunkCacheValue struct {
// 	readResponse *common.ClientMasterReadResponse
// 	timeStamp  time.Time
// }

func NewClient(masterServer string) *Client {
	logger := common.DefaultLogger(common.ERROR)
	return &Client{
		logger:         logger,
		WriteOffsets:   make(map[int64][]int64),
		readChunkCache: make(map[ReadChunkCacheKey]ReadChunkCacheValue),
		// writeChunkCache: make(map[WriteChunkCacheKey]common.ClientMasterWriteResponse),
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
func (client *Client) ReadFromMasterServer(readRequest common.ClientMasterReadRequest) (*common.ClientMasterReadResponse, error) {

	// connect to the master
	// conn, dialErr := net.Dial("tcp", client.masterServer)
	conn, dialErr := helper.DialWithRetry(client.masterServer, 3)
	if dialErr != nil {
		return nil, dialErr
	}
	defer conn.Close()

	// serialize the request
	requestBytes, serializeErr := helper.EncodeMessage(common.ClientMasterReadRequestType, readRequest)
	if serializeErr != nil {
		return nil, common.ErrEncodeMessage
	}

	// write the request
	_, writeErr := conn.Write(requestBytes)
	if writeErr != nil {
		client.logger.Warningf("failed to write read request to connection: %w", writeErr)
		return nil, common.ErrWriteConnection
	}

	// read the response
	messageType, messageBody, err := helper.ReadMessage(conn)
	if err != nil {
		return nil, common.ErrReadMessage
	}

	// if the response is not of intended type return an error
	if messageType != common.ClientMasterReadResponseType {
		client.logger.Warningf("expected response type %d but got %d", common.PrimaryChunkCommitResponseType, messageType)

		return nil, common.ErrIntendedResponseType
	}

	// decode the bytes to a struct
	response, err := helper.DecodeMessage[common.ClientMasterReadResponse](messageBody)
	if err != nil {
		return nil, common.ErrDecodeMessage
	}
	return response, nil

}

/* Client->ChunkServer Operation*/
// The client to chunkServer read request is pretty straightforward,
// In my implemenation we iterate through the list of chunkServers which we have received in our
// response from the master and then issue readRequests to the chunkServers.
// When the request succeeds we break from our loop.
//
func (client *Client) ReadFromChunkServer(readResponse *common.ClientMasterReadResponse) ([]byte, error) {
	// iterate through all the chunkServers
	for _, chunkServer := range readResponse.ChunkServers {

		// initiate a connection with the chunkServer
		conn, dialErr := helper.DialWithRetry(chunkServer, 3)
		if dialErr != nil {
			client.logger.Infof("failed to dial chunk server: %v", dialErr)
			continue
		}
		defer conn.Close() // close the connection after the request ends

		client.logger.Infof("successfully connected to chunkServer ", chunkServer)

		// In our request we only send the chunkHandle , we could send both the chunkHandle
		// and also the start and end offsets which we have to read but in our implementation
		// the chunkSize isnt really that large so reading the entire chunk into the memory
		// should not be a problem. However if the chunkSize is larger then this would cause problems obviously.
		// the ChunkSize according to the official GFS paper is 64mb
		// but that is too large for a toy implementation.
		chunkServerReadRequest := common.ClientChunkServerReadRequest{
			ChunkHandle: readResponse.ChunkHandle,
			ChunkVersion:readResponse.ChunkVersion,
		}

		// we serialize the message
		requestBytes, serializeErr := helper.EncodeMessage(common.ClientChunkServerReadRequestType, chunkServerReadRequest)

		if serializeErr != nil {
			return nil, common.ErrEncodeMessage
		}

		// send the message
		_, writeErr := conn.Write(requestBytes)
		if writeErr != nil {
			conn.Close()
			client.logger.Warningf("error while sending the message to ", chunkServer)
			continue
		}

		// The chunkServer responds by first sending a single byte which indicates whether the chunkIs present or
		// not on its server , if chunkPresent=1 then its is present otherwise it is not
		chunkPresent := make([]byte, 1)
		_, err := conn.Read(chunkPresent)
		log.Println(chunkPresent[0])
		if err != nil {
			log.Println("connection closed",err.Error())
			conn.Close()
			client.logger.Warningf(err.Error())
			// return nil,err
			continue
		}
		if chunkPresent[0] == 0 {
			client.logger.Infof("chunk with handle %d is not present on %s", readResponse.ChunkHandle, chunkServer)
			conn.Close()
			continue
		}
		// Read file size-> 32 bit number
		sizeBuf := make([]byte, 4)
		_, err = conn.Read(sizeBuf)
		if err != nil {
			client.logger.Warningf(err.Error())
			conn.Close()
			continue
		}

		fileSize := binary.LittleEndian.Uint32(sizeBuf)
		client.logger.Infof("Receiving file of size: %d bytes\n", fileSize)

		fileData := make([]byte, fileSize)

		// Read file contents from connection into the byte array
		// we use io.ReadFull as it is more efficient in copying large files
		bytesReceived, err := io.ReadFull(conn, fileData)
		if err != nil {
			client.logger.Warningf(err.Error())
			conn.Close()
			// return nil,err
			continue
		}

		client.logger.Infof("File received successfully. %d bytes read\n", bytesReceived)

		return fileData, nil

	}

	return nil, common.ErrReadFromChunkServer
}

// During a client Read the file name and the offset in the file is specified,
// the client first requests metadata information from the master,
// the master response includes the chunkId to which this offset belongs and the chunkServers which hold this chunkId
// The client then caches this response and further reads for these chunks no longer require an interaction with
// the master. The client then requests any of the chunkServers (most likely the closest) for the data
// Since in this implementation there is no difference in terms of distance between the client and the chunkServers,
// we can send our request to any of the servers.
func (client *Client) Read(filename string, chunkIndex int) ([]byte, error) {

	// formulates the request
	masterReadRequest := common.ClientMasterReadRequest{
		Filename:   filename,
		ChunkIndex: chunkIndex,
	}

	readCacheKey := ReadChunkCacheKey{
		FileName:   filename,
		ChunkIndex: chunkIndex,
	}

	var readResponse *common.ClientMasterReadResponse
	var err error
	// check if the chunkServers are already cached
	readCacheVal, ok := client.presentInReadCache(readCacheKey)
	if !ok {
		// if not present then we send the request to the 
		client.logger.Infof("sending read request to master for file %s", filename)

		// sends the request to the master
		readResponse, err = client.ReadFromMasterServer(masterReadRequest)
		if err != nil {
			return nil, err
		}

		readCacheValue:=ReadChunkCacheValue{
			readResponse: readResponse,
			lastRead:     time.Now(),
		}
		// cache the chunkServers which are present in the response
		client.cacheMasterReadResponse(readCacheKey, readCacheValue)

	} else {
		client.logger.Infof("using cached chunkServers for file %s", filename)
		readResponse=readCacheVal.readResponse
	}
	// if the master replies with an error message then something has gone wrong
	if readResponse.ErrorMessage != "" {
		return nil, errors.New(readResponse.ErrorMessage)
	}

	client.logger.Infof("servers returned from master", readResponse.ChunkServers)

	// read from any of the chunkServers
	chunkData, err := client.ReadFromChunkServer(readResponse)
	if err != nil {
		return nil, err
	}

	client.logger.Infof("received chunk data", chunkData)

	return chunkData, nil
}

/* CLIENT WRITE OPERATIONS */

// Client->Master Operation
// The client first sends the write request to the master.
// the master replies with the latest ChunkHandle of the file, the primary chunk server ,
//
//	the secondary chunkServers, a MutationId and a optional error message.
//
// The MutationId is used to provide a serial order to multiple writes/mutations in case there are multiple writes.
func (client *Client) WriteToMasterServer(request common.ClientMasterWriteRequest) (*common.ClientMasterWriteResponse, error) {

	// connect to the master
	conn, err := helper.DialWithRetry(client.masterServer, 3)
	if err != nil {
		client.logger.Warningf("Failed to connect to master server at", client.masterServer)
		return nil, err
	}
	defer conn.Close()

	requestBytes, err := helper.EncodeMessage(common.ClientMasterWriteRequestType, request)
	if err != nil {
		return nil, common.ErrEncodeMessage
	}
	
	_, err = conn.Write(requestBytes)
	if err != nil {
		return nil, common.ErrWriteConnection
	}

	messageType, messageBody, err := helper.ReadMessage(conn)
	if err != nil {
		return nil, common.ErrReadMessage
	}

	// if the response is not of intended type return an error
	if messageType != common.ClientMasterWriteResponseType {
		client.logger.Infof("expected response type %d but got %d", common.PrimaryChunkCommitResponseType, messageType)
		return nil, common.ErrIntendedResponseType
	}

	// decode the response
	response, err := helper.DecodeMessage[common.ClientMasterWriteResponse](messageBody)
	if err != nil {
		return nil, common.ErrDecodeMessage
	}

	if response.ErrorMessage != "" {
		return nil, errors.New(response.ErrorMessage)
	}

	return response, err
}

/* Client->ChunkServer Operation*/
// This function is used to push data to the chunkServers.
// It consists of a retry mechanism with exponential backoff along with Jitter.
// We will try multiple times to push the data to the server and only if all of our attempts
// fail will we stop and return an error.
func (client *Client) WriteChunkToChunkServer(chunkServerPort string, request common.ClientChunkServerWriteRequest) error {
	maxRetries := 5                          // setting the maxAttempts
	initialBackoff := 100 * time.Millisecond // setting the initial backoff
	maxBackoff := 5 * time.Second            // setting the max backoff period

	for attempt := range maxRetries {
		// Try to send the data
		err := func() error {
			// Establish connection to the chunk server
			conn, err := net.Dial("tcp", chunkServerPort)
			if err != nil {
				client.logger.Warningf("failed to connect to chunk server %s: %w", chunkServerPort, err)
				return common.ErrDialServer
			}
			defer conn.Close()

			// Once we send this request the chunkServer is aware that it needs to read data and
			// uses io.ReadFull to read the data
			requestBytes, err := helper.EncodeMessage(common.ClientChunkServerWriteRequestType, request)
			if err != nil {
				return common.ErrEncodeMessage
			}
			_, err = conn.Write(requestBytes)
			if err != nil {
				return common.ErrWriteConnection
			}

			// read the response from the ChunkServer
			messageType, messageBytes, err := helper.ReadMessage(conn)
			if err != nil {
				client.logger.Infof("error on one of the chunk Servers")
				return common.ErrReadMessage
			}

			// check if there is any error
			if messageType != common.ClientChunkServerWriteResponseType {
				return common.ErrIntendedResponseType
			}

			// receives the acknowledgement response from the chunkServer
			response, err := helper.DecodeMessage[common.ClientChunkServerWriteResponse](messageBytes)
			if err != nil {
				return common.ErrDecodeMessage
			}
			// checks if there is any error on the server side
			if !response.Status {
				return common.ErrOnChunkServer
			}

			return nil
		}()

		// If successful, return nil
		if err == nil {
			return nil
		}

		// If this was the last attempt, return the error
		if attempt == maxRetries-1 {
			client.logger.Errorf("failed to write to chunk server after %d attempts: %w", maxRetries, err)
			return common.ErrWriteToChunkServer
		}

		// Calculate backoff with exponential increase and jitter
		backoff := min(initialBackoff*time.Duration(1<<uint(attempt)), maxBackoff)

		// Add jitter (±25% of backoff)
		jitter := time.Duration(rand.Int64N(int64(backoff) / 2))
		if rand.IntN(2) == 0 {
			backoff -= jitter
		} else {
			backoff += jitter
		}

		client.logger.Infof("Write to chunk server %s failed (attempt %d/%d): %v. Retrying in %v",
			chunkServerPort, attempt+1, maxRetries, err, backoff)

		time.Sleep(backoff)
	}

	// This should never be reached due to the return in the last iteration of the loop
	return common.ErrWriteToChunkServer
}

// Client->ChunkServer Operation
// Final request in the entire write lifecycle which is sent to the primary ChunkServer.
// This request is responsible for the mutation which is applied to the chunk
func (client *Client) SendCommitRequestToPrimary(port string, writeRequestToPrimary common.PrimaryChunkCommitRequest) (int64, error) {
	conn, err := helper.DialWithRetry(port, 3)
	if err != nil {
		client.logger.Warningf("Failed to connect to primary chunk server")
		return -1, err
	}
	defer conn.Close()

	messageBytes, err := helper.EncodeMessage(common.PrimaryChunkCommitRequestType, writeRequestToPrimary)
	if err != nil {
		return -1, common.ErrEncodeMessage
	}
	_, err = conn.Write(messageBytes)
	if err != nil {
		return -1, common.ErrWriteConnection
	}

	client.logger.Infof("SENT PRIMARY WRITE REQUEST")

	messageType, messageBytes, err := helper.ReadMessage(conn)
	if err != nil {
		return -1, common.ErrReadMessage
	}
	if messageType != common.PrimaryChunkCommitResponseType {
		client.logger.Infof("expected response type %d but got %d", common.PrimaryChunkCommitResponseType, messageType)
		return -1, common.ErrIntendedResponseType
	}
	// var responseBody common.PrimaryChunkCommitResponse
	// decoder:=gob.NewDecoder(bytes.NewReader(messageBytes))
	// err=decoder.Decode(&responseBody)
	responseBody, err := helper.DecodeMessage[common.PrimaryChunkCommitResponse](messageBytes)
	if err != nil {
		return -1, common.ErrDecodeMessage
	}

	if !responseBody.Status {
		// returns errors.New() with the same message body as custom error will not be the same as returning the custom error
		if responseBody.ErrorMessage == common.ErrChunkFull.Error() {
			return -1, common.ErrChunkFull
		}
		return -1, errors.New(responseBody.ErrorMessage)
	}
	return responseBody.Offset, nil

}

// This function sends a request to the master calling for the creation of a new chunkHandle for
// the file we have specified.
func (client *Client) CreateNewChunkForFile(filename string, lastChunkHandle int64) error {

	newChunkRequest := common.ClientMasterCreateNewChunkRequest{
		Filename:        filename,
		LastChunkHandle: lastChunkHandle,
	}

	requestBytes, err := helper.EncodeMessage(common.ClientMasterCreateNewChunkRequestType, newChunkRequest)
	if err != nil {
		return common.ErrEncodeMessage
	}
	conn, err := helper.DialWithRetry(client.masterServer, 3)
	if err != nil {
		client.logger.Warningf("Failed to connect to master server")
		return err
	}
	defer conn.Close()
	_, err = conn.Write(requestBytes)
	if err != nil {
		return common.ErrWriteConnection
	}

	messageType, messageBytes, err := helper.ReadMessage(conn)
	if err != nil {
		return common.ErrReadConnection
	}
	if messageType != common.ClientMasterCreateNewChunkResponseType {
		return common.ErrIntendedResponseType
	}

	responseBody, err := helper.DecodeMessage[common.ClientMasterCreateNewChunkResponse](messageBytes)
	if err != nil {
		return common.ErrDecodeMessage
	}

	if !responseBody.Status {
		return common.ErrCreateNewChunk
	}

	client.logger.Infof("new chunkHandle created for " + filename)
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
func (client *Client) Write(filename string, data []byte) error {

	// formulate the request
	masterWriteRequest := common.ClientMasterWriteRequest{
		Filename: filename,
	}

	// send the request to the master
	// In our implementaion of writes, the client just specifies which file it wants to write to,
	// the master replies with the latest chunkHandle and also the primary and seconday servers of this chunk
	writeResponse, err := client.WriteToMasterServer(masterWriteRequest)
	if err != nil {
		client.logger.Warningf("failed during -> WriteToMasterServer")
		return errors.Join(err, common.ErrWriteToMasterServer)
	}

	// push the data to be written to all the chunkServers which the master has returned to us
	err = client.ReplicateChunkDataToAllServers(writeResponse, data)
	if err != nil {
		client.logger.Warningf("failed during -> ReplicateChunkDataToAllServers")
		return err
	}

	// the write request to the primary is the final request which is responsible for mutating
	// the chunk, this request contains the ChunkHandle,mutationId as well as the secondaryServers
	// and also the SizeOfData ?? why tf
	writeRequestToPrimary := common.PrimaryChunkCommitRequest{
		ChunkHandle:      writeResponse.ChunkHandle,
		MutationId:       writeResponse.MutationId,
		SecondaryServers: writeResponse.SecondaryChunkServers,
		// SizeOfData:       int(len(data)),
	}

	chunkHandle := writeRequestToPrimary.ChunkHandle
	// send the write request to the primary
	writeOffset, err := client.SendCommitRequestToPrimary(writeResponse.PrimaryChunkServer, writeRequestToPrimary)
	if err != nil {
		// there are two type of errors that can be returned
		// one is a specific error which is occurs only if the chunk which the client is trying to
		// mutate is already full or the current append/write causes it to overflow
		// in such a case we will try to create a new chunk by sending a request to the master
		// and then retry the entire write operation from the beginning
		if err == common.ErrChunkFull {
			// creates a new chunk for the file
			client.logger.Infof("SENDING CREATE NEW CHUNK AS OVERFLOW IS GOING TO OCCUR REQUEST")
			err = client.CreateNewChunkForFile(filename, chunkHandle)
			if err != nil {
				return err
			}
			err = client.Write(filename, data)
			return err
		}
		client.logger.Infof("failed during -> SendCommitRequestToPrimary")
		return err
	}

	client.clientMu.Lock()
	if _, ok := client.WriteOffsets[chunkHandle]; !ok {
		client.WriteOffsets[chunkHandle] = make([]int64, 0)
	}
	client.WriteOffsets[chunkHandle] = append(client.WriteOffsets[chunkHandle], writeOffset)
	client.clientMu.Unlock()

	return nil
}

/* Client->ChunkServer Operation*/
// We push the data we want to write to the chunk to all the chunkServers
// first we push it to the primary, if the write to the primary fails we stop our operation there itself,
// if it doesnt fail then we continue to push the data to all the secondary servers.
func (client *Client) ReplicateChunkDataToAllServers(writeResponse *common.ClientMasterWriteResponse, data []byte) error {

	// the initial request contains the metadata associated with the mutation, namely the mutationId and the chunkHandle
	// upon receiving this request the chunkServer will prepare itself to read the data .
	request := common.ClientChunkServerWriteRequest{
		ChunkHandle: writeResponse.ChunkHandle,
		MutationId:  writeResponse.MutationId,
		ChunkData:   data,
	}

	// write chunk to primary server
	err := client.WriteChunkToChunkServer(writeResponse.PrimaryChunkServer, request)
	if err != nil {
		return errors.New("error because we were unable to write the chunk to all chunkServers")
	}

	// write chunk to secondary chunk servers
	for i := range writeResponse.SecondaryChunkServers {
		err = client.WriteChunkToChunkServer(writeResponse.SecondaryChunkServers[i], request)
		if err != nil {
			return errors.New("error because we were unable to write the chunk to all chunkServers")
		}
	}

	return nil

}

// Client Delete Operation
// Sends a delete request to the master and then depending on the response status , the delete
// request could have succeeded or failed.
func (client *Client) SendDeleteRequestToMaster(deleteRequest common.ClientMasterDeleteRequest) error {
	conn, dialErr := helper.DialWithRetry(client.masterServer, 3)
	if dialErr != nil {
		return dialErr
	}
	defer conn.Close()

	requestBytes, serializeErr := helper.EncodeMessage(common.ClientMasterDeleteRequestType, deleteRequest)
	if serializeErr != nil {
		return common.ErrEncodeMessage
	}

	_, writeErr := conn.Write(requestBytes)
	if writeErr != nil {
		return common.ErrWriteConnection
	}

	messageType, messageBody, err := helper.ReadMessage(conn)
	if err != nil {
		return common.ErrReadMessage
	}
	if messageType != common.ClientMasterDeleteResponseType {
		client.logger.Warningf("expected response type %d but got %d", common.PrimaryChunkCommitResponseType, messageType)
		return common.ErrIntendedResponseType
	}

	response, err := helper.DecodeMessage[common.ClientMasterDeleteResponse](messageBody)
	if err != nil {
		return common.ErrDecodeMessage
	}
	if !response.Status {
		return common.ErrDeleteFromMaster
	}
	return nil
}

// Deletes the file which we have entered.
// Deletion from the client said is only a client->master operation.
func (client *Client) Delete(fileName string) error {

	deleteRequest := common.ClientMasterDeleteRequest{
		Filename: fileName,
	}

	// sends the delete request to the master
	err := client.SendDeleteRequestToMaster(deleteRequest)
	if err != nil {
		return err
	}
	return nil
}
