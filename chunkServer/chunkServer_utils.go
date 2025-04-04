package chunkserver

import (
	"encoding/binary"
	"errors"
	"io"
	"io/fs"
	"net"
	"path/filepath"

	// "io/fs"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/involk-secure-1609/goGFS/common"
	"github.com/involk-secure-1609/goGFS/helper"
)

func (chunkServer *ChunkServer) handleMasterCloneRequest(requestBodyBytes []byte) error {

	// Unmarshal the request body into the appropriate struct

	request, err := helper.DecodeMessage[common.MasterChunkServerCloneRequest](requestBodyBytes)
	if err != nil {
		return err
	}
	err = chunkServer.sendInterChunkServerCloneRequest(request.SourceChunkServer, request.ChunkHandle)
	if err != nil {
		return err
	}
	return nil
}

func (chunkServer *ChunkServer) sendInterChunkServerCloneRequest(sourceChunkServer string, chunkHandle int64) error {
	// This function will be called when we receive a clone request from the master server

	conn, err := helper.DialWithRetry(sourceChunkServer, 3)
	if err != nil {
		return err
	}
	defer conn.Close()

	request := common.InterChunkServerCloneRequest{
		ChunkHandle: chunkHandle,
	}

	requestBody, err := helper.EncodeMessage(common.InterChunkServerCloneRequestType, request)
	if err != nil {
		return err
	}

	_, err = conn.Write(requestBody)
	if err != nil {
		return err
	}

	// The chunkServer responds by first sending a single byte which indicates whether the chunkIs present or
	// not on its server , if chunkPresent=1 then its is present otherwise it is not
	chunkPresent := make([]byte, 1)
	_, err = conn.Read(chunkPresent)
	if err != nil {
		conn.Close()
		return err
	}
	if chunkPresent[0] == 0 {
		conn.Close()
		return err
	}
	// Read file size-> 32 bit number
	sizeBuf := make([]byte, 4)
	_, err = conn.Read(sizeBuf)
	if err != nil {
		conn.Close()
		return err
	}

	fileSize := binary.LittleEndian.Uint32(sizeBuf)

	fileData := make([]byte, fileSize)

	// Read file contents from connection into the byte array
	// we use io.ReadFull as it is more efficient in copying large files
	_, err = io.ReadFull(conn, fileData)
	if err != nil {
		conn.Close()
		return err
	}

	chunk,err:=chunkServer.openChunk(chunkHandle)
	if err!=nil{
		return err
	}

	defer chunk.Close()
	_,err=chunk.Write(fileData)
	return err
}

func (chunkServer *ChunkServer) transferFile(conn net.Conn, fileName string) error {
	var chunkPresent byte // flag used to detect if any error has occurred
	chunkPresent = 1

	chunk, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		log.Println("ERROR ", err)
		chunkPresent = 0
		_, err = conn.Write([]byte{chunkPresent})
		if err != nil {
			log.Println("ERROR IN WRITING MESSAGE TO CLIENT WHICH SAYS CHUNK IS NOT PRESENT")
			return err
		}
		return nil
	}
	defer chunk.Close()

	fileInfo, err := chunk.Stat()
	if err != nil {
		return err
	}
	// if the files size is 0 then also we return an error
	if fileInfo.Size() == 0 {
		chunkPresent = 0
	}

	// we write the flag back to the client
	_, err = conn.Write([]byte{chunkPresent})
	if err != nil {
		return err
	}

	// if the flag is 0 we return
	if chunkPresent == 0 {
		return nil
	}

	// otherwise we first transfer the chunk size and then transfer the
	// entire chunk using io.Copy
	sizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBuf, uint32(fileInfo.Size()))
	_, err = conn.Write(sizeBuf)
	if err != nil {
		return err
	}

	_, err = io.Copy(conn, chunk)
	if err != nil {
		return err
	}
	return nil
}


// This function will be started in a goroutine an continuously handles
// primary commit requests. It buffers commit requests and then after a certain period of time
// goes through the commit requests and separates them according to the chunkHandle.
// Now the requests are anyways in a certain order, we can extract the MutationId from these requests and
// send inter-chunkServer commit requests to the secondary chunkServers specifying this mutation order.
func (chunkServer *ChunkServer) startCommitRequestHandler() {

	log.Println("started commit request handler")

	const batchDuration = 2 * time.Second // specifies a batch duration
	const maxBatchSize = 3

	for {
		// Use a slice to accumulate the commit requests
		pendingCommits := make([]CommitRequest, 0, maxBatchSize)

		// Set up a timer for batching
		timer := time.NewTimer(batchDuration)

		// Accumulate commit requests until either:
		// 1. The batch duration expires
		// 2. We hit the max batch size
		batchComplete := false

		for !batchComplete && len(pendingCommits) < maxBatchSize {
			select {
			case req, ok := <-chunkServer.commitRequestChannel:
				if !ok {
					// Channel was closed, exit the goroutine
					return
				}
				pendingCommits = append(pendingCommits, req)

			case <-timer.C:
				// Timer expired, process the batch
				batchComplete = true
			}
		}

		// If timer hasn't fired yet, stop it to avoid leaks
		if !batchComplete {
			timer.Stop()
		}

		// Skip processing if no requests were accumulated
		if len(pendingCommits) == 0 {
			continue
		}

		// Process the batch of commit requests
		chunkServer.processCommitBatch(pendingCommits)
	}
}

func (chunkServer *ChunkServer) openChunk(chunkHandle int64) (*os.File,error){
	chunkFileName:=chunkServer.translateChunkHandleToFileName(chunkHandle)
	chunk, err := os.OpenFile(chunkFileName, os.O_CREATE|os.O_RDWR, 0600)
	return chunk,err	
}

func (chunkServer *ChunkServer) translateChunkHandleToFileName(chunkHandle int64) string {
	fileName := strconv.FormatInt(chunkHandle, 10)
	log.Println(fileName)
	fullPath := filepath.Join(chunkServer.ChunkDirectory, fileName)
	log.Println(fullPath)
	return fullPath + ".chunk"
}
func (chunkServer *ChunkServer) deleteChunk(chunkHandle int64) {
	chunkServer.mu.Lock()
	defer chunkServer.mu.Unlock()
	fileName := chunkServer.translateChunkHandleToFileName(chunkHandle)
	err := os.Remove(fileName)
	if err != nil {
		log.Println(err)
	}
	newChunkHandles := make([]int64, 0)

	for _, val := range chunkServer.ChunkHandles {
		if val != chunkHandle {
			newChunkHandles = append(newChunkHandles, val)
		}
	}
	chunkServer.ChunkHandles = newChunkHandles
	log.Println(len(chunkServer.ChunkHandles))
}

// loads the chunk handles from the directory which we have passed into the function
func (chunkServer *ChunkServer) loadChunks() error {
	// Create a slice to store chunk files
	var chunkHandles []int64

	// Read all entries in the directory
	entries, err := os.ReadDir(chunkServer.ChunkDirectory)
	if err != nil {
		log.Println(err)
		var pathErr *fs.PathError
		if !errors.As(err, &pathErr) {
			return err
		}
		err = os.Mkdir(chunkServer.ChunkDirectory, 0600)
		if err != nil {
			return err
		}
	}

	for _, entry := range entries {
		// Skip directories
		if entry.IsDir() {
			continue
		}

		// Get the filename
		filename := entry.Name()

		// Check if the file ends with ".chunk"
		if strings.HasSuffix(filename, ".chunk") {
			// Remove the ".chunk" extension
			numberPart := strings.TrimSuffix(filename, ".chunk")

			// Convert the remaining part to a 64-bit unsigned integer
			chunkNumber, err := strconv.ParseInt(numberPart, 10, 64)
			if err != nil {
				// If conversion fails, log the error and skip this file
				log.Printf("Could not convert %s to number: %v", filename, err)
				continue
			}

			chunkHandles = append(chunkHandles, chunkNumber)
		}
	}

	chunkServer.ChunkHandles = chunkHandles
	return nil

}

// Checks if the lease which the chunk server has on the chunk is still valid
func (chunkServer *ChunkServer) checkIfPrimary(chunkHandle int64) bool {
	chunkServer.mu.RLock()
	defer chunkServer.mu.RUnlock()
	leaseGrant, isPrimary := chunkServer.LeaseGrants[chunkHandle]
	if !isPrimary {
		return false
	}
	if time.Since(leaseGrant.GrantTime) >= 60*time.Second {
		return false
	}
	return true
}

// processCommitBatch handles a batch of commit requests
// It basically separates out the commit requests based on the chunkHandle.
// Then it launches goroutines that handle the commits for each chunk Separately
func (chunkServer *ChunkServer) processCommitBatch(requests []CommitRequest) {

	log.Printf("Processing batch of %d commit requests", len(requests))
	// chunkServer.mu.Lock()
	// Group requests by chunk ID for more efficient processing
	chunkBatches := make(map[int64][]CommitRequest)
	for _, req := range requests {
		chunkBatches[req.commitRequest.ChunkHandle] = append(chunkBatches[req.commitRequest.ChunkHandle], req)
	}
	// chunkServer.mu.Unlock()
	// launches separate goroutines for each chunkHandle
	for key, value := range chunkBatches {
		go chunkServer.handleChunkPrimaryCommit(key, value)
	}
}

// inserts the data into the chunkServers LRU cache, the LRU cache is basically a mapping between the mutationId and the data
// we could store the chunkHandle as well as part of the mapping but I dont think thats necessary as we are anyways associating the
// chunkHandle with the mutationId in the subsequent commit requests which are sent between the chunkServers so I guess its fine.
func (chunkServer *ChunkServer) writeChunkToCache(mutationId int64, data []byte) error {
	chunkServer.LruCache.Put(mutationId, data)
	return nil
}

// Mutates the chunk by first extracting the data from the LRU cache according to the mutationId
// and then wrting it at the prescribed offset. We return the error if the write fails
// or if the data is not present in the cache
func (chunkServer *ChunkServer) mutateChunk(file *os.File, mutationId int64, chunkOffset int64) (int64, error) {

	data, present := chunkServer.LruCache.Get(mutationId)
	if !present {
		log.Println("data not present in LRU CACHE WTF HOW ")
		return 0, errors.New("data not present in lru cache")
	}

	// if the length of our mutation causes the chunk to exceed maximum ChunkSize then we will
	// ask the client to retry the write after the creation of a new ChunkHandle
	if chunkOffset+int64(len(data)) > common.ChunkSize {
		return 0, common.ErrChunkFull
	}

	amountWritten, err := file.WriteAt(data, chunkOffset)
	if err != nil {
		return 0, err
	}
	err = file.Sync()
	if err != nil {
		return 0, err
	}
	return int64(amountWritten), nil

}
