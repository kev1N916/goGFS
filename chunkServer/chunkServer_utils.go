package chunkserver

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"io/fs"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/involk-secure-1609/goGFS/common"
	"github.com/involk-secure-1609/goGFS/helper"
)

func (chunkServer *ChunkServer) calculateChunkCheckSum(chunkData []byte) ([]byte, error) {
	checkSum := bytes.Buffer{}
	lengthOfChunkData := len(chunkData)
	log.Println(lengthOfChunkData)
	for i := 0; i < lengthOfChunkData; i += 100 {
		if i+100 < lengthOfChunkData {
			crcChecksum := crc32.Checksum(chunkData[i:i+100], chunkServer.checkSummer.crcTable)
			binary.Write(&checkSum, binary.LittleEndian, crcChecksum)
		} else {
			crcChecksum := crc32.Checksum(chunkData[i:lengthOfChunkData], chunkServer.checkSummer.crcTable)
			binary.Write(&checkSum, binary.LittleEndian, crcChecksum)
		}
	}
	return checkSum.Bytes(), nil
}

// opens a checksum file
func (chunkServer *ChunkServer) openChunkCheckSum(chunkHandle int64) (*os.File, error) {
	chunkFileName := chunkServer.translateChunkHandleToFileName(chunkHandle, true)
	chunk, err := os.OpenFile(chunkFileName, os.O_CREATE|os.O_RDWR, 0600)
	return chunk, err
}

// overwrites a checksum file
func (chunkServer *ChunkServer) mutateChunkCheckSum(chunkHandle int64) error {

	chunkServer.mu.RLock()
	// defer chunkServer.mu.Unlock()

	chunkCheckSum := chunkServer.CheckSums[chunkHandle]

	chunkServer.mu.RUnlock()

	checkSumFile, err := chunkServer.openChunkCheckSum(chunkHandle)
	if err != nil {
		return err
	}

	_, err = checkSumFile.WriteAt(chunkCheckSum, 0)
	if err != nil {
		return err
	}
	return checkSumFile.Sync()
}

// verifies the checksum
func (chunkServer *ChunkServer) verifyChunkCheckSum(chunkFile *os.File, chunkHandle int64) (*bytes.Buffer, bool, error) {
	chunkBytes := bytes.Buffer{}

	written, err := io.Copy(&chunkBytes, bufio.NewReader(chunkFile))
	if err != nil {
		if err != io.EOF {
			return nil, false, err
		}
	}

	log.Println("number of bytes written", written)

	log.Println("length of chunkBytes during verification", chunkBytes.Len())
	if chunkBytes.Len() == 0 {
		chunkServer.setCheckSum(chunkHandle, -1, []byte{}, []byte{})
		return &chunkBytes, true, nil
	}
	presentCheckSum, present := chunkServer.getCheckSum(chunkHandle)
	if !present {
		return nil, false, errors.New("checksum not present")
	}

	log.Println("length of calculatedChecksum", len(presentCheckSum))

	validateChkSum := func(calculatedChksum uint32, chunkIndex int) error {
		validCheckSum := binary.LittleEndian.Uint32(presentCheckSum[chunkIndex*4 : chunkIndex*4+4])
		if calculatedChksum != validCheckSum {
			log.Println(validCheckSum, calculatedChksum)
			return errors.New("checksum does not match")
		}
		return nil
	}

	chunkData := chunkBytes.Bytes()
	lengthOfChunkData := len(chunkData)
	for i := 0; i < lengthOfChunkData; i += 100 {
		chunkIndex := i / 100

		if i+100 < lengthOfChunkData {
			crcChecksum := crc32.Checksum(chunkData[i:i+100], chunkServer.checkSummer.crcTable)
			err = validateChkSum(crcChecksum, chunkIndex)
			if err != nil {
				return nil, false, nil
			}
		} else {
			crcChecksum := crc32.Checksum(chunkData[i:lengthOfChunkData], chunkServer.checkSummer.crcTable)
			err = validateChkSum(crcChecksum, chunkIndex)
			if err != nil {
				return nil, false, nil
			}
		}
	}

	log.Println("validation successfull")

	return &chunkBytes, true, nil
}

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

	chunk, err := chunkServer.openChunk(chunkHandle)
	if err != nil {
		return err
	}

	defer chunk.Close()
	_, err = chunk.Write(fileData)
	return err
}

func (chunkServer *ChunkServer) transferFile(conn net.Conn, chunkHandle int64) error {
	var chunkPresent byte // flag used to detect if any error has occurred
	chunkPresent = 1

	chunk, err := chunkServer.openChunk(chunkHandle)
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

	chunkData, verified, err := chunkServer.verifyChunkCheckSum(chunk, chunkHandle)
	if err != nil || !verified {
		log.Println("ERROR ", err)
		chunkPresent = 0
		_, err = conn.Write([]byte{chunkPresent})
		if err != nil {
			log.Println("ERROR IN WRITING MESSAGE TO CLIENT WHICH SAYS CHUNK IS NOT PRESENT")
			return err
		}
		return nil
	}

	chunkLength := len(chunkData.Bytes())

	// if the files size is 0 then also we return an error
	if chunkLength == 0 {
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
	binary.LittleEndian.PutUint32(sizeBuf, uint32(chunkLength))
	_, err = conn.Write(sizeBuf)
	if err != nil {
		return err
	}

	_, err = io.Copy(conn, chunkData)
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

		log.Println("Processing commit request batch of length",len(pendingCommits))
		chunkServer.processCommitBatch(pendingCommits)
	}
}

func (chunkServer *ChunkServer) openChunk(chunkHandle int64) (*os.File, error) {
	chunkFileName := chunkServer.translateChunkHandleToFileName(chunkHandle, false)
	chunk, err := os.OpenFile(chunkFileName, os.O_CREATE|os.O_RDWR, 0666)
	if err!=nil{
		log.Println(err)
		return nil,err
	}
	_, err = chunk.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	return chunk, err
}

func (chunkServer *ChunkServer) translateChunkHandleToFileName(chunkHandle int64, ifCheckSum bool) string {
	fileName := strconv.FormatInt(chunkHandle, 10)
	log.Println(fileName)
	fullPath := filepath.Join(chunkServer.ChunkDirectory, fileName)
	log.Println(fullPath)

	if ifCheckSum {
		return fullPath + ".chksum"
	}
	return fullPath + ".chunk"
}
func (chunkServer *ChunkServer) deleteChunk(chunkHandle int64) {
	chunkServer.mu.Lock()
	defer chunkServer.mu.Unlock()
	fileName := chunkServer.translateChunkHandleToFileName(chunkHandle, false)
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

func (chunkServer *ChunkServer) getLastPartOfChunk(chunkBuffer *bytes.Buffer) *bytes.Buffer {
	chunkBytes := chunkBuffer.Bytes()
	chunkLength := len(chunkBytes)

	lastChunkIndex := chunkLength / 100
	lastChunkBytes := chunkBytes[lastChunkIndex*100 : chunkLength]
	lastChunkBuffer := &bytes.Buffer{}
	lastChunkBuffer.Write(lastChunkBytes)
	return lastChunkBuffer
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
		err = os.Mkdir(chunkServer.ChunkDirectory, 0755)
		return err
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
		} else if strings.HasSuffix(filename, ".chksum") {
			// Remove the ".chunk" extension
			numberPart := strings.TrimSuffix(filename, ".chksum")

			// Convert the remaining part to a 64-bit unsigned integer
			chunkCheckSumNumber, err := strconv.ParseInt(numberPart, 10, 64)
			if err != nil {
				// If conversion fails, log the error and skip this file
				log.Printf("Could not convert %s to number: %v", filename, err)
				continue
			}

			chunkCheckSumFile, err := chunkServer.openChunkCheckSum(chunkCheckSumNumber)
			if err != nil {
				continue
			}
			chunkCheckSumBuffer := bytes.Buffer{}
			_, err = io.Copy(&chunkCheckSumBuffer, chunkCheckSumFile)
			if err != nil {
				continue
			}
			chunkCheckSumFile.Close()
			chunkServer.CheckSums[chunkCheckSumNumber] = chunkCheckSumBuffer.Bytes()
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

func (chunkServer *ChunkServer) getCheckSum(chunkHandle int64) ([]byte, bool) {
	chunkServer.mu.RLock()
	defer chunkServer.mu.RUnlock()
	calculatedCheckSum, present := chunkServer.CheckSums[chunkHandle]
	return calculatedCheckSum, present
}

func (chunkServer *ChunkServer) setCheckSum(chunkHandle int64, oldLength int, newLastCheckSum []byte, restOfTheCheckSum []byte) {
	chunkServer.mu.Lock()
	log.Println("oldLength is", oldLength)
	if oldLength != -1 && oldLength!=0 {
		chunkServer.CheckSums[chunkHandle] = chunkServer.CheckSums[chunkHandle][0 : oldLength-4]
	}

	log.Println(newLastCheckSum)
	chunkServer.CheckSums[chunkHandle] = append(chunkServer.CheckSums[chunkHandle], newLastCheckSum...)
	if len(restOfTheCheckSum) != 0 {
		log.Println("appending restOfTheChecksum", restOfTheCheckSum)
		chunkServer.CheckSums[chunkHandle] = append(chunkServer.CheckSums[chunkHandle], restOfTheCheckSum...)
	}
	log.Println("new length ", len(chunkServer.CheckSums[chunkHandle]))
	chunkServer.mu.Unlock()
}
func (chunkServer *ChunkServer) getChunkMutex(chunkHandle int64) (*sync.Mutex, bool) {

	log.Println("acquiring read lock to get chunk mutex")
	chunkServer.mu.RLock()
	defer chunkServer.mu.RUnlock()

	chunkMutex, exists := chunkServer.chunkManager[chunkHandle]
	log.Println("releasing read lock acquired while trying to get chunk mutex")

	return chunkMutex, exists
}

func (chunkServer *ChunkServer) setChunkMutex(chunkHandle int64) *sync.Mutex {
	log.Println("acquiring write lock to get chunk mutex")

	chunkServer.mu.Lock()
	defer chunkServer.mu.Unlock()
	// Check again in case another goroutine created it while we were waiting
	chunkMutex, exists := chunkServer.chunkManager[chunkHandle]
	if !exists {
		chunkMutex = &sync.Mutex{}
		chunkServer.chunkManager[chunkHandle] = chunkMutex
	}

	log.Println("releasing write lock acquired while trying to get chunk mutex")

	return chunkMutex
}

// Mutates the chunk by first extracting the data from the LRU cache according to the mutationId
// and then wrting it at the prescribed offset. We return the error if the write fails
// or if the data is not present in the cache
func (chunkServer *ChunkServer) mutateChunk(
	file *os.File, chunkHandle int64,
	mutationId int64, chunkOffset int64,
	lastChunkBuffer *bytes.Buffer,
) (int64, error) {

	data, present := chunkServer.LruCache.Get(mutationId)
	if !present {
		log.Println("data not present in LRU CACHE WTF HOW ")
		return 0, errors.New("data not present in lru cache")
	}

	lengthOfMutation := len(data)

	// if the length of our mutation causes the chunk to exceed maximum ChunkSize then we will
	// ask the client to retry the write after the creation of a new ChunkHandle
	if chunkOffset+int64(lengthOfMutation) > common.ChunkSize {
		return 0, common.ErrChunkFull
	}

	var lengthOfExistingChecksum int
	chunkServer.mu.RLock()
	lengthOfExistingChecksum = int(len(chunkServer.CheckSums[chunkHandle]))
	chunkServer.mu.RUnlock()

	// amount of data needed to move to a new part of the chunk
	amountNeeded := 100 - lastChunkBuffer.Len()

	if lengthOfMutation > amountNeeded { // we have more data than needed

		// completes the last part of the chunk
		_, err := lastChunkBuffer.Write(data[0:amountNeeded])
		if err != nil {
			return 0, errors.New("calculating checksum failed")
		}
		// gets the new check sum for the last part of the chunk
		newCheckSumForLastChunk, err := chunkServer.calculateChunkCheckSum(lastChunkBuffer.Bytes())
		if err != nil {
			return 0, errors.New("calculating checksum failed")
		}

		lastChunkBuffer.Reset()

		_, err = lastChunkBuffer.Write(data[amountNeeded:])
		if err != nil {
			return 0, errors.New("calculating checksum failed")
		}

		// gets the new check sum for the rest of the bytes
		checkSumForRest, err := chunkServer.calculateChunkCheckSum(lastChunkBuffer.Bytes())
		if err != nil {
			return 0, errors.New("calculating checksum failed")
		}

		chunkServer.setCheckSum(chunkHandle, lengthOfExistingChecksum, newCheckSumForLastChunk, checkSumForRest)
	} else {

		_, err := lastChunkBuffer.Write(data)
		if err != nil {
			return 0, errors.New("calculating checksum failed")
		}

		newCheckSumForLastChunk, err := chunkServer.calculateChunkCheckSum(lastChunkBuffer.Bytes())
		if err != nil {
			return 0, errors.New("calculating checksum failed")
		}
		if amountNeeded == lengthOfMutation {
			chunkServer.setCheckSum(chunkHandle, -1, newCheckSumForLastChunk, []byte{})
		} else {
			chunkServer.setCheckSum(chunkHandle, lengthOfExistingChecksum, newCheckSumForLastChunk, []byte{})
		}
	}

	lastChunkBufferLength := lastChunkBuffer.Len()
	lastChunkBufferIndex := lastChunkBufferLength / 100

	newLastChunkBytes := lastChunkBuffer.Bytes()[lastChunkBufferIndex*100 : lastChunkBufferLength]

	lastChunkBuffer.Reset()
	lastChunkBuffer.Write(newLastChunkBytes)

	amountWritten, err := file.WriteAt(data, chunkOffset)
	if err != nil {
		return 0, err
	}

	err = file.Sync()
	if err != nil {
		return 0, err
	}

	err = chunkServer.mutateChunkCheckSum(chunkHandle)
	if err != nil {
		return 0, err
	}

	return int64(amountWritten), nil
}

func (chunkServer *ChunkServer) handleMasterIncreaseVersionNumberRequest(requestBodyBytes []byte) {

	request, err := helper.DecodeMessage[common.MasterChunkServerIncreaseVersionNumberRequest](requestBodyBytes)
	if err != nil {
		return
	}

	chunkServer.mu.Lock()
	defer chunkServer.mu.Unlock()

	currentVersionNumber, present := chunkServer.ChunkVersionNumbers[request.ChunkHandle]
	if !present {
		chunkServer.ChunkVersionNumbers[request.ChunkHandle] = 0
	}

	response := common.MasterChunkServerIncreaseVersionNumberResponse{
		Status:                true,
		ChunkHandle:           request.ChunkHandle,
		PreviousVersionNumber: request.PreviousVersionNumber,
	}

	if currentVersionNumber == request.PreviousVersionNumber {
		chunkServer.ChunkVersionNumbers[request.ChunkHandle]++
	} else {
		response.Status = false
	}

	responseBytes, err := helper.EncodeMessage(common.MasterChunkServerIncreaseVersionNumberResponseType, response)
	if err != nil {
		return
	}

	_, err = chunkServer.MasterConnection.Write(responseBytes)
	if err != nil {
		return
	}
}
