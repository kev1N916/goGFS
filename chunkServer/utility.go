package chunkserver

import (
	"encoding/binary"
	"errors"
	"log"
	"net"
	"os"
	"time"

	"github.com/involk-secure-1609/goGFS/common"
	"github.com/involk-secure-1609/goGFS/helper"
)


func (chunkServer *ChunkServer) startCommitRequestHandler() {
	const batchDuration = 3 * time.Second
	const maxBatchSize = 100

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


func (chunkServer *ChunkServer) loadChunks() {
	// Open the file
	file, err := os.Open("chunkIds.txt")
	if err != nil {
		log.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// Read numberOfChunks (4 bytes)
	var numberOfChunks uint32
	err = binary.Read(file, binary.LittleEndian, &numberOfChunks)
	if err != nil {
		log.Println("Error reading numberOfChunks:", err)
		return
	}

	log.Println("Number of chunks:", numberOfChunks)

	// Read chunk IDs (each 64-bit = 8 bytes)
	chunkIds := make([]int64, numberOfChunks)
	for i := uint32(0); i < numberOfChunks; i++ {
		err = binary.Read(file, binary.LittleEndian, &chunkIds[i])
		if err != nil {
			log.Println("Error reading chunk ID:", err)
			return
		}
	}
	chunkServer.chunkIds = chunkIds

	// Print loaded chunk IDs
	log.Println("Loaded chunk IDs:", chunkIds)
}



func (chunkServer *ChunkServer) handlePrimaryChunkCommitRequest(conn net.Conn, requestBodyBytes []byte) {
	req, err := helper.DecodeMessage[common.PrimaryChunkCommitRequest](requestBodyBytes)
	if err != nil {
		return
	}
	commitRequest := CommitRequest{
		conn:          conn,
		commitRequest: *req,
	}

	chunkServer.commitRequestChannel <- commitRequest
}
// func (chunk *ChunkServer) addTimeoutForTheConnection(conn net.Conn, interval time.Duration) error {
// 	err := conn.SetDeadline(time.Now().Add(interval))
// 	return err
// }

// processCommitBatch handles a batch of commit requests
func (chunkServer *ChunkServer) processCommitBatch(requests []CommitRequest) {

	log.Printf("Processing batch of %d commit requests", len(requests))
	chunkServer.chunkServerMu.Lock()
	// Group requests by chunk ID for more efficient processing
	chunkBatches := make(map[int64][]CommitRequest)
	for _, req := range requests {
		chunkBatches[req.commitRequest.ChunkHandle] = append(chunkBatches[req.commitRequest.ChunkHandle], req)
	}
	chunkServer.chunkServerMu.Unlock()

	for key, value := range chunkBatches {
		go chunkServer.handleChunkPrimaryCommit(key, value)
	}
}

func (chunkServer *ChunkServer) writeChunkToCache(mutationId int64, data []byte) error {
	chunkServer.lruCache.Put(mutationId, data)
	return nil
}

func (chunkServer *ChunkServer) mutateChunk(file *os.File, mutationId int64, offset int64) (int64, error) {

	data, present := chunkServer.lruCache.Get(mutationId)
	if !present {
		return 0, errors.New("data not present in lru cache")
	}

	amountWritten, err := file.WriteAt(data, offset)
	if err != nil {
		return 0, err
	}
	return int64(amountWritten), nil

}
