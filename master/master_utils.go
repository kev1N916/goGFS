package master

import (
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"net"
	"os"
	"strings"
	"time"

	"github.com/involk-secure-1609/goGFS/common"
	"github.com/involk-secure-1609/goGFS/helper"
)

func (master *Master) getMetadataForFile(filename string) (Chunk, []string, error) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return Chunk{}, nil, err
	}
	chunkOffset := fileInfo.Size() / common.ChunkSize

	master.mu.Lock()
	defer master.mu.Unlock()

	chunk := master.fileMap[filename][chunkOffset]
	chunkServers := master.chunkHandler[chunk.ChunkHandle]

	return chunk, chunkServers, nil
}

// Chooses the first threee chunk servers from our min_heap
// server list. In the paper chunk servers are chosen for clients
// based on how close the chunk servers are to the client however in this implementation
// that really is not possible so I have just used a min heap.
func (master *Master) chooseChunkServers() []string {
	// master.mu.Lock()
	// defer master.mu.Unlock()
	servers := make([]string, 0)
	for i := range 3 {
		server := master.serverList[i]
		servers = append(servers, server.server)
		master.serverList.update(server, server.NumberOfChunks+1)
	}

	return servers
}

// Uses the snowflake package to generate a globally unique int64 id
func (master *Master) generateNewChunkId() int64 {
	id := master.idGenerator.Generate()
	return id.Int64()
}

// If the lease does not exists we just randomly shuffle the servers and choose the first server as our
// primary and the rest of the servers as our secondary servers
func (master *Master) choosePrimaryIfLeaseDoesNotExist(servers []string) (string, []string) {
	rand.Shuffle(len(servers), func(i, j int) {
		servers[i], servers[j] = servers[j], servers[i]
	})

	primaryServer := servers[0] // First server after shuffling is primary
	var secondaryServers []string
	if len(servers) > 1 {
		secondaryServers = servers[1:] // Remaining servers after shuffling are secondaries
	} else {
		secondaryServers = []string{} // No secondary servers
	}
	return primaryServer, secondaryServers
}

func (master *Master) chooseSecondaryIfLeaseDoesExist(primary string, servers []string) []string {

	secondaryServers := make([]string, 0)
	for _, server := range servers {
		if server != primary {
			secondaryServers = append(secondaryServers, server)
		}
	}
	return secondaryServers
}

// checks if we have a valid lease
func (master *Master) isLeaseValid(lease *Lease, port string) bool {
	if lease == nil {
		return false
	}
	if port != "" {
		if lease.server != port {
			return false
		}
	}
	// checks if the time difference between the lease grant time and the current time is
	// less than 60 seconds
	return time.Now().Unix()-lease.grantTime.Unix() < 60
}

// renews the lease grant by adding a certain 60 seconds of time
// to the lease
func (master *Master) renewLeaseGrant(lease *Lease) {

	newTime := lease.grantTime.Add(60 * time.Second)
	lease.grantTime = newTime
}

// Chooses the primary and secondary servers.
// If there does not exist a valid lease on the chunkHandle then the we choose a new primary server and the secondary servers
// and assign the lease to the primary server chosen. If we already have a pre-existen lease then
//
//	we first check if the lease is valid, if it isnt we do the same thing when there isnt a valid lease.
//	If the lease is valid we renew the lease and choose new secondary servers for the chunk
func (master *Master) choosePrimaryAndSecondary(chunkHandle int64) (string, []string, error) {
	lease, doesLeaseExist := master.leaseGrants[chunkHandle]
	// checks if there is already an existing lease
	if !doesLeaseExist {
		// if there isnt we choose new secondary and primary servers and assign the lease to the primary
		primaryServer, secondaryServers := master.choosePrimaryIfLeaseDoesNotExist(master.chunkHandler[chunkHandle])
		master.leaseGrants[chunkHandle].grantTime = time.Now()
		master.leaseGrants[chunkHandle].server = primaryServer
		err := master.grantLeaseToPrimaryServer(primaryServer, chunkHandle)
		if err != nil {
			return "", nil, err
		}
		return primaryServer, secondaryServers, nil
	}

	// checks if the existing lease is valid
	if master.isLeaseValid(lease, "") {
		// if it is we renew the lease
		master.renewLeaseGrant(lease)
		err := master.grantLeaseToPrimaryServer(lease.server, chunkHandle)
		if err != nil {
			return "", nil, err
		}
		secondaryServers := master.chooseSecondaryIfLeaseDoesExist(lease.server, master.chunkHandler[chunkHandle])
		return lease.server, secondaryServers, nil
	} else {
		// if it isnt valid we choose new primary and secondary servers
		primaryServer, secondaryServers := master.choosePrimaryIfLeaseDoesNotExist(master.chunkHandler[chunkHandle])
		master.leaseGrants[chunkHandle].grantTime = time.Now()
		master.leaseGrants[chunkHandle].server = primaryServer
		err := master.grantLeaseToPrimaryServer(primaryServer, chunkHandle)
		if err != nil {
			return "", nil, err
		}
		return primaryServer, secondaryServers, nil
	}
}

// finds the connection corresponding to a chunk server
func (master *Master) findChunkServerConnection(server string) net.Conn {
	for _, connection := range master.chunkServerConnections {
		if connection.port == server {
			return connection.conn
		}
	}

	return nil
}
func (master *Master) grantLeaseToPrimaryServer(primaryServer string, chunkHandle int64) error {
	conn := master.findChunkServerConnection(primaryServer)
	if conn == nil {
		return errors.New("connection to chunk Server does not exist")
	}

	grantLeaseRequest := common.MasterChunkServerLeaseRequest{
		ChunkHandle: chunkHandle,
	}

	messageBytes, err := helper.EncodeMessage(common.MasterChunkServerLeaseRequestType, grantLeaseRequest)
	if err != nil {
		return err
	}
	_, err = conn.Write(messageBytes)
	if err != nil {
		return nil
	}
	return nil
}

// For deleting the file, we first retrieve the chunks of the file which we have to delete,
// we then rename the file and change the mapping from oldFileName->chunks => newFileName->chunks.
// Before we make the metadata change we log the operation.
func (master *Master) deleteFile(fileName string) error {
	master.mu.Lock()
	chunks, ok := master.fileMap[fileName]
	if !ok {
		master.mu.Unlock()
		return errors.New("file does not exist")
	}
	newFileName := fileName + "/" + time.Now().String() + "/" + ".deleted"
	op := Operation{
		Type:        common.ClientMasterDeleteRequestType,
		File:        fileName,
		ChunkHandle: -1,
		NewName:     newFileName,
	}
	err := master.opLogger.writeToOpLog(op)
	if err != nil {
		return err
	}
	master.fileMap[newFileName] = chunks
	delete(master.fileMap, fileName)
	master.mu.Unlock()
	return nil
}

func (master *Master) tempDeleteFile(fileName string, newName string) {
	master.mu.Lock()
	defer master.mu.Unlock()
	chunks, ok := master.fileMap[fileName]
	if !ok {
		return
	}
	delete(master.fileMap, fileName)
	var newFileName string
	if newName == "" {
		newFileName = fileName + "/" + time.Now().Format(time.DateTime) + "/" + ".deleted"
	} else {
		newFileName = newName
	}
	master.fileMap[newFileName] = chunks
}

func (master *Master) handleChunkCreation(fileName string) (int64, string, []string, error) {
	// opsToLog := make([], 0)
	op := Operation{
		Type:        common.ClientMasterWriteRequestType,
		File:        fileName,
		ChunkHandle: -1,
	}
	var chunk Chunk
	master.mu.Lock()
	defer master.mu.Unlock()

	// if the file does not exist the master creates a new one and
	// creates a new chunk for that file
	_, fileAlreadyExists := master.fileMap[fileName]
	if !fileAlreadyExists {
		master.fileMap[fileName] = make([]Chunk, 0)
		chunk = Chunk{
			ChunkHandle: master.generateNewChunkId(),
		}
		op.ChunkHandle = chunk.ChunkHandle
	}
	if op.ChunkHandle != -1 {
		// log the operation if it has resulted in a new Chunk creation otherwise there is no need to log it
		err := master.opLogger.writeToOpLog(op)
		if err != nil {
			return -1, "", nil, err
		}
		master.fileMap[fileName] = append(master.fileMap[fileName], chunk)
	}
	chunkHandle := master.fileMap[fileName][len(master.fileMap[fileName])-1].ChunkHandle
	// if chunk servers have not been designated for the chunkHandle then we choose them
	_, chunkServerExists := master.chunkHandler[chunkHandle]
	if !chunkServerExists {
		master.chunkHandler[chunkHandle] = master.chooseChunkServers()
	}
	// assign primary and secondary chunkServers
	primaryServer, secondaryServers, err := master.choosePrimaryAndSecondary(chunkHandle)
	if err != nil {
		return -1, "", nil, err
	}
	// writeResponse := common.ClientMasterWriteResponse{
	// 	ChunkHandle:           chunkHandle,
	// 	// a mutationId is generated so that during the commit request we can mantain an order
	// 	// between concurrent requests
	// 	MutationId:            master.idGenerator.Generate().Int64(),
	// 	PrimaryChunkServer:    primaryServer,
	// 	SecondaryChunkServers: secondaryServers,
	// }
	return chunkHandle, primaryServer, secondaryServers, nil

}

// Helper function to add a file-chunk mapping to the master's state
func (master *Master) addFileChunkMapping(file string, chunkHandle int64) {
	master.mu.Lock()
	defer master.mu.Unlock()
	master.fileMap[file] = append(master.fileMap[file], Chunk{ChunkHandle: chunkHandle})

}

func (master *Master) handleMasterLeaseRequest(conn net.Conn, messageBytes []byte) error {
	leaseRequest, err := helper.DecodeMessage[common.MasterChunkServerLeaseRequest](messageBytes)
	if err != nil {
		return err
	}
	// master
	master.leaseGrants[leaseRequest.ChunkHandle].grantTime = master.leaseGrants[leaseRequest.ChunkHandle].grantTime.Add(30 * time.Second)
	return nil
}

// Appends a new chunkHandle to the file->chunkHandle mapping on the master,
// before we change the mapping we log the operation so that we dont lose any mutations in case of
// a crash.
func (master *Master) createNewChunk(fileName string) error {
	op := Operation{
		Type:        common.ClientMasterWriteRequestType,
		File:        fileName,
		ChunkHandle: -1,
	}
	chunk := Chunk{
		ChunkHandle: master.generateNewChunkId(),
		ChunkSize:   0,
	}
	op.ChunkHandle = chunk.ChunkHandle
	master.mu.Lock()
	defer master.mu.Unlock()
	err := master.opLogger.writeToOpLog(op)
	if err != nil {
		return err
	}
	master.fileMap[fileName] = append(master.fileMap[fileName], chunk)
	return nil
}

func encodeFileAndChunks(file string, chunks []Chunk) []byte {
	// Calculate the total buffer size needed
	// 2 bytes for file length + file bytes + 2 bytes for chunks length + 8 bytes per chunk
	totalSize := 2 + len(file) + 2 + len(chunks)*8
	buffer := make([]byte, totalSize)

	offset := 0

	// Encode file length as 16-bit number (2 bytes)
	fileLen := uint16(len(file))
	binary.BigEndian.PutUint16(buffer[offset:offset+2], fileLen)
	offset += 2

	// Encode file string as bytes
	copy(buffer[offset:offset+len(file)], file)
	offset += len(file)

	// Encode number of chunks as 16-bit number (2 bytes)
	chunksLen := uint16(len(chunks))
	binary.BigEndian.PutUint16(buffer[offset:offset+2], chunksLen)
	offset += 2

	// Encode each 64-bit chunk
	for _, chunk := range chunks {
		binary.BigEndian.PutUint64(buffer[offset:offset+8], uint64(chunk.ChunkHandle))
		offset += 8
	}

	return buffer
}

func (master *Master) startBackgroundCheckpoint() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop() // Stop the ticker when the function exits

	for {
			select {
			case <-ticker.C:				
					master.buildCheckpoint()
					ticker.Reset(60*time.Second)
			default:
					// Optional: Add some non-blocking logic here
					// to run between ticks.
			}
	}
}
func (master *Master) recover() error {
	// First try to load the latest checkpoint
	checkpoint, err := os.Open("checkpoint.chk")
	if err == nil {
		fileInfo, err := checkpoint.Stat()
		if err != nil {
			return err
		}
		fileSize := fileInfo.Size()

		// Read the totalMappings count (last 8 bytes of the file)
		countBuf := make([]byte, 4)
		_, err = checkpoint.ReadAt(countBuf, fileSize-4)
		if err != nil {
			return err
		}

		// Convert bytes to int64
		totalMappings := binary.LittleEndian.Uint32(countBuf)

		// Now read the actual mapping data
		checkPointData := make([]byte, fileSize-4)
		_, err = checkpoint.ReadAt(checkPointData, 0)
		if err != nil {
			return err
		}

		err = master.readCheckpoint(checkPointData, int64(totalMappings))
		return err
	}

	err = master.opLogger.readOpLog()
	if err != nil {
		return err
	}
	return nil
}

func (master *Master) readCheckpoint(checkpointBytes []byte, totalMappings int64) error {
	master.mu.Lock()

	// Clear existing state
	master.fileMap = make(map[string][]Chunk)

	// Decode each mapping
	for range int(totalMappings) {
		offset := 0

		// Decode file length
		fileLen := binary.BigEndian.Uint16(checkpointBytes[offset : offset+2])
		offset += 2

		// Check if there's enough data for the file
		if len(checkpointBytes) < int(2+fileLen+2) {
			return errors.New("data too short for file content")
		}

		// Decode file string
		file := string(checkpointBytes[offset : offset+int(fileLen)])
		offset += int(fileLen)

		isDeletedFile := false
		fileSplit := strings.Split(file, "/")
		if len(fileSplit) == 3 {
			fileDeletionTime, err := time.Parse(time.DateTime, fileSplit[1])
			if err != nil {

			}
			if time.Since(fileDeletionTime) >= 72*time.Hour {
				isDeletedFile = true
			}
		}
		// Decode chunks length
		chunksLen := binary.BigEndian.Uint16(checkpointBytes[offset : offset+2])
		offset += 2

		// Check if there's enough data for the chunks
		if len(checkpointBytes) < int(2+fileLen+2+chunksLen*8) {
			return errors.New("data too short for chunks content")
		}

		// Decode chunks
		chunks := make([]Chunk, chunksLen)
		for i := range int(chunksLen) {
			chunks[i].ChunkHandle = int64(binary.BigEndian.Uint64(checkpointBytes[offset : offset+8]))
			offset += 8
		}
		if !isDeletedFile {
			master.fileMap[file] = chunks
		}
	}
	master.mu.Unlock()
	return nil
}

func (master *Master) buildCheckpoint() error {
	// Start a new goroutine to build the checkpoint without blocking mutations
	go func() {
		// Create a temporary checkpoint file
		tempCpFile, err := os.Create("checkpoint.tmp")
		if err != nil {
			// Handle error
			return
		}
		// Lock the master's state to get a consistent snapshot
		master.mu.Lock()
		defer master.mu.Unlock()
		totalMappings := 0
		fileChunksEncoded := make([]byte, 0)
		for file, chunks := range master.fileMap {
			totalMappings++
			fileBytes := encodeFileAndChunks(file, chunks)
			fileChunksEncoded = append(fileChunksEncoded, fileBytes...)
		}

		fileChunksEncoded, err = binary.Append(fileChunksEncoded, binary.LittleEndian, uint32(totalMappings))
		if err != nil {
			return
		}
		_, err = tempCpFile.WriteAt(fileChunksEncoded, 0)
		if err != nil {
			return
		}

		// Ensure data is written to disk
		err = tempCpFile.Sync()
		if err != nil {
			return
		}

		// Ensure data is written to disk
		err = tempCpFile.Close()
		if err != nil {
			return
		}

		// Rename the temporary file to the actual checkpoint file
		os.Rename("checkpoint.tmp", "checkpoint.chk")

		// Update the checkpoint sequence number or timestamp
		master.LastCheckpointTime = time.Now()

		err = master.opLogger.switchOpLog()
		if err != nil {
			// Handle error
			return
		}

	}()

	return nil
}

// // func (master *Master) renameFile(fileName string,fileNewName string){

// // }

// func (master *Master) permDeleteFile(fileName string) {
// 	master.mu.Lock()
// 	defer master.mu.Unlock()
// 	delete(master.fileMap,fileName)
// }
