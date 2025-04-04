package master

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/involk-secure-1609/goGFS/common"
	"github.com/involk-secure-1609/goGFS/helper"
)

func (master *Master) sendCloneRequestToChunkServer (request common.MasterChunkServerCloneRequest){

	conn:=master.findChunkServerConnection(request.DestinationChunkServer)
	if (conn==nil){
		return
	}

	messageBytes, err := helper.EncodeMessage(common.MasterChunkServerCloneRequestType, request)
	if err != nil {		
		return 
	}

	_,err=conn.Write(messageBytes)
	if err!=nil{
		return
	}

}
func (master *Master) cloneChunk(chunkServers []string, chunkHandle int64) {

	numberOfClones := 3 - len(chunkServers)
	if numberOfClones <= 0 {
		return
	}
	cloneServers := make([]*Server, 0)

	for _, server := range *master.ServerList {
		if slices.Contains(chunkServers, server.Server) {
			continue
		}
		cloneServers = append(cloneServers, server)
	}

	sourceChunkServer:=chunkServers[0]
	if len(cloneServers) < numberOfClones {
		return
	}
	for range len(cloneServers) {
		server := cloneServers[0]

		cloneRequest := common.MasterChunkServerCloneRequest{
			ChunkHandle: chunkHandle,
			SourceChunkServer:sourceChunkServer,
			DestinationChunkServer: server.Server,
		}

		go master.sendCloneRequestToChunkServer(cloneRequest)

	}

}

// tested
func (master *Master) getMetadataForFile(filename string, chunkIndex int) (Chunk, []string, error) {
	master.mu.RLock()
	defer master.mu.RUnlock()

	chunkList, ok := master.FileMap[filename]
	if !ok {
		return Chunk{}, nil, errors.New("no chunks present for this file")
	}
	if chunkIndex >= int(len(chunkList)) {
		return Chunk{}, nil, errors.New("invalid chunkOffset")
	}

	chunk := chunkList[chunkIndex]
	chunkHandle:=chunk.ChunkHandle
	chunkServers, ok := master.ChunkServerHandler[chunkHandle]

	if !ok {
		return Chunk{}, nil, errors.New("no chunk servers present for chunk")
	}

	if len(chunkServers) < 3 {
		go master.cloneChunk(chunkServers,chunkHandle)
	}
	return chunk, chunkServers, nil
}

// tested indirectly
// Chooses the first 3 chunk servers from our min_heap
// server list. In the paper chunk servers are chosen for clients
// based on how close the chunk servers are to the client however in this implementation
// that really is not possible so I have just used a min heap.
func (master *Master) chooseChunkServers() []string {
	// master.mu.Lock()
	// defer master.mu.Unlock()
	servers := make([]*Server, 0)
	for range 3 {
		if master.ServerList.Len() > 0 {
			server := master.ServerList.Pop().(*Server)
			servers = append(servers, server)
		}
	}

	serverNames := make([]string, 0)
	for i := range servers {
		serverNames = append(serverNames, servers[i].Server)
		servers[i].NumberOfChunks++
		master.ServerList.Push(servers[i])
	}
	return serverNames
}

// tested indirectly
// Uses the snowflake package to generate a globally unique int64 id
func (master *Master) generateNewChunkId() int64 {
	id := master.idGenerator.Generate()
	return id.Int64()
}

// tested indirectly
// If the lease does not exists we just randomly shuffle the servers and choose the first server as our
// primary and the rest of the servers as our secondary servers
func (master *Master) choosePrimaryIfLeaseDoesNotExist(servers []string) (string, []string) {
	rand.Shuffle(len(servers), func(i, j int) {
		servers[i], servers[j] = servers[j], servers[i]
	})
	if len(servers) == 0 {
		return "", []string{}
	}
	primaryServer := servers[0] // First server after shuffling is primary
	var secondaryServers []string
	if len(servers) > 1 {
		secondaryServers = servers[1:] // Remaining servers after shuffling are secondaries
	} else {
		secondaryServers = []string{} // No secondary servers
	}
	return primaryServer, secondaryServers
}

// tested indirectly
func (master *Master) chooseSecondaryIfLeaseDoesExist(primary string, servers []string) []string {

	secondaryServers := make([]string, 0)
	for _, server := range servers {
		if server != primary {
			secondaryServers = append(secondaryServers, server)
		}
	}
	return secondaryServers
}

// tested
// checks if we have a valid lease
func (master *Master) isLeaseValid(lease *Lease, server string) bool {
	if lease == nil {
		return false
	}
	if server != "" {
		if lease.Server != server {
			return false
		}
	}
	// checks if the time difference between the lease grant time and the current time is
	// less than 60 seconds
	return (time.Now().Unix()-lease.GrantTime.Unix() < 60)
}

// tested indirectly
// renews the lease grant by adding a certain 60 seconds of time
// to the lease
func (master *Master) renewLeaseGrant(lease *Lease) {

	newTime := lease.GrantTime.Add(60 * time.Second)
	lease.GrantTime = newTime
}

// tested
// Chooses the primary and secondary servers.
// If there does not exist a valid lease on the chunkHandle then the we choose a new primary server and the secondary servers
// and assign the lease to the primary server chosen. If we already have a pre-existen lease then
//
//	we first check if the lease is valid, if it isnt we do the same thing when there isnt a valid lease.
//	If the lease is valid we renew the lease and choose new secondary servers for the chunk
func (master *Master) choosePrimaryAndSecondary(chunkHandle int64) (string, []string, error) {
	chunkServers, ok := master.ChunkServerHandler[chunkHandle]
	if !ok {
		return "", nil, errors.New("chunk servers do not exist for this chunk handle")
	}
	lease, doesLeaseExist := master.LeaseGrants[chunkHandle]

	// checks if there is already an existing lease or if the existing lease is invalid
	if !doesLeaseExist || !master.isLeaseValid(lease, "") {
		// if there isnt we choose new secondary and primary servers and assign the lease to the primary
		primaryServer, secondaryServers := master.choosePrimaryIfLeaseDoesNotExist(chunkServers)
		if !master.inTestMode {
			err := master.grantLeaseToPrimaryServer(primaryServer, chunkHandle)
			if err != nil {
				return "", nil, err
			}
		}
		newLease := &Lease{}
		newLease.GrantTime = time.Now()
		newLease.Server = primaryServer
		master.LeaseGrants[chunkHandle] = newLease
		return primaryServer, secondaryServers, nil
	}

	// If the existing lease is valid we first renew it
	// and then choose the secondary servers
	master.renewLeaseGrant(lease)
	if !master.inTestMode {
		err := master.grantLeaseToPrimaryServer(lease.Server, chunkHandle)
		if err != nil {
			return "", nil, err
		}
	}
	secondaryServers := master.chooseSecondaryIfLeaseDoesExist(lease.Server, chunkServers)
	return lease.Server, secondaryServers, nil
}

// finds the connection corresponding to a chunk server
func (master *Master) findChunkServerConnection(server string) net.Conn {
	for _, connection := range master.ChunkServerConnections {
		if connection.Port == server {
			return connection.Conn
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

// tested
// For deleting the file, we first retrieve the chunks of the file which we have to delete,
// we then rename the file and change the mapping from oldFileName->chunks => newFileName->chunks.
// Before we make the metadata change we log the operation.
func (master *Master) deleteFile(fileName string) error {
	master.mu.Lock()
	chunks, ok := master.FileMap[fileName]
	if !ok {
		master.mu.Unlock()
		return errors.New("file does not exist")
	}
	newFileName := fileName + "/" + time.Now().String() + "/" + ".deleted"

	if !master.inTestMode {
		op := Operation{
			Type:        common.ClientMasterDeleteRequestType,
			File:        fileName,
			ChunkHandle: -1,
			NewFileName: newFileName,
		}
		err := master.opLogger.writeToOpLog(op)
		if err != nil {
			return err
		}
	}
	master.FileMap[newFileName] = chunks
	delete(master.FileMap, fileName)
	master.mu.Unlock()
	return nil
}

// tested indirectly through opLog tests
func (master *Master) tempDeleteFile(fileName string, newName string) {
	master.mu.Lock()
	defer master.mu.Unlock()
	chunks, ok := master.FileMap[fileName]
	if !ok {
		return
	}
	delete(master.FileMap, fileName)
	var newFileName string
	if newName == "" {
		newFileName = fileName + "/" + time.Now().Format(time.DateTime) + "/" + ".deleted"
	} else {
		newFileName = newName
	}
	master.FileMap[newFileName] = chunks
}

// Called when we have to assign chunk servers to a chunkHandle for the first time
func (master *Master) assignChunkServers(chunkHandle int64) (string, []string, error) {
	master.mu.Lock()
	defer master.mu.Unlock()
	_, chunkServerExists := master.ChunkServerHandler[chunkHandle]
	if !chunkServerExists {
		// chooses the chunk servers from the serverList
		chosenServers := master.chooseChunkServers()
		// if we have insufficent chunkServers connected to the master
		// we return an error
		if len(chosenServers) > 1 {
			master.ChunkServerHandler[chunkHandle] = chosenServers
		} else {
			delete(master.ChunkServerHandler, chunkHandle)
			return "", nil, errors.New("no chunk servers available")
		}
	}
	// assign primary and secondary chunkServers
	primaryServer, secondaryServers, err := master.choosePrimaryAndSecondary(chunkHandle)
	if err != nil {
		return "", nil, err
	}
	return primaryServer, secondaryServers, nil
}

// tested
func (master *Master) handleChunkCreation(fileName string) (int64, error) {
	// opsToLog := make([], 0)
	op := Operation{
		Type:        common.ClientMasterWriteRequestType,
		File:        fileName,
		ChunkHandle: -1,
		NewFileName: "NULL",
	}
	var chunk Chunk
	master.mu.Lock()
	defer master.mu.Unlock()

	// if the file does not exist the master creates a new one and
	// creates a new chunk for that file
	_, fileAlreadyExists := master.FileMap[fileName]
	if !fileAlreadyExists || len(master.FileMap[fileName]) == 0 {
		master.FileMap[fileName] = make([]Chunk, 0)
		chunk = Chunk{
			ChunkHandle: master.generateNewChunkId(),
		}
		op.ChunkHandle = chunk.ChunkHandle
	}
	if op.ChunkHandle != -1 {
		// log the operation if it has resulted in a new Chunk creation otherwise there is no need to log it
		if !master.inTestMode {
			err := master.opLogger.writeToOpLog(op)
			if err != nil {
				return -1, err
			}
		}
		master.FileMap[fileName] = append(master.FileMap[fileName], chunk)
	}
	chunkHandle := master.FileMap[fileName][len(master.FileMap[fileName])-1].ChunkHandle

	return chunkHandle, nil

}

// tested indirectly through opLog tests
// Helper function to add a file-chunk mapping to the master's state
func (master *Master) addFileChunkMapping(file string, chunkHandle int64) {
	master.mu.Lock()
	defer master.mu.Unlock()
	master.FileMap[file] = append(master.FileMap[file], Chunk{ChunkHandle: chunkHandle})

}

func (master *Master) handleMasterLeaseRequest(conn net.Conn, messageBytes []byte) error {
	leaseRequest, err := helper.DecodeMessage[common.MasterChunkServerLeaseRequest](messageBytes)
	if err != nil {
		return err
	}

	lease, ok := master.LeaseGrants[leaseRequest.ChunkHandle]
	if !ok {
		newLease := &Lease{}
		newLease.Server = leaseRequest.Server
		newLease.GrantTime = time.Now()
		master.LeaseGrants[leaseRequest.ChunkHandle] = newLease
		return nil
	}
	lease.GrantTime = lease.GrantTime.Add(20 * time.Second)
	return nil
}

// tested
// Appends a new chunkHandle to the file->chunkHandle mapping on the master,
// before we change the mapping we log the operation so that we dont lose any mutations in case of
// a crash.
func (master *Master) createNewChunk(fileName string, lastChunkHandle int64) error {

	log.Println("STARTING CREATION OF NEW CHUNK LESS GOOO")
	op := Operation{
		Type:        common.ClientMasterWriteRequestType,
		File:        fileName,
		ChunkHandle: -1,
		NewFileName: "NULL",
	}
	// op.ChunkHandle = chunk.ChunkHandle
	master.mu.Lock()
	defer master.mu.Unlock()

	chunksIds := master.FileMap[fileName]
	sz := len(chunksIds)

	previousChunkHandle := chunksIds[sz-1].ChunkHandle
	if previousChunkHandle == lastChunkHandle {
		log.Println("need to make a new chunk")
		chunk := Chunk{
			ChunkHandle: master.generateNewChunkId(),
		}
		op.ChunkHandle = chunk.ChunkHandle
	}

	if !master.inTestMode && op.ChunkHandle != -1 {
		err := master.opLogger.writeToOpLog(op)
		if err != nil {
			return err
		}
	}

	if op.ChunkHandle != -1 {
		log.Println("new chunk handle created")
		master.FileMap[fileName] = append(master.FileMap[fileName], Chunk{ChunkHandle: op.ChunkHandle})
	}
	return nil
}

func (master *Master) startBackgroundCheckpoint() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop() // Stop the ticker when the function exits

	for {
		select {
		case <-ticker.C:
			go master.buildCheckpoint()
			ticker.Reset(60 * time.Second)
		default:
			// Optional: Add some non-blocking logic here
			// to run between ticks.
		}
	}
}

// tested cuz readCheckpoint and readOpLog are tested
func (master *Master) recover() error {
	err := master.readCheckpoint()
	if err != nil {
		return err
	}

	if !master.inTestMode {
		err = master.opLogger.readOpLog()
		if err != nil {
			return err
		}
	}
	return nil
}

// tested
func (master *Master) readCheckpoint() error {
	checkpoint, err := os.Open("checkpoint.chk")
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		fp, err := helper.OpenTruncFile("checkpoint.chk")
		if err != nil {
			return err
		}
		intialMappings := make([]byte, 4)
		intialMappings = binary.LittleEndian.AppendUint32(intialMappings, 0)
		if _, err := fp.Write(intialMappings); err != nil {
			fp.Close()
			return err
		}
		if err := fp.Sync(); err != nil {
			fp.Close()
			return err
		}

		if _, err := fp.Seek(0, io.SeekStart); err != nil {
			fp.Close()
			return err
		}

		checkpoint = fp
	}

	fileInfo, err := checkpoint.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()

	// Read the totalMappings count (last 4 bytes of the file)
	countBuf := make([]byte, 4)
	_, err = checkpoint.ReadAt(countBuf, fileSize-4)
	if err != nil {
		return err
	}
	if _, err := checkpoint.Seek(0, io.SeekStart); err != nil {
		checkpoint.Close()
		return err
	}

	// Convert bytes to int32
	totalMappings := int32(binary.LittleEndian.Uint32(countBuf))

	if totalMappings > 0 {
		// Now read the actual mapping data
		checkPointData := make([]byte, fileSize-4)
		_, err = checkpoint.Read(checkPointData)
		if err != nil {
			checkpoint.Close()
			return err
		}
		checkpoint.Close()

		master.mu.Lock()
		defer master.mu.Unlock()
		// Clear existing state
		master.FileMap = make(map[string][]Chunk)

		offset := 0
		// Decode each mapping
		for range int(totalMappings) {
			bytesRead, file, chunks, err := decodeFileAndChunks(checkPointData, offset)
			if err != nil {
				err = os.Truncate("checkpoint.chk", int64(offset))
				if err != nil {
					return err
				}
				break
			}
			isDeletedFile := false
			fileSplit := strings.Split(file, "/")
			if len(fileSplit) == 3 {
				fileDeletionTime, err := time.Parse(time.DateTime, fileSplit[1])
				if err != nil {
					continue
				}
				if time.Since(fileDeletionTime) >= 72*time.Hour {
					isDeletedFile = true
				}
			}
			if !isDeletedFile {
				chunkHandles := make([]int64, 0)
				for _, val := range chunks {
					chunkHandles = append(chunkHandles, val.ChunkHandle)
				}
				master.FileMap[file] = chunks
				master.ChunkHandles = append(master.ChunkHandles, chunkHandles...)
			}
			offset += bytesRead
		}
	}
	return nil
}

// tested
func (master *Master) buildCheckpoint() error {

	// Create a temporary checkpoint file
	tempCpFile, err := os.Create("checkpoint.tmp")
	if err != nil {
		// Handle error
		return err
	}

	// Lock the master's state to get a consistent snapshot
	master.mu.Lock()
	defer master.mu.Unlock()
	totalMappings := 0
	fileChunksEncoded := make([]byte, 0)
	for file, chunks := range master.FileMap {
		totalMappings++
		fileBytes := encodeFileAndChunks(file, chunks)
		fileChunksEncoded = append(fileChunksEncoded, fileBytes...)
	}

	fileChunksEncoded, err = binary.Append(fileChunksEncoded, binary.LittleEndian, uint32(totalMappings))
	if err != nil {
		tempCpFile.Close()
		return err
	}
	_, err = tempCpFile.Write(fileChunksEncoded)
	if err != nil {
		tempCpFile.Close()
		return err
	}

	// Ensure data is written to disk
	err = tempCpFile.Sync()
	if err != nil {
		tempCpFile.Close()
		return err
	}

	err = tempCpFile.Close()
	if err != nil {
		return err
	}

	// Rename the temporary file to the actual checkpoint file
	err = os.Rename("checkpoint.tmp", "checkpoint.chk")
	if err != nil {
		return err
	}
	if !master.inTestMode {
		// Update the checkpoint sequence number or timestamp
		master.LastCheckpointTime = time.Now()
		err = master.opLogger.switchOpLog()
		if err != nil {
			// Handle error
			return err
		}
	}
	return nil
}

// tested
func encodeFileAndChunks(file string, chunks []Chunk) []byte {
	// Calculate the total buffer size needed
	// 2 bytes for file length + file bytes + 2 bytes for chunks length + 8 bytes per chunk
	totalSize := 2 + len(file) + 2 + len(chunks)*8
	buffer := make([]byte, totalSize)

	offset := 0

	// Encode file length as 16-bit number (2 bytes)
	fileLen := uint16(len(file))
	binary.LittleEndian.PutUint16(buffer[offset:offset+2], fileLen)
	offset += 2

	// Encode file string as bytes
	copy(buffer[offset:offset+len(file)], file)
	offset += len(file)

	// Encode number of chunks as 16-bit number (2 bytes)
	chunksLen := uint16(len(chunks))
	binary.LittleEndian.PutUint16(buffer[offset:offset+2], chunksLen)
	offset += 2

	// Encode each 64-bit chunk
	for _, chunk := range chunks {
		binary.LittleEndian.PutUint64(buffer[offset:offset+8], uint64(chunk.ChunkHandle))
		offset += 8
	}

	return buffer
}

// tested
func decodeFileAndChunks(data []byte, offset int) (int, string, []Chunk, error) {
	// Ensure there's enough data to read file length (2 bytes)
	if offset+2 > len(data) {
		return 0, "", nil, errors.New("insufficient data to read file length")
	}

	// Read file length (2 bytes)
	fileLen := binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Ensure there's enough data to read the file name
	if offset+int(fileLen) > len(data) {
		return 0, "", nil, errors.New("insufficient data to read file name")
	}

	// Read file name
	fileName := string(data[offset : offset+int(fileLen)])
	offset += int(fileLen)

	// Ensure there's enough data to read chunks length (2 bytes)
	if offset+2 > len(data) {
		return 0, "", nil, errors.New("insufficient data to read chunks length")
	}

	// Read number of chunks (2 bytes)
	chunksLen := binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Ensure there's enough data to read all chunks
	if offset+int(chunksLen)*8 > len(data) {
		return 0, "", nil, errors.New("insufficient data to read chunks")
	}

	// Decode chunks
	chunks := make([]Chunk, chunksLen)
	for i := range chunks {
		chunks[i].ChunkHandle = int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		offset += 8
	}

	// Calculate total bytes read
	totalBytesRead := 2 + len(fileName) + 2 + len(chunks)*8

	return totalBytesRead, fileName, chunks, nil
}
