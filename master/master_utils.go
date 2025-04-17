package master

import (
	"container/heap"
	"encoding/binary"
	"errors"
	"io"
	"io/fs"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/involk-secure-1609/goGFS/common"
	"github.com/involk-secure-1609/goGFS/helper"
)

func (master *Master) sendCloneRequestToChunkServer(request common.MasterChunkServerCloneRequest) {

	conn := master.findChunkServerConnection(request.DestinationChunkServer)
	if conn == nil {
		return
	}

	messageBytes, err := helper.EncodeMessage(common.MasterChunkServerCloneRequestType, request)
	if err != nil {
		return
	}

	_, err = conn.Write(messageBytes)
	if err != nil {
		return
	}

}
func (master *Master) cloneChunk(chunkServers []string, chunkHandle int64) error {

	numberOfClones := 3 - len(chunkServers)
	if numberOfClones <= 0 {
		return nil
	}
	cloneServers := make([]*Server, 0)

	for _, server := range *master.ServerList {
		if slices.Contains(chunkServers, server.Server) {
			continue
		}
		cloneServers = append(cloneServers, server)
		if len(cloneServers) >= numberOfClones {
			break
		}
	}

	sourceChunkServer := chunkServers[0]
	if len(cloneServers) < numberOfClones {
		return errors.New("chunk servers not availible")
	}
	for range len(cloneServers) {
		server := cloneServers[0]

		cloneRequest := common.MasterChunkServerCloneRequest{
			ChunkHandle:            chunkHandle,
			SourceChunkServer:      sourceChunkServer,
			DestinationChunkServer: server.Server,
		}

		go master.sendCloneRequestToChunkServer(cloneRequest)

	}

	return nil

}

// tested
func (master *Master) getMetadataForFile(filename string, chunkIndex int) (Chunk, []string, error) {
	master.mu.RLock()
	defer master.mu.RUnlock()

	chunkList, ok := master.FileMap[filename]
	if !ok {
		return Chunk{}, nil, errors.New("no chunk handles present for this file")
	}
	if chunkIndex >= int(len(chunkList)) {
		return Chunk{}, nil, errors.New("invalid chunkOffset")
	}

	chunkHandle := chunkList[chunkIndex]
	// chunkHandle := chunk.ChunkHandle
	chunkServers, ok := master.ChunkServerHandler[chunkHandle]
	if !ok {
		return Chunk{}, nil, errors.New("no chunk servers present for chunk handle ")
	}

	chunkServersList := make([]string, 0)
	for server, present := range chunkServers {
		if present {
			chunkServersList = append(chunkServersList, server)
		}
	}

	if len(chunkServers) < 3 && !master.inTestMode {
		err := master.cloneChunk(chunkServersList, chunkHandle)
		log.Println(err)
	}

	chunk, ok := master.ChunkHandles[chunkHandle]
	if !ok {
		return Chunk{}, nil, errors.New("no chunk present for this chunk handle")
	}
	return *chunk, chunkServersList, nil
}

// tested indirectly
// Chooses the first 3 chunk servers from our min_heap
// server list. In the paper chunk servers are chosen for clients
// based on how close the chunk servers are to the client however in this implementation
// that really is not possible so I have just used a min heap.
func (master *Master) chooseChunkServers(chunkHandle int64) error {
	master.mu.Lock()
	defer master.mu.Unlock()
	servers := make([]*Server, 0)
	for range 3 {
		if master.ServerList.Len() > 0 {
			server := master.ServerList.Pop().(*Server)
			servers = append(servers, server)
		}
	}

	if len(servers) > 1 {
		for _, server := range servers {
			server.NumberOfChunks++
			master.ServerList.Push(server)

			_, present := master.ChunkServerHandler[chunkHandle]
			if !present {
				master.ChunkServerHandler[chunkHandle] = make(map[string]bool)
			}
			master.ChunkServerHandler[chunkHandle][server.Server] = true
		}
		return nil
	} else {
		for _, server := range servers {
			master.ServerList.Push(server)
		}
		delete(master.ChunkServerHandler, chunkHandle)
		return errors.New("no chunk servers available")
	}
	return nil
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
func (master *Master) isLeaseValid(lease Lease, server string) bool {
	// if lease == nil {
	// 	return false
	// }

	if lease.Server == "" {
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

	newTime := time.Now().Add(60 * time.Second)
	lease.GrantTime = newTime
}

// tested
// Chooses the primary and secondary servers.
// If there does not exist a valid lease on the chunkHandle then the we choose a new primary server and the secondary servers
// and assign the lease to the primary server chosen. If we already have a pre-existen lease then
//
//	we first check if the lease is valid, if it isnt we do the same thing when there isnt a valid lease.
//	If the lease is valid we renew the lease and choose new secondary servers for the chunk
func (master *Master) choosePrimaryAndSecondary(chunkHandle int64) (string, []string, bool, error) {

	master.mu.RLock()
	chunkServers, ok := master.ChunkServerHandler[chunkHandle]

	chunkServersList := make([]string, 0)

	for server, present := range chunkServers {
		if present {
			chunkServersList = append(chunkServersList, server)
		}
	}

	if !ok || len(chunkServersList) == 0 {
		master.mu.RUnlock()
		return "", nil, false, errors.New("chunk servers do not exist for this chunk handle")
	}

	lease, doesLeaseExist := master.LeaseGrants[chunkHandle]
	master.mu.RUnlock()

	// checks if there is already an existing lease or if the existing lease is invalid
	if !doesLeaseExist || !master.isLeaseValid(*lease, "") {
		// if there isnt we choose new secondary and primary servers and assign the lease to the primary
		primaryServer, secondaryServers := master.choosePrimaryIfLeaseDoesNotExist(chunkServersList)
		return primaryServer, secondaryServers, false, nil
	}

	// If the existing lease is valid we first renew it
	// and then choose the secondary servers
	master.mu.Lock()
	master.renewLeaseGrant(lease)
	master.mu.Unlock()

	secondaryServers := master.chooseSecondaryIfLeaseDoesExist(lease.Server, chunkServersList)
	return lease.Server, secondaryServers, true, nil
}

// finds the connection corresponding to a chunk server
func (master *Master) findChunkServerConnection(server string) net.Conn {
	master.mu.RLock()
	defer master.mu.RUnlock()
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
func (master *Master) assignChunkServers(chunkHandle int64) (string, []string, bool, error) {
	master.mu.RLock()
	_, chunkServerExists := master.ChunkServerHandler[chunkHandle]
	master.mu.RUnlock()
	if !chunkServerExists {
		// chooses the chunk servers from the serverList
		err := master.chooseChunkServers(chunkHandle)
		// if we have insufficent chunkServers connected to the master
		// we return an error
		if err != nil {
			return "", nil, false, err
		}
	}
	// assign primary and secondary chunkServers
	primaryServer, secondaryServers, leaseValid, err := master.choosePrimaryAndSecondary(chunkHandle)
	if err != nil {
		return "", nil, false, err
	}
	return primaryServer, secondaryServers, leaseValid, nil
}

// tested
func (master *Master) handleChunkCreation(fileName string) (*Chunk, error) {
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
	chunkHandleList, fileAlreadyExists := master.FileMap[fileName]
	if !fileAlreadyExists || len(chunkHandleList) == 0 {
		master.FileMap[fileName] = make([]int64, 0)
		chunk = Chunk{
			ChunkHandle:  master.generateNewChunkId(),
			ChunkVersion: 0,
		}
		op.ChunkHandle = chunk.ChunkHandle
	} else{
		chunkHandle:=master.FileMap[fileName][len(chunkHandleList)-1]
		chunk=*master.ChunkHandles[chunkHandle]
	}


	if op.ChunkHandle != -1 {
		// log the operation if it has resulted in a new Chunk creation otherwise there is no need to log it
		if !master.inTestMode {
			err := master.opLogger.writeToOpLog(op)
			if err != nil {
				return nil, err
			}
		}
		master.FileMap[fileName] = append(master.FileMap[fileName], chunk.ChunkHandle)
		master.ChunkHandles[op.ChunkHandle] = &Chunk{
			ChunkHandle:  op.ChunkHandle,
			ChunkVersion: 0,
		}
	}
	return &chunk, nil

}

// tested indirectly through opLog tests
// Helper function to add a file-chunk mapping to the master's state
func (master *Master) addFileChunkMapping(file string, chunkHandle int64) {
	master.mu.Lock()
	defer master.mu.Unlock()
	master.FileMap[file] = append(master.FileMap[file], chunkHandle)

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

	if sz == 0 {
		chunk := Chunk{
			ChunkHandle: master.generateNewChunkId(),
		}
		op.ChunkHandle = chunk.ChunkHandle

	}
	if sz > 0 {
		previousChunkHandle := chunksIds[sz-1]
		if previousChunkHandle == lastChunkHandle {
			log.Println("need to make a new chunk")
			chunk := Chunk{
				ChunkHandle: master.generateNewChunkId(),
			}
			op.ChunkHandle = chunk.ChunkHandle
		}
	}

	if !master.inTestMode && op.ChunkHandle != -1 {
		err := master.opLogger.writeToOpLog(op)
		if err != nil {
			return err
		}
	}

	if op.ChunkHandle != -1 {
		log.Println("new chunk handle created")
		master.FileMap[fileName] = append(master.FileMap[fileName], op.ChunkHandle)
		master.ChunkHandles[op.ChunkHandle] = &Chunk{
			ChunkHandle:  op.ChunkHandle,
			ChunkVersion: 0,
		}
	}
	return nil
}

func (master *Master) startBackgroundCheckpoint() {
	// ticker := time.NewTicker(60 * time.Second)
	// defer ticker.Stop() // Stop the ticker when the function exits

	for {

		// case <-ticker.C:
		time.Sleep(60 * time.Second)
		err := master.buildCheckpoint()
		if err != nil {
			log.Fatalln(err)
		}
		// ticker.Reset(60 * time.Second)

		// Optional: Add some non-blocking logic here
		// to run between ticks.

	}
}

// tested cuz readCheckpoint and readOpLog are tested
func (master *Master) recover() error {

	log.Println("reading checkpoint of master")
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

	log.Println("finding the path name")
	log.Println(master.MasterDirectory)
	fileName:=filepath.Join(master.MasterDirectory,"checkpoint.chk")
	log.Println("why is this not logging ",fileName)

	checkpoint, err := os.Open(fileName)
	if err != nil {
		var pathErr *fs.PathError
		if !errors.As(err,&pathErr) {
			log.Println(err)
			log.Println("os.IsNotExist occurs")
			return err
		}

		fp, err := helper.OpenTruncFile(filepath.Join(master.MasterDirectory,"checkpoint.chk"))
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
		master.FileMap = make(map[string][]int64)

		offset := 0
		// Decode each mapping
		for range int(totalMappings) {
			bytesRead, file, chunks, err := master.decodeFileAndChunks(checkPointData, offset)
			if err != nil {
				err = os.Truncate(filepath.Join(master.MasterDirectory,"checkpoint.chk"), int64(offset))
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
				for _, chunk := range chunks {
					chunkHandles = append(chunkHandles, chunk.ChunkHandle)
					master.ChunkHandles[chunk.ChunkHandle] = chunk
				}
				master.FileMap[file] = chunkHandles
				// master.ChunkHandles = append(master.ChunkHandles, chunkHandles...)
			}
			offset += bytesRead
		}
	}
	return nil
}


// tested
func (master *Master) buildCheckpoint() error {

	// Create a temporary checkpoint file
	tempCpFile, err := os.Create(filepath.Join(master.MasterDirectory,"checkpoint.tmp"))
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
		fileBytes := master.encodeFileAndChunks(file, chunks)
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
	err = os.Rename(filepath.Join(master.MasterDirectory,"checkpoint.tmp"), filepath.Join(master.MasterDirectory,"checkpoint.chk"))
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
func (master *Master) encodeFileAndChunks(file string, chunkHandles []int64) []byte {

	// Calculate the total buffer size needed
	// 2 bytes for file length + file bytes + 2 bytes for number of chunks length
	// + 8 bytes per chunk + 8 bytes for each chunk version number
	totalSize := 2 + len(file) + 2 + len(chunkHandles)*16
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
	chunksLen := uint16(len(chunkHandles))
	binary.LittleEndian.PutUint16(buffer[offset:offset+2], chunksLen)
	offset += 2

	// Encode each 64-bit chunk and the chunkVersion number
	for _, chunkHandle := range chunkHandles {
		binary.LittleEndian.PutUint64(buffer[offset:offset+8], uint64(chunkHandle))
		offset += 8
		chunk, present := master.ChunkHandles[chunkHandle]
		if !present {
			binary.LittleEndian.PutUint64(buffer[offset:offset+8], 0)
		} else {
			binary.LittleEndian.PutUint64(buffer[offset:offset+8], uint64(chunk.ChunkVersion))
		}
		offset += 8
	}

	return buffer
}

// tested
func (master *Master) decodeFileAndChunks(data []byte, offset int) (int, string, []*Chunk, error) {
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
	if offset+int(chunksLen)*16 > len(data) {
		return 0, "", nil, errors.New("insufficient data to read chunks")
	}

	// Decode chunks
	chunks := make([]*Chunk, chunksLen)
	for i := range chunks {
		chunks[i]=&Chunk{}
		chunks[i].ChunkHandle = int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		offset += 8
		chunks[i].ChunkVersion = int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		offset += 8
	}

	// Calculate total bytes read
	totalBytesRead := 2 + len(fileName) + 2 + len(chunks)*16

	return totalBytesRead, fileName, chunks, nil
}

func (master *Master) handleChunkServerIncreaseVersionNumberResponse(responseBytes []byte) error {
	response, err := helper.DecodeMessage[common.MasterChunkServerIncreaseVersionNumberResponse](responseBytes)
	if err != nil {
		return err
	}

	versionIdentifer := strconv.FormatInt(response.ChunkHandle, 10) + "#" + strconv.FormatInt(response.PreviousVersionNumber, 10)

	master.mu.Lock()
	defer master.mu.Unlock()

	// Check if the synchronizer still exists
	synchronizer, exists := master.VersionNumberSynchronizer[versionIdentifer]
	if !exists {
		// The synchronizer might have been removed after a timeout
		return nil
	}

	// Safely send to the channel using select to avoid panics on closed channel
	select {
	case synchronizer.ErrorChan <- response.Status:
	default:
		log.Println("channel closed")
	}

	return nil
}
func (master *Master) sendIncreaseVersionNumberToChunkServers(chunk *Chunk, servers []string) error {

	// master.mu.RLock()

	request := common.MasterChunkServerIncreaseVersionNumberRequest{
		ChunkHandle:           chunk.ChunkHandle,
		PreviousVersionNumber: chunk.ChunkVersion,
	}

	versionIdentifer := strconv.FormatInt(chunk.ChunkHandle, 10) + "#" + strconv.FormatInt(chunk.ChunkVersion, 10)

	master.mu.Lock()
	synchronizer := &IncreaseVersionNumberSynchronizer{ErrorChan: make(chan bool)}
	master.VersionNumberSynchronizer[versionIdentifer] = synchronizer
	master.mu.Unlock()
	requestBytes, err := helper.EncodeMessage(common.MasterChunkServerIncreaseVersionNumberRequestType, request)
	if err != nil {
		return err
	}
	go func() {
		time.Sleep(4 * time.Second)
		close(synchronizer.ErrorChan)
	}()

	numberOfRequests := 0
	for _, connection := range master.ChunkServerConnections {
		if slices.Contains(servers, connection.Port) {
			conn := connection.Conn
			numberOfRequests++
			go func() {
				_, err = conn.Write(requestBytes)
				if err != nil {
					select {
					case synchronizer.ErrorChan <- false:
					default:
						log.Println("channel closed")
					}
					return
				}
			}()
		}
	}

	// Collect all messages in a slice
	var requestStatus []bool

	// Read messages until the channel is closed
	for msg := range synchronizer.ErrorChan {
		requestStatus = append(requestStatus, msg)
		if len(requestStatus) == numberOfRequests {
			break
		}
	}
	master.mu.Lock()
	delete(master.VersionNumberSynchronizer, versionIdentifer)
	master.mu.Unlock()

	IVNsuccess := true

	if len(requestStatus) < numberOfRequests {
		IVNsuccess = false
	}

	for _, status := range requestStatus {
		if !status {
			IVNsuccess = false
		}
	}

	if IVNsuccess {
		master.mu.Lock()
		op := Operation{
			Type:             common.MasterChunkServerIncreaseVersionNumberRequestType,
			NewVersionNumber: chunk.ChunkVersion + 1,
			ChunkHandle:      chunk.ChunkHandle,
		}
		err = master.opLogger.writeToOpLog(op)
		if err != nil {
			return err
		}
		chunk.ChunkVersion++
		master.mu.Unlock()
	}
	return nil
}

func (master *Master) setChunkVersionNumber(chunkHandle int64, versionNumber int64) {
	chunk, present := master.ChunkHandles[chunkHandle]
	if !present {
		master.ChunkHandles[chunkHandle] = &Chunk{
			ChunkHandle:  chunkHandle,
			ChunkVersion: versionNumber,
		}
		return
	}
	chunk.ChunkVersion = versionNumber
}

func (master *Master) handleChunkServerIsNotUpToDate(port string, chunkHandle int64) {
	master.mu.Lock()
	defer master.mu.Unlock()
	delete(master.ChunkServerHandler[chunkHandle], port)
}

func (master *Master) handleChunkServerFailure(chunkServerConnection *ChunkServerConnection) {
	master.mu.Lock()
	defer master.mu.Unlock()
	numberOfConnections := 0
	for _, connections := range master.ChunkServerConnections {
		if connections == chunkServerConnection {
			index := slices.Index(master.ChunkServerConnections, chunkServerConnection)
			master.ChunkServerConnections = slices.Delete(master.ChunkServerConnections, index, index)
		} else if connections.Port == chunkServerConnection.Port {
			numberOfConnections++
		}
	}
	if numberOfConnections == 1 {
		for chunkHandle, chunkServers := range master.ChunkServerHandler {
			log.Println(chunkHandle)
			delete(chunkServers, chunkServerConnection.Port)
		}

		for _, server := range *master.ServerList {
			if server.Server == chunkServerConnection.Port {
				heap.Remove(master.ServerList, server.index)
			}
		}
	}
}
