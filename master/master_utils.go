package master

import (
	"errors"
	"math/rand/v2"
	"net"
	"os"
	"time"

	"github.com/involk-secure-1609/goGFS/common"
	"github.com/involk-secure-1609/goGFS/helper"
)

func (master *Master) getMetadataForFile(filename string) (Chunk,[]string,error) {
	fileInfo, err := os.Stat(filename)
	if err!=nil{
		return Chunk{},nil,err
	}
	chunkOffset := fileInfo.Size() / common.ChunkSize
	
	master.mu.Lock()
	defer master.mu.Unlock()
	
	chunk := master.fileMap[filename][chunkOffset]
	chunkServers := master.chunkHandler[chunk.ChunkHandle]
	
	return chunk,chunkServers, nil
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
func (master *Master) choosePrimaryIfLeaseDoesNotExist(servers []string) (string,[]string){
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
	return primaryServer,secondaryServers
}

func (master *Master) chooseSecondaryIfLeaseDoesExist(primary string,servers []string) ([]string){

	secondaryServers:=make([]string,0)
	for _,server:=range(servers){
		if(server!=primary){
			secondaryServers=append(secondaryServers, server)
		}
	}
	return secondaryServers
}

// checks if we have a valid lease
func (master *Master) isLeaseValid(lease *Lease,port string) bool{
	if (lease==nil){
		return false
	}
	if (port!=""){
		if(lease.server!=port){
			return false
		}
	}
	// checks if the time difference between the lease grant time and the current time is 
	// less than 60 seconds
	return time.Now().Unix()-lease.grantTime.Unix()<60
}

// renews the lease grant by adding a certain 60 seconds of time
// to the lease 
func (master *Master) renewLeaseGrant(lease *Lease){
	
	newTime:=lease.grantTime.Add(60*time.Second)
	lease.grantTime=newTime
}

// Chooses the primary and secondary servers.
// If there does not exist a valid lease on the chunkHandle then the we choose a new primary server and the secondary servers
// and assign the lease to the primary server chosen. If we already have a pre-existen lease then
//  we first check if the lease is valid, if it isnt we do the same thing when there isnt a valid lease.
//  If the lease is valid we renew the lease and choose new secondary servers for the chunk
func(master *Master) choosePrimaryAndSecondary(chunkHandle int64) (string,[]string,error){
	lease,doesLeaseExist:=master.leaseGrants[chunkHandle]
	// checks if there is already an existing lease
	if !doesLeaseExist{
		// if there isnt we choose new secondary and primary servers and assign the lease to the primary
		primaryServer,secondaryServers:=master.choosePrimaryIfLeaseDoesNotExist(master.chunkHandler[chunkHandle])
		master.leaseGrants[chunkHandle].grantTime=time.Now()
		master.leaseGrants[chunkHandle].server=primaryServer
		err := master.grantLeaseToPrimaryServer(primaryServer, chunkHandle)
		if err!=nil{
			return "",nil,err
		}
		return primaryServer,secondaryServers,nil
	}

	// checks if the existing lease is valid
	if(master.isLeaseValid(lease,"")){
		// if it is we renew the lease 
		master.renewLeaseGrant(lease)
		err := master.grantLeaseToPrimaryServer(lease.server, chunkHandle)
		if err!=nil{
			return "",nil,err
		}
		secondaryServers:=master.chooseSecondaryIfLeaseDoesExist(lease.server,master.chunkHandler[chunkHandle])
		return lease.server,secondaryServers,nil
	}else{
		// if it isnt valid we choose new primary and secondary servers
		primaryServer,secondaryServers:=master.choosePrimaryIfLeaseDoesNotExist(master.chunkHandler[chunkHandle])
		master.leaseGrants[chunkHandle].grantTime=time.Now()
		master.leaseGrants[chunkHandle].server=primaryServer
		err := master.grantLeaseToPrimaryServer(primaryServer, chunkHandle)
		if err!=nil{
			return "",nil,err
		}
		return primaryServer,secondaryServers,nil
	}
}

// finds the connection corresponding to a chunk server
func (master *Master) findChunkServerConnection(server string) net.Conn{
	for _,connection :=range(master.chunkServerConnections){
		if (connection.port==server){
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
func(master *Master) deleteFile(fileName string) error{
	master.mu.Lock()
	chunks, ok := master.fileMap[fileName]
	if !ok{
		master.mu.Unlock()
		return errors.New("file does not exist")
	}
	newFileName:=fileName+"/"+time.Now().String()+"/"+".deleted"
	op:=Operation{
		Type:        common.ClientMasterDeleteRequestType,
		File:        fileName,
		ChunkHandle: -1,
		NewName: newFileName,
	}
	err:=master.writeToOpLog(op)
	if err!=nil{
		return err
	}
	master.fileMap[newFileName] = chunks
	delete(master.fileMap,fileName)
	master.mu.Unlock()
	return nil
}

func (master *Master) handleChunkCreation(fileName string) (int64,string,[]string,error){
	// opsToLog := make([], 0)
	op := Operation{
		Type:        common.ClientMasterWriteRequestType,
		File:       fileName,
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
		err := master.writeToOpLog(op)
		if err != nil {
			return -1,"",nil,err
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
		return -1,"",nil,err
	}
	// writeResponse := common.ClientMasterWriteResponse{
	// 	ChunkHandle:           chunkHandle,
	// 	// a mutationId is generated so that during the commit request we can mantain an order 
	// 	// between concurrent requests
	// 	MutationId:            master.idGenerator.Generate().Int64(),
	// 	PrimaryChunkServer:    primaryServer,
	// 	SecondaryChunkServers: secondaryServers,
	// }
	return chunkHandle,primaryServer,secondaryServers,nil

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

func (master *Master) createNewChunk(fileName string) error{
	op := Operation{
		Type:        common.ClientMasterWriteRequestType,
		File:       fileName,
		ChunkHandle: -1,
	}
	chunk := Chunk{
		ChunkHandle: master.generateNewChunkId(),
		ChunkSize:   0,
	}
	op.ChunkHandle=chunk.ChunkHandle
	master.mu.Lock()
	defer master.mu.Unlock()
	err:=master.writeToOpLog(op)
	if err!=nil{
		return err
	}
	master.fileMap[fileName]=append(master.fileMap[fileName], chunk)
	return nil
}

func (master *Master) tempDeleteFile(fileName string,newName string){
	master.mu.Lock()
	defer master.mu.Unlock()
	chunks, ok := master.fileMap[fileName]
	if !ok{
		return 
	}
	delete(master.fileMap,fileName)
	var newFileName string
	if(newName==""){
		newFileName=fileName+"/"+time.Now().Format(time.DateTime)+"/"+".deleted"
	}else{
		newFileName=newName
	}
	master.fileMap[newFileName] = chunks
}

// // func (master *Master) renameFile(fileName string,fileNewName string){

// // }

// func (master *Master) permDeleteFile(fileName string) {
// 	master.mu.Lock()
// 	defer master.mu.Unlock()
// 	delete(master.fileMap,fileName)
// }

