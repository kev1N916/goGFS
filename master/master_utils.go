package master

import (
	"errors"
	"math/rand/v2"
	"net"
	"time"

	"github.com/involk-secure-1609/goGFS/common"
	"github.com/involk-secure-1609/goGFS/helper"
)

func (master *Master) chooseSecondaryServers() []string {
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

func (master *Master) generateNewChunkId() int64 {
	id := master.idGenerator.Generate()
	return id.Int64()
}

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

func (master *Master) isLeaseValid(lease *Lease,port string) bool{
	if (lease==nil){
		return false
	}
	if (port!=""){
		if(lease.server!=port){
			return false
		}
	}
	return time.Now().Unix()-lease.grantTime.Unix()<60
}

func (master *Master) renewLeaseGrant(lease *Lease){
	
	newTime:=lease.grantTime.Add(60*time.Second)
	lease.grantTime=newTime
}
func(master *Master) choosePrimaryAndSecondary(chunkHandle int64) (string,[]string,error){
	lease,doesLeaseExist:=master.leaseGrants[chunkHandle]
	if !doesLeaseExist{
		primaryServer,secondaryServers:=master.choosePrimaryIfLeaseDoesNotExist(master.chunkHandler[chunkHandle])
		master.leaseGrants[chunkHandle].grantTime=time.Now()
		master.leaseGrants[chunkHandle].server=primaryServer
		err := master.grantLeaseToPrimaryServer(primaryServer, chunkHandle)
		if err!=nil{
			return "",nil,err
		}
		return primaryServer,secondaryServers,nil
	}

	if(master.isLeaseValid(lease,"")){
		master.renewLeaseGrant(lease)
		err := master.grantLeaseToPrimaryServer(lease.server, chunkHandle)
		if err!=nil{
			return "",nil,err
		}
		secondaryServers:=master.chooseSecondaryIfLeaseDoesExist(lease.server,master.chunkHandler[chunkHandle])
		return lease.server,secondaryServers,nil
	}else{
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

func(master *Master) deleteFile(fileName string) error{
	master.mu.Lock()
	chunks, ok := master.fileMap[fileName]
	if !ok{
		master.mu.Unlock()
		return errors.New("file does not exist")
	}
	delete(master.fileMap,fileName)
	newFileName:=fileName+"/"+time.Now().String()+"/"+".deleted"
	master.fileMap[newFileName] = chunks
	master.mu.Unlock()
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
	return nil
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

func (master *Master) createNewChunk(fileName string) {
	chunk := Chunk{
		ChunkHandle: master.generateNewChunkId(),
		ChunkSize:   0,
	}
	master.mu.Lock()
	defer master.mu.Unlock()

	master.fileMap[fileName]=append(master.fileMap[fileName], chunk)
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

