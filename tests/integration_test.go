package tests

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	chunkserver "github.com/involk-secure-1609/goGFS/chunkServer"
	"github.com/involk-secure-1609/goGFS/client"
	"github.com/involk-secure-1609/goGFS/common"
	"github.com/involk-secure-1609/goGFS/master"
	"github.com/stretchr/testify/assert"
)

// TestChunkServerInitAndMasterHandshake verifies that a chunk server can initialize
// and successfully complete the handshake process with the master
func TestChunkServerInitAndMasterHandshake(t *testing.T) {

	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	TestDirectory := "chunkServer1"
	os.RemoveAll(TestDirectory)
	defer os.RemoveAll(TestDirectory)

	err := os.Mkdir(TestDirectory, 0600)
	assert.Nil(t, err)

	master, err := master.NewMaster(true)
	assert.Nil(t, err)
	assert.NotNil(t, master)

	err = master.Start()
	assert.Nil(t, err)
	masterPort := master.Listener.Addr().String()

	chunkServer := chunkserver.NewChunkServer(TestDirectory, masterPort)
	chunkServer.InTestMode = true

	chunkServerPort, err := chunkServer.Start()
	assert.Nil(t, err)
	log.Println(chunkServerPort)
	assert.Equal(t, 1, len(master.ChunkServerConnections))
	host, port, err := net.SplitHostPort(master.ChunkServerConnections[0].Port)
	assert.Nil(t, err)
	_, chunkServerPortAfterSplit, err := net.SplitHostPort(chunkServerPort)
	assert.Nil(t, err)
	log.Println(master.ChunkServerConnections[0].Port)
	assert.Nil(t, err)
	t.Log("logging host and port", host, port)
	assert.Equal(t, chunkServerPortAfterSplit, port)
}

// TestMasterHeartbeat verifies that the master's heartbeat mechanism
// communicates properly with chunk servers
func TestMasterHeartbeat(t *testing.T) {

	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	TestDirectory := "chunkServer1"
	os.RemoveAll(TestDirectory)
	defer os.RemoveAll(TestDirectory)

	master, err := master.NewMaster(true)
	assert.Nil(t, err)
	assert.NotNil(t, master)

	err = master.Start()
	assert.Nil(t, err)
	masterPort := master.Listener.Addr().String()

	chunkServer := chunkserver.NewChunkServer(TestDirectory, masterPort)
	chunkServer.InTestMode = true

	chunkServerPort, err := chunkServer.Start()
	assert.Nil(t, err)
	log.Println(chunkServerPort)
	err = master.SendHeartBeatToChunkServer(master.ChunkServerConnections[0])
	assert.Nil(t, err)

	time.Sleep(1 * time.Second)

	assert.Equal(t, int32(1), chunkServer.HeartbeatRequestsReceived.Load())
	assert.Equal(t, int32(1), chunkServer.HeartbeatResponsesReceived.Load())
}

// TestMasterHeartbeatChunkDeletion checks if chunk servers properly delete
// chunk handles when requested by the master during heartbeats
func TestMasterHeartbeatChunkDeletion(t *testing.T) {

	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	TestDirectory := "chunkServer1"
	TestChunkName := "1"
	os.RemoveAll(TestDirectory)
	defer os.RemoveAll(TestDirectory)

	chunkPath := filepath.Join(TestDirectory, TestChunkName)
	log.Println(chunkPath)
	err := os.Mkdir(TestDirectory, 0755)
	assert.Nil(t, err)
	chunkFile, err := os.Create(chunkPath + ".chunk")
	assert.Nil(t, err)
	_, err = chunkFile.Write([]byte{'t', 'e', 's', 't'})
	assert.Nil(t, err)
	chunkFile.Close()

	master, err := master.NewMaster(true)
	assert.Nil(t, err)
	assert.NotNil(t, master)

	err = master.Start()
	masterPort := master.Listener.Addr().String()

	assert.Nil(t, err)

	chunkServer := chunkserver.NewChunkServer(TestDirectory, masterPort)
	chunkServer.InTestMode = true

	chunkServerPort, err := chunkServer.Start()
	assert.Nil(t, err)
	log.Println(chunkServerPort)

	assert.Equal(t, 1, len(chunkServer.ChunkHandles))
	assert.Equal(t, int64(1), chunkServer.ChunkHandles[0])

	err = master.SendHeartBeatToChunkServer(master.ChunkServerConnections[0])
	assert.Nil(t, err)

	time.Sleep(3 * time.Second)

	assert.Equal(t, int32(1), chunkServer.HeartbeatRequestsReceived.Load())
	assert.Equal(t, int32(1), chunkServer.HeartbeatResponsesReceived.Load())
	assert.Equal(t, 0, len(chunkServer.ChunkHandles))
}

// TestClientReadFromChunkServerWithChunkPresent verifies client can successfully
// read from a chunk server when the requested chunk is present
func TestClientReadFromChunkServerWithChunkPresent(t *testing.T) {
	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	TestDirectory := "chunkServer1"
	TestChunkName := "1"
	os.RemoveAll(TestDirectory)
	defer os.RemoveAll(TestDirectory)

	testData := []byte{'t', 'e', 's', 't'}
	chunkPath := filepath.Join(TestDirectory, TestChunkName)
	log.Println(chunkPath)
	err := os.Mkdir(TestDirectory, 0755)
	assert.Nil(t, err)
	chunkFile, err := os.Create(chunkPath + ".chunk")
	assert.Nil(t, err)
	_, err = chunkFile.Write(testData)
	assert.Nil(t, err)
	chunkFile.Close()

	chunkChekSumFile, err := os.Create(chunkPath + ".chksum")
	assert.Nil(t, err)
	checkSum := bytes.Buffer{}
	crcChecksum := crc32.Checksum(testData, crc32.MakeTable(crc32.Castagnoli))
	binary.Write(&checkSum, binary.LittleEndian, crcChecksum)
	_, err = chunkChekSumFile.Write(checkSum.Bytes())
	assert.Nil(t, err)
	chunkChekSumFile.Close()

	master, err := master.NewMaster(true)
	assert.Nil(t, err)
	assert.NotNil(t, master)

	err = master.Start()
	masterPort := master.Listener.Addr().String()

	assert.Nil(t, err)

	chunkServer := chunkserver.NewChunkServer(TestDirectory, masterPort)
	chunkServer.InTestMode = true

	chunkServerPort, err := chunkServer.Start()
	assert.Nil(t, err)
	log.Println(chunkServerPort)

	response := common.ClientMasterReadResponse{
		ChunkHandle:  1,
		ChunkServers: []string{chunkServerPort},
	}
	client := client.NewClient(masterPort)
	chunkBytes, err := client.ReadFromChunkServer(&response)
	assert.Nil(t, err)
	log.Println(chunkBytes)
	assert.Equal(t, 4, len(chunkBytes))
}

// TestClientReadFromChunkServerWithChunkAbsent verifies client properly handles
// the case when the requested chunk is not present on the chunk server
func TestClientReadFromChunkServerWithChunkAbsent(t *testing.T) {
	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	TestDirectory := "chunkServer1"
	TestChunkName := "1"
	os.RemoveAll(TestDirectory)
	defer os.RemoveAll(TestDirectory)

	testData := []byte{'t', 'e', 's', 't'}
	chunkPath := filepath.Join(TestDirectory, TestChunkName)
	log.Println(chunkPath)
	err := os.Mkdir(TestDirectory, 0755)
	assert.Nil(t, err)
	chunkFile, err := os.Create(chunkPath + ".chunk")
	assert.Nil(t, err)
	_, err = chunkFile.Write(testData)
	assert.Nil(t, err)
	chunkFile.Close()

	chunkChekSumFile, err := os.Create(chunkPath + ".chksum")
	assert.Nil(t, err)
	checkSum := bytes.Buffer{}
	crcChecksum := crc32.Checksum(testData, crc32.MakeTable(crc32.Castagnoli))
	binary.Write(&checkSum, binary.LittleEndian, crcChecksum)
	_, err = chunkChekSumFile.Write(checkSum.Bytes())
	assert.Nil(t, err)
	chunkChekSumFile.Close()

	master, err := master.NewMaster(true)
	assert.Nil(t, err)
	assert.NotNil(t, master)

	err = master.Start()
	masterPort := master.Listener.Addr().String()

	assert.Nil(t, err)

	chunkServer := chunkserver.NewChunkServer(TestDirectory, masterPort)
	chunkServer.InTestMode = true

	chunkServerPort, err := chunkServer.Start()
	assert.Nil(t, err)
	log.Println(chunkServerPort)

	response := common.ClientMasterReadResponse{
		ChunkHandle:  2,
		ChunkServers: []string{chunkServerPort},
	}
	client := client.NewClient(masterPort)
	chunkBytes, err := client.ReadFromChunkServer(&response)
	assert.Error(t, err)
	assert.Nil(t, chunkBytes)
}

// TestClientReadFromMaster checks if the master returns appropriate metadata
// for read requests from the client
func TestClientReadFromMaster(t *testing.T) {

	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	TestDirectory := "chunkServer1"
	// TestChunkName := "1"
	os.RemoveAll(TestDirectory)
	defer os.RemoveAll(TestDirectory)

	masterServer, err := master.NewMaster(true)
	assert.Nil(t, err)
	assert.NotNil(t, masterServer)

	err = masterServer.Start()
	masterPort := masterServer.Listener.Addr().(*net.TCPAddr).String()

	assert.Nil(t, err)

	testFileMap := map[string][]int64{
		"test_file_1": {
			2, 3, 0,
		},
		"test_file_2": {
			2432, 31321, 320, 311, 32,
		},
	}
	masterServer.FileMap = testFileMap
	testChunkServerHandler := map[int64]map[string]bool{
		3: {
			":server1": true,
			":server2": true,
			":server3": true,
		},
	}
	testChunks := map[int64]*master.Chunk{
		3: {
			ChunkHandle:  3,
			ChunkVersion: 2,
		},
	}
	masterServer.ChunkServerHandler = testChunkServerHandler
	masterServer.ChunkHandles = testChunks

	client := client.NewClient(masterPort)

	request1 := common.ClientMasterReadRequest{
		Filename:   "test_file_1",
		ChunkIndex: 2,
	}
	response1, err := client.ReadFromMasterServer(request1)
	assert.Nil(t, err)
	assert.Equal(t, "no chunk servers present for chunk handle ", response1.ErrorMessage)

	request2 := common.ClientMasterReadRequest{
		Filename:   "test_file_1",
		ChunkIndex: 7,
	}
	response2, err := client.ReadFromMasterServer(request2)
	assert.Nil(t, err)
	assert.Equal(t, "invalid chunkOffset", response2.ErrorMessage)

	request3 := common.ClientMasterReadRequest{
		Filename:   "test_file_1",
		ChunkIndex: 1,
	}
	response3, err := client.ReadFromMasterServer(request3)
	assert.Nil(t, err)
	assert.Equal(t, "", response3.ErrorMessage)
	assert.Equal(t, int64(3), response3.ChunkHandle)
}

// TestClientReadWorkflow verifies the complete client read workflow,
// including fetching metadata from master and reading from chunk servers
func TestClientReadWorkflow(t *testing.T) {
	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	testData := []byte{'t', 'e', 's', 't'}

	checkSum := bytes.Buffer{}
	crcChecksum := crc32.Checksum(testData, crc32.MakeTable(crc32.Castagnoli))
	binary.Write(&checkSum, binary.LittleEndian, crcChecksum)

	TestDirectory1 := "chunkServer1"
	os.RemoveAll(TestDirectory1)
	defer os.RemoveAll(TestDirectory1)
	err := os.Mkdir(TestDirectory1, 0755)
	assert.Nil(t, err)

	TestDirectory2 := "chunkServer2"
	os.RemoveAll(TestDirectory2)
	defer os.RemoveAll(TestDirectory2)
	err = os.Mkdir(TestDirectory2, 0755)
	assert.Nil(t, err)

	TestDirectory3 := "chunkServer3"
	os.RemoveAll(TestDirectory3)
	defer os.RemoveAll(TestDirectory3)
	err = os.Mkdir(TestDirectory3, 0755)
	assert.Nil(t, err)

	// chunk1->server 2,3
	TestChunkName1 := "1"

	chunkPath := filepath.Join(TestDirectory2, TestChunkName1)
	chunkFile, err := os.Create(chunkPath + ".chunk")
	assert.Nil(t, err)
	_, err = chunkFile.Write(testData)
	assert.Nil(t, err)
	chunkFile.Close()
	chunkCheckSumFile, err := os.Create(chunkPath + ".chksum")
	assert.Nil(t, err)
	_, err = chunkCheckSumFile.Write(checkSum.Bytes())
	assert.Nil(t, err)
	chunkCheckSumFile.Close()

	chunkPath = filepath.Join(TestDirectory3, TestChunkName1)
	chunkFile, err = os.Create(chunkPath + ".chunk")
	assert.Nil(t, err)
	_, err = chunkFile.Write(testData)
	assert.Nil(t, err)
	chunkFile.Close()
	chunkCheckSumFile, err = os.Create(chunkPath + ".chksum")
	assert.Nil(t, err)
	_, err = chunkCheckSumFile.Write(checkSum.Bytes())
	assert.Nil(t, err)
	chunkCheckSumFile.Close()

	// chunk3->server 1,2
	TestChunkName3 := "3"

	chunkPath = filepath.Join(TestDirectory1, TestChunkName3)
	chunkFile, err = os.Create(chunkPath + ".chunk")
	assert.Nil(t, err)
	_, err = chunkFile.Write(testData)
	assert.Nil(t, err)
	chunkFile.Close()
	chunkCheckSumFile, err = os.Create(chunkPath + ".chksum")
	assert.Nil(t, err)
	_, err = chunkCheckSumFile.Write(checkSum.Bytes())
	assert.Nil(t, err)
	chunkCheckSumFile.Close()

	chunkPath = filepath.Join(TestDirectory2, TestChunkName3)
	chunkFile, err = os.Create(chunkPath + ".chunk")
	assert.Nil(t, err)
	_, err = chunkFile.Write(testData)
	assert.Nil(t, err)
	chunkFile.Close()
	chunkCheckSumFile, err = os.Create(chunkPath + ".chksum")
	assert.Nil(t, err)
	_, err = chunkCheckSumFile.Write(checkSum.Bytes())
	assert.Nil(t, err)
	chunkCheckSumFile.Close()

	masterServer, err := master.NewMaster(true)
	assert.Nil(t, err)
	assert.NotNil(t, masterServer)

	err = masterServer.Start()
	masterPort := masterServer.Listener.Addr().(*net.TCPAddr).String()

	assert.Nil(t, err)

	testFileMap := map[string][]int64{
		"test_file_1": {
			1, 2, 0,
		},
		"test_file_2": {
			3, 4, 5,
			7, 6,
		},
	}
	masterServer.FileMap = testFileMap

	chunkServer1 := chunkserver.NewChunkServer("chunkServer1", masterPort)
	chunkServer1port, err := chunkServer1.Start()
	assert.Nil(t, err)
	chunkServer2 := chunkserver.NewChunkServer("chunkServer2", masterPort)
	chunkServer2port, err := chunkServer2.Start()
	assert.Nil(t, err)
	chunkServer3 := chunkserver.NewChunkServer("chunkServer3", masterPort)
	chunkServer3port, err := chunkServer3.Start()
	assert.Nil(t, err)

	t.Log(chunkServer1port, chunkServer2port, chunkServer3port)

	testChunkServerHandler := map[int64]map[string]bool{
		1: {
			chunkServer2port: true,
			chunkServer3port: true,
		},
		3: {
			chunkServer1port: true,
			chunkServer2port: true,
		},
	}

	testChunks := map[int64]*master.Chunk{
		1: {
			ChunkHandle:  1,
			ChunkVersion: 2,
		},
	}
	masterServer.ChunkServerHandler = testChunkServerHandler
	masterServer.ChunkHandles = testChunks

	client := client.NewClient(masterPort)
	dataRead1, err := client.Read("test_file_1", 3)
	assert.NotNil(t, err)
	assert.Equal(t, "invalid chunkOffset", err.Error())
	assert.Nil(t, dataRead1)

	dataRead2, err := client.Read("test_file_1", 1)
	assert.NotNil(t, err)
	assert.Equal(t, "no chunk servers present for chunk handle ", err.Error())
	assert.Nil(t, dataRead2)

	dataRead3, err := client.Read("test_file_1", 0)
	assert.Nil(t, err)
	assert.Equal(t, nil, err)
	t.Log("data received", dataRead3)
	assert.Equal(t, testData, dataRead3)
}

// TestClientWriteToMaster tests the client's ability to write to master
// when the file does not exist on master server and no chunk servers are availible
func TestClientWriteToMaster_1(t *testing.T) {
	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	testFileName := "testFile"
	masterServer, err := master.NewMaster(true)

	assert.NotNil(t, masterServer)
	assert.Nil(t, err)
	err = masterServer.Start()
	masterPort := masterServer.Listener.Addr().(*net.TCPAddr).String()

	assert.Nil(t, err)

	client := client.NewClient(masterPort)
	assert.NotNil(t, client)

	request := common.ClientMasterWriteRequest{
		Filename: testFileName,
	}
	response, err := client.WriteToMasterServer(request)
	assert.NotNil(t, err)
	assert.Nil(t, response)
	assert.Equal(t, "no chunk servers available", err.Error())

}

// TestClientWriteToMaster tests the client's ability to write to master
// when the file does not exist on master server and chunk servers are available
func TestClientWriteToMaster_2(t *testing.T) {
	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	TestDirectory1 := "chunkServer1"
	os.RemoveAll(TestDirectory1)
	defer os.RemoveAll(TestDirectory1)

	TestDirectory2 := "chunkServer2"
	os.RemoveAll(TestDirectory2)
	defer os.RemoveAll(TestDirectory2)

	TestDirectory3 := "chunkServer3"
	os.RemoveAll(TestDirectory3)
	defer os.RemoveAll(TestDirectory3)

	testFileName := "testFile"

	masterServer, err := master.NewMaster(false)
	assert.NotNil(t, masterServer)
	assert.Nil(t, err)

	err = masterServer.Start()
	masterPort := masterServer.Listener.Addr().(*net.TCPAddr).String()

	assert.Nil(t, err)

	client := client.NewClient(masterPort)
	assert.NotNil(t, client)

	chunkServer1 := chunkserver.NewChunkServer("chunkServer1", masterPort)
	chunkServer1port, err := chunkServer1.Start()
	assert.Nil(t, err)

	chunkServer2 := chunkserver.NewChunkServer("chunkServer2", masterPort)
	chunkServer2port, err := chunkServer2.Start()
	assert.Nil(t, err)

	chunkServer3 := chunkserver.NewChunkServer("chunkServer3", masterPort)
	chunkServer3port, err := chunkServer3.Start()
	assert.Nil(t, err)

	log.Println(chunkServer1port, chunkServer2port, chunkServer3port)

	request := common.ClientMasterWriteRequest{
		Filename: testFileName,
	}

	response, err := client.WriteToMasterServer(request)
	assert.Nil(t, err)
	assert.NotNil(t, response)
	assert.NotEmpty(t, len(response.SecondaryChunkServers))
	assert.NotEmpty(t, response.PrimaryChunkServer)
	assert.Empty(t, response.ErrorMessage)
	assert.NotEqual(t, -1, response.ChunkHandle)

	time.Sleep(1 * time.Second)

	if response.PrimaryChunkServer == chunkServer1port {
		assert.NotEmpty(t, chunkServer1.LeaseGrants)
	} else if response.PrimaryChunkServer == chunkServer2port {
		assert.NotEmpty(t, chunkServer2.LeaseGrants)
	} else if response.PrimaryChunkServer == chunkServer3port {
		assert.NotEmpty(t, chunkServer3.LeaseGrants)
	} else {
		t.Error("server returned is not appropriate")
	}

	err = masterServer.Shutdown()
	assert.Nil(t, err)
}

// TestClientWriteToMaster_3 tests the client's ability to write to master 2 times consecutively
// when the file already exists on master server and chunk servers are available
func TestClientWriteToMaster_3(t *testing.T) {
	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	TestDirectory1 := "chunkServer1"
	os.RemoveAll(TestDirectory1)
	defer os.RemoveAll(TestDirectory1)

	TestDirectory2 := "chunkServer2"
	os.RemoveAll(TestDirectory2)
	defer os.RemoveAll(TestDirectory2)

	TestDirectory3 := "chunkServer3"
	os.RemoveAll(TestDirectory3)
	defer os.RemoveAll(TestDirectory3)

	testFileName := "testFile"

	masterServer, err := master.NewMaster(false)
	assert.NotNil(t, masterServer)
	assert.Nil(t, err)

	err = masterServer.Start()
	masterPort := masterServer.Listener.Addr().(*net.TCPAddr).String()

	assert.Nil(t, err)

	client := client.NewClient(masterPort)
	assert.NotNil(t, client)

	chunkServer1 := chunkserver.NewChunkServer(TestDirectory1, masterPort)
	chunkServer1port, err := chunkServer1.Start()
	assert.Nil(t, err)

	chunkServer2 := chunkserver.NewChunkServer(TestDirectory2, masterPort)
	chunkServer2port, err := chunkServer2.Start()
	assert.Nil(t, err)

	chunkServer3 := chunkserver.NewChunkServer(TestDirectory3, masterPort)
	chunkServer3port, err := chunkServer3.Start()
	assert.Nil(t, err)

	log.Println(chunkServer1port, chunkServer2port, chunkServer3port)

	request := common.ClientMasterWriteRequest{
		Filename: testFileName,
	}

	response1, err := client.WriteToMasterServer(request)

	time.Sleep(1 * time.Second)

	assert.Nil(t, err)
	assert.NotNil(t, response1)
	assert.NotEmpty(t, len(response1.SecondaryChunkServers))
	assert.NotEmpty(t, response1.PrimaryChunkServer)
	assert.Empty(t, response1.ErrorMessage)
	assert.NotEqual(t, -1, response1.ChunkHandle)

	if response1.PrimaryChunkServer == chunkServer1port {
		assert.NotEmpty(t, len(chunkServer1.LeaseGrants))
		_, present := chunkServer1.LeaseGrants[response1.ChunkHandle]
		assert.True(t, present)
	} else if response1.PrimaryChunkServer == chunkServer2port {
		assert.NotEmpty(t, len(chunkServer2.LeaseGrants))
		_, present := chunkServer2.LeaseGrants[response1.ChunkHandle]
		assert.True(t, present)
	} else if response1.PrimaryChunkServer == chunkServer3port {
		assert.NotEmpty(t, len(chunkServer3.LeaseGrants))
		_, present := chunkServer3.LeaseGrants[response1.ChunkHandle]
		assert.True(t, present)
	} else {
		t.Error("server returned is not appropriate")
	}

	response2, err := client.WriteToMasterServer(request)
	time.Sleep(1 * time.Second)

	assert.Nil(t, err)
	assert.NotNil(t, response2)
	assert.NotEmpty(t, len(response2.SecondaryChunkServers))
	assert.NotEmpty(t, response2.PrimaryChunkServer)
	assert.Empty(t, response2.ErrorMessage)
	assert.NotEqual(t, -1, response2.ChunkHandle)

	assert.Equal(t, response2.ChunkHandle, response1.ChunkHandle)
	assert.NotEqual(t, response2.MutationId, response1.MutationId)
	assert.Equal(t, response1.PrimaryChunkServer, response2.PrimaryChunkServer)

	err = masterServer.Shutdown()
	assert.Nil(t, err)
}

// TestClientWriteToMaster_4 tests the client's ability to write to master 2 times consecutively
// when the file already exists on master server and chunk servers are available however the lease on the chunkId
// expires in between the 2 consecutive writes
func TestClientWriteToMaster_4(t *testing.T) {

	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	TestDirectory1 := "chunkServer1"
	os.RemoveAll(TestDirectory1)
	defer os.RemoveAll(TestDirectory1)

	TestDirectory2 := "chunkServer2"
	os.RemoveAll(TestDirectory2)
	defer os.RemoveAll(TestDirectory2)

	TestDirectory3 := "chunkServer3"
	os.RemoveAll(TestDirectory3)
	defer os.RemoveAll(TestDirectory3)

	testFileName := "testFile"

	masterServer, err := master.NewMaster(false)
	assert.NotNil(t, masterServer)
	assert.Nil(t, err)

	err = masterServer.Start()
	masterPort := masterServer.Listener.Addr().(*net.TCPAddr).String()
	assert.Nil(t, err)

	client := client.NewClient(masterPort)
	assert.NotNil(t, client)

	chunkServer1 := chunkserver.NewChunkServer(TestDirectory1, masterPort)
	chunkServer1port, err := chunkServer1.Start()
	assert.Nil(t, err)

	chunkServer2 := chunkserver.NewChunkServer(TestDirectory2, masterPort)
	chunkServer2port, err := chunkServer2.Start()
	assert.Nil(t, err)

	chunkServer3 := chunkserver.NewChunkServer(TestDirectory3, masterPort)
	chunkServer3port, err := chunkServer3.Start()
	assert.Nil(t, err)

	assert.Equal(t, 3, masterServer.ServerList.Len())
	log.Println(chunkServer1port, chunkServer2port, chunkServer3port)
	request := common.ClientMasterWriteRequest{
		Filename: testFileName,
	}

	response1, err := client.WriteToMasterServer(request)

	time.Sleep(1 * time.Second)

	assert.Nil(t, err)
	assert.NotNil(t, response1)
	assert.NotEmpty(t, len(response1.SecondaryChunkServers))
	assert.NotEmpty(t, response1.PrimaryChunkServer)
	assert.Empty(t, response1.ErrorMessage)
	assert.NotEqual(t, -1, response1.ChunkHandle)

	if response1.PrimaryChunkServer == chunkServer1port {
		assert.NotEmpty(t, len(chunkServer1.LeaseGrants))
		_, present := chunkServer1.LeaseGrants[response1.ChunkHandle]
		assert.True(t, present)
	} else if response1.PrimaryChunkServer == chunkServer2port {
		assert.NotEmpty(t, len(chunkServer2.LeaseGrants))
		_, present := chunkServer2.LeaseGrants[response1.ChunkHandle]
		assert.True(t, present)
	} else if response1.PrimaryChunkServer == chunkServer3port {
		assert.NotEmpty(t, len(chunkServer3.LeaseGrants))
		_, present := chunkServer3.LeaseGrants[response1.ChunkHandle]
		assert.True(t, present)
	} else {
		t.Error("server returned is not appropriate")
	}

	newGrantTime := masterServer.LeaseGrants[response1.ChunkHandle].GrantTime.Add(-65 * time.Second)
	masterServer.LeaseGrants[response1.ChunkHandle].GrantTime = newGrantTime

	response2, err := client.WriteToMasterServer(request)

	time.Sleep(1 * time.Second)

	assert.Nil(t, err)
	assert.NotNil(t, response2)
	assert.NotEmpty(t, len(response2.SecondaryChunkServers))
	assert.NotEmpty(t, response2.PrimaryChunkServer)
	assert.Empty(t, response2.ErrorMessage)
	assert.NotEqual(t, -1, response2.ChunkHandle)

	assert.Equal(t, response2.ChunkHandle, response1.ChunkHandle)
	assert.NotEqual(t, response2.MutationId, response1.MutationId)

	if response2.PrimaryChunkServer == chunkServer1port {
		assert.NotEmpty(t, len(chunkServer1.LeaseGrants))
		_, present := chunkServer1.LeaseGrants[response2.ChunkHandle]
		assert.True(t, present)
	} else if response2.PrimaryChunkServer == chunkServer2port {
		assert.NotEmpty(t, len(chunkServer2.LeaseGrants))
		_, present := chunkServer2.LeaseGrants[response2.ChunkHandle]
		assert.True(t, present)
	} else if response2.PrimaryChunkServer == chunkServer3port {
		assert.NotEmpty(t, len(chunkServer3.LeaseGrants))
		_, present := chunkServer3.LeaseGrants[response2.ChunkHandle]
		assert.True(t, present)
	} else {
		t.Error("server returned is not appropriate")
	}

	err = masterServer.Shutdown()
	assert.Nil(t, err)
}

/*
1. failing at primary
2. passing at primary but failing in one of the secondary
3. passing at all9
*/
func TestReplicateChunkDataToAllServers_1(t *testing.T) {
	testData := []byte{'t', 'e', 's', 't'}

	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	TestDirectory1 := "chunkServer1"
	os.RemoveAll(TestDirectory1)
	defer os.RemoveAll(TestDirectory1)

	TestDirectory2 := "chunkServer2"
	os.RemoveAll(TestDirectory2)
	defer os.RemoveAll(TestDirectory2)

	TestDirectory3 := "chunkServer3"
	os.RemoveAll(TestDirectory3)
	defer os.RemoveAll(TestDirectory3)

	// testFileName := "testFile"

	masterServer, err := master.NewMaster(false)
	assert.NotNil(t, masterServer)
	assert.Nil(t, err)

	err = masterServer.Start()
	masterPort := masterServer.Listener.Addr().(*net.TCPAddr).String()
	assert.Nil(t, err)

	client := client.NewClient(masterPort)
	assert.NotNil(t, client)

	chunkServer1 := chunkserver.NewChunkServer(TestDirectory1, masterPort)
	chunkServer1port, err := chunkServer1.Start()
	assert.Nil(t, err)

	chunkServer2 := chunkserver.NewChunkServer(TestDirectory2, masterPort)
	chunkServer2port, err := chunkServer2.Start()
	assert.Nil(t, err)

	chunkServer3 := chunkserver.NewChunkServer(TestDirectory3, masterPort)
	chunkServer3port, err := chunkServer3.Start()
	assert.Nil(t, err)

	assert.Equal(t, 3, masterServer.ServerList.Len())
	log.Println(chunkServer1port, chunkServer2port, chunkServer3port)

	request := common.ClientMasterWriteResponse{
		MutationId:            243,
		ChunkHandle:           978,
		PrimaryChunkServer:    "csasaaca",
		SecondaryChunkServers: []string{chunkServer1port, chunkServer2port},
		ErrorMessage:          "",
	}

	err = client.ReplicateChunkDataToAllServers(&request, testData)
	assert.NotNil(t, err)
	assert.Equal(t, "error because we were unable to write the chunk to all chunkServers", err.Error())

	err = masterServer.Shutdown()
	assert.Nil(t, err)
}

func TestReplicateChunkDataToAllServers_2(t *testing.T) {
	testData := []byte{'t', 'e', 's', 't'}
	
	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	TestDirectory1 := "chunkServer1"
	os.RemoveAll(TestDirectory1)
	defer os.RemoveAll(TestDirectory1)

	TestDirectory2 := "chunkServer2"
	os.RemoveAll(TestDirectory2)
	defer os.RemoveAll(TestDirectory2)

	TestDirectory3 := "chunkServer3"
	os.RemoveAll(TestDirectory3)
	defer os.RemoveAll(TestDirectory3)
	
	masterServer, err := master.NewMaster(false)
	assert.NotNil(t, masterServer)
	assert.Nil(t, err)

	err = masterServer.Start()
	masterPort := masterServer.Listener.Addr().(*net.TCPAddr).String()
	assert.Nil(t, err)

	client := client.NewClient(masterPort)
	assert.NotNil(t, client)

	chunkServer1 := chunkserver.NewChunkServer(TestDirectory1, masterPort)
	chunkServer1port, err := chunkServer1.Start()
	assert.Nil(t, err)

	chunkServer2 := chunkserver.NewChunkServer(TestDirectory2, masterPort)
	chunkServer2port, err := chunkServer2.Start()
	assert.Nil(t, err)

	chunkServer3 := chunkserver.NewChunkServer(TestDirectory3, masterPort)
	chunkServer3port, err := chunkServer3.Start()
	assert.Nil(t, err)

	assert.Equal(t, 3, masterServer.ServerList.Len())
	log.Println(chunkServer1port, chunkServer2port, chunkServer3port)

	request := common.ClientMasterWriteResponse{
		MutationId:            243,
		ChunkHandle:           978,
		PrimaryChunkServer:    chunkServer1port,
		SecondaryChunkServers: []string{"testServer", chunkServer2port},
		ErrorMessage:          "",
	}

	err = client.ReplicateChunkDataToAllServers(&request, testData)
	assert.NotNil(t, err)
	assert.Equal(t,"error because we were unable to write the chunk to all chunkServers", err.Error())

	dataTransferred, presentOnPrimaryServer := chunkServer1.LruCache.Get(request.MutationId)
	assert.Equal(t, true, presentOnPrimaryServer)
	assert.Equal(t, testData, dataTransferred)

	err = masterServer.Shutdown()
	assert.Nil(t, err)
}

func TestReplicateChunkDataToAllServers_3(t *testing.T) {
	testData := []byte{'t', 'e', 's', 't'}
	
	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	TestDirectory1 := "chunkServer1"
	os.RemoveAll(TestDirectory1)
	defer os.RemoveAll(TestDirectory1)

	TestDirectory2 := "chunkServer2"
	os.RemoveAll(TestDirectory2)
	defer os.RemoveAll(TestDirectory2)

	TestDirectory3 := "chunkServer3"
	os.RemoveAll(TestDirectory3)
	defer os.RemoveAll(TestDirectory3)

	masterServer, err := master.NewMaster(false)
	assert.NotNil(t, masterServer)
	assert.Nil(t, err)

	err = masterServer.Start()
	masterPort := masterServer.Listener.Addr().(*net.TCPAddr).String()
	assert.Nil(t, err)

	client := client.NewClient(masterPort)
	assert.NotNil(t, client)

	chunkServer1 := chunkserver.NewChunkServer(TestDirectory1, masterPort)
	chunkServer1port, err := chunkServer1.Start()
	assert.Nil(t, err)

	chunkServer2 := chunkserver.NewChunkServer(TestDirectory2, masterPort)
	chunkServer2port, err := chunkServer2.Start()
	assert.Nil(t, err)

	chunkServer3 := chunkserver.NewChunkServer(TestDirectory3, masterPort)
	chunkServer3port, err := chunkServer3.Start()
	assert.Nil(t, err)

	assert.Equal(t, 3, masterServer.ServerList.Len())

	log.Println(chunkServer1port, chunkServer2port, chunkServer3port)

	request := common.ClientMasterWriteResponse{
		MutationId:            243,
		ChunkHandle:           978,
		PrimaryChunkServer:    chunkServer1port,
		SecondaryChunkServers: []string{chunkServer3port, chunkServer2port},
		ErrorMessage:          "",
	}

	err = client.ReplicateChunkDataToAllServers(&request, testData)
	assert.Nil(t, err)

	for _, server := range []*chunkserver.ChunkServer{chunkServer1, chunkServer2, chunkServer3} {
		dataTransferred, presentOnServer := server.LruCache.Get(request.MutationId)
		assert.Equal(t, true, presentOnServer)
		assert.Equal(t, testData, dataTransferred)

	}

	err = masterServer.Shutdown()
	assert.Nil(t, err)
}

// send the request when the primaryServer does not have a valid lease
// if the lease does not exist the connection should time out and an error will be
// returned
func TestSendCommitRequestToPrimary_1(t *testing.T) {
	testData := []byte{'t', 'e', 's', 't'}

	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	TestDirectory1 := "chunkServer1"
	os.RemoveAll(TestDirectory1)
	defer os.RemoveAll(TestDirectory1)

	TestDirectory2 := "chunkServer2"
	os.RemoveAll(TestDirectory2)
	defer os.RemoveAll(TestDirectory2)

	TestDirectory3 := "chunkServer3"
	os.RemoveAll(TestDirectory3)
	defer os.RemoveAll(TestDirectory3)

	// testFileName:="testFile"
	masterServer,err:=master.NewMaster(false)
	assert.NotNil(t, masterServer)
	assert.Nil(t, err)

	err = masterServer.Start()
	masterPort:=masterServer.Listener.Addr().(*net.TCPAddr).String()
	assert.Nil(t, err)

	client := client.NewClient(masterPort)
	assert.NotNil(t, client)

	chunkServer1 := chunkserver.NewChunkServer(TestDirectory1, masterPort)
	chunkServer1port, err := chunkServer1.Start()
	assert.Nil(t, err)

	chunkServer2 := chunkserver.NewChunkServer(TestDirectory2, masterPort)
	chunkServer2port, err := chunkServer2.Start()
	assert.Nil(t, err)

	chunkServer3 := chunkserver.NewChunkServer(TestDirectory3, masterPort)
	chunkServer3port, err := chunkServer3.Start()
	assert.Nil(t, err)

	assert.Equal(t, 3, masterServer.ServerList.Len())
	log.Println(chunkServer1port, chunkServer2port, chunkServer3port)
	request := common.ClientMasterWriteResponse{
		MutationId:            243,
		ChunkHandle:           978,
		PrimaryChunkServer:    chunkServer1port,
		SecondaryChunkServers: []string{chunkServer3port, chunkServer2port},
		ErrorMessage:          "",
	}

	err = client.ReplicateChunkDataToAllServers(&request, testData)
	assert.Nil(t, err)

	for _, server := range []*chunkserver.ChunkServer{chunkServer1, chunkServer2, chunkServer3} {
		dataTransferred, presentOnServer := server.LruCache.Get(request.MutationId)
		assert.Equal(t, true, presentOnServer)
		assert.Equal(t, testData, dataTransferred)
	}

	commitRequest := common.PrimaryChunkCommitRequest{
		ChunkHandle:      request.ChunkHandle,
		MutationId:       request.MutationId,
		SecondaryServers: request.SecondaryChunkServers,
	}

	_,err = client.SendCommitRequestToPrimary(chunkServer1port, commitRequest)
	assert.NotNil(t, err)

	err = masterServer.Shutdown()
	assert.Nil(t, err)
}

// after pushing the data to all the chunk servers, the primary chunk server is shutdown somehow
// while sending the primary commit request
func TestSendCommitRequestToPrimary_2(t *testing.T) {
	testData := []byte{'t', 'e', 's', 't'}

	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	TestDirectory1 := "chunkServer1"
	os.RemoveAll(TestDirectory1)
	defer os.RemoveAll(TestDirectory1)

	TestDirectory2 := "chunkServer2"
	os.RemoveAll(TestDirectory2)
	defer os.RemoveAll(TestDirectory2)

	TestDirectory3 := "chunkServer3"
	os.RemoveAll(TestDirectory3)
	defer os.RemoveAll(TestDirectory3)

	// testFileName:="testFile"
	masterServer, err := master.NewMaster(false)

	assert.NotNil(t, masterServer)
	assert.Nil(t, err)

	err = masterServer.Start()
	masterPort := masterServer.Listener.Addr().(*net.TCPAddr).String()

	assert.Nil(t, err)

	client := client.NewClient(masterPort)
	assert.NotNil(t, client)

	chunkServer1 := chunkserver.NewChunkServer(TestDirectory1, masterPort)
	chunkServer1port, err := chunkServer1.Start()
	assert.Nil(t, err)

	chunkServer2 := chunkserver.NewChunkServer(TestDirectory2, masterPort)
	chunkServer2port, err := chunkServer2.Start()
	assert.Nil(t, err)

	chunkServer3 := chunkserver.NewChunkServer(TestDirectory3, masterPort)
	chunkServer3port, err := chunkServer3.Start()
	assert.Nil(t, err)

	assert.Equal(t, 3, masterServer.ServerList.Len())
	log.Println(chunkServer1port, chunkServer2port, chunkServer3port)
	request := common.ClientMasterWriteResponse{
		MutationId:            243,
		ChunkHandle:           978,
		PrimaryChunkServer:    chunkServer1port,
		SecondaryChunkServers: []string{chunkServer3port, chunkServer2port},
		ErrorMessage:          "",
	}

	err = client.ReplicateChunkDataToAllServers(&request, testData)
	assert.Nil(t, err)

	for _, server := range []*chunkserver.ChunkServer{chunkServer1, chunkServer2, chunkServer3} {
		dataTransferred, presentOnServer := server.LruCache.Get(request.MutationId)
		assert.Equal(t, true, presentOnServer)
		assert.Equal(t, testData, dataTransferred)
	}

	err = chunkServer1.Shutdown()
	assert.Nil(t, err)
	t.Log(err)

	commitRequest := common.PrimaryChunkCommitRequest{
		ChunkHandle:      request.ChunkHandle,
		MutationId:       request.MutationId,
		SecondaryServers: request.SecondaryChunkServers,
	}

	_, err = client.SendCommitRequestToPrimary(chunkServer1port, commitRequest)
	assert.NotNil(t, err)

	err = masterServer.Shutdown()
	assert.Nil(t, err)
}

// the primary chunkServer does not have the data in its LRU cache on receiving
// the primary commit request
func TestSendCommitRequestToPrimary_3(t *testing.T) {
	testData := []byte{'t', 'e', 's', 't'}

	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	TestDirectory1 := "chunkServer1"
	os.RemoveAll(TestDirectory1)
	defer os.RemoveAll(TestDirectory1)

	TestDirectory2 := "chunkServer2"
	os.RemoveAll(TestDirectory2)
	defer os.RemoveAll(TestDirectory2)

	TestDirectory3 := "chunkServer3"
	os.RemoveAll(TestDirectory3)
	defer os.RemoveAll(TestDirectory3)

	masterServer, err := master.NewMaster(false)

	assert.NotNil(t, masterServer)
	assert.Nil(t, err)

	err = masterServer.Start()
	masterPort := masterServer.Listener.Addr().(*net.TCPAddr).String()

	assert.Nil(t, err)

	client := client.NewClient(masterPort)
	assert.NotNil(t, client)

	chunkServer1 := chunkserver.NewChunkServer(TestDirectory1, masterPort)
	chunkServer1port, err := chunkServer1.Start()
	assert.Nil(t, err)

	chunkServer2 := chunkserver.NewChunkServer(TestDirectory2, masterPort)
	chunkServer2port, err := chunkServer2.Start()
	assert.Nil(t, err)

	chunkServer3 := chunkserver.NewChunkServer(TestDirectory3, masterPort)
	chunkServer3port, err := chunkServer3.Start()
	assert.Nil(t, err)

	assert.Equal(t, 3, masterServer.ServerList.Len())

	log.Println(chunkServer1port, chunkServer2port, chunkServer3port)

	request := common.ClientMasterWriteResponse{
		MutationId:            243,
		ChunkHandle:           978,
		PrimaryChunkServer:    chunkServer1port,
		SecondaryChunkServers: []string{chunkServer3port, chunkServer2port},
		ErrorMessage:          "",
	}

	err = client.ReplicateChunkDataToAllServers(&request, testData)
	assert.Nil(t, err)

	for _, server := range []*chunkserver.ChunkServer{chunkServer1, chunkServer2, chunkServer3} {
		dataTransferred, presentOnServer := server.LruCache.Get(request.MutationId)
		assert.Equal(t, true, presentOnServer)
		assert.Equal(t, testData, dataTransferred)
	}

	removedFromPrimary := chunkServer1.LruCache.Remove(request.MutationId)
	assert.Equal(t, true, removedFromPrimary)

	commitRequest := common.PrimaryChunkCommitRequest{
		ChunkHandle:      request.ChunkHandle,
		MutationId:       request.MutationId,
		SecondaryServers: request.SecondaryChunkServers,
	}

	chunkServer1.LeaseGrants[commitRequest.ChunkHandle] = &chunkserver.LeaseGrant{
		GrantTime:   time.Now(),
		ChunkHandle: commitRequest.ChunkHandle,
	}
	_, err = client.SendCommitRequestToPrimary(chunkServer1port, commitRequest)
	assert.NotNil(t, err)

	err = masterServer.Shutdown()
	assert.Nil(t, err)
}

// the primary chunkServer has the data in its LRU cache on receiving
// the primary commit request, so it mutates its chunk however the inter chunk server commit
// request fails(the connection times out after 10 secs) as one of the secondary chunk servers is down
func TestSendCommitRequestToPrimary_4(t *testing.T) {
	testData := []byte{'t', 'e', 's', 't'}

	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	TestDirectory1 := "chunkServer1"
	os.RemoveAll(TestDirectory1)
	defer os.RemoveAll(TestDirectory1)

	TestDirectory2 := "chunkServer2"
	os.RemoveAll(TestDirectory2)
	defer os.RemoveAll(TestDirectory2)

	TestDirectory3 := "chunkServer3"
	os.RemoveAll(TestDirectory3)
	defer os.RemoveAll(TestDirectory3)

	// testFileName:="testFile"
	masterServer, err := master.NewMaster(false)

	assert.NotNil(t, masterServer)
	assert.Nil(t, err)

	err = masterServer.Start()
	masterPort := masterServer.Listener.Addr().(*net.TCPAddr).String()

	assert.Nil(t, err)

	client := client.NewClient(masterPort)
	assert.NotNil(t, client)

	chunkServer1 := chunkserver.NewChunkServer(TestDirectory1, masterPort)
	chunkServer1port, err := chunkServer1.Start()
	assert.Nil(t, err)

	chunkServer2 := chunkserver.NewChunkServer(TestDirectory2, masterPort)
	chunkServer2port, err := chunkServer2.Start()
	assert.Nil(t, err)

	chunkServer3 := chunkserver.NewChunkServer(TestDirectory3, masterPort)
	chunkServer3port, err := chunkServer3.Start()
	assert.Nil(t, err)

	assert.Equal(t, 3, masterServer.ServerList.Len())

	log.Println(chunkServer1port, chunkServer2port, chunkServer3port)

	request := common.ClientMasterWriteResponse{
		MutationId:            243,
		ChunkHandle:           978,
		PrimaryChunkServer:    chunkServer1port,
		SecondaryChunkServers: []string{chunkServer3port, chunkServer2port},
		ErrorMessage:          "",
	}

	err = client.ReplicateChunkDataToAllServers(&request, testData)
	assert.Nil(t, err)

	for _, server := range []*chunkserver.ChunkServer{chunkServer1, chunkServer2, chunkServer3} {
		dataTransferred, presentOnServer := server.LruCache.Get(request.MutationId)
		assert.Equal(t, true, presentOnServer)
		assert.Equal(t, testData, dataTransferred)
	}

	err = chunkServer2.Shutdown()
	assert.Nil(t, err)

	commitRequest := common.PrimaryChunkCommitRequest{
		ChunkHandle:      request.ChunkHandle,
		MutationId:       request.MutationId,
		SecondaryServers: request.SecondaryChunkServers,
	}

	chunkServer1.LeaseGrants[commitRequest.ChunkHandle] = &chunkserver.LeaseGrant{
		GrantTime:   time.Now(),
		ChunkHandle: commitRequest.ChunkHandle,
	}

	_, err = client.SendCommitRequestToPrimary(chunkServer1port, commitRequest)
	assert.NotNil(t, err)

	err = masterServer.Shutdown()
	assert.Nil(t, err)
}

// the primary chunkServer has the data in its LRU cache on receiving
// the primary commit request, so it mutates its chunk and the inter chunk server commit
// requests all succeed
func TestSendCommitRequestToPrimary_5(t *testing.T) {
	testData := []byte{'t', 'e', 's', 't'}

	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	TestDirectory1 := "chunkServer1"
	os.RemoveAll(TestDirectory1)
	defer os.RemoveAll(TestDirectory1)

	TestDirectory2 := "chunkServer2"
	os.RemoveAll(TestDirectory2)
	defer os.RemoveAll(TestDirectory2)

	TestDirectory3 := "chunkServer3"
	os.RemoveAll(TestDirectory3)
	defer os.RemoveAll(TestDirectory3)

	// testFileName:="testFile"
	masterServer, err := master.NewMaster(false)

	assert.NotNil(t, masterServer)
	assert.Nil(t, err)

	err = masterServer.Start()
	masterPort := masterServer.Listener.Addr().(*net.TCPAddr).String()

	assert.Nil(t, err)

	client := client.NewClient(masterPort)
	assert.NotNil(t, client)

	chunkServer1 := chunkserver.NewChunkServer(TestDirectory1, masterPort)
	chunkServer1port, err := chunkServer1.Start()
	assert.Nil(t, err)

	chunkServer2 := chunkserver.NewChunkServer(TestDirectory2, masterPort)
	chunkServer2port, err := chunkServer2.Start()
	assert.Nil(t, err)

	chunkServer3 := chunkserver.NewChunkServer(TestDirectory3, masterPort)
	chunkServer3port, err := chunkServer3.Start()
	assert.Nil(t, err)

	assert.Equal(t, 3, masterServer.ServerList.Len())

	log.Println(chunkServer1port, chunkServer2port, chunkServer3port)

	request := common.ClientMasterWriteResponse{
		MutationId:            243,
		ChunkHandle:           978,
		PrimaryChunkServer:    chunkServer1port,
		SecondaryChunkServers: []string{chunkServer3port, chunkServer2port},
		ErrorMessage:          "",
	}

	err = client.ReplicateChunkDataToAllServers(&request, testData)
	assert.Nil(t, err)

	for _, server := range []*chunkserver.ChunkServer{chunkServer1, chunkServer2, chunkServer3} {
		dataTransferred, presentOnServer := server.LruCache.Get(request.MutationId)
		assert.Equal(t, true, presentOnServer)
		assert.Equal(t, testData, dataTransferred)
	}

	commitRequest := common.PrimaryChunkCommitRequest{
		ChunkHandle:      request.ChunkHandle,
		MutationId:       request.MutationId,
		SecondaryServers: request.SecondaryChunkServers,
	}

	chunkServer1.LeaseGrants[commitRequest.ChunkHandle] = &chunkserver.LeaseGrant{
		GrantTime:   time.Now(),
		ChunkHandle: commitRequest.ChunkHandle,
	}

	writeOffset, err := client.SendCommitRequestToPrimary(chunkServer1port, commitRequest)
	assert.Nil(t, err)
	// assert.Equal(t,"failed to write data on secondary chunk server ",err.Error())

	for _, directory := range []string{TestDirectory1, TestDirectory2, TestDirectory3} {
		chunkPath := filepath.Join(directory, "978")
		log.Println(chunkPath)
		chunkFile, err := os.Open(chunkPath + ".chunk")
		assert.Nil(t, err)
		chunkInfo, err := chunkFile.Stat()
		assert.Nil(t, err)
		chunkSize := chunkInfo.Size()
		assert.NotEqual(t, int64(0), chunkSize)
		assert.Equal(t, int64(4), chunkSize)

		writtenData := make([]byte, 4)
		_, err = chunkFile.ReadAt(writtenData, writeOffset)
		assert.Nil(t, err)
		assert.Equal(t, testData, writtenData)
		chunkFile.Close()
	}

	err = masterServer.Shutdown()
	assert.Nil(t, err)

	err = chunkServer1.Shutdown()
	assert.Nil(t, err)

	err = chunkServer2.Shutdown()
	assert.Nil(t, err)

	err = chunkServer3.Shutdown()
	assert.Nil(t, err)
}

// the primary chunkServer has the data in its LRU cache on receiving
// the primary commit request, so it mutates its chunk and the inter chunk server commit
// requests all succeed
func TestSendCommitRequestToPrimary_6(t *testing.T) {
	type ChunkWriteResult struct {
		ChunkHandle int64
		WriteOffset int64
		Error       error
		DataWritten []byte
	}

	testData := []byte{'t', 'e', 's', 't'}
	
	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	TestDirectory1 := "chunkServer1"
	os.RemoveAll(TestDirectory1)
	defer os.RemoveAll(TestDirectory1)
	
	TestDirectory2 := "chunkServer2"
	os.RemoveAll(TestDirectory2)


	TestDirectory3 := "chunkServer3"
	os.RemoveAll(TestDirectory3)

	// testFileName:="testFile"
	masterServer, err := master.NewMaster(false)

	assert.NotNil(t, masterServer)
	assert.Nil(t, err)

	err = masterServer.Start()
	masterPort := masterServer.Listener.Addr().String()

	assert.Nil(t, err)

	portToServerMap := map[string]*chunkserver.ChunkServer{}
	chunkServer1 := chunkserver.NewChunkServer(TestDirectory1, masterPort)
	chunkServer1port, err := chunkServer1.Start()
	assert.Nil(t, err)
	portToServerMap[chunkServer1port] = chunkServer1

	chunkServer2 := chunkserver.NewChunkServer(TestDirectory2, masterPort)
	chunkServer2port, err := chunkServer2.Start()
	assert.Nil(t, err)
	portToServerMap[chunkServer2port] = chunkServer2

	chunkServer3 := chunkserver.NewChunkServer(TestDirectory3, masterPort)
	chunkServer3port, err := chunkServer3.Start()
	assert.Nil(t, err)
	portToServerMap[chunkServer3port] = chunkServer3

	client := client.NewClient(masterPort)
	assert.NotNil(t, client)

	assert.Equal(t, 3, masterServer.ServerList.Len())
	log.Println(chunkServer1port, chunkServer2port, chunkServer3port)

	// request := common.ClientMasterWriteResponse{
	// 	MutationId:            243,
	// 	ChunkHandle:           978,
	// 	PrimaryChunkServer:    chunkServer1port,
	// 	SecondaryChunkServers: []string{chunkServer3port, chunkServer2port},
	// 	ErrorMessage:          "",
	// }

	requests := make([]common.ClientMasterWriteResponse, 10)

	primaryServers := []string{chunkServer1port, chunkServer2port, chunkServer3port}
	secondaryCombinations := [][]string{
		{chunkServer2port, chunkServer3port},
		{chunkServer1port, chunkServer3port},
		{chunkServer2port, chunkServer1port},
	}

	for i := range 10 {
		primaryIdx := i % len(primaryServers)
		secondaryIdx := i % len(secondaryCombinations)
		requests[i] = common.ClientMasterWriteResponse{
			MutationId:            int64(243 + i),
			ChunkHandle:           int64(i%3) + 1,
			PrimaryChunkServer:    primaryServers[primaryIdx],
			SecondaryChunkServers: secondaryCombinations[secondaryIdx],
			ErrorMessage:          "",
		}
	}

	// Create a waitgroup to synchronize goroutines
	var wg sync.WaitGroup

	// Create a channel to collect responses
	resultChan := make(chan ChunkWriteResult, 10)

	// Launch 10 goroutines
	for i, request := range requests {
		wg.Add(1)
		// just generating different data
		newTestData := append(testData, byte('0'+i))
		go func(i int, req common.ClientMasterWriteResponse, data []byte) {
			defer wg.Done()

			// Replicate chunk to all servers
			err := client.ReplicateChunkDataToAllServers(&req, data)
			assert.Nil(t, err)

			for _, server := range []*chunkserver.ChunkServer{chunkServer1, chunkServer2, chunkServer3} {
				dataTransferred, presentOnServer := server.LruCache.Get(request.MutationId)
				assert.Equal(t, true, presentOnServer)
				assert.Equal(t, data, dataTransferred)
			}

			// Create commit request
			commitRequest := common.PrimaryChunkCommitRequest{
				ChunkHandle:      req.ChunkHandle,
				MutationId:       req.MutationId,
				SecondaryServers: req.SecondaryChunkServers,
			}

			// Grant lease to primary
			portToServerMap[req.PrimaryChunkServer].LeaseGrants[commitRequest.ChunkHandle] = &chunkserver.LeaseGrant{
				GrantTime:   time.Now().Add(-20 * time.Second),
				ChunkHandle: commitRequest.ChunkHandle,
			}

			// Send commit request to primary
			writeOffset, err := client.SendCommitRequestToPrimary(req.PrimaryChunkServer, commitRequest)

			// Send result to channel
			resultChan <- ChunkWriteResult{
				ChunkHandle: req.ChunkHandle,
				WriteOffset: writeOffset,
				Error:       err,
				DataWritten: data,
			}
		}(i, request, newTestData)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(resultChan)

	for result := range resultChan {

		for _, directory := range []string{TestDirectory1, TestDirectory2, TestDirectory3} {
			chunkName := strconv.FormatInt(result.ChunkHandle, 10)
			chunkPath := filepath.Join(directory, chunkName)
			log.Println(chunkPath)
			chunkFile, err := os.Open(chunkPath + ".chunk")
			assert.Nil(t, err)
			chunkInfo, err := chunkFile.Stat()
			assert.Nil(t, err)
			chunkSize := chunkInfo.Size()
			assert.NotEqual(t, int64(0), chunkSize)
			writtenData := make([]byte, len(result.DataWritten))
			_, err = chunkFile.ReadAt(writtenData, result.WriteOffset)
			assert.Nil(t, err)
			assert.Equal(t, result.DataWritten, writtenData)
			chunkFile.Close()
		}

	}

	for _, chunkHandle := range []int64{1, 2, 3} {
		chunkName := strconv.FormatInt(chunkHandle, 10)
		chunkSizeMap := map[int64]string{}
		for _, directory := range []string{TestDirectory1, TestDirectory2, TestDirectory3} {
			chunkPath := filepath.Join(directory, chunkName)
			log.Println(chunkPath)
			chunkFile, err := os.Open(chunkPath + ".chunk")
			assert.Nil(t, err)
			chunkInfo, err := chunkFile.Stat()
			assert.Nil(t, err)
			chunkSize := chunkInfo.Size()
			assert.NotEqual(t, int64(0), chunkSize)
			chunkSizeMap[chunkSize] = chunkName
			chunkFile.Close()
		}
		assert.Equal(t, 1, len(chunkSizeMap))
	}

	err = masterServer.Shutdown()
	assert.Nil(t, err)

	err = chunkServer1.Shutdown()
	assert.Nil(t, err)

	err = chunkServer2.Shutdown()
	assert.Nil(t, err)

	err = chunkServer3.Shutdown()
	assert.Nil(t, err)
}

// Testing the entire write workflow
func TestClientWrite_1(t *testing.T) {

	testData := []byte{'t', 'e', 's', 't'}

	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	TestDirectory1 := "chunkServer1"
	os.RemoveAll(TestDirectory1)
	defer os.RemoveAll(TestDirectory1)
	
	TestDirectory2 := "chunkServer2"
	os.RemoveAll(TestDirectory2)
	defer os.RemoveAll(TestDirectory2)

	TestDirectory3 := "chunkServer3"
	os.RemoveAll(TestDirectory3)
	defer os.RemoveAll(TestDirectory3)

	masterServer, err := master.NewMaster(false)

	assert.NotNil(t, masterServer)
	assert.Nil(t, err)

	err = masterServer.Start()
	masterPort := masterServer.Listener.Addr().String()

	assert.Nil(t, err)

	portToServerMap := map[string]*chunkserver.ChunkServer{}
	chunkServer1 := chunkserver.NewChunkServer(TestDirectory1, masterPort)
	chunkServer1port, err := chunkServer1.Start()
	assert.Nil(t, err)
	portToServerMap[chunkServer1port] = chunkServer1

	chunkServer2 := chunkserver.NewChunkServer(TestDirectory2, masterPort)
	chunkServer2port, err := chunkServer2.Start()
	assert.Nil(t, err)
	portToServerMap[chunkServer2port] = chunkServer2

	chunkServer3 := chunkserver.NewChunkServer(TestDirectory3, masterPort)
	chunkServer3port, err := chunkServer3.Start()
	assert.Nil(t, err)
	portToServerMap[chunkServer3port] = chunkServer3

	client := client.NewClient(masterPort)
	assert.NotNil(t, client)

	assert.Equal(t, 3, masterServer.ServerList.Len())
	log.Println(chunkServer1port, chunkServer2port, chunkServer3port)

	testFile := "testFile"
	err = client.Write(testFile, testData)
	assert.NoError(t, err)

	err = masterServer.Shutdown()
	assert.Nil(t, err)

	err = chunkServer1.Shutdown()
	assert.Nil(t, err)

	err = chunkServer2.Shutdown()
	assert.Nil(t, err)

	err = chunkServer3.Shutdown()
	assert.Nil(t, err)
}

// Testing the entire write workflow by writing concurrently to multiple files
func TestClientWrite_2(t *testing.T) {

	testData := []byte{'t', 'e', 's', 't'}
	
	TestMasterDirectory := "masterDir"
	os.RemoveAll(TestMasterDirectory)
	defer os.RemoveAll(TestMasterDirectory)

	TestDirectory1 := "chunkServer1"
	os.RemoveAll(TestDirectory1)
	defer os.RemoveAll(TestDirectory1)

	TestDirectory2 := "chunkServer2"
	os.RemoveAll(TestDirectory2)
	defer os.RemoveAll(TestDirectory2)
	
	TestDirectory3 := "chunkServer3"
	os.RemoveAll(TestDirectory3)
	defer os.RemoveAll(TestDirectory3)

	// testFileName:="testFile"
	masterServer, err := master.NewMaster(false)

	assert.NotNil(t, masterServer)
	assert.Nil(t, err)

	err = masterServer.Start()
	masterPort := masterServer.Listener.Addr().String()
	assert.Nil(t, err)
	log.Println(masterPort)

	chunkServer1 := chunkserver.NewChunkServer("chunkServer1", masterPort)
	chunkServer1port, err := chunkServer1.Start()
	assert.Nil(t, err)

	chunkServer2 := chunkserver.NewChunkServer("chunkServer2", masterPort)
	chunkServer2port, err := chunkServer2.Start()
	assert.Nil(t, err)

	chunkServer3 := chunkserver.NewChunkServer("chunkServer3", masterPort)
	chunkServer3port, err := chunkServer3.Start()
	assert.Nil(t, err)

	client := client.NewClient(masterPort)
	assert.NotNil(t, client)

	assert.Equal(t, 3, masterServer.ServerList.Len())

	log.Println(chunkServer1port, chunkServer2port, chunkServer3port)

	writeRequests := 2

	var wg sync.WaitGroup
	for i := range writeRequests {
		testFile := "testFile" + string(rune(i-'0'))
		sentData := append(testData, byte(i-'0'))
		for j := range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// client := client.NewClient(masterPort)
				// assert.NotNil(t, client)
				sentData = append(sentData, byte(j-'0'))
				// t.Log("LENGTH OF DATA WRITTEN ",len(sentData))
				err = client.Write(testFile, testData)
				assert.NoError(t, err)
			}()
		}
	}

	wg.Wait()

	// time.Sleep(3*time.Second)

	err = chunkServer1.Shutdown()
	assert.Nil(t, err)

	err = chunkServer2.Shutdown()
	assert.Nil(t, err)

	err = chunkServer3.Shutdown()
	assert.Nil(t, err)

	err = masterServer.Shutdown()
	assert.Nil(t, err)

}
