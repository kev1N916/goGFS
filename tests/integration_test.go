package tests

import (
	"log"
	"net"
	"os"
	"path/filepath"
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
	masterPort := "8000"
	master, err := master.NewMaster(masterPort, true)
	assert.Nil(t, err)
	assert.NotNil(t, master)

	err = master.Start()
	assert.Nil(t, err)

	chunkServer := chunkserver.NewChunkServer("chunkServer1", masterPort)
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
	masterPort := "8000"
	master, err := master.NewMaster(masterPort, true)
	assert.Nil(t, err)
	assert.NotNil(t, master)

	err = master.Start()
	assert.Nil(t, err)

	chunkServer := chunkserver.NewChunkServer("chunkServer1", masterPort)
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
	TestDirectory := "chunkServer1"
	TestChunkName := "1"
	defer os.RemoveAll(TestDirectory)
	chunkPath := filepath.Join(TestDirectory, TestChunkName)
	log.Println(chunkPath)
	err := os.Mkdir(TestDirectory, 0600)
	assert.Nil(t, err)
	chunkFile, err := os.Create(chunkPath + ".chunk")
	assert.Nil(t, err)
	_, err = chunkFile.Write([]byte{'t', 'e', 's', 't'})
	assert.Nil(t, err)
	chunkFile.Close()
	masterPort := "8000"
	master, err := master.NewMaster(masterPort, true)
	assert.Nil(t, err)
	assert.NotNil(t, master)

	err = master.Start()
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
	TestDirectory := "chunkServer"
	TestChunkName := "1"
	defer os.RemoveAll(TestDirectory)
	chunkPath := filepath.Join(TestDirectory, TestChunkName)
	log.Println(chunkPath)
	err := os.Mkdir(TestDirectory, 0600)
	assert.Nil(t, err)
	chunkFile, err := os.Create(chunkPath + ".chunk")
	assert.Nil(t, err)
	_, err = chunkFile.Write([]byte{'t', 'e', 's', 't'})
	assert.Nil(t, err)
	chunkFile.Close()
	masterPort := "8000"
	master, err := master.NewMaster(masterPort, true)
	assert.Nil(t, err)
	assert.NotNil(t, master)

	err = master.Start()
	assert.Nil(t, err)

	chunkServer := chunkserver.NewChunkServer(TestDirectory, masterPort)
	chunkServer.InTestMode = true

	chunkServerPort, err := chunkServer.Start()
	assert.Nil(t, err)
	log.Println(chunkServerPort)

	client := &client.Client{}
	chunkBytes, err := client.ReadFromChunkServer(1, []string{chunkServerPort})
	assert.Nil(t, err)
	log.Println(chunkBytes)
	assert.Equal(t, 4, len(chunkBytes))
}

// TestClientReadFromChunkServerWithChunkAbsent verifies client properly handles
// the case when the requested chunk is not present on the chunk server
func TestClientReadFromChunkServerWithChunkAbsent(t *testing.T) {
	TestDirectory := "chunkServer1"
	TestChunkName := "1"
	defer os.RemoveAll(TestDirectory)
	chunkPath := filepath.Join(TestDirectory, TestChunkName)
	log.Println(chunkPath)
	err := os.Mkdir(TestDirectory, 0600)
	assert.Nil(t, err)
	chunkFile, err := os.Create(chunkPath + ".chunk")
	assert.Nil(t, err)
	_, err = chunkFile.Write([]byte{'t', 'e', 's', 't'})
	assert.Nil(t, err)
	chunkFile.Close()
	masterPort := "8000"
	master, err := master.NewMaster(masterPort, true)
	assert.Nil(t, err)
	assert.NotNil(t, master)

	err = master.Start()
	assert.Nil(t, err)

	chunkServer := chunkserver.NewChunkServer(TestDirectory, masterPort)
	chunkServer.InTestMode = true

	chunkServerPort, err := chunkServer.Start()
	assert.Nil(t, err)
	log.Println(chunkServerPort)

	client := &client.Client{}
	chunkBytes, err := client.ReadFromChunkServer(2, []string{chunkServerPort})
	assert.NotNil(t, err)
	log.Println(chunkBytes)
	assert.Equal(t, 0, len(chunkBytes))
}

// TestClientReadFromMaster checks if the master returns appropriate metadata
// for read requests from the client
func TestClientReadFromMaster(t *testing.T) {
	masterPort := "8000"
	masterServer, err := master.NewMaster(masterPort, true)
	assert.Nil(t, err)
	assert.NotNil(t, masterServer)

	err = masterServer.Start()
	assert.Nil(t, err)

	testFileMap := map[string][]master.Chunk{
		"test_file_1": {
			{ChunkHandle: 2}, {ChunkHandle: 3}, {ChunkHandle: 0},
		},
		"test_file_2": {
			{ChunkHandle: 2432}, {ChunkHandle: 31321}, {ChunkHandle: 320},
			{ChunkHandle: 311}, {ChunkHandle: 32},
		},
	}
	masterServer.FileMap = testFileMap
	testChunkServerHandler := map[int64][]string{
		3: {":server1", ":server2", ":server3"},
	}
	masterServer.ChunkServerHandler = testChunkServerHandler
	client := client.NewClient(masterPort)
	request1 := common.ClientMasterReadRequest{
		Filename:   "test_file_1",
		ChunkIndex: 2,
	}
	response1, err := client.ReadFromMasterServer(request1)
	assert.Nil(t, err)
	assert.Equal(t, "no chunk servers present for chunk", response1.ErrorMessage)
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
	testData := []byte{'t', 'e', 's', 't'}
	TestDirectory1 := "chunkServer1"
	defer os.RemoveAll(TestDirectory1)
	err := os.Mkdir(TestDirectory1, 0600)
	assert.Nil(t, err)

	TestDirectory2 := "chunkServer2"
	defer os.RemoveAll(TestDirectory2)
	err = os.Mkdir(TestDirectory2, 0600)
	assert.Nil(t, err)

	TestDirectory3 := "chunkServer3"
	defer os.RemoveAll(TestDirectory3)
	err = os.Mkdir(TestDirectory3, 0600)
	assert.Nil(t, err)

	// chunk1->server 2,3
	TestChunkName1 := "1"

	chunkPath := filepath.Join(TestDirectory2, TestChunkName1)
	chunkFile, err := os.Create(chunkPath + ".chunk")
	assert.Nil(t, err)
	_, err = chunkFile.Write(testData)
	assert.Nil(t, err)
	chunkFile.Close()

	chunkPath = filepath.Join(TestDirectory3, TestChunkName1)
	chunkFile, err = os.Create(chunkPath + ".chunk")
	assert.Nil(t, err)
	_, err = chunkFile.Write(testData)
	assert.Nil(t, err)
	chunkFile.Close()

	// chunk3->server 1,2
	TestChunkName3 := "3"

	chunkPath = filepath.Join(TestDirectory1, TestChunkName3)
	chunkFile, err = os.Create(chunkPath + ".chunk")
	assert.Nil(t, err)
	_, err = chunkFile.Write(testData)
	assert.Nil(t, err)
	chunkFile.Close()

	chunkPath = filepath.Join(TestDirectory2, TestChunkName3)
	chunkFile, err = os.Create(chunkPath + ".chunk")
	assert.Nil(t, err)
	_, err = chunkFile.Write(testData)
	assert.Nil(t, err)
	chunkFile.Close()

	masterPort := "8000"
	masterServer, err := master.NewMaster(masterPort, true)
	assert.Nil(t, err)
	assert.NotNil(t, masterServer)

	err = masterServer.Start()
	assert.Nil(t, err)

	testFileMap := map[string][]master.Chunk{
		"test_file_1": {
			{ChunkHandle: 1}, {ChunkHandle: 2}, {ChunkHandle: 0},
		},
		"test_file_2": {
			{ChunkHandle: 3}, {ChunkHandle: 4}, {ChunkHandle: 5},
			{ChunkHandle: 7}, {ChunkHandle: 6},
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

	// testChunkServerHandler:=map[int64][]string{
	//  1:{chunkServer2port,chunkServer3port},
	//  3:{chunkServer1port,chunkServer2port},
	// }
	// masterServer.ChunkServerHandler=testChunkServerHandler
	client := client.NewClient(masterPort)
	dataRead1, err := client.Read("test_file_1", 3)
	assert.NotNil(t, err)
	assert.Equal(t, "invalid chunkOffset", err.Error())
	assert.Equal(t, []byte{}, dataRead1)

	dataRead2, err := client.Read("test_file_1", 1)
	assert.NotNil(t, err)
	assert.Equal(t, "no chunk servers present for chunk", err.Error())
	assert.Equal(t, []byte{}, dataRead2)

	dataRead3, err := client.Read("test_file_1", 0)
	assert.Nil(t, err)
	assert.Equal(t, nil, err)
	t.Log("data received", dataRead3)
	assert.Equal(t, testData, dataRead3)
}

// TestClientWriteToMaster tests the client's ability to write to master
// when the file does not exist on master server and no chunk servers are availible
func TestClientWriteToMaster_1(t *testing.T) {
	testFileName := "testFile"
	masterPort := "8000"
	masterServer, err := master.NewMaster(masterPort, true)
	assert.NotNil(t, masterServer)
	assert.Nil(t, err)
	err = masterServer.Start()
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
	defer os.RemoveAll("chunkServer1")
	defer os.RemoveAll("chunkServer2")
	defer os.RemoveAll("chunkServer3")
	defer os.Remove("OPLOG.opLog")
	testFileName := "testFile"
	masterPort := "8000"
	masterServer, err := master.NewMaster(masterPort, false)
	assert.NotNil(t, masterServer)
	assert.Nil(t, err)

	err = masterServer.Start()
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
	defer os.RemoveAll("chunkServer1")
	defer os.RemoveAll("chunkServer2")
	defer os.RemoveAll("chunkServer3")
	defer os.Remove("OPLOG.opLog")
	testFileName := "testFile"
	masterPort := "8000"
	masterServer, err := master.NewMaster(masterPort, false)
	assert.NotNil(t, masterServer)
	assert.Nil(t, err)

	err = masterServer.Start()
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
	defer os.RemoveAll("chunkServer1")
	defer os.RemoveAll("chunkServer2")
	defer os.RemoveAll("chunkServer3")
	defer os.Remove("OPLOG.opLog")
	testFileName := "testFile"
	masterPort := "8000"
	masterServer, err := master.NewMaster(masterPort, false)
	assert.NotNil(t, masterServer)
	assert.Nil(t, err)

	err = masterServer.Start()
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
func TestReplicateChunkToAllServers_1(t *testing.T) {
	testData := []byte{'t', 'e', 's', 't'}
	defer os.RemoveAll("chunkServer1")
	defer os.RemoveAll("chunkServer2")
	defer os.RemoveAll("chunkServer3")
	defer os.Remove("OPLOG.opLog")
	// testFileName:="testFile"
	masterPort := "8000"
	masterServer, err := master.NewMaster(masterPort, false)
	assert.NotNil(t, masterServer)
	assert.Nil(t, err)

	err = masterServer.Start()
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

	assert.Equal(t, 3, masterServer.ServerList.Len())
	log.Println(chunkServer1port, chunkServer2port, chunkServer3port)
	request := common.ClientMasterWriteResponse{
		MutationId:            243,
		ChunkHandle:           978,
		PrimaryChunkServer:    "csasaaca",
		SecondaryChunkServers: []string{chunkServer1port, chunkServer2port},
		ErrorMessage:          "",
	}

	err = client.ReplicateChunkToAllServers(&request, testData)
	assert.NotNil(t, err)
	assert.Equal(t, "writing to primary chunk server failed", err.Error())

	err = masterServer.Shutdown()
	assert.Nil(t, err)
}

func TestReplicateChunkToAllServers_2(t *testing.T) {
	testData := []byte{'t', 'e', 's', 't'}
	defer os.RemoveAll("chunkServer1")
	defer os.RemoveAll("chunkServer2")
	defer os.RemoveAll("chunkServer3")
	defer os.Remove("OPLOG.opLog")
	// testFileName:="testFile"
	masterPort := "8000"
	masterServer, err := master.NewMaster(masterPort, false)
	assert.NotNil(t, masterServer)
	assert.Nil(t, err)

	err = masterServer.Start()
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

	assert.Equal(t, 3, masterServer.ServerList.Len())
	log.Println(chunkServer1port, chunkServer2port, chunkServer3port)
	request := common.ClientMasterWriteResponse{
		MutationId:            243,
		ChunkHandle:           978,
		PrimaryChunkServer:    chunkServer1port,
		SecondaryChunkServers: []string{"testServer", chunkServer2port},
		ErrorMessage:          "",
	}

	err = client.ReplicateChunkToAllServers(&request, testData)
	assert.NotNil(t, err)
	assert.Equal(t, "writing to secondary chunk server failed", err.Error())

	dataTransferred, presentOnPrimaryServer := chunkServer1.LruCache.Get(request.MutationId)
	assert.Equal(t, true, presentOnPrimaryServer)
	assert.Equal(t, testData, dataTransferred)

	err = masterServer.Shutdown()
	assert.Nil(t, err)
}

func TestReplicateChunkToAllServers_3(t *testing.T) {
	testData := []byte{'t', 'e', 's', 't'}
	defer os.RemoveAll("chunkServer1")
	defer os.RemoveAll("chunkServer2")
	defer os.RemoveAll("chunkServer3")
	defer os.Remove("OPLOG.opLog")
	// testFileName:="testFile"
	masterPort := "8000"
	masterServer, err := master.NewMaster(masterPort, false)
	assert.NotNil(t, masterServer)
	assert.Nil(t, err)

	err = masterServer.Start()
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

	assert.Equal(t, 3, masterServer.ServerList.Len())
	log.Println(chunkServer1port, chunkServer2port, chunkServer3port)
	request := common.ClientMasterWriteResponse{
		MutationId:            243,
		ChunkHandle:           978,
		PrimaryChunkServer:    chunkServer1port,
		SecondaryChunkServers: []string{chunkServer3port, chunkServer2port},
		ErrorMessage:          "",
	}

	err = client.ReplicateChunkToAllServers(&request, testData)
	assert.Nil(t, err)

	for _,server := range []*chunkserver.ChunkServer{chunkServer1, chunkServer2, chunkServer3} {
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
func TestSendWriteRequestToPrimary_1(t *testing.T) {
	testData := []byte{'t', 'e', 's', 't'}
	defer os.RemoveAll("chunkServer1")
	defer os.RemoveAll("chunkServer2")
	defer os.RemoveAll("chunkServer3")
	defer os.Remove("OPLOG.opLog")
	// testFileName:="testFile"
	masterPort := "8000"
	masterServer, err := master.NewMaster(masterPort, false)
	assert.NotNil(t, masterServer)
	assert.Nil(t, err)

	err = masterServer.Start()
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

	assert.Equal(t, 3, masterServer.ServerList.Len())
	log.Println(chunkServer1port, chunkServer2port, chunkServer3port)
	request := common.ClientMasterWriteResponse{
		MutationId:            243,
		ChunkHandle:           978,
		PrimaryChunkServer:    chunkServer1port,
		SecondaryChunkServers: []string{chunkServer3port, chunkServer2port},
		ErrorMessage:          "",
	}

	err = client.ReplicateChunkToAllServers(&request, testData)
	assert.Nil(t, err)

	for _,server := range []*chunkserver.ChunkServer{chunkServer1, chunkServer2, chunkServer3} {
		dataTransferred, presentOnServer := server.LruCache.Get(request.MutationId)
		assert.Equal(t, true, presentOnServer)
		assert.Equal(t, testData, dataTransferred)
	}

	commitRequest:=common.PrimaryChunkCommitRequest{
		ChunkHandle: request.ChunkHandle,
		MutationId: request.MutationId,
		SecondaryServers: request.SecondaryChunkServers,
	}

	err=client.SendWriteRequestToPrimary(chunkServer1port,commitRequest)
	assert.NotNil(t,err)

	err = masterServer.Shutdown()
	assert.Nil(t, err)
}



