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

func TestChunkServerMasterCommunication(t *testing.T) {

	t.Run("checking basic chunkServer initialization and master handshake", func(t *testing.T) {
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
	})

	t.Run("testing if master heartbeat is working properly", func(t *testing.T) {
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

	})

	t.Run("testing if master heartbeat is working properly and client is deleting chunkHandles requested by master during heartbeats", func(t *testing.T) {
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

	})

	t.Run("checking if client read from chunkServers work as intended when chunk is present on chunkServer", func(t *testing.T) {

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
		chunkBytes, err := client.ReadFromChunkServer(1, []string{chunkServerPort})
		assert.Nil(t, err)
		log.Println(chunkBytes)
		assert.Equal(t, 4, len(chunkBytes))

	})

	t.Run("checking if client read from chunkServers work as intended when chunk is absent on chunkServer", func(t *testing.T) {

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
	})


	t.Run("checking if client read from master is returning the appropriate metadata", func(t *testing.T) {

		masterPort := "8000"
		masterServer, err := master.NewMaster(masterPort, true)
		assert.Nil(t, err)
		assert.NotNil(t, masterServer)

		err = masterServer.Start()
		assert.Nil(t, err)

		testFileMap:=map[string][]master.Chunk{
			"test_file_1":{
				{ChunkHandle: 2},{ChunkHandle: 3},{ChunkHandle: 0},
			},
			"test_file_2":{
				{ChunkHandle: 2432},{ChunkHandle: 31321},{ChunkHandle: 320},
				{ChunkHandle: 311},{ChunkHandle: 32},
			},
		}
		masterServer.FileMap=testFileMap
		testChunkServerHandler:=map[int64][]string{
			3:{":server1",":server2",":server3"},
		}
		masterServer.ChunkServerHandler=testChunkServerHandler
		client:=client.NewClient(masterPort)
		request1:=common.ClientMasterReadRequest{
			Filename: "test_file_1",
			ChunkIndex: 2,
		}
		response1,err:=client.ReadFromMasterServer(request1)
		assert.Nil(t,err)
		assert.Equal(t,"no chunk servers present for chunk",response1.ErrorMessage)
		request2:=common.ClientMasterReadRequest{
			Filename: "test_file_1",
			ChunkIndex: 7,
		}
		response2,err:=client.ReadFromMasterServer(request2)
		assert.Nil(t,err)
		assert.Equal(t,"invalid chunkOffset",response2.ErrorMessage)
		request3:=common.ClientMasterReadRequest{
			Filename: "test_file_1",
			ChunkIndex: 1,
		}
		response3,err:=client.ReadFromMasterServer(request3)
		assert.Nil(t,err)
		assert.Equal(t,"",response3.ErrorMessage)
		assert.Equal(t,int64(3),response3.ChunkHandle)
	})


	t.Run("checking if client Read workflow is working", func(t *testing.T) {

		testData:=[]byte{'t', 'e', 's', 't'}
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
		TestChunkName1:="1"

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

		testFileMap:=map[string][]master.Chunk{
			"test_file_1":{
				{ChunkHandle: 1},{ChunkHandle: 2},{ChunkHandle: 0},
			},
			"test_file_2":{
				{ChunkHandle: 3},{ChunkHandle: 4},{ChunkHandle: 5},
				{ChunkHandle: 7},{ChunkHandle: 6},
			},
		}
		masterServer.FileMap=testFileMap
		chunkServer1:=chunkserver.NewChunkServer("chunkServer1",masterPort)
		chunkServer1port,err:=chunkServer1.Start();
		assert.Nil(t,err)
		chunkServer2:=chunkserver.NewChunkServer("chunkServer2",masterPort)
		chunkServer2port,err:=chunkServer2.Start();
		assert.Nil(t,err)
		chunkServer3:=chunkserver.NewChunkServer("chunkServer3",masterPort)
		chunkServer3port,err:=chunkServer3.Start();
		assert.Nil(t,err)

		t.Log(chunkServer1port,chunkServer2port,chunkServer3port)

		// testChunkServerHandler:=map[int64][]string{
		// 	1:{chunkServer2port,chunkServer3port},
		// 	3:{chunkServer1port,chunkServer2port},
		// }
		// masterServer.ChunkServerHandler=testChunkServerHandler
		client:=client.NewClient(masterPort)
		dataRead1,err:=client.Read("test_file_1",3)
		assert.NotNil(t,err)
		assert.Equal(t,"invalid chunkOffset",err.Error())
		assert.Equal(t,[]byte{},dataRead1)

		dataRead2,err:=client.Read("test_file_1",1)
		assert.NotNil(t,err)
		assert.Equal(t,"no chunk servers present for chunk",err.Error())
		assert.Equal(t,[]byte{},dataRead2)
		
		dataRead3,err:=client.Read("test_file_1",0)
		assert.Nil(t,err)
		assert.Equal(t,nil,err)
		t.Log("data received",dataRead3)
		assert.Equal(t,testData,dataRead3)

	})

}
