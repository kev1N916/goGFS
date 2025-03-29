package tests

import (
	"log"
	"net"
	"testing"
	"time"

	chunkserver "github.com/involk-secure-1609/goGFS/chunkServer"
	"github.com/involk-secure-1609/goGFS/master"
	"github.com/stretchr/testify/assert"
)

func TestChunkServerMasterCommunication(t *testing.T){

	t.Run("checking basic chunkServer initialization and master handshake",func (t *testing.T){
		masterPort:="8000"
		master,err:=master.NewMaster(masterPort,true)
		assert.Nil(t,err)
		assert.NotNil(t,master)

		err=master.Start()
		assert.Nil(t,err)

		chunkServer:=chunkserver.NewChunkServer("chunkServer1",masterPort)
		chunkServer.InTestMode=true

		chunkServerPort,err:=chunkServer.Start()
		assert.Nil(t,err)
		log.Println(chunkServerPort)
		assert.Equal(t,1,len(master.ChunkServerConnections))
		host,port,err:=net.SplitHostPort(master.ChunkServerConnections[0].Port)
		assert.Nil(t,err)
		_,chunkServerPortAfterSplit,err:=net.SplitHostPort(chunkServerPort)
		assert.Nil(t,err)
		log.Println(master.ChunkServerConnections[0].Port)
		assert.Nil(t,err)
		t.Log("logging host and port",host,port)
		assert.Equal(t,chunkServerPortAfterSplit,port)
	})

	t.Run("testing if master heartbeat is working properly",func (t *testing.T){
		masterPort:="8000"
		master,err:=master.NewMaster(masterPort,true)
		assert.Nil(t,err)
		assert.NotNil(t,master)

		err=master.Start()
		assert.Nil(t,err)

		chunkServer:=chunkserver.NewChunkServer("chunkServer1",masterPort)
		chunkServer.InTestMode=true

		chunkServerPort,err:=chunkServer.Start()
		assert.Nil(t,err)
		log.Println(chunkServerPort)
		err=master.SendHeartBeatToChunkServer(master.ChunkServerConnections[0])
		assert.Nil(t,err)

		time.Sleep(1*time.Second)

		assert.Equal(t,int32(1),chunkServer.HeartbeatRequestsReceived.Load())
		assert.Equal(t,int32(1),chunkServer.HeartbeatResponsesReceived.Load())

	})

	t.Run("testing if master heartbeat is working properly",func (t *testing.T){
		masterPort:="8000"
		master,err:=master.NewMaster(masterPort,true)
		assert.Nil(t,err)
		assert.NotNil(t,master)

		err=master.Start()
		assert.Nil(t,err)

		chunkServer:=chunkserver.NewChunkServer("chunkServer1",masterPort)
		chunkServer.InTestMode=true

		chunkServerPort,err:=chunkServer.Start()
		assert.Nil(t,err)
		log.Println(chunkServerPort)
		err=master.SendHeartBeatToChunkServer(master.ChunkServerConnections[0])
		assert.Nil(t,err)

		time.Sleep(1*time.Second)

		assert.Equal(t,int32(1),chunkServer.HeartbeatRequestsReceived.Load())
		assert.Equal(t,int32(1),chunkServer.HeartbeatResponsesReceived.Load())

	})



}