package main

import (
	"bytes"
	"encoding/binary"
	"net"

	"github.com/involk-secure-1609/goGFS/constants"
)


func (master *Master) handleMasterReadRequest(conn net.Conn,requestBodyBytes []byte){

}

func (master *Master) handleMasterWriteRequest(conn net.Conn,requestBodyBytes []byte){

}

func (master *Master) handleMasterHeartbeat(conn net.Conn,requestBodyBytes []byte){


}

func (master *Master) handleMasterHandshake(conn net.Conn,requestBodyBytes []byte){
	var response constants.MasterChunkServerHandshake
	responseReader := bytes.NewReader(requestBodyBytes)
	deserializeErr := binary.Read(responseReader, binary.LittleEndian, &response)
	if deserializeErr != nil {
		return
	}

	for _,chunkId :=range(response.ChunkIds){
		master.chunkHandler[chunkId]=append(master.chunkHandler[chunkId],response.Port)
	}

	handshakeResponse:=constants.MasterChunkServerHandshakeResponse{
		Message: "Handshake successful",
	}
	handshakeResponseInBytes:=make([]byte,0)
	handshakeResponseInBytes=append(handshakeResponseInBytes,byte(constants.MasterChunkServerHandshakeResponseType))
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.LittleEndian, handshakeResponse) // Choose endianness
	if err != nil {
		return 
	}
	responseBytes := buf.Bytes()
	lengthOfRequest:=len(responseBytes)
	binary.LittleEndian.AppendUint16(handshakeResponseInBytes,uint16(lengthOfRequest))
	handshakeResponseInBytes=append(handshakeResponseInBytes,responseBytes...)
	
	conn.Write(handshakeResponseInBytes)

}