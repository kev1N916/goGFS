package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net"

	"github.com/involk-secure-1609/goGFS/constants"
)


func (client *Client) readGenericResponse(conn net.Conn) (*constants.UnserializedResponse,error){
	responseTypeBytes := make([]byte, 1)
	bytesReadType, readTypeErr := conn.Read(responseTypeBytes)
	if readTypeErr != nil {
		return nil,fmt.Errorf("failed to read response type: %w", readTypeErr) // Wrap error
	}
	if bytesReadType != 1 {
		return nil,fmt.Errorf("expected to read 1 byte for response type, but read %d bytes", bytesReadType) // More informative error
	}
	log.Printf("Bytes read for response type: %d", bytesReadType)
	responseMessageType:=int(responseTypeBytes[0])
	responseLengthBytes := make([]byte, 2)
	bytesReadLength, readLengthErr := conn.Read(responseLengthBytes)
	if readLengthErr != nil {
		return nil,fmt.Errorf("failed to read response length: %w", readLengthErr) // Wrap error
	}
	if bytesReadLength != 2 {
		return nil,fmt.Errorf("expected to read 2 bytes for response length, but read %d bytes", bytesReadLength) // More informative error
	}

	responseLength := binary.LittleEndian.Uint16(responseLengthBytes)
	responseBodyBytes := make([]byte, responseLength)
	bytesReadBody, readBodyErr := conn.Read(responseBodyBytes)
	if readBodyErr != nil {
		return nil,fmt.Errorf("failed to read response body: %w", readBodyErr) // Wrap error
	}
	if bytesReadBody != int(responseLength) {
		return nil,fmt.Errorf("expected to read %d bytes for response body, but read %d bytes", responseLength, bytesReadBody) // More informative error
	}

	return &constants.UnserializedResponse{MessageType:constants.MessageType(responseMessageType),ResponseBodyBytes: responseBodyBytes},nil
}
func serializeChunkServerReadRequest(chunkServerReadRequest *constants.ClientChunkServerReadRequest) ([]byte,error){
	readRequestInBytes:=make([]byte,0)
	readRequestInBytes=append(readRequestInBytes,byte(constants.ClientChunkServerReadRequestType))
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.LittleEndian, chunkServerReadRequest) // Choose endianness
	if err != nil {
		return nil,err
	}
	requestBytes := buf.Bytes()
	lengthOfRequest:=len(requestBytes)
	binary.LittleEndian.AppendUint16(readRequestInBytes,uint16(lengthOfRequest))
	readRequestInBytes=append(readRequestInBytes,requestBytes...)
	return readRequestInBytes,nil
}


func serializeMasterReadRequest(masterReadRequest *constants.ClientMasterReadRequest) ([]byte,error){
	readRequestInBytes:=make([]byte,0)
	readRequestInBytes=append(readRequestInBytes,byte(constants.ClientMasterReadRequestType))
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.LittleEndian, masterReadRequest) // Choose endianness
	if err != nil {
		return nil,err
	}
	requestBytes := buf.Bytes()
	lengthOfRequest:=len(requestBytes)
	binary.LittleEndian.AppendUint16(readRequestInBytes,uint16(lengthOfRequest))
	readRequestInBytes=append(readRequestInBytes,requestBytes...)
	return readRequestInBytes,nil
}

func (client *Client) readFromMasterServer(readRequest constants.ClientMasterReadRequest) (*constants.ClientMasterReadResponse,error){

	conn, dialErr := net.Dial("tcp", client.masterServer)
	if dialErr != nil {
		return nil,fmt.Errorf("failed to dial master server: %w", dialErr) // Wrap error for more context
	}
	defer conn.Close() // Ensure connection is closed when function exits

	requestBytes, serializeErr := serializeMasterReadRequest(&readRequest)
	if serializeErr != nil {
		return nil,fmt.Errorf("failed to serialize read request: %w", serializeErr) // Wrap error
	}

	bytesWritten, writeErr := conn.Write(requestBytes)
	if writeErr != nil {
		return nil,fmt.Errorf("failed to write read request to connection: %w", writeErr) // Wrap error
	}
	log.Printf("Bytes written for request: %d", bytesWritten) // More descriptive log

	unserializedResponse,err:=client.readGenericResponse(conn)
	if err!=nil{
		return nil,err
	}
	// To reconstruct (deserialize):
	var response constants.ClientMasterReadResponse
	responseReader := bytes.NewReader(unserializedResponse.ResponseBodyBytes)
	deserializeErr := binary.Read(responseReader, binary.LittleEndian, &response)
	if deserializeErr != nil {
		return nil,fmt.Errorf("failed to deserialize read response: %w", deserializeErr) // Wrap error
	}
	fmt.Printf("Reconstructed Response Struct: %+v\n", response) // Renamed variable in Printf

	return &response,nil

}


func (client *Client) cacheChunkServers(fileName string,readResponse *constants.ClientMasterReadResponse){
	client.chunkCache[fileName]=readResponse.ChunkServers
}

func (client *Client) readFromChunkServer(offsetStart int64,offsetEnd int64,readResponse *constants.ClientMasterReadResponse)(error){
	for _,chunkServer:=range readResponse.ChunkServers{
		conn, dialErr := net.Dial("tcp", chunkServer)
		if dialErr != nil {
			log.Printf("failed to dial chunk server: %v", dialErr) // Log error
			continue
		}
		defer conn.Close() // Ensure connection is closed when function exits

		chunkServerReadRequest:=constants.ClientChunkServerReadRequest{
			ChunkHandle: readResponse.ChunkHandle,
			OffsetStart: offsetStart,
			OffsetEnd: offsetEnd,
		}

		requestBytes, serializeErr := serializeChunkServerReadRequest(&chunkServerReadRequest)
		
		if serializeErr != nil {
		return fmt.Errorf("failed to serialize read request: %w", serializeErr) // Wrap error
	}


	bytesWritten, writeErr := conn.Write(requestBytes)
	if writeErr != nil {
		return fmt.Errorf("failed to write read request to connection: %w", writeErr) // Wrap error
	}
	log.Printf("Bytes written for request: %d", bytesWritten) // More descriptive log

	unserializedResponse,err:=client.readGenericResponse(conn)
	if err!=nil{
		return err
	}
	
	var response constants.ClientChunkServerReadResponse
	responseReader := bytes.NewReader(unserializedResponse.ResponseBodyBytes)
	deserializeErr := binary.Read(responseReader, binary.LittleEndian, &response)
	if deserializeErr != nil {
		return fmt.Errorf("failed to deserialize read response: %w", deserializeErr) // Wrap error
	}
	fmt.Printf("Reconstructed Response Struct: %+v\n", response) // Renamed variable in Printf
	return nil


	}

	return fmt.Errorf("failed to read from any chunk server")
}
func (client *Client) read(filename string, offset int) error {

	masterReadRequest := constants.ClientMasterReadRequest{
		Filename: filename,
		Offset:   offset,
	}
	readResponse,err:=client.readFromMasterServer(masterReadRequest)
	if err!=nil{
		return err
	}

	client.cacheChunkServers(filename,readResponse)

	err=client.readFromChunkServer(0,80,readResponse)
	if err!=nil{
		return err
	}
	return nil
}