package helper

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	constants "github.com/involk-secure-1609/goGFS/common"
)

func AddTimeoutForTheConnection(conn net.Conn, interval time.Duration) error {
	err := conn.SetDeadline(time.Now().Add(interval))
	return err
}

// EncodeMessage is a generic function that encodes a struct into a byte slice with message type header
func EncodeMessage[T any](messageType constants.MessageType, message T) ([]byte, error) {
	// Encode the struct using gob
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(message)
	if err != nil {
		return nil, errors.New("encoding failed")
	}

	// Get encoded bytes
	messageBytes := buf.Bytes()
	messageLength := len(messageBytes)

	// Create result byte slice with proper capacity
	result := make([]byte, 0, 1+2+messageLength)

	// Append message type
	result = append(result, byte(messageType))

	// Append message length
	result = binary.LittleEndian.AppendUint16(result, uint16(messageLength))

	// Append encoded message
	result = append(result, messageBytes...)

	return result, nil
}

// DecodeMessage is a generic function that decodes bytes into a specified struct type
func DecodeMessage[T any](requestBodyBytes []byte) (*T, error) {
	var response T
	responseReader := bytes.NewReader(requestBodyBytes)
	decoder := gob.NewDecoder(responseReader)
	err := decoder.Decode(&response)
	if err != nil {
		return nil, fmt.Errorf("decoding failed: %w", err)
	}
	return &response, nil
}

func ReadMessage(conn net.Conn) (constants.MessageType, []byte, error) {
	messageType := make([]byte, 1)
	n, err := conn.Read(messageType)
	if err != nil {
		if err == io.EOF {
			log.Println("Connection closed by client during message type read")
			return constants.MessageType(0), nil, err
		}
		log.Printf("Error reading request type: %v", err)
		return constants.MessageType(0), nil, constants.ErrReadMessageType
	}
	if n == 0 {
		log.Println("No data read for message type, connection might be closing") // Log this case
		return constants.MessageType(0), nil, io.ErrUnexpectedEOF                 // Or return a more appropriate error, maybe retry logic in caller?
	}
	// if ( constants.MessageType(messageType[0])==constants.ClientChunkServerWriteRequestType){
	// 	return constants.MessageType(messageType[0]),nil,nil
	// }
	// Read the request length (2 bytes)
	messageLength := make([]byte, 2)
	_, err = io.ReadFull(conn, messageLength)
	if err != nil {
		log.Printf("Error reading request length: %v", err)
		return constants.MessageType(0), nil, constants.ErrReadMessageLength
	}

	// Get the length as uint16
	length := binary.LittleEndian.Uint16(messageLength)

	// Read the request body
	requestBodyBytes := make([]byte, length)
	_, err = io.ReadFull(conn, requestBodyBytes)
	if err != nil {
		log.Printf("Error reading request body (length %d): %v", length, err)
		return constants.MessageType(0), nil, constants.ErrReadMessageBody
	}

	return constants.MessageType(messageType[0]), requestBodyBytes, nil
}

func OpenExistingFile(path string) (*os.File, error) {
	flags := os.O_RDWR 
	fp, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, err
	}
	return fp,err
}

func OpenTruncFile(path string) (*os.File, error) {
	flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	fp, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, err
	}
	return fp,err
}
