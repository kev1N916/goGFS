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

	"github.com/involk-secure-1609/goGFS/common"
)

func AddTimeoutForTheConnection(conn net.Conn, interval time.Duration) error {
	err := conn.SetDeadline(time.Now().Add(interval))
	return err
}

func EncodeMessage(messageType common.MessageType, message interface{}) ([]byte, error) {
    // First encode the message to determine its exact size
    var msgBuf bytes.Buffer
    encoder := gob.NewEncoder(&msgBuf)
    if err := encoder.Encode(message); err != nil {
        return nil, errors.New("encoding failed")
    }
    messageBytes := msgBuf.Bytes()
    messageLength := len(messageBytes)
    
    // Now create the final buffer with exact known size
    // 1 byte for type + 2 bytes for length + message length
    result := make([]byte, 1+2+messageLength)
    
    // Write message type
    result[0] = byte(messageType)
    
    // Write message length
    binary.LittleEndian.PutUint16(result[1:3], uint16(messageLength))
    
    // Copy message bytes
    copy(result[3:], messageBytes)
    
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

func ReadMessage(conn net.Conn) (common.MessageType, []byte, error) {
    // Read message type (1 byte)
    messageType := make([]byte, 1)
    _, err := io.ReadFull(conn, messageType)
    if err != nil {
        if err == io.EOF {
            log.Println("Connection closed by client during message type read")
            return common.MessageType(0), nil, err
        }
        log.Printf("Error reading message type: %v", err)
        return common.MessageType(0), nil, common.ErrReadMessage
    }

    // Read message length (2 bytes)
    messageLength := make([]byte, 2)
    _, err = io.ReadFull(conn, messageLength)
    if err != nil {
        log.Printf("Error reading message length: %v", err)
        return common.MessageType(0), nil, common.ErrReadMessage
    }

    // Get the length as uint16
    length := binary.LittleEndian.Uint16(messageLength)
    
    // Only allocate the exact amount needed for the message body
    messageBody := make([]byte, length)
    _, err = io.ReadFull(conn, messageBody)
    if err != nil {
        log.Printf("Error reading message body (length %d): %v", length, err)
        return common.MessageType(0), nil, common.ErrReadMessage
    }

    return common.MessageType(messageType[0]), messageBody, nil
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

	log.Println("3")
	fp, err := os.OpenFile(path, flags, 0666)
	if err != nil {
		return nil, err
	}
	return fp,err
}

// Using this to solve the error "No connection could be made because the target machine actively refused it."
// The target machine actively refused it occasionally , it is likely because the server has a full 'backlog' .
// Regardless of whether you can increase the server backlog , you do need retry logic in your client code, 
// sometimes it cope with this issue; as even with a long backlog the server
// might be receiving lots of other requests on that port at that time.
func DialWithRetry(address string, maxRetries int) (net.Conn, error) {
	var conn net.Conn
	var err error
	var lastErr error

	for attempt := range maxRetries {
		conn, err = net.Dial("tcp", address)
		if err == nil {
			return conn, nil // Successfully connected
		}

		lastErr = err
		
		log.Printf("Connection attempt %d/%d failed: %v. Retrying...",
			attempt+1, maxRetries, err)

		// Add a small delay before retrying with exponential backoff
		// Start with 200ms, then 400ms, 800ms
		backoffTime := time.Duration(200*(1<<attempt)) * time.Millisecond
		time.Sleep(backoffTime)
	}

	log.Printf(lastErr.Error() + fmt.Sprintf("failed to dial master after %d attempts", maxRetries))
	// All retries failed
	return nil, common.ErrDialServer
}
