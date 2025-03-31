package client

import (
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/involk-secure-1609/goGFS/common"
)

// Contains Helper functions for the client struct

// Caches the mapping between the chunkHandle and the chunk servers.
func (client *Client) cacheChunkServers(chunkHandle int64, readResponse *common.ClientMasterReadResponse) {
	client.clientMu.Lock()
	defer client.clientMu.Unlock()
	client.chunkCache[chunkHandle] = readResponse.ChunkServers
}


func (client *Client) dialWithRetry(address string, maxRetries int) (net.Conn, error) {
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
    
    // All retries failed
    return nil, errors.Join(lastErr, 
        fmt.Errorf("failed to dial master after %d attempts", maxRetries))
}
