package client

import (
	"fmt"
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


// Using this to solve the error "No connection could be made because the target machine actively refused it."
// The target machine actively refused it occasionally , it is likely because the server has a full 'backlog' .
// Regardless of whether you can increase the server backlog , you do need retry logic in your client code, 
// sometimes it cope with this issue; as even with a long backlog the server
// might be receiving lots of other requests on that port at that time.
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
		client.logger.Infof("Connection attempt %d/%d failed: %v. Retrying...",
			attempt+1, maxRetries, err)

		// Add a small delay before retrying with exponential backoff
		// Start with 200ms, then 400ms, 800ms
		backoffTime := time.Duration(200*(1<<attempt)) * time.Millisecond
		time.Sleep(backoffTime)
	}

	client.logger.Warningf(lastErr.Error() + fmt.Sprintf("failed to dial master after %d attempts", maxRetries))
	// All retries failed
	return nil, common.ErrDialServer
}
