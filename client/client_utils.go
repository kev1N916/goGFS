package client

import "github.com/involk-secure-1609/goGFS/common"

// Contains Helper functions for the client struct

// Caches the mapping between the chunkHandle and the chunk servers.
func (client *Client) cacheChunkServers(chunkHandle int64, readResponse *common.ClientMasterReadResponse) {
	client.clientMu.Lock()
	defer client.clientMu.Unlock()
	client.chunkCache[chunkHandle] = readResponse.ChunkServers
}
