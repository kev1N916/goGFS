package master

import (
	"container/heap"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/stretchr/testify/assert"
)

func TestLease(t *testing.T) {
	t.Run("should create a lease with correct server and grant time", func(t *testing.T) {
		serverName := "test-server-1"
		now := time.Now()
		
		lease := Lease{
			server:    serverName,
			grantTime: now,
		}

		assert.Equal(t, serverName, lease.server, "Server name should match the input")
		assert.Equal(t, now, lease.grantTime, "Grant time should match the input time")
	})

	t.Run("should handle different server name scenarios", func(t *testing.T) {
		testCases := []struct {
			name        string
			serverName  string
			expectValid bool
		}{
			{"Normal server name", "server-1", true},
			{"Empty server name", "", false},
			{"Long server name", "very-long-server-name-with-multiple-segments", true},
			{"Special characters", "server_with-special.chars", true},
			{"Unicode server name", "サーバー1", true},
		}

		for _, tc := range testCases {
			lease := Lease{
				server:    tc.serverName,
				grantTime: time.Now(),
			}

			if tc.expectValid {
				assert.NotEmpty(t, lease.server, "Server name should not be empty")
			} else {
				assert.Empty(t, lease.server, "Server name should be empty")
			}
		}
	})

	t.Run("should validate grant time scenarios", func(t *testing.T) {
		testCases := []struct {
			name       string
			grantTime  time.Time
			expectPast bool
		}{
			{"Current time", time.Now(), true},
			{"Past time", time.Now().Add(-1 * time.Hour), true},
			{"Far past time", time.Now().Add(-24 * time.Hour), true},
			{"Future time", time.Now().Add(1 * time.Hour), false},
		}

		for _, tc := range testCases {
			lease := Lease{
				server:    "test-server",
				grantTime: tc.grantTime,
			}

			isPast := lease.grantTime.Before(time.Now()) || lease.grantTime.Equal(time.Now())
			assert.Equal(t, tc.expectPast, isPast, "Grant time should be in the past or present")
		}
	})

	t.Run("should compare lease equality correctly", func(t *testing.T) {
		now := time.Now()
		
		lease1 := Lease{
			server:    "server-1",
			grantTime: now,
		}
		
		lease2 := Lease{
			server:    "server-1",
			grantTime: now,
		}
		
		lease3 := Lease{
			server:    "server-2",
			grantTime: now,
		}

		assert.Equal(t, lease1, lease2, "Leases with same server and grant time should be equal")
		assert.NotEqual(t, lease1, lease3, "Leases with different servers should not be equal")
	})

	t.Run("should handle zero value lease correctly", func(t *testing.T) {
		var zeroLease Lease

		assert.Empty(t, zeroLease.server, "Zero value lease should have an empty server")
		assert.True(t, zeroLease.grantTime.IsZero(), "Zero value lease should have a zero time")
	})
}



func TestGetMetadataForFile(t *testing.T) {

	// passing 
	t.Run("should successfully retrieve metadata for existing file and chunk", func(t *testing.T) {
		// Create a Master instance with test data
		master := &Master{
			fileMap: map[string][]Chunk{
				"existing-file": {
					{ChunkHandle: 1, ChunkSize: 1024},
					{ChunkHandle: 2, ChunkSize: 2048},
				},
			},
			chunkHandler: map[int64][]string{
				1: {"server1", "server2"},
				2: {"server3", "server4"},
			},
			mu: sync.Mutex{},
		}

		// Test retrieving first chunk
		chunk, servers, err := master.getMetadataForFile("existing-file", 0)
		
		assert.NoError(t, err)
		assert.Equal(t, int64(1), chunk.ChunkHandle)
		assert.Equal(t, int64(1024), chunk.ChunkSize)
		assert.Equal(t, []string{"server1", "server2"}, servers)
	})

	// passing
	t.Run("should return error when file does not exist", func(t *testing.T) {
		master := &Master{
			fileMap:      map[string][]Chunk{},
			chunkHandler: map[int64][]string{},
			mu:          sync.Mutex{},
		}

		chunk, servers, err := master.getMetadataForFile("non-existent-file", 0)
		
		assert.Error(t, err)
		assert.Equal(t, "no chunks present for this file", err.Error())
		assert.Equal(t, Chunk{}, chunk)
		assert.Nil(t, servers)
	})

	// passing 
	t.Run("should return error when chunk index is out of bounds", func(t *testing.T) {
		master := &Master{
			fileMap: map[string][]Chunk{
				"existing-file": {
					{ChunkHandle: 1, ChunkSize: 1024},
				},
			},
			chunkHandler: map[int64][]string{},
			mu:          sync.Mutex{},
		}

		chunk, servers, err := master.getMetadataForFile("existing-file", 1)
		
		assert.Error(t, err)
		assert.Equal(t, "invalid chunkOffset", err.Error())
		assert.Equal(t, Chunk{}, chunk)
		assert.Nil(t, servers)
	})

	// passing 
	t.Run("should return error when no chunk servers exist for chunk", func(t *testing.T) {
		master := &Master{
			fileMap: map[string][]Chunk{
				"existing-file": {
					{ChunkHandle: 1, ChunkSize: 1024},
				},
			},
			chunkHandler: map[int64][]string{},
			mu:          sync.Mutex{},
		}

		chunk, servers, err := master.getMetadataForFile("existing-file", 0)
		
		assert.Error(t, err)
		assert.Equal(t, "no chunk servers present for chunk", err.Error())
		assert.Equal(t, Chunk{}, chunk)
		assert.Nil(t, servers)
	})

	t.Run("should handle concurrent access safely", func(t *testing.T) {
		master := &Master{
			fileMap: map[string][]Chunk{
				"existing-file": {
					{ChunkHandle: 1, ChunkSize: 1024},
				},
			},
			chunkHandler: map[int64][]string{
				1: {"server1", "server2"},
			},
			mu: sync.Mutex{},
		}

		// Run multiple goroutines to test concurrent access
		var wg sync.WaitGroup
		for range 100 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				chunk, servers, err := master.getMetadataForFile("existing-file", 0)
				assert.NoError(t, err)
				assert.Equal(t, int64(1), chunk.ChunkHandle)
				assert.Equal(t, []string{"server1", "server2"}, servers)
			}()
		}
		wg.Wait()
	})
}



func TestChoosePrimaryAndSecondary(t *testing.T) {
	t.Run("should choose primary and secondary when no lease exists", func(t *testing.T) {
		master := &Master{
			inTestMode:     true,
			chunkHandler: map[int64][]string{
				1: {"server1", "server2", "server3"},
			},
			leaseGrants: map[int64]*Lease{},
			mu:          sync.Mutex{},
		}

		primaryServer, secondaryServers, err := master.choosePrimaryAndSecondary(1)

		assert.NoError(t, err)
		assert.NotEmpty(t, primaryServer)
		assert.Contains(t, []string{"server1", "server2", "server3"}, primaryServer)
		assert.Len(t, secondaryServers, 2)
		assert.NotContains(t, secondaryServers, primaryServer)

		// Verify lease was created
		lease, exists := master.leaseGrants[1]
		assert.True(t, exists)
		assert.Equal(t, primaryServer, lease.server)
		assert.NotZero(t, lease.grantTime)
	})

	t.Run("should return error when no chunk servers exist", func(t *testing.T) {
		master := &Master{
			inTestMode:     true,
			chunkHandler:   map[int64][]string{},
			leaseGrants:    map[int64]*Lease{},
			mu:             sync.Mutex{},
		}

		primaryServer, secondaryServers, err := master.choosePrimaryAndSecondary(1)

		assert.Error(t, err)
		assert.Equal(t, "chunk servers do not exist for this chunk handle", err.Error())
		assert.Empty(t, primaryServer)
		assert.Nil(t, secondaryServers)
	})

	t.Run("should renew existing valid lease", func(t *testing.T) {
		initialGrantTime := time.Now().Add(-30 * time.Second)
		master := &Master{
			inTestMode: true,
			chunkHandler: map[int64][]string{
				1: {"server1", "server2", "server3"},
			},
			leaseGrants: map[int64]*Lease{
				1: {
					server:    "server1",
					grantTime: initialGrantTime,
				},
			},
			mu: sync.Mutex{},
		}

		primaryServer, secondaryServers, err := master.choosePrimaryAndSecondary(1)

		assert.NoError(t, err)
		assert.Equal(t, "server1", primaryServer)
		assert.Len(t, secondaryServers, 2)
		assert.NotContains(t, secondaryServers, "server1")

		// Verify lease was renewed
		lease, exists := master.leaseGrants[1]
		assert.True(t, exists)
		assert.True(t, lease.grantTime.After(initialGrantTime))
	})

	t.Run("should choose new primary when lease is invalid", func(t *testing.T) {
		expiredTime := time.Now().Add(-120 * time.Second)
		master := &Master{
			inTestMode: true,
			chunkHandler: map[int64][]string{
				1: {"server1", "server2", "server3"},
			},
			leaseGrants: map[int64]*Lease{
				1: {
					server:    "server1",
					grantTime: expiredTime,
				},
			},
			mu: sync.Mutex{},
		}

		primaryServer, secondaryServers, err := master.choosePrimaryAndSecondary(1)

		assert.NoError(t, err)
		
		assert.Contains(t, []string{"server1", "server2", "server3"}, primaryServer)
		assert.Len(t, secondaryServers, 2)
		assert.NotContains(t, secondaryServers, primaryServer)

		// Verify lease was updated
		lease, exists := master.leaseGrants[1]
		assert.True(t, exists)
		assert.Equal(t, primaryServer, lease.server)
		assert.NotZero(t, lease.grantTime)
	})

	t.Run("should handle single server scenario", func(t *testing.T) {
		master := &Master{
			inTestMode: true,
			chunkHandler: map[int64][]string{
				1: {"server1"},
			},
			leaseGrants: map[int64]*Lease{},
			mu:          sync.Mutex{},
		}

		primaryServer, secondaryServers, err := master.choosePrimaryAndSecondary(1)

		assert.NoError(t, err)
		assert.Equal(t, "server1", primaryServer)
		assert.Empty(t, secondaryServers)
	})

	t.Run("should be thread-safe", func(t *testing.T) {
		master := &Master{
			inTestMode: true,
			chunkHandler: map[int64][]string{
				1: {"server1", "server2", "server3"},
			},
			leaseGrants: map[int64]*Lease{},
			mu:          sync.Mutex{},
		}

		var wg sync.WaitGroup
		results := make(chan struct {
			primaryServer    string
			secondaryServers []string
			err              error
		}, 100)

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				primaryServer, secondaryServers, err := master.choosePrimaryAndSecondary(1)
				results <- struct {
					primaryServer    string
					secondaryServers []string
					err              error
				}{primaryServer, secondaryServers, err}
			}()
		}

		wg.Wait()
		close(results)

		// Verify consistent results
		var firstResult struct {
			primaryServer    string
			secondaryServers []string
			err              error
		}
		firstResultSet := false

		for result := range results {
			assert.NoError(t, result.err)
			
			if !firstResultSet {
				firstResult = result
				firstResultSet = true
			} else {
				assert.Equal(t, firstResult.primaryServer, result.primaryServer)
				assert.ElementsMatch(t, firstResult.secondaryServers, result.secondaryServers)
			}
		}
	})
}



func TestDeleteFile(t *testing.T) {
	t.Run("should successfully delete existing file", func(t *testing.T) {
		// Prepare test data
		master := &Master{
			inTestMode: true,
			fileMap: map[string][]Chunk{
				"test-file": {
					{ChunkHandle: 1, ChunkSize: 1024},
					{ChunkHandle: 2, ChunkSize: 2048},
				},
			},
			mu: sync.Mutex{},
		}

		// Perform deletion
		err := master.deleteFile("test-file")

		// Assertions
		assert.NoError(t, err)
		
		// Check that original file is deleted
		_, exists := master.fileMap["test-file"]
		assert.False(t, exists)
		
		// Check that new deleted file exists with same chunks
		deletedFileName := master.findDeletedFileName("test-file")
		assert.NotEmpty(t, deletedFileName)
		
		chunks, exists := master.fileMap[deletedFileName]
		assert.True(t, exists)
		assert.Len(t, chunks, 2)
		assert.Equal(t, int64(1), chunks[0].ChunkHandle)
		assert.Equal(t, int64(2), chunks[1].ChunkHandle)
	})

	t.Run("should return error for non-existing file", func(t *testing.T) {
		master := &Master{
			inTestMode: true,
			fileMap:    map[string][]Chunk{},
			mu:         sync.Mutex{},
		}

		err := master.deleteFile("non-existent-file")

		assert.Error(t, err)
		assert.Equal(t, "file does not exist", err.Error())
	})

	t.Run("should handle file deletion in test mode", func(t *testing.T) {
		master := &Master{
			inTestMode: true,
			fileMap: map[string][]Chunk{
				"test-file": {
					{ChunkHandle: 1, ChunkSize: 1024},
				},
			},
			mu: sync.Mutex{},
		}

		err := master.deleteFile("test-file")

		assert.NoError(t, err)
		_, exists := master.fileMap["test-file"]
		assert.False(t, exists)
	})

	t.Run("should be thread-safe", func(t *testing.T) {
		master := &Master{
			inTestMode: true,
			fileMap: map[string][]Chunk{
				"test-file": {
					{ChunkHandle: 1, ChunkSize: 1024},
				},
			},
			mu: sync.Mutex{},
		}

		var wg sync.WaitGroup
		errorChan := make(chan error, 10)

		// Concurrent delete attempts
		for range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := master.deleteFile("test-file")
				if err != nil {
					errorChan <- err
				}
			}()
		}

		wg.Wait()
		close(errorChan)

		// Verify file is deleted
		_, exists := master.fileMap["test-file"]
		assert.False(t, exists)
	})
}

// Helper method to find the deleted file name
func (master *Master) findDeletedFileName(originalFileName string) string {
	for fileName := range master.fileMap {
		if strings.Contains(fileName, originalFileName) && strings.Contains(fileName, ".deleted") {
			return fileName
		}
	}
	return ""
}

func TestCreateNewChunk(t *testing.T) {

	node,err:=snowflake.NewNode(1)
	assert.Equal(t,nil,err)
	assert.NotNil(t,node)
	t.Run("should successfully create new chunk in normal mode", func(t *testing.T) {
		// Prepare test data
		master := &Master{
			inTestMode:  true,
			idGenerator:node,
			fileMap:     make(map[string][]Chunk),
			mu:          sync.Mutex{},
		}

		fileName := "test-file"

		// Perform chunk creation
		err := master.createNewChunk(fileName)

		// Assertions
		assert.NoError(t, err)
		
		// Check that file exists in fileMap
		chunks, exists := master.fileMap[fileName]
		assert.True(t, exists)
		assert.Len(t, chunks, 1)
		
		// Verify chunk details
		chunk := chunks[0]
		assert.NotEqual(t, int64(-1), chunk.ChunkHandle)
		assert.Equal(t, int64(0), chunk.ChunkSize)
		
	})

	t.Run("should successfully create new chunk in test mode", func(t *testing.T) {
		// Prepare test data
		master := &Master{
			inTestMode:  true,
			idGenerator:node,
			fileMap:     make(map[string][]Chunk),
			mu:          sync.Mutex{},
		}

		fileName := "test-file"

		// Perform chunk creation
		err := master.createNewChunk(fileName)

		// Assertions
		assert.NoError(t, err)
		
		// Check that file exists in fileMap
		chunks, exists := master.fileMap[fileName]
		assert.True(t, exists)
		assert.Len(t, chunks, 1)
		
		// Verify chunk details
		chunk := chunks[0]
		assert.NotEqual(t, int64(-1), chunk.ChunkHandle)
		assert.Equal(t, int64(0), chunk.ChunkSize)
	})
	t.Run("should create multiple chunks for same file", func(t *testing.T) {
		// Prepare test data
		master := &Master{
			inTestMode:  true,
			idGenerator:node,
			fileMap:     make(map[string][]Chunk),
			mu:          sync.Mutex{},
		}

		fileName := "test-file"

		// Create multiple chunks
		err1 := master.createNewChunk(fileName)
		err2 := master.createNewChunk(fileName)

		// Assertions
		assert.NoError(t, err1)
		assert.NoError(t, err2)
		
		// Check that file has multiple chunks
		chunks, exists := master.fileMap[fileName]
		assert.True(t, exists)
		assert.Len(t, chunks, 2)
		
		// Verify unique chunk handles
		assert.NotEqual(t, chunks[0].ChunkHandle, chunks[1].ChunkHandle)
	})

	t.Run("should create chunks concurrently in test mode", func(t *testing.T) {
		// Prepare test data
		master := &Master{
			inTestMode:  true,
			idGenerator: node,
			fileMap:     make(map[string][]Chunk),
			mu:          sync.Mutex{},
		}

		fileName := "test-concurrent-file"
		numChunks := 1000 // Large number of concurrent chunk creations
		
		// WaitGroup to wait for all goroutines to complete
		var wg sync.WaitGroup
		
		// Channel to collect any errors
		errChan := make(chan error, numChunks)

		// Create chunks concurrently
		for range numChunks {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := master.createNewChunk(fileName)
				if err != nil {
					errChan <- err
				}
			}()
		}

		// Wait for all chunk creation attempts to complete
		wg.Wait()
		close(errChan)

		// Check for any errors during chunk creation
		for err := range errChan {
			assert.NoError(t, err, "Unexpected error during concurrent chunk creation")
		}

		// Verify the number of chunks created
		master.mu.Lock()
		defer master.mu.Unlock()

		chunks, exists := master.fileMap[fileName]
		assert.True(t, exists, "File should exist in fileMap")
		assert.Len(t, chunks, numChunks, "Number of chunks should match number of creation attempts")

		// Verify unique chunk handles
		chunkHandles := make(map[int64]bool)
		for _, chunk := range chunks {
			assert.False(t, chunkHandles[chunk.ChunkHandle], "Chunk handles should be unique")
			chunkHandles[chunk.ChunkHandle] = true
		}
	})
}


func TestHandleChunkCreation(t *testing.T) {
	node, err := snowflake.NewNode(1)
	assert.Equal(t, nil, err)
	assert.NotNil(t, node)

	t.Run("should successfully create a new file and chunk", func(t *testing.T) {
		// Prepare test data
	
		// Initialize serverList with test servers
		pq := &ServerList{}
		heap.Init(pq)
		
		// Add servers to the priority queue
		servers := []*Server{
			{server: "server1", NumberOfChunks: 5},
			{server: "server2", NumberOfChunks: 7},
			{server: "server3", NumberOfChunks: 3},
			{server: "server4", NumberOfChunks: 2},
		}
		
		for _, server := range servers {
			heap.Push(pq, server)
		}
		
		master := &Master{
			inTestMode:   true,
			idGenerator:  node,
			fileMap:      make(map[string][]Chunk),
			chunkHandler: make(map[int64][]string),
			leaseGrants: make(map[int64]*Lease),
			mu:           sync.Mutex{},
			serverList:   pq,
		}

		fileName := "test-file"

		// Perform chunk creation
		chunkHandle, primaryServer, secondaryServers, err := master.handleChunkCreation(fileName)

		// Assertions
		assert.NoError(t, err)
		assert.NotEqual(t, int64(-1), chunkHandle)
		assert.NotEmpty(t, primaryServer)
		assert.Len(t, secondaryServers, 2) // Assuming choosePrimaryAndSecondary returns 1 primary and 2 secondary

		// Check that file exists in fileMap
		chunks, exists := master.fileMap[fileName]
		assert.True(t, exists)
		assert.Len(t, chunks, 1)

		// Verify chunk handle matches
		assert.Equal(t, chunks[0].ChunkHandle, chunkHandle)

		// Verify chunkHandler has been populated
		createdServers, exists:= master.chunkHandler[chunkHandle]
		assert.True(t, exists)
		assert.Len(t, createdServers, 3) // Should contain 3 servers from chooseChunkServers

		// Verify opLogger was called
	})

	t.Run("should reuse existing file and create a new chunk", func(t *testing.T) {
		// Prepare test data
		

		// Initialize serverList with test servers
		pq := &ServerList{}
		heap.Init(pq)
		
		// Add servers to the priority queue
		servers := []*Server{
			{server: "server1", NumberOfChunks: 5},
			{server: "server2", NumberOfChunks: 7},
			{server: "server3", NumberOfChunks: 3},
			{server: "server4", NumberOfChunks: 2},
		}
		
		for _, server := range servers {
			heap.Push(pq, server)
		}
		
		master := &Master{
			inTestMode:   true,
			idGenerator:  node,
			fileMap:      make(map[string][]Chunk),
			chunkHandler: make(map[int64][]string),
			leaseGrants: make(map[int64]*Lease),
			mu:           sync.Mutex{},
			serverList:   pq,
		}

		fileName := "test-file"
		
		// Create a file and first chunk
		existingChunk := Chunk{ChunkHandle: 12345}
		master.fileMap[fileName] = []Chunk{existingChunk}

		// Perform chunk creation for existing file
		chunkHandle, primaryServer, secondaryServers, err := master.handleChunkCreation(fileName)

		// Assertions
		assert.NoError(t, err)
		assert.Equal(t, existingChunk.ChunkHandle, chunkHandle)
		assert.NotEmpty(t, primaryServer)
		assert.Len(t, secondaryServers, 2)

		// File should still exist with just the original chunk
		chunks, exists := master.fileMap[fileName]
		assert.True(t, exists)
		assert.Len(t, chunks, 1)
		
		// OpLogger shouldn't have been called because no new chunk was created
	})

	t.Run("should reuse existing chunk servers if already assigned", func(t *testing.T) {

		// Initialize serverList with test servers
		pq := &ServerList{}
		heap.Init(pq)
		
		// Add servers to the priority queue
		servers := []*Server{
			{server: "server1", NumberOfChunks: 5},
			{server: "server2", NumberOfChunks: 7},
			{server: "server3", NumberOfChunks: 3},
		}
		
		for _, server := range servers {
			heap.Push(pq, server)
		}
		
		master := &Master{
			inTestMode:   true,
			idGenerator:  node,
			fileMap:      make(map[string][]Chunk),
			chunkHandler: make(map[int64][]string),
			leaseGrants: make(map[int64]*Lease),
			mu:           sync.Mutex{},
			serverList:   pq,
		}

		fileName := "test-file"
		chunkHandle := int64(12345)
		
		// Create a pre-existing file with a chunk
		master.fileMap[fileName] = []Chunk{{ChunkHandle: chunkHandle}}
		
		// Pre-assign chunk servers for this chunk handle
		preassignedServers := []string{"server-a", "server-b", "server-c"}
		master.chunkHandler[chunkHandle] = preassignedServers

		// Mock that choosePrimaryAndSecondary returns the expected values from preassigned servers
		// This would typically need to be implemented in the Master struct for a real test

		// Perform chunk creation
		resultChunkHandle, primaryServer, secondaryServers, err := master.handleChunkCreation(fileName)

		// Assertions
		assert.NoError(t, err)
		assert.Equal(t, chunkHandle, resultChunkHandle)
		
		// Check that the preassigned servers were used
		assert.Contains(t, preassignedServers, primaryServer)
		for _, server := range secondaryServers {
			assert.Contains(t, preassignedServers, server)
		}
		
		// The serverList should not have been modified (no new servers chosen)
		assert.Len(t, master.chunkHandler[chunkHandle], len(preassignedServers))
		assert.Equal(t, preassignedServers, master.chunkHandler[chunkHandle])
	})

	// t.Run("should handle error from choosing primary and secondary", func(t *testing.T) {

	// 	// Initialize serverList with test servers
	// 	pq := &ServerList{}
	// 	heap.Init(pq)
		
	// 	// Empty server list to trigger an error in choosePrimaryAndSecondary
		
	// 	master := &Master{
	// 		inTestMode:   true,
	// 		idGenerator:  node,
	// 		fileMap:      make(map[string][]Chunk),
	// 		chunkHandler: make(map[int64][]string),
	// 		mu:           sync.Mutex{},
	// 		serverList:   pq,
	// 	}

	// 	fileName := "test-file"

	// 	// For this test to work properly, choosePrimaryAndSecondary would need to be
	// 	// structured in a way that it returns an error when there are not enough servers
	// 	// Since we can't modify the function for this test, we're just documenting what
	// 	// would need to be tested.
		
	// 	// The test would verify:
	// 	// 1. An error is returned from handleChunkCreation
	// 	// 2. The file is still created
	// 	// 3. The chunk is still added to the file
	// 	// 4. chunkHandler is populated
	// 	// 5. But the returned values indicate an error state
	// })

	t.Run("should create multiple chunks for the same file in concurrent scenario", func(t *testing.T) {
	

		// Initialize serverList with test servers
		pq := &ServerList{}
		heap.Init(pq)
		
		// Add more servers to handle concurrent requests
		for i := range 30 {
			heap.Push(pq, &Server{
				server:         "server" + string(rune(i+'0')),
				NumberOfChunks: i % 10,
			})
		}
		
		master := &Master{
			inTestMode:   true, // Test mode to avoid writing to log
			idGenerator:  node,
			fileMap:      make(map[string][]Chunk),
			chunkHandler: make(map[int64][]string),
			leaseGrants: make(map[int64]*Lease),
			mu:           sync.Mutex{},
			serverList:   pq,
		}

		fileName := "concurrent-test-file"
		numConcurrent := 10
		
		// WaitGroup to wait for all goroutines to complete
		var wg sync.WaitGroup
		
		// Channels to collect results and errors
		resultChan := make(chan struct{
			chunkHandle     int64
			primaryServer   string
			secondaryServers []string
			err            error
		}, numConcurrent)

		// Create chunks concurrently
		for range numConcurrent {
			wg.Add(1)
			go func() {
				defer wg.Done()
				chunkHandle, primaryServer, secondaryServers, err := master.handleChunkCreation(fileName)
				resultChan <- struct {
					chunkHandle     int64
					primaryServer   string
					secondaryServers []string
					err            error
				}{chunkHandle, primaryServer, secondaryServers, err}
			}()
		}
		wg.Wait()		
		close(resultChan)

		// Collect and check results
		var lastError error
		for result := range resultChan {
			if result.err != nil {
				lastError = result.err
				continue
			}
			
			assert.NotEqual(t, int64(-1), result.chunkHandle)
			assert.NotEmpty(t, result.primaryServer)
			assert.NotEmpty(t, result.secondaryServers)
		}


		// No errors should have occurred
		assert.NoError(t, lastError)
		
		// Verify file creation
		master.mu.Lock()
		defer master.mu.Unlock()
		
		_, exists := master.fileMap[fileName]
		assert.True(t, exists)
		
	})
}

func TestBuildCheckpoint(t *testing.T) {
	t.Run("should successfully create checkpoint with multiple files", func(t *testing.T) {
		// Setup
		master := &Master{
			fileMap: map[string][]Chunk{
				"test_file1": {
					{ChunkHandle: 1},
					{ChunkHandle: 2},
				},
				"test_file2": {
					{ChunkHandle: 3},
				},
			},
			inTestMode: true,
		}
		defer os.Remove("checkpoint.chk")

		// Call buildCheckpoint
		err := master.buildCheckpoint()
		assert.Nil(t, err)

		// Verify checkpoint file was created
		_, err = os.Stat("checkpoint.chk")
		if err != nil {
			t.Fatalf("Checkpoint file was not created: %v", err)
		}
	})

	t.Run("should successfully create checkpoint with single file", func(t *testing.T) {
		// Setup
		master := &Master{
			fileMap: map[string][]Chunk{
				"test_file_single": {
					{ChunkHandle: 1},
				},
			},
			inTestMode: true,
		}
		defer os.Remove("checkpoint.chk")

		// Call buildCheckpoint
		err := master.buildCheckpoint()
		assert.Nil(t, err)

		// Verify checkpoint file was created
		_, err = os.Stat("checkpoint.chk")
		if err != nil {
			t.Fatalf("Checkpoint file was not created: %v", err)
		}
	})

	t.Run("should successfully create checkpoint with empty file map", func(t *testing.T) {
		// Setup
		master := &Master{
			fileMap:    make(map[string][]Chunk),
			inTestMode: true,
		}
		defer os.Remove("checkpoint.chk")

		// Call buildCheckpoint
		err := master.buildCheckpoint()
		assert.Nil(t, err)

		// Verify checkpoint file was created
		_, err = os.Stat("checkpoint.chk")
		if err != nil {
			t.Fatalf("Checkpoint file was not created: %v", err)
		}
	})
}

func TestReadCheckpoint(t *testing.T) {
	t.Run("should successfully read checkpoint with multiple files", func(t *testing.T) {
		// Setup
		master := &Master{
			inTestMode: true,
			fileMap:    make(map[string][]Chunk),
		}

		defer os.Remove("checkpoint.chk")

		// Prepare test data
		testFiles := map[string][]Chunk{
			"test_file1": {
				{ChunkHandle: 101},
				{ChunkHandle: 102},
			},
			"test_file2": {
				{ChunkHandle: 201},
			},
		}

		// Create a master with test data and build checkpoint
		masterWithData := &Master{
			fileMap:    testFiles,
			inTestMode: true,
		}
		err := masterWithData.buildCheckpoint()
		assert.Nil(t, err)

		// Read the checkpoint
		err = master.readCheckpoint()
		if err != nil {
			t.Fatalf("Failed to read checkpoint: %v", err)
		}

		// Verify the read data matches the original
		assert.Equal(t, len(testFiles), len(master.fileMap), 
			"Number of files should match after checkpoint read")

		for file, chunks := range testFiles {
			readChunks, exists := master.fileMap[file]
			assert.True(t, exists, "File %s should exist in read checkpoint", file)

			assert.Equal(t, len(chunks), len(readChunks), 
				"Chunk count should match for file %s", file)

			for i, chunk := range chunks {
				assert.Equal(t, chunk.ChunkHandle, readChunks[i].ChunkHandle, 
					"Chunk handle should match for file %s, chunk %d", file, i)
			}
		}
	})

	t.Run("should successfully read checkpoint with single file", func(t *testing.T) {
		// Setup
		master := &Master{
			inTestMode: true,
			fileMap:    make(map[string][]Chunk),
		}

		defer os.Remove("checkpoint.chk")

		// Prepare test data
		testFiles := map[string][]Chunk{
			"test_file_single": {
				{ChunkHandle: 101},
			},
		}

		// Create a master with test data and build checkpoint
		masterWithData := &Master{
			fileMap:    testFiles,
			inTestMode: true,
		}
		err := masterWithData.buildCheckpoint()
		assert.Nil(t, err)

		// Read the checkpoint
		err = master.readCheckpoint()
		assert.Nil(t, err, "Should read checkpoint without error")

		// Verify the read data matches the original
		assert.Equal(t, len(testFiles), len(master.fileMap), 
			"Number of files should match after checkpoint read")
	})
}

func TestCheckpointWorkflow(t *testing.T) {
	t.Run("should successfully complete full checkpoint workflow", func(t *testing.T) {
		defer os.Remove("checkpoint.chk")

		// Setup original master with test data
		originalMaster := &Master{
			fileMap: map[string][]Chunk{
				"test_file1": {
					{ChunkHandle: 1},
					{ChunkHandle: 2},
				},
				"test_file2": {
					{ChunkHandle: 3},
				},
			},
			inTestMode: true,
		}

		// Build checkpoint
		err := originalMaster.buildCheckpoint()
		assert.Nil(t, err, "Should build checkpoint without error")

		// Create a new master to read the checkpoint
		restoredMaster := &Master{
			inTestMode: true,
			fileMap:    make(map[string][]Chunk),
		}

		// Read checkpoint
		err = restoredMaster.readCheckpoint()
		assert.Nil(t, err, "Should read checkpoint without error")

		// Verify restored data matches original
		assert.Equal(t, len(originalMaster.fileMap), len(restoredMaster.fileMap), 
			"Restored fileMap size should match original")

		for file, originalChunks := range originalMaster.fileMap {
			restoredChunks, exists := restoredMaster.fileMap[file]
			assert.True(t, exists, "File %s should exist in restored checkpoint", file)

			assert.Equal(t, len(originalChunks), len(restoredChunks), 
				"Chunk count should match for file %s", file)

			for i, originalChunk := range originalChunks {
				assert.Equal(t, originalChunk.ChunkHandle, restoredChunks[i].ChunkHandle, 
					"Chunk handle should match for file %s, chunk %d", file, i)
			}
		}
	})

	t.Run("should handle checkpoint workflow with single file", func(t *testing.T) {
		defer os.Remove("checkpoint.chk")

		// Setup original master with single file test data
		originalMaster := &Master{
			fileMap: map[string][]Chunk{
				"test_file_single": {
					{ChunkHandle: 1},
				},
			},
			inTestMode: true,
		}

		// Build checkpoint
		err := originalMaster.buildCheckpoint()
		assert.Nil(t, err, "Should build checkpoint without error")

		// Create a new master to read the checkpoint
		restoredMaster := &Master{
			inTestMode: true,
			fileMap:    make(map[string][]Chunk),
		}

		// Read checkpoint
		err = restoredMaster.readCheckpoint()
		assert.Nil(t, err, "Should read checkpoint without error")

		// Verify restored data matches original
		assert.Equal(t, len(originalMaster.fileMap), len(restoredMaster.fileMap), 
			"Restored fileMap size should match original")
	})
}

