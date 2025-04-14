package master

import (
	"container/heap"
	"os"
	"slices"
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
			Server:    serverName,
			GrantTime: now,
		}

		assert.Equal(t, serverName, lease.Server, "Server name should match the input")
		assert.Equal(t, now, lease.GrantTime, "Grant time should match the input time")
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
				Server:    tc.serverName,
				GrantTime: time.Now(),
			}

			if tc.expectValid {
				assert.NotEmpty(t, lease.Server, "Server name should not be empty")
			} else {
				assert.Empty(t, lease.Server, "Server name should be empty")
			}
		}
	})

	t.Run("should validate grant time scenarios", func(t *testing.T) {
		testCases := []struct {
			name       string
			GrantTime  time.Time
			expectPast bool
		}{
			{"Current time", time.Now(), true},
			{"Past time", time.Now().Add(-1 * time.Hour), true},
			{"Far past time", time.Now().Add(-24 * time.Hour), true},
			{"Future time", time.Now().Add(1 * time.Hour), false},
		}

		for _, tc := range testCases {
			lease := Lease{
				Server:    "test-server",
				GrantTime: tc.GrantTime,
			}

			isPast := lease.GrantTime.Before(time.Now()) || lease.GrantTime.Equal(time.Now())
			assert.Equal(t, tc.expectPast, isPast, "Grant time should be in the past or present")
		}
	})

	t.Run("should compare lease equality correctly", func(t *testing.T) {
		now := time.Now()

		lease1 := Lease{
			Server:    "server-1",
			GrantTime: now,
		}

		lease2 := Lease{
			Server:    "server-1",
			GrantTime: now,
		}

		lease3 := Lease{
			Server:    "server-2",
			GrantTime: now,
		}

		assert.Equal(t, lease1, lease2, "Leases with same server and grant time should be equal")
		assert.NotEqual(t, lease1, lease3, "Leases with different servers should not be equal")
	})

	t.Run("should handle zero value lease correctly", func(t *testing.T) {
		var zeroLease Lease

		assert.Empty(t, zeroLease.Server, "Zero value lease should have an empty server")
		assert.True(t, zeroLease.GrantTime.IsZero(), "Zero value lease should have a zero time")
	})
}

func TestGetMetadataForFile(t *testing.T) {

	// passing
	t.Run("should successfully retrieve metadata for existing file and chunk", func(t *testing.T) {
		// Create a Master instance with test data
		master := &Master{
			FileMap: map[string][]int64{
				"existing-file": {
					1, 2,
				},
			},
			ChunkServerHandler: map[int64]map[string]bool{
				1: {
					"server1": true,
					"server2": true,
				},
				2: {
					"server3": true,
					"server4": true,
				},
			},
			ChunkHandles: map[int64]*Chunk{
				1: {
					ChunkHandle:  1,
					ChunkVersion: 1,
				},
			},
			mu:         sync.RWMutex{},
			inTestMode: true,
		}

		// Test retrieving first chunk
		chunk, servers, err := master.getMetadataForFile("existing-file", 0)

		assert.NoError(t, err)
		assert.Equal(t, int64(1), chunk.ChunkHandle)
		assert.Equal(t, []string{"server1", "server2"}, servers)
	})

	// passing
	t.Run("should return error when file does not exist", func(t *testing.T) {
		master := &Master{
			FileMap:            make(map[string][]int64),
			ChunkServerHandler: make(map[int64]map[string]bool),
			mu:                 sync.RWMutex{},
		}

		chunk, servers, err := master.getMetadataForFile("non-existent-file", 0)

		assert.Error(t, err)
		assert.Equal(t, "no chunk handles present for this file", err.Error())
		assert.Equal(t, Chunk{}, chunk)
		assert.Nil(t, servers)
	})

	// passing
	t.Run("should return error when chunk index is out of bounds", func(t *testing.T) {
		master := &Master{
			FileMap: map[string][]int64{
				"existing-file": {
					1, 2,
				},
			},
			ChunkServerHandler: make(map[int64]map[string]bool),
			mu:                 sync.RWMutex{},
		}

		chunk, servers, err := master.getMetadataForFile("existing-file", 3)

		assert.Error(t, err)
		assert.Equal(t, "invalid chunkOffset", err.Error())
		assert.Equal(t, Chunk{}, chunk)
		assert.Nil(t, servers)
	})

	// passing
	t.Run("should return error when no chunk servers exist for chunk", func(t *testing.T) {
		master := &Master{
			FileMap: map[string][]int64{
				"existing-file": {
					1, 2,
				},
			},
			ChunkServerHandler: make(map[int64]map[string]bool),
			ChunkHandles: map[int64]*Chunk{
				1: {
					ChunkHandle:  1,
					ChunkVersion: 1,
				},
			}, mu: sync.RWMutex{},
		}

		chunk, servers, err := master.getMetadataForFile("existing-file", 0)

		assert.Error(t, err)
		assert.Equal(t, "no chunk servers present for chunk handle ", err.Error())
		assert.Equal(t, Chunk{}, chunk)
		assert.Nil(t, servers)
	})

	t.Run("should handle concurrent access safely", func(t *testing.T) {
		master := &Master{
			FileMap: map[string][]int64{
				"existing-file": {
					1, 2,
				},
			},
			ChunkServerHandler: map[int64]map[string]bool{
				1: {
					"server1": true,
					"server2": true,
				},
			},
			ChunkHandles: map[int64]*Chunk{
				1: {
					ChunkHandle:  1,
					ChunkVersion: 1,
				},
				2:{
					ChunkHandle: 2,
					ChunkVersion: 3,
				},
			},
			mu: sync.RWMutex{},
			inTestMode: true,
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
				assert.True(t, slices.Contains(servers,"server1"))
				assert.True(t, slices.Contains(servers,"server2"))

			}()
		}
		wg.Wait()
	})
}

func TestChoosePrimaryAndSecondary(t *testing.T) {
	t.Run("should choose primary and secondary when no lease exists", func(t *testing.T) {
		master := &Master{
			inTestMode: true,
			ChunkServerHandler: map[int64]map[string]bool{
				1: {
					"server1": true,
					"server2": true,
					"server3": true,
				},
			},
			LeaseGrants: map[int64]*Lease{},
			mu:          sync.RWMutex{},
		}

		primaryServer, secondaryServers, _, err := master.choosePrimaryAndSecondary(1)

		assert.NoError(t, err)
		assert.NotEmpty(t, primaryServer)
		assert.Contains(t, []string{"server1", "server2", "server3"}, primaryServer)
		assert.Len(t, secondaryServers, 2)
		assert.NotContains(t, secondaryServers, primaryServer)

	})

	t.Run("should return error when no chunk servers exist", func(t *testing.T) {
		master := &Master{
			inTestMode:         true,
			ChunkServerHandler: map[int64]map[string]bool{},
			LeaseGrants:        map[int64]*Lease{},
			mu:                 sync.RWMutex{},
		}

		primaryServer, secondaryServers, _, err := master.choosePrimaryAndSecondary(1)

		assert.Error(t, err)
		assert.Equal(t, "chunk servers do not exist for this chunk handle", err.Error())
		assert.Empty(t, primaryServer)
		assert.Nil(t, secondaryServers)
	})

	t.Run("should renew existing valid lease", func(t *testing.T) {
		initialGrantTime := time.Now().Add(-30 * time.Second)
		master := &Master{
			inTestMode: true,
			ChunkServerHandler: map[int64]map[string]bool{
				1: {
					"server1": true,
					"server2": true,
					"server3": true,
				},
			},
			LeaseGrants: map[int64]*Lease{
				1: {
					Server:    "server1",
					GrantTime: initialGrantTime,
				},
			},
			mu: sync.RWMutex{},
		}

		primaryServer, secondaryServers, isLeaseValid, err := master.choosePrimaryAndSecondary(1)

		assert.NoError(t, err)
		assert.Equal(t, "server1", primaryServer)
		assert.Len(t, secondaryServers, 2)
		assert.NotContains(t, secondaryServers, "server1")
		assert.True(t, isLeaseValid)

		// Verify lease was renewed
		lease, exists := master.LeaseGrants[1]
		assert.True(t, exists)
		assert.True(t, lease.GrantTime.After(initialGrantTime))
	})

	t.Run("should choose new primary when lease is invalid", func(t *testing.T) {
		expiredTime := time.Now().Add(-120 * time.Second)
		master := &Master{
			inTestMode: true,
			ChunkServerHandler: map[int64]map[string]bool{
				1: {
					"server1": true,
					"server2": true,
					"server3": true,
				},
			},
			LeaseGrants: map[int64]*Lease{
				1: {
					Server:    "server1",
					GrantTime: expiredTime,
				},
			},
			mu: sync.RWMutex{},
		}

		primaryServer, secondaryServers, _, err := master.choosePrimaryAndSecondary(1)

		assert.NoError(t, err)

		assert.Contains(t, []string{"server1", "server2", "server3"}, primaryServer)
		assert.Len(t, secondaryServers, 2)
		assert.NotContains(t, secondaryServers, primaryServer)

		// Verify lease was updated
		lease, exists := master.LeaseGrants[1]
		assert.True(t, exists)
		assert.Equal(t, primaryServer, lease.Server)
		assert.NotZero(t, lease.GrantTime)
	})

	t.Run("should handle single server scenario", func(t *testing.T) {
		master := &Master{
			inTestMode: true,
			ChunkServerHandler: map[int64]map[string]bool{
				1: {
					"server1": true,
				},
			},
			LeaseGrants: map[int64]*Lease{},
			mu:          sync.RWMutex{},
		}

		primaryServer, secondaryServers, _, err := master.choosePrimaryAndSecondary(1)

		assert.NoError(t, err)
		assert.Equal(t, "server1", primaryServer)
		assert.Empty(t, secondaryServers)
	})

	t.Run("should be thread-safe", func(t *testing.T) {
		master := &Master{
			inTestMode: true,
			ChunkServerHandler: map[int64]map[string]bool{
				1: {
					"server1": true,
					"server2": true,
					"server3": true,
				},
			},
			LeaseGrants: map[int64]*Lease{},
			mu:          sync.RWMutex{},
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
				primaryServer, secondaryServers, _, err := master.choosePrimaryAndSecondary(1)
				assert.Nil(t,err)
				results <- struct {
					primaryServer    string
					secondaryServers []string
					err              error
				}{primaryServer, secondaryServers, err}
			}()
		}

		wg.Wait()
		close(results)

		// // Verify consistent results
		// var firstResult struct {
		// 	primaryServer    string
		// 	secondaryServers []string
		// 	err              error
		// }
		// firstResultSet := false

		// for result := range results {
		// 	assert.NoError(t, result.err)

		// 	if !firstResultSet {
		// 		firstResult = result
		// 		firstResultSet = true
		// 	} else {
		// 		assert.Equal(t, firstResult.primaryServer, result.primaryServer)
		// 		assert.ElementsMatch(t, firstResult.secondaryServers, result.secondaryServers)
		// 	}
		// }
	})
}

func TestDeleteFile(t *testing.T) {
	t.Run("should successfully delete existing file", func(t *testing.T) {
		// Prepare test data
		master := &Master{
			inTestMode: true,
			FileMap: map[string][]int64{
				"test-file": {
					1, 2,
				},
			},
			mu: sync.RWMutex{},
		}

		// Perform deletion
		err := master.deleteFile("test-file")

		// Assertions
		assert.NoError(t, err)

		// Check that original file is deleted
		_, exists := master.FileMap["test-file"]
		assert.False(t, exists)

		// Check that new deleted file exists with same chunks
		deletedFileName := master.findDeletedFileName("test-file")
		assert.NotEmpty(t, deletedFileName)

		chunks, exists := master.FileMap[deletedFileName]
		assert.True(t, exists)
		assert.Len(t, chunks, 2)
		assert.Equal(t, int64(1), chunks[0])
		assert.Equal(t, int64(2), chunks[1])
	})

	t.Run("should return error for non-existing file", func(t *testing.T) {
		master := &Master{
			inTestMode: true,
			FileMap:    make(map[string][]int64),
			mu:         sync.RWMutex{},
		}

		err := master.deleteFile("non-existent-file")

		assert.Error(t, err)
		assert.Equal(t, "file does not exist", err.Error())
	})

	t.Run("should handle file deletion in test mode", func(t *testing.T) {
		master := &Master{
			inTestMode: true,
			FileMap: map[string][]int64{
				"test-file": {
					1,
				},
			},
			mu: sync.RWMutex{},
		}

		err := master.deleteFile("test-file")

		assert.NoError(t, err)
		_, exists := master.FileMap["test-file"]
		assert.False(t, exists)
	})

	t.Run("should be thread-safe", func(t *testing.T) {
		master := &Master{
			inTestMode: true,
			FileMap: map[string][]int64{
				"test-file": {
					1,
				},
			},
			mu: sync.RWMutex{},
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
		_, exists := master.FileMap["test-file"]
		assert.False(t, exists)
	})
}

// Helper method to find the deleted file name
func (master *Master) findDeletedFileName(originalFileName string) string {
	for fileName := range master.FileMap {
		if strings.Contains(fileName, originalFileName) && strings.Contains(fileName, ".deleted") {
			return fileName
		}
	}
	return ""
}

func TestCreateNewChunk(t *testing.T) {

	node, err := snowflake.NewNode(1)
	assert.Equal(t, nil, err)
	assert.NotNil(t, node)
	t.Run("should successfully create new chunk in normal mode", func(t *testing.T) {
		// Prepare test data
		master := &Master{
			inTestMode:  true,
			idGenerator: node,
			FileMap:     make(map[string][]int64),
			ChunkHandles: make(map[int64]*Chunk),
			mu:          sync.RWMutex{},
		}

		fileName := "test-file"

		// Perform chunk creation
		err := master.createNewChunk(fileName, -1)

		// Assertions
		assert.NoError(t, err)

		// Check that file exists in FileMap
		chunks, exists := master.FileMap[fileName]
		assert.True(t, exists)
		assert.Len(t, chunks, 1)

		// Verify chunk details
		chunk := chunks[0]
		assert.NotEqual(t, int64(-1), chunk)
		// assert.Equal(t, int64(0), chunk.ChunkVersion)

	})

	t.Run("should successfully create new chunk in test mode", func(t *testing.T) {
		// Prepare test data
		master := &Master{
			inTestMode:  true,
			idGenerator: node,
			FileMap:     make(map[string][]int64),
			ChunkHandles: make(map[int64]*Chunk),
			mu:          sync.RWMutex{},
		}

		fileName := "test-file"

		// Perform chunk creation
		err := master.createNewChunk(fileName, -1)

		// Assertions
		assert.NoError(t, err)

		// Check that file exists in FileMap
		chunks, exists := master.FileMap[fileName]
		assert.True(t, exists)
		assert.Len(t, chunks, 1)

		// Verify chunk details
		chunk := chunks[0]
		assert.NotEqual(t, int64(-1), chunk)
	})
	t.Run("should result only in a single chunk for same file", func(t *testing.T) {
		// Prepare test data
		master := &Master{
			inTestMode:  true,
			idGenerator: node,
			FileMap:     make(map[string][]int64),
			ChunkHandles: make(map[int64]*Chunk),
			mu:          sync.RWMutex{},
		}

		fileName := "test-file"

		// Create multiple chunks
		err1 := master.createNewChunk(fileName, -1)
		err2 := master.createNewChunk(fileName, 0)

		// Assertions
		assert.NoError(t, err1)
		assert.NoError(t, err2)

		// Check that file has only one chunk
		chunks, exists := master.FileMap[fileName]
		assert.True(t, exists)
		assert.Len(t, chunks, 1)

	})

	t.Run("creation of chunk concurrently should result only in one created", func(t *testing.T) {
		// Prepare test data
		master := &Master{
			inTestMode:  true,
			idGenerator: node,
			FileMap:     make(map[string][]int64),
			ChunkHandles: make(map[int64]*Chunk),
			mu:          sync.RWMutex{},
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
				err := master.createNewChunk(fileName, -1)
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

		chunks, exists := master.FileMap[fileName]
		assert.True(t, exists, "File should exist in FileMap")
		assert.Len(t, chunks, 1, "Number of chunks created should only be oone")

	})
}

func TestHandleChunkCreation(t *testing.T) {
	node, err := snowflake.NewNode(1)
	assert.Equal(t, nil, err)
	assert.NotNil(t, node)

	t.Run("should successfully create a new file and chunk", func(t *testing.T) {
		// Prepare test data

		// Initialize ServerList with test servers
		pq := &ServerList{}
		heap.Init(pq)

		// Add servers to the priority queue
		servers := []*Server{
			{Server: "server1", NumberOfChunks: 5},
			{Server: "server2", NumberOfChunks: 7},
			{Server: "server3", NumberOfChunks: 3},
			{Server: "server4", NumberOfChunks: 2},
		}

		for _, server := range servers {
			heap.Push(pq, server)
		}

		master := &Master{
			inTestMode:         true,
			idGenerator:        node,
			FileMap:            make(map[string][]int64),
			ChunkHandles:       make(map[int64]*Chunk),
			ChunkServerHandler: make(map[int64]map[string]bool),
			LeaseGrants:        make(map[int64]*Lease),
			mu:                 sync.RWMutex{},
			ServerList:         pq,
		}

		fileName := "test-file"

		// Perform chunk creation
		chunkHandle, err := master.handleChunkCreation(fileName)
		assert.NoError(t,err)
		primaryServer, secondaryServers, _, err := master.assignChunkServers(chunkHandle.ChunkHandle)
		// Assertions
		assert.NoError(t, err)
		assert.NotEqual(t, int64(-1), chunkHandle)
		assert.NotEmpty(t, primaryServer)
		assert.Len(t, secondaryServers, 2) // Assuming choosePrimaryAndSecondary returns 1 primary and 2 secondary

		// Check that file exists in FileMap
		chunks, exists := master.FileMap[fileName]
		assert.True(t, exists)
		assert.Len(t, chunks, 1)

		// Verify chunk handle matches
		assert.Equal(t, chunks[0], chunkHandle.ChunkHandle)

		// Verify ChunkServerHandler has been populated
		createdServers, exists := master.ChunkServerHandler[chunkHandle.ChunkHandle]
		assert.True(t, exists)
		assert.Len(t, createdServers, 3) // Should contain 3 servers from chooseChunkServers

		// Verify opLogger was called
	})

	t.Run("should reuse existing file and reuse the old chunk", func(t *testing.T) {
		// Prepare test data

		// Initialize ServerList with test servers
		pq := &ServerList{}
		heap.Init(pq)

		// Add servers to the priority queue
		servers := []*Server{
			{Server: "server1", NumberOfChunks: 5},
			{Server: "server2", NumberOfChunks: 7},
			{Server: "server3", NumberOfChunks: 3},
			{Server: "server4", NumberOfChunks: 2},
		}

		for _, server := range servers {
			heap.Push(pq, server)
		}

		master := &Master{
			inTestMode:         true,
			idGenerator:        node,
			FileMap:            make(map[string][]int64),
			ChunkServerHandler: make(map[int64]map[string]bool),
			ChunkHandles: make(map[int64]*Chunk),
			LeaseGrants:        make(map[int64]*Lease),
			mu:                 sync.RWMutex{},
			ServerList:         pq,
		}

		fileName := "test-file"

		// Create a file and first chunk
		existingChunk := &Chunk{ChunkHandle: 12345}
		master.FileMap[fileName] = []int64{existingChunk.ChunkHandle}
		master.ChunkHandles[existingChunk.ChunkHandle]=existingChunk
		// Perform chunk creation for existing file
		chunkHandle, err := master.handleChunkCreation(fileName)
		assert.Nil(t,err)
		primaryServer, secondaryServers, _, err := master.assignChunkServers(chunkHandle.ChunkHandle)
		// Assertions
		assert.NoError(t, err)
		assert.Equal(t, existingChunk.ChunkHandle, chunkHandle.ChunkHandle)
		assert.NotEmpty(t, primaryServer)
		assert.Len(t, secondaryServers, 2)

		// File should still exist with just the original chunk
		chunks, exists := master.FileMap[fileName]
		assert.True(t, exists)
		assert.Len(t, chunks, 1)

		// OpLogger shouldn't have been called because no new chunk was created
	})

	t.Run("should reuse existing chunk servers if already assigned", func(t *testing.T) {

		// Initialize ServerList with test servers
		pq := &ServerList{}
		heap.Init(pq)

		// Add servers to the priority queue
		servers := []*Server{
			{Server: "server1", NumberOfChunks: 5},
			{Server: "server2", NumberOfChunks: 7},
			{Server: "server3", NumberOfChunks: 3},
		}

		for _, server := range servers {
			heap.Push(pq, server)
		}

		master := &Master{
			inTestMode:         true,
			idGenerator:        node,
			FileMap:            make(map[string][]int64),
			ChunkServerHandler: make(map[int64]map[string]bool),
			ChunkHandles: make(map[int64]*Chunk),
			LeaseGrants:        make(map[int64]*Lease),
			mu:                 sync.RWMutex{},
			ServerList:         pq,
		}

		fileName := "test-file"
		chunkHandle := int64(12345)

		// Create a pre-existing file with a chunk
		master.FileMap[fileName] = []int64{chunkHandle}
		master.ChunkHandles[chunkHandle]=&Chunk{
			ChunkHandle: chunkHandle,
		}

		// Pre-assign chunk servers for this chunk handle
		preassignedServers := map[string]bool{
			"server1": true,
			"server2": true,
			"server3": true,
		}
		master.ChunkServerHandler[chunkHandle] = preassignedServers

		// Perform chunk creation
		resultChunk,err := master.handleChunkCreation(fileName)
		assert.NoError(t, err)

		primaryServer, secondaryServers, _, err := master.assignChunkServers(resultChunk.ChunkHandle)
		assert.NoError(t, err)
		assert.Equal(t, chunkHandle, resultChunk.ChunkHandle)

		// Check that the preassigned servers were used
		assert.Contains(t, preassignedServers, primaryServer)
		for _, server := range secondaryServers {
			assert.Contains(t, preassignedServers, server)
		}

		// The ServerList should not have been modified (no new servers chosen)
		assert.Len(t, master.ChunkServerHandler[chunkHandle], len(preassignedServers))
		assert.Equal(t, preassignedServers, master.ChunkServerHandler[chunkHandle])
	})

	// t.Run("should handle error from choosing primary and secondary", func(t *testing.T) {

	// 	// Initialize ServerList with test servers
	// 	pq := &ServerList{}
	// 	heap.Init(pq)

	// 	// Empty server list to trigger an error in choosePrimaryAndSecondary

	// 	master := &Master{
	// 		inTestMode:   true,
	// 		idGenerator:  node,
	// 		FileMap:      make(map[string][]Chunk),
	// 		ChunkServerHandler: make(map[int64][]string),
	// 		mu:           sync.RWMutex{},
	// 		ServerList:   pq,
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
	// 	// 4. ChunkServerHandler is populated
	// 	// 5. But the returned values indicate an error state
	// })

	t.Run("should create multiple chunks for the same file in concurrent scenario", func(t *testing.T) {

		// Initialize ServerList with test servers
		pq := &ServerList{}
		heap.Init(pq)

		// Add more servers to handle concurrent requests
		for i := range 30 {
			heap.Push(pq, &Server{
				Server:         "server" + string(rune(i+'0')),
				NumberOfChunks: i % 10,
			})
		}

		master := &Master{
			inTestMode:         true, // Test mode to avoid writing to log
			idGenerator:        node,
			FileMap:            make(map[string][]int64),
			ChunkServerHandler: make(map[int64]map[string]bool),
			ChunkHandles: make(map[int64]*Chunk),
			LeaseGrants:        make(map[int64]*Lease),
			mu:                 sync.RWMutex{},
			ServerList:         pq,
		}

		fileName := "concurrent-test-file"
		numConcurrent := 10

		// WaitGroup to wait for all goroutines to complete
		var wg sync.WaitGroup

		// Channels to collect results and errors
		resultChan := make(chan struct {
			chunkHandle      int64
			primaryServer    string
			secondaryServers []string
			err              error
		}, numConcurrent)

		// Create chunks concurrently
		for range numConcurrent {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resultChunkHandle, _ := master.handleChunkCreation(fileName)
				primaryServer, secondaryServers, _, err := master.assignChunkServers(resultChunkHandle.ChunkHandle)
				resultChan <- struct {
					chunkHandle      int64
					primaryServer    string
					secondaryServers []string
					err              error
				}{resultChunkHandle.ChunkHandle, primaryServer, secondaryServers, err}
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

		_, exists := master.FileMap[fileName]
		assert.True(t, exists)

	})
}

func TestBuildCheckpoint(t *testing.T) {
	t.Run("should successfully create checkpoint with multiple files", func(t *testing.T) {
		// Setup
		master := &Master{
			FileMap: map[string][]int64{
				"test_file1": {
					1, 2,
				},
				"test_file2": {
					3,
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
			FileMap: map[string][]int64{
				"test_file_single": {
					1,
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
			FileMap:    make(map[string][]int64),
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
			FileMap:    make(map[string][]int64),
			ChunkHandles: make(map[int64]*Chunk),
			ChunkServerHandler: make(map[int64]map[string]bool),
		}

		defer os.Remove("checkpoint.chk")

		// Prepare test data
		testFiles := map[string][]int64{
			"test_file1": {
				101,
				102,
			},
			"test_file2": {
				201,
			},
		}

		// Create a master with test data and build checkpoint
		masterWithData := &Master{
			FileMap:    testFiles,
			ChunkHandles: make(map[int64]*Chunk),
			ChunkServerHandler: make(map[int64]map[string]bool),
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
		assert.Equal(t, len(testFiles), len(master.FileMap),
			"Number of files should match after checkpoint read")

		for file, chunks := range testFiles {
			readChunks, exists := master.FileMap[file]
			assert.True(t, exists, "File %s should exist in read checkpoint", file)

			assert.Equal(t, len(chunks), len(readChunks),
				"Chunk count should match for file %s", file)

			for i, chunk := range chunks {
				assert.Equal(t, chunk, readChunks[i],
					"Chunk handle should match for file %s, chunk %d", file, i)
			}
		}
	})

	t.Run("should successfully read checkpoint with single file", func(t *testing.T) {
		// Setup
		master := &Master{
			inTestMode: true,
			FileMap:    make(map[string][]int64),
			ChunkHandles: make(map[int64]*Chunk),
			ChunkServerHandler: make(map[int64]map[string]bool),
		}

		defer os.Remove("checkpoint.chk")

		// Prepare test data
		testFiles := map[string][]int64{
			"test_file_single": {
				101,
			},
		}

		// Create a master with test data and build checkpoint
		masterWithData := &Master{
			FileMap:    testFiles,
			inTestMode: true,
		}
		err := masterWithData.buildCheckpoint()
		assert.Nil(t, err)

		// Read the checkpoint
		err = master.readCheckpoint()
		assert.Nil(t, err, "Should read checkpoint without error")

		// Verify the read data matches the original
		assert.Equal(t, len(testFiles), len(master.FileMap),
			"Number of files should match after checkpoint read")
	})
}

func TestCheckpointWorkflow(t *testing.T) {
	t.Run("should successfully complete full checkpoint workflow", func(t *testing.T) {
		defer os.Remove("checkpoint.chk")

		// Setup original master with test data
		originalMaster := &Master{
			FileMap: map[string][]int64{
				"test_file1": {
					1,
					2,
				},
				"test_file2": {
					3,
				},
			},
			ChunkHandles: make(map[int64]*Chunk),
			ChunkServerHandler: make(map[int64]map[string]bool),
			inTestMode: true,
		}

		// Build checkpoint
		err := originalMaster.buildCheckpoint()
		assert.Nil(t, err, "Should build checkpoint without error")

		// Create a new master to read the checkpoint
		restoredMaster := &Master{
			inTestMode: true,
			FileMap:    make(map[string][]int64),
			ChunkHandles: make(map[int64]*Chunk),
			ChunkServerHandler: make(map[int64]map[string]bool),
		}

		// Read checkpoint
		err = restoredMaster.readCheckpoint()
		assert.Nil(t, err, "Should read checkpoint without error")

		// Verify restored data matches original
		assert.Equal(t, len(originalMaster.FileMap), len(restoredMaster.FileMap),
			"Restored FileMap size should match original")

		for file, originalChunks := range originalMaster.FileMap {
			restoredChunks, exists := restoredMaster.FileMap[file]
			assert.True(t, exists, "File %s should exist in restored checkpoint", file)

			assert.Equal(t, len(originalChunks), len(restoredChunks),
				"Chunk count should match for file %s", file)

			for i, originalChunk := range originalChunks {
				assert.Equal(t, originalChunk, restoredChunks[i],
					"Chunk handle should match for file %s, chunk %d", file, i)
			}
		}
	})

	t.Run("should handle checkpoint workflow with single file", func(t *testing.T) {
		defer os.Remove("checkpoint.chk")

		// Setup original master with single file test data
		originalMaster := &Master{
			FileMap: map[string][]int64{
				"test_file_single": {
					1,
				},
			},
			ChunkHandles: make(map[int64]*Chunk),
			// ChunkHandles: ,
			inTestMode: true,
		}

		// Build checkpoint
		err := originalMaster.buildCheckpoint()
		assert.Nil(t, err, "Should build checkpoint without error")

		// Create a new master to read the checkpoint
		restoredMaster := &Master{
			inTestMode: true,
			FileMap:    make(map[string][]int64),
			ChunkHandles: make(map[int64]*Chunk),
		}

		// Read checkpoint
		err = restoredMaster.readCheckpoint()
		assert.Nil(t, err, "Should read checkpoint without error")

		// Verify restored data matches original
		assert.Equal(t, len(originalMaster.FileMap), len(restoredMaster.FileMap),
			"Restored FileMap size should match original")
	})
}
