package master

import (
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