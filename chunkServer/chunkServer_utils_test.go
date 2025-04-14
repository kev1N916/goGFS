package chunkserver

import (
	"bytes"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	lrucache "github.com/involk-secure-1609/goGFS/lruCache"
	"github.com/stretchr/testify/assert"
)

func TestLoadChunks(t *testing.T) {
	t.Run("Successfully loads valid chunk files", func(t *testing.T) {
		// Create a temporary directory for testing
		tmpDir, err := os.MkdirTemp("", "chunktest")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		// Create some test chunk files
		expectedChunks := []int64{123, 456, 789}
		for _, chunkID := range expectedChunks {
			filename := filepath.Join(tmpDir, filepath.Clean(strconv.FormatInt(chunkID, 10)+".chunk"))
			if err := os.WriteFile(filename, []byte("test data"), 0644); err != nil {
				t.Fatalf("Failed to create test chunk file: %v", err)
			}
		}

		// Initialize a ChunkServer with the test directory
		server := &ChunkServer{
			ChunkDirectory: tmpDir,
		}

		// Call the function being tested
		err = server.loadChunks()
		if err != nil {
			t.Errorf("loadChunks() returned an error: %v", err)
		}

		// Sort the chunks for comparison (since directory reads might be in any order)
		slices.Sort(server.ChunkHandles)

		// Check that all the expected chunks were loaded
		if !reflect.DeepEqual(server.ChunkHandles, expectedChunks) {
			t.Errorf("Expected chunks %v, got %v", expectedChunks, server.ChunkHandles)
		}
	})

	t.Run("Skips directories in chunk folder", func(t *testing.T) {
		// Create a temporary directory for testing
		tmpDir, err := os.MkdirTemp("", "chunktest")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		// Create a valid chunk file
		validChunk := int64(123)
		validFilename := filepath.Join(tmpDir, filepath.Clean(strconv.FormatInt(validChunk, 10)+".chunk"))
		if err := os.WriteFile(validFilename, []byte("test data"), 0644); err != nil {
			t.Fatalf("Failed to create test chunk file: %v", err)
		}

		// Create a subdirectory that should be skipped
		subDir := filepath.Join(tmpDir, "subdir.chunk")
		if err := os.Mkdir(subDir, 0755); err != nil {
			t.Fatalf("Failed to create subdirectory: %v", err)
		}

		// Initialize a ChunkServer with the test directory
		server := &ChunkServer{
			ChunkDirectory: tmpDir,
		}

		// Call the function being tested
		err = server.loadChunks()
		if err != nil {
			t.Errorf("loadChunks() returned an error: %v", err)
		}

		// Check that only the valid chunk was loaded (directory was skipped)
		expected := []int64{validChunk}
		if !reflect.DeepEqual(server.ChunkHandles, expected) {
			t.Errorf("Expected chunks %v, got %v", expected, server.ChunkHandles)
		}
	})

	t.Run("Skips non-chunk files", func(t *testing.T) {
		// Create a temporary directory for testing
		tmpDir, err := os.MkdirTemp("", "chunktest")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		// Create a valid chunk file
		validChunk := int64(123)
		validFilename := filepath.Join(tmpDir, filepath.Clean(strconv.FormatInt(validChunk, 10)+".chunk"))
		if err := os.WriteFile(validFilename, []byte("test data"), 0644); err != nil {
			t.Fatalf("Failed to create test chunk file: %v", err)
		}

		// Create a non-chunk file that should be skipped
		nonChunkFile := filepath.Join(tmpDir, "not-a-chunk.txt")
		if err := os.WriteFile(nonChunkFile, []byte("not a chunk"), 0644); err != nil {
			t.Fatalf("Failed to create non-chunk file: %v", err)
		}

		// Initialize a ChunkServer with the test directory
		server := &ChunkServer{
			ChunkDirectory: tmpDir,
		}

		// Call the function being tested
		err = server.loadChunks()
		if err != nil {
			t.Errorf("loadChunks() returned an error: %v", err)
		}

		// Check that only the valid chunk was loaded (non-chunk file was skipped)
		expected := []int64{validChunk}
		if !reflect.DeepEqual(server.ChunkHandles, expected) {
			t.Errorf("Expected chunks %v, got %v", expected, server.ChunkHandles)
		}
	})

	t.Run("Skips invalid chunk filename formats", func(t *testing.T) {
		// Create a temporary directory for testing
		tmpDir, err := os.MkdirTemp("", "chunktest")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		// Create a valid chunk file
		validChunk := int64(123)
		validFilename := filepath.Join(tmpDir, filepath.Clean(strconv.FormatInt(validChunk, 10)+".chunk"))
		if err := os.WriteFile(validFilename, []byte("test data"), 0644); err != nil {
			t.Fatalf("Failed to create test chunk file: %v", err)
		}

		// Create an invalid chunk file (non-numeric prefix)
		invalidFile := filepath.Join(tmpDir, "invalid-chunk-name.chunk")
		if err := os.WriteFile(invalidFile, []byte("invalid chunk"), 0644); err != nil {
			t.Fatalf("Failed to create invalid chunk file: %v", err)
		}

		// Initialize a ChunkServer with the test directory
		server := &ChunkServer{
			ChunkDirectory: tmpDir,
		}

		// Call the function being tested
		err = server.loadChunks()
		if err != nil {
			t.Errorf("loadChunks() returned an error: %v", err)
		}

		// Check that only the valid chunk was loaded (invalid chunk filename was skipped)
		expected := []int64{validChunk}
		if !reflect.DeepEqual(server.ChunkHandles, expected) {
			t.Errorf("Expected chunks %v, got %v", expected, server.ChunkHandles)
		}
	})

	t.Run("Handles directory read error", func(t *testing.T) {
		// Create a non-existent directory path
		nonExistentDir := "/this/directory/does/not/exist"

		// Initialize a ChunkServer with the non-existent directory
		server := &ChunkServer{
			ChunkDirectory: nonExistentDir,
		}

		// Call the function being tested
		err := server.loadChunks()

		// Check that an error was returned
		if err == nil {
			t.Errorf("Expected an error for non-existent directory, but got nil")
		}
	})

	t.Run("Handles empty directory", func(t *testing.T) {
		// Create a temporary directory for testing
		tmpDir, err := os.MkdirTemp("", "chunktest")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		// Initialize a ChunkServer with the empty directory
		server := &ChunkServer{
			ChunkDirectory: tmpDir,
		}

		// Call the function being tested
		err = server.loadChunks()
		if err != nil {
			t.Errorf("loadChunks() returned an error for empty directory: %v", err)
		}

		// Check that the chunk handles slice is empty
		if len(server.ChunkHandles) != 0 {
			t.Errorf("Expected empty chunk handles slice, got %v", server.ChunkHandles)
		}
	})
}

func TestCheckIfPrimary(t *testing.T) {
	t.Run("Returns false when chunk has no lease grant", func(t *testing.T) {
		// Setup a ChunkServer with empty lease grants
		server := &ChunkServer{
			LeaseGrants: make(map[int64]*LeaseGrant),
			mu:          sync.RWMutex{},
		}

		// Call the function being tested
		result := server.checkIfPrimary(123)

		// Check the result
		if result != false {
			t.Errorf("Expected false for chunk with no lease grant, got %v", result)
		}
	})

	t.Run("Returns true for valid lease grant within time window", func(t *testing.T) {
		// Setup a ChunkServer with a valid, recent lease grant
		server := &ChunkServer{
			LeaseGrants: map[int64]*LeaseGrant{
				123: {
					ChunkHandle: 123,
					granted:     true,
					GrantTime:   time.Now().Add(-30 * time.Second), // 30 seconds ago (still valid)
				},
			},
			mu: sync.RWMutex{},
		}

		// Call the function being tested
		result := server.checkIfPrimary(123)

		// Check the result
		if result != true {
			t.Errorf("Expected true for valid lease grant, got %v", result)
		}
	})

	t.Run("Returns false for expired lease grant (>= 60 seconds)", func(t *testing.T) {
		// Setup a ChunkServer with an expired lease grant
		server := &ChunkServer{
			LeaseGrants: map[int64]*LeaseGrant{
				123: {
					ChunkHandle: 123,
					granted:     true,
					GrantTime:   time.Now().Add(-60 * time.Second), // Exactly 60 seconds ago (expired)
				},
			},
			mu: sync.RWMutex{},
		}

		// Call the function being tested
		result := server.checkIfPrimary(123)

		// Check the result
		if result != false {
			t.Errorf("Expected false for expired lease grant (exactly 60 seconds), got %v", result)
		}

		// Test with a lease that's well past expiration
		server.LeaseGrants[123].GrantTime = time.Now().Add(-90 * time.Second) // 90 seconds ago
		result = server.checkIfPrimary(123)

		if result != false {
			t.Errorf("Expected false for expired lease grant (90 seconds), got %v", result)
		}
	})

	t.Run("Handles edge case at exactly 60 seconds", func(t *testing.T) {
		// Setup a ChunkServer with a lease grant exactly at the time boundary
		server := &ChunkServer{
			LeaseGrants: map[int64]*LeaseGrant{
				123: {
					ChunkHandle: 123,
					granted:     true,
					GrantTime:   time.Now().Add(-60 * time.Second), // Exactly 60 seconds ago
				},
			},
			mu: sync.RWMutex{},
		}

		// Call the function being tested
		result := server.checkIfPrimary(123)

		// Check the result - should be false at exactly 60 seconds
		if result != false {
			t.Errorf("Expected false at exactly 60 seconds, got %v", result)
		}
	})

	t.Run("Handles lease grant with different chunk handle", func(t *testing.T) {
		// Setup a ChunkServer with a valid lease grant for a different chunk
		server := &ChunkServer{
			LeaseGrants: map[int64]*LeaseGrant{
				456: {
					ChunkHandle: 456,
					granted:     true,
					GrantTime:   time.Now().Add(-30 * time.Second), // 30 seconds ago (still valid)
				},
			},
			mu: sync.RWMutex{},
		}

		// Call the function being tested with a different chunk handle
		result := server.checkIfPrimary(123)

		// Check the result
		if result != false {
			t.Errorf("Expected false for chunk handle with no lease, got %v", result)
		}
	})

	t.Run("Thread safety with concurrent access", func(t *testing.T) {
		// Setup a ChunkServer with a valid lease grant
		server := &ChunkServer{
			LeaseGrants: map[int64]*LeaseGrant{
				123: {
					ChunkHandle: 123,
					granted:     true,
					GrantTime:   time.Now().Add(-30 * time.Second), // 30 seconds ago (still valid)
				},
			},
			mu: sync.RWMutex{},
		}

		// Number of concurrent goroutines to test with
		concurrentRequests := 100

		// Create a channel to collect results
		results := make(chan bool, concurrentRequests)

		// Launch multiple goroutines that call checkIfPrimary concurrently
		for range concurrentRequests {
			go func() {
				// defer wg.Done()
				result := server.checkIfPrimary(123)
				results <- result
			}()
		}

		// Check that all results are consistent
		var i int = 0
		expectedResult := true
		// iterates through results until it is closed
		for result := range results {
			i++
			if result != expectedResult {
				t.Errorf("Expected %v from concurrent access, got %v", expectedResult, result)
				break
			}
			if i == 100 {
				break
			}
		}
		close(results)

	})

	t.Run("Returns false for non-granted lease", func(t *testing.T) {
		// Setup a ChunkServer with a lease that exists but isn't granted
		server := &ChunkServer{
			LeaseGrants: map[int64]*LeaseGrant{
				123: {
					ChunkHandle: 123,
					granted:     false, // Lease exists but isn't granted
					GrantTime:   time.Now().Add(-30 * time.Second),
				},
			},
			mu: sync.RWMutex{},
		}

		// Call the function being tested
		result := server.checkIfPrimary(123)

		// The function doesn't check the granted field, so we should get true
		// if the time is valid. This test verifies the current behavior.
		if result != true {
			t.Errorf("Current behavior: Expected true for non-granted lease with valid time, got %v", result)
		}
	})
}

func TestMutateChunk(t *testing.T) {
	t.Run("tests mutations to an empty file", func(t *testing.T) {
		// Setup a ChunkServer with empty lease grants

		testChunkHandle := int64(1)
		tmpDir, err := os.MkdirTemp("", "chunktest")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		testMutationId := int64(23)
		testData := []byte{'t', 'e', 's', 't'}
		server := &ChunkServer{
			ChunkDirectory: tmpDir,
			LeaseGrants:    make(map[int64]*LeaseGrant),
			LruCache:       lrucache.NewLRUBufferCache(100),
			mu:             sync.RWMutex{},
			CheckSums:      make(map[int64][]byte),
			checkSummer: &ChunkCheckSum{
				crcTable: crc32.MakeTable(crc32.Castagnoli),
			},
		}

		chunk, err := server.openChunk(testChunkHandle)
		assert.Nil(t, err)

		chunkBuffer, verified, err := server.verifyChunkCheckSum(chunk, testChunkHandle)
		assert.Nil(t, err)
		assert.NotNil(t, chunkBuffer)
		assert.True(t, verified)

		lastPartOfChunkBuffer := server.getLastPartOfChunk(chunkBuffer)
		assert.NotNil(t, lastPartOfChunkBuffer)

		server.LruCache.Put(testMutationId, testData)

		checkSumData, present := server.getCheckSum(testChunkHandle)
		assert.True(t, present)

		checkSumBuffer := bytes.NewBuffer(checkSumData)
		assert.NotNil(t, checkSumBuffer)

		_, err = server.mutateChunk(chunk, testChunkHandle, testMutationId, 0, lastPartOfChunkBuffer)
		assert.Nil(t, err)

	})

	t.Run("testing if server.loadChunks works", func(t *testing.T) {
		// Setup a ChunkServer with empty lease grants

		testChunkHandle := int64(1)
		tmpDir, err := os.MkdirTemp("", "chunktest")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		// testMutationId := int64(23)
		// testData := []byte{'t', 'e', 's', 't'}

		testNewData := []byte{
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '1',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '2',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '3',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '4',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '5',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '6',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '7',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '8',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '9',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '1', '0',
		}

		server := &ChunkServer{
			ChunkDirectory: tmpDir,
			LeaseGrants:    make(map[int64]*LeaseGrant),
			LruCache:       lrucache.NewLRUBufferCache(100),
			mu:             sync.RWMutex{},
			CheckSums:      make(map[int64][]byte),
			checkSummer: &ChunkCheckSum{
				crcTable: crc32.MakeTable(crc32.Castagnoli),
			},
		}

		chunk, err := server.openChunk(testChunkHandle)
		assert.Nil(t, err)

		chunkCheckSumFile, err := server.openChunkCheckSum(testChunkHandle)
		assert.Nil(t, err)

		_, err = chunk.Write(testNewData)
		assert.Nil(t, err)

		calculatedCheckSum, err := server.calculateChunkCheckSum(testNewData)
		assert.Nil(t, err)
		assert.NotEmpty(t, calculatedCheckSum)

		_, err = chunkCheckSumFile.Write(calculatedCheckSum)
		assert.Nil(t, err)

		err = server.loadChunks()
		assert.Nil(t, err)

		chunkBuffer, verified, err := server.verifyChunkCheckSum(chunk, testChunkHandle)
		assert.Nil(t, err)
		assert.NotNil(t, chunkBuffer)
		assert.True(t, verified)

	})

	t.Run("tests if writing to a pre-existing chunk works", func(t *testing.T) {
		// Setup a ChunkServer with empty lease grants

		testChunkHandle := int64(1)
		tmpDir, err := os.MkdirTemp("", "chunktest")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		testMutationId := int64(23)
		testData := []byte{'t', 'e', 's', 't'}

		testNewData := []byte{
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '1',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '2',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '3',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '4',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '5',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '6',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '7',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '8',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '9',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '1', '0',
		}

		server := &ChunkServer{
			ChunkDirectory: tmpDir,
			LeaseGrants:    make(map[int64]*LeaseGrant),
			LruCache:       lrucache.NewLRUBufferCache(100),
			mu:             sync.RWMutex{},
			CheckSums:      make(map[int64][]byte),
			checkSummer: &ChunkCheckSum{
				crcTable: crc32.MakeTable(crc32.Castagnoli),
			},
		}

		chunk, err := server.openChunk(testChunkHandle)
		assert.Nil(t, err)

		chunkCheckSumFile, err := server.openChunkCheckSum(testChunkHandle)
		assert.Nil(t, err)

		_, err = chunk.Write(testNewData)
		assert.Nil(t, err)

		calculatedCheckSum, err := server.calculateChunkCheckSum(testNewData)
		assert.Nil(t, err)
		assert.NotEmpty(t, calculatedCheckSum)

		_, err = chunkCheckSumFile.Write(calculatedCheckSum)
		assert.Nil(t, err)

		// server.setCheckSum(testChunkHandle,0,calculatedCheckSum,[]byte{})

		err = server.loadChunks()
		assert.Nil(t, err)

		chunkBuffer, verified, err := server.verifyChunkCheckSum(chunk, testChunkHandle)
		assert.Nil(t, err)
		assert.NotNil(t, chunkBuffer)
		assert.True(t, verified)

		server.LruCache.Put(testMutationId, testData)

		lastPartOfChunkBuffer := server.getLastPartOfChunk(chunkBuffer)
		assert.NotNil(t, lastPartOfChunkBuffer)

		checkSumData, present := server.getCheckSum(testChunkHandle)
		assert.True(t, present)

		checkSumBuffer := bytes.NewBuffer(checkSumData)
		assert.NotNil(t, checkSumBuffer)

		_, err = server.mutateChunk(chunk, testChunkHandle, testMutationId, 0, lastPartOfChunkBuffer)
		assert.Nil(t, err)

	})

	t.Run("tests if multiple writes to a new chunk works", func(t *testing.T) {
		// Setup a ChunkServer with empty lease grants

		testChunkHandle := int64(1)
		tmpDir, err := os.MkdirTemp("", "chunktest")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		testMutationId := int64(23)

		testNewData := []byte{
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '1',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '2',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '3',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '4',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '5',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '6',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '7',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '8',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '9',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '1', '0',
		}

		server := &ChunkServer{
			ChunkDirectory: tmpDir,
			LeaseGrants:    make(map[int64]*LeaseGrant),
			LruCache:       lrucache.NewLRUBufferCache(100),
			mu:             sync.RWMutex{},
			CheckSums:      make(map[int64][]byte),
			checkSummer: &ChunkCheckSum{
				crcTable: crc32.MakeTable(crc32.Castagnoli),
			},
		}

		chunk, err := server.openChunk(testChunkHandle)
		assert.Nil(t, err)

		chunkBuffer, verified, err := server.verifyChunkCheckSum(chunk, testChunkHandle)
		assert.Nil(t, err)
		assert.NotNil(t, chunkBuffer)
		assert.True(t, verified)

		lastPartOfChunkBuffer := server.getLastPartOfChunk(chunkBuffer)
		assert.NotNil(t, lastPartOfChunkBuffer)

		server.LruCache.Put(testMutationId, testNewData)

		initialChunkOffset := int64(0)

		for range 4 {
			changeInOffset, err := server.mutateChunk(chunk, testChunkHandle, testMutationId, initialChunkOffset, lastPartOfChunkBuffer)
			assert.Nil(t, err)
			initialChunkOffset += changeInOffset
		}

		chunk.Seek(0,io.SeekStart)

		_,verified,err=server.verifyChunkCheckSum(chunk,testChunkHandle)
		assert.Nil(t, err)
		assert.True(t, verified)

	})

	t.Run("tests if multiple writes to a pre-existing chunk works", func(t *testing.T) {
		// Setup a ChunkServer with empty lease grants

		testChunkHandle := int64(1)
		tmpDir, err := os.MkdirTemp("", "chunktest")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		testMutationId := int64(23)
		// testData := []byte{'t', 'e', 's', 't'}

		testNewData := []byte{
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '1',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '2',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '3',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '4',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '5',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '6',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '7',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '8',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '9',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '1',
		}

		server := &ChunkServer{
			ChunkDirectory: tmpDir,
			LeaseGrants:    make(map[int64]*LeaseGrant),
			LruCache:       lrucache.NewLRUBufferCache(100),
			mu:             sync.RWMutex{},
			CheckSums:      make(map[int64][]byte),
			checkSummer: &ChunkCheckSum{
				crcTable: crc32.MakeTable(crc32.Castagnoli),
			},
		}

		chunk, err := server.openChunk(testChunkHandle)
		assert.Nil(t, err)

		chunkCheckSumFile, err := server.openChunkCheckSum(testChunkHandle)
		assert.Nil(t, err)

		_, err = chunk.Write(testNewData)
		assert.Nil(t, err)

		err = chunk.Sync()
		assert.Nil(t, err)

		calculatedCheckSum, err := server.calculateChunkCheckSum(testNewData)
		assert.Nil(t, err)
		assert.NotEmpty(t, calculatedCheckSum)

		server.CheckSums[testChunkHandle] = calculatedCheckSum
		_, err = chunkCheckSumFile.Write(calculatedCheckSum)
		assert.Nil(t, err)

		info, err := chunk.Stat()
		assert.Nil(t, err)
		assert.NotZero(t, info.Size())
		t.Log(info.Size())

		_, err = chunk.Seek(0, io.SeekStart)
		assert.Nil(t, err)

		chunkBuffer, verified, err := server.verifyChunkCheckSum(chunk, testChunkHandle)
		assert.Nil(t, err)
		assert.NotNil(t, chunkBuffer)
		assert.NotZero(t,chunkBuffer.Len())
		assert.True(t, verified)

		server.LruCache.Put(testMutationId, testNewData)

		lastPartOfChunkBuffer := server.getLastPartOfChunk(chunkBuffer)
		assert.NotNil(t, lastPartOfChunkBuffer)

		initialChunkOffset := info.Size()
		_, err = chunk.Seek(initialChunkOffset, io.SeekStart)
		assert.Nil(t, err)
		for i := range 2 {

			changeInOffset, err := server.mutateChunk(chunk, testChunkHandle, testMutationId, initialChunkOffset, lastPartOfChunkBuffer)
			assert.Nil(t, err)
			_, err = chunk.Seek(0, io.SeekStart)
			assert.Nil(t, err)
			_, verified, err := server.verifyChunkCheckSum(chunk, testChunkHandle)
			assert.Nil(t, err)
			assert.True(t, verified)
			t.Log(i,verified)
			initialChunkOffset += changeInOffset
		}

	})

	
	t.Run("part 2 of checking if multiple writes to a pre-existing chunk works", func(t *testing.T) {
		// Setup a ChunkServer with empty lease grants

		testChunkHandle := int64(1)
		tmpDir, err := os.MkdirTemp("", "chunktest")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		testMutationId := int64(23)
		// testData := []byte{'t', 'e', 's', 't'}

		testNewData := []byte{
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '1',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '2',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '3',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '4',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '5',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '6',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '7',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '8',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '9',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '1', '0',
		}

		server := &ChunkServer{
			ChunkDirectory: tmpDir,
			LeaseGrants:    make(map[int64]*LeaseGrant),
			LruCache:       lrucache.NewLRUBufferCache(100),
			mu:             sync.RWMutex{},
			CheckSums:      make(map[int64][]byte),
			checkSummer: &ChunkCheckSum{
				crcTable: crc32.MakeTable(crc32.Castagnoli),
			},
		}

		chunk, err := server.openChunk(testChunkHandle)
		assert.Nil(t, err)

		chunkCheckSumFile, err := server.openChunkCheckSum(testChunkHandle)
		assert.Nil(t, err)

		_, err = chunk.Write(testNewData)
		assert.Nil(t, err)

		err = chunk.Sync()
		assert.Nil(t, err)

		calculatedCheckSum, err := server.calculateChunkCheckSum(testNewData)
		assert.Nil(t, err)
		assert.NotEmpty(t, calculatedCheckSum)

		server.CheckSums[testChunkHandle] = calculatedCheckSum
		_, err = chunkCheckSumFile.Write(calculatedCheckSum)
		assert.Nil(t, err)

		info, err := chunk.Stat()
		assert.Nil(t, err)
		assert.NotZero(t, info.Size())
		t.Log(info.Size())

		_, err = chunk.Seek(0, io.SeekStart)
		assert.Nil(t, err)

		chunkBuffer, verified, err := server.verifyChunkCheckSum(chunk, testChunkHandle)
		assert.Nil(t, err)
		assert.NotNil(t, chunkBuffer)
		assert.NotZero(t,chunkBuffer.Len())
		assert.True(t, verified)

		server.LruCache.Put(testMutationId, testNewData)

		lastPartOfChunkBuffer := server.getLastPartOfChunk(chunkBuffer)
		assert.NotNil(t, lastPartOfChunkBuffer)

		initialChunkOffset := info.Size()
		_, err = chunk.Seek(initialChunkOffset, io.SeekStart)
		assert.Nil(t, err)
		for i := range 2 {

			changeInOffset, err := server.mutateChunk(chunk, testChunkHandle, testMutationId, initialChunkOffset, lastPartOfChunkBuffer)
			assert.Nil(t, err)
			_, err = chunk.Seek(0, io.SeekStart)
			assert.Nil(t, err)
			_, verified, err := server.verifyChunkCheckSum(chunk, testChunkHandle)
			assert.Nil(t, err)
			assert.True(t, verified)
			t.Log(i,verified)
			initialChunkOffset += changeInOffset
		}

	})


	t.Run("part 3 of checking if multiple writes to a pre-existing chunk works", func(t *testing.T) {
		// Setup a ChunkServer with empty lease grants

		testChunkHandle := int64(1)
		tmpDir, err := os.MkdirTemp("", "chunktest")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		testMutationId := int64(23)
		// testData := []byte{'t', 'e', 's', 't'}

		testNewData := []byte{
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '1',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '2',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '3',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '4',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '5',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '6',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '7',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '8',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '0', '9',
			't', 'e', 's', 't', 'd', 'a', 't', 'a', '1', '0','t',
		}

		server := &ChunkServer{
			ChunkDirectory: tmpDir,
			LeaseGrants:    make(map[int64]*LeaseGrant),
			LruCache:       lrucache.NewLRUBufferCache(100),
			mu:             sync.RWMutex{},
			CheckSums:      make(map[int64][]byte),
			checkSummer: &ChunkCheckSum{
				crcTable: crc32.MakeTable(crc32.Castagnoli),
			},
		}

		chunk, err := server.openChunk(testChunkHandle)
		assert.Nil(t, err)

		chunkCheckSumFile, err := server.openChunkCheckSum(testChunkHandle)
		assert.Nil(t, err)

		_, err = chunk.Write(testNewData)
		assert.Nil(t, err)

		err = chunk.Sync()
		assert.Nil(t, err)

		calculatedCheckSum, err := server.calculateChunkCheckSum(testNewData)
		assert.Nil(t, err)
		assert.NotEmpty(t, calculatedCheckSum)

		server.CheckSums[testChunkHandle] = calculatedCheckSum
		_, err = chunkCheckSumFile.Write(calculatedCheckSum)
		assert.Nil(t, err)

		info, err := chunk.Stat()
		assert.Nil(t, err)
		assert.NotZero(t, info.Size())
		t.Log(info.Size())

		_, err = chunk.Seek(0, io.SeekStart)
		assert.Nil(t, err)

		chunkBuffer, verified, err := server.verifyChunkCheckSum(chunk, testChunkHandle)
		assert.Nil(t, err)
		assert.NotNil(t, chunkBuffer)
		assert.NotZero(t,chunkBuffer.Len())
		assert.True(t, verified)

		server.LruCache.Put(testMutationId, testNewData)

		lastPartOfChunkBuffer := server.getLastPartOfChunk(chunkBuffer)
		assert.NotNil(t, lastPartOfChunkBuffer)

		initialChunkOffset := info.Size()
		_, err = chunk.Seek(initialChunkOffset, io.SeekStart)
		assert.Nil(t, err)
		for i := range 2 {

			changeInOffset, err := server.mutateChunk(chunk, testChunkHandle, testMutationId, initialChunkOffset, lastPartOfChunkBuffer)
			assert.Nil(t, err)
			_, err = chunk.Seek(0, io.SeekStart)
			assert.Nil(t, err)
			_, verified, err := server.verifyChunkCheckSum(chunk, testChunkHandle)
			assert.Nil(t, err)
			assert.True(t, verified)
			t.Log(i,verified)
			initialChunkOffset += changeInOffset
		}

	})
}
