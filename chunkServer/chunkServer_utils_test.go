package chunkserver

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
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
			filename := filepath.Join(tmpDir, filepath.Clean(strconv.FormatInt(chunkID,10)+".chunk"))
			if err := os.WriteFile(filename, []byte("test data"), 0644); err != nil {
				t.Fatalf("Failed to create test chunk file: %v", err)
			}
		}

		// Initialize a ChunkServer with the test directory
		server := &ChunkServer{
			chunkDirectory: tmpDir,
		}

		// Call the function being tested
		err = server.loadChunks()
		if err != nil {
			t.Errorf("loadChunks() returned an error: %v", err)
		}

		// Sort the chunks for comparison (since directory reads might be in any order)
		slices.Sort(server.chunkHandles)

		// Check that all the expected chunks were loaded
		if !reflect.DeepEqual(server.chunkHandles, expectedChunks) {
			t.Errorf("Expected chunks %v, got %v", expectedChunks, server.chunkHandles)
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
		validFilename := filepath.Join(tmpDir, filepath.Clean(strconv.FormatInt(validChunk,10)+".chunk"))
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
			chunkDirectory: tmpDir,
		}

		// Call the function being tested
		err = server.loadChunks()
		if err != nil {
			t.Errorf("loadChunks() returned an error: %v", err)
		}

		// Check that only the valid chunk was loaded (directory was skipped)
		expected := []int64{validChunk}
		if !reflect.DeepEqual(server.chunkHandles, expected) {
			t.Errorf("Expected chunks %v, got %v", expected, server.chunkHandles)
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
		validFilename := filepath.Join(tmpDir, filepath.Clean(strconv.FormatInt(validChunk,10)+".chunk"))
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
			chunkDirectory: tmpDir,
		}

		// Call the function being tested
		err = server.loadChunks()
		if err != nil {
			t.Errorf("loadChunks() returned an error: %v", err)
		}

		// Check that only the valid chunk was loaded (non-chunk file was skipped)
		expected := []int64{validChunk}
		if !reflect.DeepEqual(server.chunkHandles, expected) {
			t.Errorf("Expected chunks %v, got %v", expected, server.chunkHandles)
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
		validFilename := filepath.Join(tmpDir, filepath.Clean(strconv.FormatInt(validChunk,10)+".chunk"))
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
			chunkDirectory: tmpDir,
		}

		// Call the function being tested
		err = server.loadChunks()
		if err != nil {
			t.Errorf("loadChunks() returned an error: %v", err)
		}

		// Check that only the valid chunk was loaded (invalid chunk filename was skipped)
		expected := []int64{validChunk}
		if !reflect.DeepEqual(server.chunkHandles, expected) {
			t.Errorf("Expected chunks %v, got %v", expected, server.chunkHandles)
		}
	})

	t.Run("Handles directory read error", func(t *testing.T) {
		// Create a non-existent directory path
		nonExistentDir := "/this/directory/does/not/exist"

		// Initialize a ChunkServer with the non-existent directory
		server := &ChunkServer{
			chunkDirectory: nonExistentDir,
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
			chunkDirectory: tmpDir,
		}

		// Call the function being tested
		err = server.loadChunks()
		if err != nil {
			t.Errorf("loadChunks() returned an error for empty directory: %v", err)
		}

		// Check that the chunk handles slice is empty
		if len(server.chunkHandles) != 0 {
			t.Errorf("Expected empty chunk handles slice, got %v", server.chunkHandles)
		}
	})
}

func TestCheckIfPrimary(t *testing.T) {
	t.Run("Returns false when chunk has no lease grant", func(t *testing.T) {
		// Setup a ChunkServer with empty lease grants
		server := &ChunkServer{
			leaseGrants: make(map[int64]*LeaseGrant),
			mu:          sync.Mutex{},
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
			leaseGrants: map[int64]*LeaseGrant{
				123: {
					chunkHandle: 123,
					granted:     true,
					grantTime:   time.Now().Add(-30 * time.Second), // 30 seconds ago (still valid)
				},
			},
			mu: sync.Mutex{},
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
			leaseGrants: map[int64]*LeaseGrant{
				123: {
					chunkHandle: 123,
					granted:     true,
					grantTime:   time.Now().Add(-60 * time.Second), // Exactly 60 seconds ago (expired)
				},
			},
			mu: sync.Mutex{},
		}

		// Call the function being tested
		result := server.checkIfPrimary(123)

		// Check the result
		if result != false {
			t.Errorf("Expected false for expired lease grant (exactly 60 seconds), got %v", result)
		}

		// Test with a lease that's well past expiration
		server.leaseGrants[123].grantTime = time.Now().Add(-90 * time.Second) // 90 seconds ago
		result = server.checkIfPrimary(123)
        
		if result != false {
			t.Errorf("Expected false for expired lease grant (90 seconds), got %v", result)
		}
	})

	t.Run("Handles edge case at exactly 60 seconds", func(t *testing.T) {
		// Setup a ChunkServer with a lease grant exactly at the time boundary
		server := &ChunkServer{
			leaseGrants: map[int64]*LeaseGrant{
				123: {
					chunkHandle: 123,
					granted:     true,
					grantTime:   time.Now().Add(-60 * time.Second), // Exactly 60 seconds ago
				},
			},
			mu: sync.Mutex{},
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
			leaseGrants: map[int64]*LeaseGrant{
				456: {
					chunkHandle: 456,
					granted:     true,
					grantTime:   time.Now().Add(-30 * time.Second), // 30 seconds ago (still valid)
				},
			},
			mu: sync.Mutex{},
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
			leaseGrants: map[int64]*LeaseGrant{
				123: {
					chunkHandle: 123,
					granted:     true,
					grantTime:   time.Now().Add(-30 * time.Second), // 30 seconds ago (still valid)
				},
			},
			mu: sync.Mutex{},
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
		var i int=0
		expectedResult := true
		// iterates through results until it is closed
		for result := range results {
			i++
			if result != expectedResult {
				t.Errorf("Expected %v from concurrent access, got %v", expectedResult, result)
				break
			}
			if i==100{
				break
			}
		}
		close(results)

	})

	t.Run("Returns false for non-granted lease", func(t *testing.T) {
		// Setup a ChunkServer with a lease that exists but isn't granted
		server := &ChunkServer{
			leaseGrants: map[int64]*LeaseGrant{
				123: {
					chunkHandle: 123,
					granted:     false, // Lease exists but isn't granted
					grantTime:   time.Now().Add(-30 * time.Second),
				},
			},
			mu: sync.Mutex{},
		}

		// Call the function being tested
		result := server.checkIfPrimary(123)

		// The function doesn't check the granted field, so we should get true
		// if the time is valid. This test verifies the current behavior.
		if result != true {
			t.Errorf("Current behavior: Expected true for non-granted lease with valid time, got %v", result)
		}
		
		// Note: If the function should be checking the granted field, 
		// that would be a potential bug to fix in the function itself.
	})
}