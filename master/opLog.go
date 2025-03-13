package master

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type FileChunkMapping struct {
	File        string
	ChunkHandle int64
}

func (master *Master) writeToOpLog(opsToLog []FileChunkMapping) error {
	// Write each operation to the log
	for _, op := range opsToLog {
		// Format the operation as a line in the log
		// We could use JSON or another format, but for simplicity:
		logLine := fmt.Sprintf("%s:%d\n", op.File, op.ChunkHandle)

		if _, err := master.currentOpLog.WriteString(logLine); err != nil {
			return err
		}
	}
	// Ensure data is written to disk
	return master.currentOpLog.Sync()
}

func (master *Master) recover() error {
	// First try to load the latest checkpoint
	checkpoint, err := os.Open("checkpoint.chk")
	if err == nil {
		fileInfo, err := checkpoint.Stat()
		if err != nil {
			return err
		}
		fileSize := fileInfo.Size()
		
		// Read the totalMappings count (last 8 bytes of the file)
		countBuf := make([]byte, 8)
		_, err = checkpoint.ReadAt(countBuf, fileSize-8)
		if err != nil {
			return err
		}
		
		// Convert bytes to int64
		totalMappings := int(binary.LittleEndian.Uint64(countBuf))
		
		// Now read the actual mapping data
		mappingData := make([]byte, fileSize-8)
		_, err = checkpoint.ReadAt(mappingData, 0)
		if err != nil {
			return err
		}
		
		// Create a decoder for the mappings
		buf := bytes.NewBuffer(mappingData)
		decoder := gob.NewDecoder(buf)
		
		// Lock for state updates
		master.mu.Lock()
		
		// Clear existing state
		master.fileMap = make(map[string][]Chunk)
		
		// Decode each mapping
		for range totalMappings {
			var mapping FileChunkMapping
			if err := decoder.Decode(&mapping); err != nil {
				return err
			}
			
			// Add to master's state
			chunk := Chunk{
				ChunkHandle: mapping.ChunkHandle,
				// Set other properties as needed
			}
			
			if _, exists := master.fileMap[mapping.File]; !exists {
				master.fileMap[mapping.File] = []Chunk{}
			}
			master.fileMap[mapping.File] = append(master.fileMap[mapping.File], chunk)
		}
		master.mu.Unlock()
		return nil
	}

	// Then replay the operation log for mutations after the checkpoint
	// opLog, err := os.Open("opLog.log")
	// if err != nil {
	//     // If we can't open the log but loaded a checkpoint, that might be OK
	//     if checkpoint != nil {
	//         return nil
	//     }
	//     return err
	// }
	// defer opLog.Close()

	master.opLogMu.Lock()
	defer master.opLogMu.Unlock()
	// Read and apply each operation from the log
	scanner := bufio.NewScanner(master.currentOpLog)
	for scanner.Scan() {
		line := scanner.Text()
		// Parse the line to get file and chunk handle
		parts := strings.Split(line, ":")
		if len(parts) == 2 {
			file := parts[0]
			chunkHandle, _ := strconv.ParseInt(parts[1], 10, 64)

			// Apply the operation to the master's state
			master.addFileChunkMapping(file, chunkHandle)
		}
	}

	return nil
}

func (master *Master) buildCheckpoint() error {
	// Start a new goroutine to build the checkpoint without blocking mutations
	go func() {
		// First, switch to a new log file
		err := master.switchOpLog()
		if err != nil {
			// Handle error
			return
		}

		// Create a temporary checkpoint file
		tempCpFile, err := os.Create("checkpoint.tmp")
		if err != nil {
			// Handle error
			return
		}
		defer tempCpFile.Close()

		// Lock the master's state to get a consistent snapshot
		master.mu.Lock()
		fileChunks := make([]byte, 0)
		// Encode using gob
		var buf bytes.Buffer
		encoder := gob.NewEncoder(&buf)
		totalMappings := 0
		for file, chunks := range master.fileMap {
			for _, chunk := range chunks {
				fileChunk := FileChunkMapping{
					File:        file,
					ChunkHandle: chunk.ChunkHandle,
				}
				err := encoder.Encode(fileChunk)
				if err != nil {
					return
				}
				fileChunks = append(fileChunks, buf.Bytes()...)
				totalMappings++
				buf.Reset()
			}
		}

		master.mu.Unlock()

		// Write the checkpoint data to the temporary file
		bytesWritten, err := tempCpFile.Write(fileChunks)
		if err != nil {
			return
		}

		err = binary.Write(&buf, binary.LittleEndian, int64(totalMappings))
		if err != nil {
			return
		}
		_, err = tempCpFile.WriteAt(buf.Bytes(), int64(bytesWritten))
		if err != nil {
			return
		}

		// Ensure data is written to disk
		tempCpFile.Sync()

		// Rename the temporary file to the actual checkpoint file
		os.Rename("checkpoint.tmp", "checkpoint.chk")

		// Update the checkpoint sequence number or timestamp
		master.lastCheckpointTime = time.Now()
	}()

	return nil
}

// Helper function to switch to a new operation log file
func (master *Master) switchOpLog() error {
	master.opLogMu.Lock()
	defer master.opLogMu.Unlock()

	// Close the current log file
	if master.currentOpLog != nil {
		master.currentOpLog.Close()
	}

	// Rename the current log file with a timestamp
	timestamp := time.Now().UnixNano()
	os.Rename("opLog.log", fmt.Sprintf("opLog.%d.log", timestamp))

	// Create a new log file
	newLog, err := os.Create("opLog.log")
	if err != nil {
		return err
	}

	master.currentOpLog = newLog
	master.lastLogSwitchTime = time.Now()

	return nil
}

// Helper function to add a file-chunk mapping to the master's state
func (master *Master) addFileChunkMapping(file string, chunkHandle int64) {
	master.mu.Lock()
	defer master.mu.Unlock()
	master.fileMap[file] = append(master.fileMap[file], Chunk{ChunkHandle: chunkHandle})

}
