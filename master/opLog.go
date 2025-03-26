package master

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/involk-secure-1609/goGFS/common"
)

type Operation struct {
	Type        int
	File        string
	ChunkHandle int64
	NewName     string
}

func (master *Master) writeToOpLog(op Operation) error {
	// master.opLogMu.Lock()
	// defer master.opLogMu.Unlock()

	var logLine string
	switch op.Type {
	case common.ClientMasterWriteRequestType:
		logLine = fmt.Sprintf("%s:%s:%d:%s\n", "Create", op.File, op.ChunkHandle, op.NewName)
	case common.ClientMasterDeleteRequestType:
		logLine = fmt.Sprintf("%s:%s:%d:%s\n", "TempDelete", op.File, op.ChunkHandle, op.NewName)
	default:
		return errors.New("operation type not defined correctly")
	}

	if _, err := master.currentOpLog.WriteString(logLine); err != nil {
		return err
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
		countBuf := make([]byte, 4)
		_, err = checkpoint.ReadAt(countBuf, fileSize-4)
		if err != nil {
			return err
		}

		// Convert bytes to int64
		totalMappings := binary.LittleEndian.Uint32(countBuf)

		// Now read the actual mapping data
		checkPointData := make([]byte, fileSize-4)
		_, err = checkpoint.ReadAt(checkPointData, 0)
		if err != nil {
			return err
		}

		err = master.readCheckpoint(checkPointData, int64(totalMappings))
		return err
	}

	err = master.readOpLog()
	if err != nil {
		return err
	}
	return nil
}

func (master *Master) readCheckpoint(checkpointBytes []byte, totalMappings int64) error {
	master.mu.Lock()

	// Clear existing state
	master.fileMap = make(map[string][]Chunk)

	// Decode each mapping
	for range int(totalMappings) {
		offset := 0

		// Decode file length
		fileLen := binary.BigEndian.Uint16(checkpointBytes[offset : offset+2])
		offset += 2

		// Check if there's enough data for the file
		if len(checkpointBytes) < int(2+fileLen+2) {
			return errors.New("data too short for file content")
		}

		// Decode file string
		file := string(checkpointBytes[offset : offset+int(fileLen)])
		offset += int(fileLen)

		isDeletedFile := false
		fileSplit := strings.Split(file, "/")
		if len(fileSplit) == 3 {
			fileDeletionTime, err := time.Parse(time.DateTime, fileSplit[1])
			if err != nil {

			}
			if time.Since(fileDeletionTime) >= 72*time.Hour {
				isDeletedFile = true
			}
		}
		// Decode chunks length
		chunksLen := binary.BigEndian.Uint16(checkpointBytes[offset : offset+2])
		offset += 2

		// Check if there's enough data for the chunks
		if len(checkpointBytes) < int(2+fileLen+2+chunksLen*8) {
			return errors.New("data too short for chunks content")
		}

		// Decode chunks
		chunks := make([]Chunk, chunksLen)
		for i := range int(chunksLen) {
			chunks[i].ChunkHandle = int64(binary.BigEndian.Uint64(checkpointBytes[offset : offset+8]))
			offset += 8
		}
		if !isDeletedFile {
			master.fileMap[file] = chunks
		}
	}
	master.mu.Unlock()
	return nil
}
func (master *Master) readOpLog() error {
	// master.opLogMu.Lock()
	// defer master.opLogMu.Unlock()
	// Read and apply each operation from the log
	scanner := bufio.NewScanner(master.currentOpLog)
	for scanner.Scan() {
		line := scanner.Text()
		// Parse the line to get file and chunk handle
		parts := strings.Split(line, ":")
		if len(parts) == 4 {
			command := parts[0]
			fileName := parts[1]
			chunkHandle, _ := strconv.ParseInt(parts[2], 10, 64)
			newFileName := parts[3]

			switch command {
			case "Create":
				master.addFileChunkMapping(fileName, chunkHandle)
			case "TempDelete":
				master.tempDeleteFile(fileName, newFileName)
			default:
				return errors.New("undefined commands")
			}
		} else {
			return errors.New("undefined format of log")
		}
	}
	return nil
}
func encodeFileAndChunks(file string, chunks []Chunk) []byte {
	// Calculate the total buffer size needed
	// 2 bytes for file length + file bytes + 2 bytes for chunks length + 8 bytes per chunk
	totalSize := 2 + len(file) + 2 + len(chunks)*8
	buffer := make([]byte, totalSize)

	offset := 0

	// Encode file length as 16-bit number (2 bytes)
	fileLen := uint16(len(file))
	binary.BigEndian.PutUint16(buffer[offset:offset+2], fileLen)
	offset += 2

	// Encode file string as bytes
	copy(buffer[offset:offset+len(file)], file)
	offset += len(file)

	// Encode number of chunks as 16-bit number (2 bytes)
	chunksLen := uint16(len(chunks))
	binary.BigEndian.PutUint16(buffer[offset:offset+2], chunksLen)
	offset += 2

	// Encode each 64-bit chunk
	for _, chunk := range chunks {
		binary.BigEndian.PutUint64(buffer[offset:offset+8], uint64(chunk.ChunkHandle))
		offset += 8
	}

	return buffer
}

func (master *Master) buildCheckpoint() error {
	// Start a new goroutine to build the checkpoint without blocking mutations
	go func() {
		// Create a temporary checkpoint file
		tempCpFile, err := os.Create("checkpoint.tmp")
		if err != nil {
			// Handle error
			return
		}
		defer tempCpFile.Close()

		// Lock the master's state to get a consistent snapshot
		master.mu.Lock()

		totalMappings := 0
		fileChunks := make([]byte, 0)
		for file, chunks := range master.fileMap {
			totalMappings++
			fileBytes := encodeFileAndChunks(file, chunks)
			fileChunks = append(fileChunks, fileBytes...)
		}

		master.mu.Unlock()

		fileChunks, err = binary.Append(fileChunks, binary.LittleEndian, uint32(totalMappings))
		if err != nil {
			return
		}
		_, err = tempCpFile.WriteAt(fileChunks, 0)
		if err != nil {
			return
		}

		// Ensure data is written to disk
		err = tempCpFile.Sync()
		if err != nil {
			return
		}

		// Rename the temporary file to the actual checkpoint file
		os.Rename("checkpoint.tmp", "checkpoint.chk")

		// Update the checkpoint sequence number or timestamp
		master.LastCheckpointTime = time.Now()

		err = master.switchOpLog()
		if err != nil {
			// Handle error
			return
		}

	}()

	return nil
}

// Helper function to switch to a new operation log file 
func (master *Master) switchOpLog() error {
	// master.opLogMu.Lock()
	// defer master.opLogMu.Unlock()

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
	master.LastLogSwitchTime = time.Now()

	return nil
}
