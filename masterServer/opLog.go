package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type FileChunkMapping struct {
	File string
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
        // We have a checkpoint, load it into memory
        scanner := bufio.NewScanner(checkpoint)
        
        for scanner.Scan() {
            line := scanner.Text()
            // Parse the line to get file and chunk handle
            parts := strings.Split(line, ":")
            if len(parts) == 2 {
                file := parts[0]
                chunkHandle, _ := strconv.ParseInt(parts[1], 10, 64)
                
                // Add to master's in-memory state
                master.addFileChunkMapping(file, chunkHandle)
            }
        }
        
        checkpoint.Close()
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
        
        // Serialize the master's state in a B-tree like structure
        // This is a simplified example - you'd need to implement 
        // a more efficient serialization format
        for file, chunks := range master.fileMap {
            for _, chunk := range chunks {
                line := fmt.Sprintf("%s:%d\n", file, chunk)
                tempCpFile.WriteString(line)
            }
        }
        
        master.mu.Unlock()
        
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
func (master *Master) switchOpLog()  error {
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