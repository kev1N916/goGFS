package master

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/involk-secure-1609/goGFS/common"
	"github.com/involk-secure-1609/goGFS/helper"
)

type OpLogger struct {
	master       *Master
	currentOpLog *os.File
}
type Operation struct {
	Type        int
	File        string
	ChunkHandle int64
	NewName     string
}

// Has to be 5 bytes.  The value can never change, ever, anyway.
var magicText = [3]byte{'G', 'F', 'S'}

const (
	// OpLogFileName is the file name for the opLog file.
	OpLogFileName = "OPLOG"
	// OpLofRewriteName is the file name for the rewrite opLog file.
	OpLogRewriteFileName = "REWRITE-OPLOG"
)

func NewOpLogger(master *Master) (*OpLogger, error) {
	opLogger := &OpLogger{
		master: master,
	}

	opLogFile, err := os.OpenFile(OpLogFileName, os.O_RDWR, 0)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		opLogFile,err=opLogger.rewriteOpLog()
		if err!=nil{
			return nil,err
		}
	}
	opLogger.currentOpLog=opLogFile
	return opLogger, nil
}

func (opLog *OpLogger) rewriteOpLog() (*os.File, error) {
	opLogRewriteFile, err := helper.OpenTruncFile(OpLogRewriteFileName)
	if err != nil {
		return nil, err
	}
	_, err = opLogRewriteFile.Write(magicText[:])
	if err != nil {
		opLogRewriteFile.Close()
		return nil, err
	}
	if err := opLogRewriteFile.Sync(); err != nil {
		opLogRewriteFile.Close()
		return nil, err
	}

	// In Windows the files should be closed before doing a Rename.
	if err = opLogRewriteFile.Close(); err != nil {
		return nil, err
	}

	if err := os.Rename(OpLogRewriteFileName, OpLogFileName); err != nil {
		return nil, err
	}
	fp, err := helper.OpenExistingFile(OpLogFileName)
	if err != nil {
		return nil, err
	}
	if _, err := fp.Seek(0, io.SeekEnd); err != nil {
		fp.Close()
		return nil, err
	}
	if err := fp.Sync(); err != nil {
		fp.Close()
		return nil, err
	}
	return fp, err
}


func (opLogger *OpLogger) writeToOpLog(op Operation) error {
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

	if _, err := opLogger.currentOpLog.WriteString(logLine); err != nil {
		return err
	}
	// Ensure data is written to disk
	return opLogger.currentOpLog.Sync()
}

func (opLogger *OpLogger) readOpLog() error {
	
	magicBuf:=make([]byte,3)
	_,err:=opLogger.currentOpLog.Seek(0,io.SeekStart)
	if err!=nil{
		return err
	}
	_,err=opLogger.currentOpLog.Read(magicBuf)
	if err!=nil{
		return err
	}
	if(!bytes.Equal(magicBuf[0:4], magicText[:])){
		return common.ErrBadMagic
	}
	// master.opLogMu.Lock()
	// defer master.opLogMu.Unlock()
	// Read and apply each operation from the log
	scanner := bufio.NewScanner(opLogger.currentOpLog)
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
				opLogger.master.addFileChunkMapping(fileName, chunkHandle)
			case "TempDelete":
				opLogger.master.tempDeleteFile(fileName, newFileName)
			default:
				return errors.New("undefined commands")
			}
		} else {
			return errors.New("undefined format of log")
		}
	}
	return nil
}

// Helper function to switch to a new operation log file
func (opLogger *OpLogger) switchOpLog() error {
	// master.opLogMu.Lock()
	// defer master.opLogMu.Unlock()

	// Close the current log file
	if opLogger.currentOpLog != nil {
		opLogger.currentOpLog.Close()
	}

	fp,err:=opLogger.rewriteOpLog()
	if err!=nil{
		return err
	}

	opLogger.currentOpLog = fp
	opLogger.master.LastLogSwitchTime = time.Now()

	return nil
}
