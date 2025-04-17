package master

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
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
	Type             int
	File             string
	ChunkHandle      int64
	NewFileName      string
	OldVersionNumber int64
	NewVersionNumber int64
}

// Has to be 5 bytes.  The value can never change, ever, anyway.
var magicText = [3]byte{'G', 'F', 'S'}

const (
	// OpLogFileName is the file name for the opLog file.
	OpLogFileName = "OPLOG" + ".opLog"
	// OpLofRewriteName is the file name for the rewrite opLog file.
	OpLogRewriteFileName = "REWRITE-OPLOG" + ".opLog"
)

// tested
func NewOpLogger(master *Master) (*OpLogger, error) {
	opLog := &OpLogger{
		master: master,
	}

	log.Println("opening opLog file")
	fileName := filepath.Join(opLog.master.MasterDirectory, OpLogFileName)
	log.Println(fileName)
	opLogFile, err := os.OpenFile(fileName, os.O_RDWR, 0)
	if err != nil {
		log.Println("opLog file does not exist")

		var pathError *fs.PathError
		if !errors.As(err, &pathError) {
			log.Println("why are we reaching here")
			return nil, err
		}

		log.Println("rewriting opLog")
		opLogFile, err = opLog.rewriteOpLog()
		if err != nil {
			return nil, err
		}
	}
	opLog.currentOpLog = opLogFile

	log.Println("finished initializing opLog")
	return opLog, nil
}

// tested
func (opLog *OpLogger) rewriteOpLog() (*os.File, error) {

	log.Println("1")
	fileName := filepath.Join(opLog.master.MasterDirectory, OpLogRewriteFileName)
	// dirName:=filepath.Dir(fileName)
	// err := os.MkdirAll(dirName, 0755)
	// if err != nil {
	// 	return nil, err
	// }
	log.Println(fileName)
	opLogRewriteFile, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Println("getting stuck here", err)
		return nil, err
	}
	log.Println("2")

	_, err = opLogRewriteFile.Write(magicText[:])
	if err != nil {
		opLogRewriteFile.Close()
		return nil, err
	}
	if err := opLogRewriteFile.Sync(); err != nil {
		opLogRewriteFile.Close()
		return nil, err
	}

	if err := os.Rename(
		filepath.Join(opLog.master.MasterDirectory, OpLogRewriteFileName),
		filepath.Join(opLog.master.MasterDirectory, OpLogFileName)); err != nil {
		return nil, err
	}
	fp, err := helper.OpenExistingFile(filepath.Join(opLog.master.MasterDirectory, OpLogFileName))
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

	log.Println("finished rewriting opLog")
	return fp, err
}

// tested
func (opLogger *OpLogger) writeToOpLog(op Operation) error {
	// master.opLogMu.Lock()
	// defer master.opLogMu.Unlock()

	var logLine string
	switch op.Type {
	case common.ClientMasterWriteRequestType:
		logLine = fmt.Sprintf("%s:%s:%d:%s\n", "Create", op.File, op.ChunkHandle, op.NewFileName)
	case common.ClientMasterDeleteRequestType:
		logLine = fmt.Sprintf("%s:%s:%d:%s\n", "TempDelete", op.File, op.ChunkHandle, op.NewFileName)
	case common.MasterChunkServerIncreaseVersionNumberRequestType:
		logLine = fmt.Sprintf("%s:%s:%d:%d\n", "SetVersionNumber", op.File, op.ChunkHandle, op.NewVersionNumber)
	default:
		return errors.New("operation type not defined correctly")
	}

	if _, err := opLogger.currentOpLog.WriteString(logLine); err != nil {
		return err
	}
	// Ensure data is written to disk
	return opLogger.currentOpLog.Sync()

}

// tested
func (opLogger *OpLogger) readOpLog() error {

	magicBuf := make([]byte, 3)
	_, err := opLogger.currentOpLog.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = opLogger.currentOpLog.Read(magicBuf)
	if err != nil {
		return err
	}
	if !bytes.Equal(magicBuf[0:3], magicText[:]) {
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
			switch command {
			case "Create":
				fileName := parts[1]
				chunkHandle, _ := strconv.ParseInt(parts[2], 10, 64)
				// newFileName := parts[3]
				opLogger.master.addFileChunkMapping(fileName, chunkHandle)
			case "TempDelete":
				fileName := parts[1]
				newFileName := parts[3]
				opLogger.master.tempDeleteFile(fileName, newFileName)
			case "IncreaseVersionNumber":
				chunkHandle, _ := strconv.ParseInt(parts[2], 10, 64)
				newVersionNumber, _ := strconv.ParseInt(parts[3], 10, 64)

				opLogger.master.setChunkVersionNumber(chunkHandle, newVersionNumber)
			default:
				return errors.New("undefined commands")
			}
		} else {
			return errors.New("undefined format of log")
		}
	}
	return nil
}

// tested
// Helper function to switch to a new operation log file
func (opLogger *OpLogger) switchOpLog() error {
	// master.opLogMu.Lock()
	// defer master.opLogMu.Unlock()

	// Close the current log file
	if opLogger.currentOpLog != nil {
		opLogger.currentOpLog.Close()
	}

	fp, err := opLogger.rewriteOpLog()
	if err != nil {
		return err
	}

	opLogger.currentOpLog = fp
	opLogger.master.LastLogSwitchTime = time.Now()

	return nil
}
