package master

import (
	"bytes"
	"os"
	"testing"

	"github.com/involk-secure-1609/goGFS/common"
	"github.com/stretchr/testify/assert"
)

// Tests the initialization of the opLogger when OPLOG.opLog does not exist
func TestNewOpLog1(t *testing.T) {
	master := &Master{}
	opLogger, err := NewOpLogger(master)
	defer func() {
		// Close file if it's open
		if opLogger != nil && opLogger.currentOpLog != nil {
			opLogger.currentOpLog.Close()
		}

		// Try to remove the file
		err := os.Remove(OpLogFileName)
		if err != nil {
			t.Logf("Failed to remove file %s: %v", OpLogFileName, err)
		}
	}()
	assert.NotEqual(t, nil, opLogger)
	assert.Equal(t, nil, err)
}


// TestNewOpLog2 tests the initialization of the opLogger when OPLOG.opLog already exists
func TestNewOpLog2(t *testing.T) {
	// Ensure clean slate before test


	opLogger:=&OpLogger{}
	fp,err:=opLogger.rewriteOpLog()
	assert.Equal(t,nil,err)
	fp.Close()

	master := &Master{}
	opLogger, err = NewOpLogger(master)

	// Deferred cleanup
	defer func() {
		// Attempt to close the file if it's open
		if opLogger != nil && opLogger.currentOpLog != nil {
			closeErr := opLogger.currentOpLog.Close()
			if closeErr != nil {
				t.Logf("Failed to close file %s: %v", OpLogFileName, closeErr)
			}
		}

		// Multiple strategies to remove the file
		removeErr := os.Remove(OpLogFileName)
		if removeErr != nil {
			t.Logf("Failed to remove file %s: %v", OpLogFileName, removeErr)
		}
	}()

	assert.NotEqual(t, nil, opLogger)
	assert.Equal(t, nil, err)
}



// Tests if rewriteOpLog() writes the magicText properly
func TestRewriteOpLog(t *testing.T) {
	// Ensure clean slate before test


	opLogger:=&OpLogger{}
	fp,err:=opLogger.rewriteOpLog()
	assert.Equal(t,nil,err)
	fp.Close()

	master := &Master{}
	opLogger, err = NewOpLogger(master)

	// Deferred cleanup
	defer func() {
		// Attempt to close the file if it's open
		if opLogger != nil && opLogger.currentOpLog != nil {
			closeErr := opLogger.currentOpLog.Close()
			if closeErr != nil {
				t.Logf("Failed to close file %s: %v", OpLogFileName, closeErr)
			}
		}

		// Multiple strategies to remove the file
		removeErr := os.Remove(OpLogFileName)
		if removeErr != nil {
			t.Logf("Failed to remove file %s: %v", OpLogFileName, removeErr)
		}
	}()

	assert.NotEqual(t, nil, opLogger)
	assert.Equal(t, nil, err)

	magicBuf:=make([]byte,3)
	_,err=opLogger.currentOpLog.Read(magicBuf)
	assert.Equal(t,nil,err)
	isMagicEqual:=bytes.Equal(magicBuf[0:3], magicText[:])
	assert.Equal(t,true,isMagicEqual)
}

// Tests if switchOpLog() writes the magicText properly
func TestSwitchOpLog(t *testing.T) {
	// Ensure clean slate before test


	opLogger:=&OpLogger{}
	fp,err:=opLogger.rewriteOpLog()
	assert.Equal(t,nil,err)
	fp.Close()

	master := &Master{}
	opLogger, err = NewOpLogger(master)

	// Deferred cleanup
	defer func() {
		// Attempt to close the file if it's open
		if opLogger != nil && opLogger.currentOpLog != nil {
			closeErr := opLogger.currentOpLog.Close()
			if closeErr != nil {
				t.Logf("Failed to close file %s: %v", OpLogFileName, closeErr)
			}
		}
		removeErr := os.Remove(OpLogFileName)
		if removeErr != nil {
			t.Logf("Failed to remove file %s: %v", OpLogFileName, removeErr)
		}
	}()

	assert.NotEqual(t, nil, opLogger)
	assert.Equal(t, nil, err)

	magicBuf:=make([]byte,3)
	_,err=opLogger.currentOpLog.Read(magicBuf)
	assert.Equal(t,nil,err)
	isMagicEqual:=bytes.Equal(magicBuf[0:3], magicText[:])
	assert.Equal(t,true,isMagicEqual)

	err=opLogger.switchOpLog()
	assert.Equal(t,nil,err)
	assert.Equal(t,OpLogFileName,opLogger.currentOpLog.Name())
}


// Tests if readOpLog() writes the magicText properly
func TestReadOpLog(t *testing.T) {
	// Ensure clean slate before test


	opLogger:=&OpLogger{}
	fp,err:=opLogger.rewriteOpLog()
	assert.Equal(t,nil,err)
	fp.Close()

	master := &Master{}
	opLogger, err = NewOpLogger(master)

	// Deferred cleanup
	defer func() {
		// Attempt to close the file if it's open
		if opLogger != nil && opLogger.currentOpLog != nil {
			closeErr := opLogger.currentOpLog.Close()
			if closeErr != nil {
				t.Logf("Failed to close file %s: %v", OpLogFileName, closeErr)
			}
		}
		removeErr := os.Remove(OpLogFileName)
		if removeErr != nil {
			t.Logf("Failed to remove file %s: %v", OpLogFileName, removeErr)
		}
	}()

	assert.NotEqual(t, nil, opLogger)
	assert.Equal(t, nil, err)

	err=opLogger.readOpLog()
	assert.Equal(t,nil,err)
}


// Tests if writeToOpLog() writes to OpLog properly
func TestWriteOpLog(t *testing.T) {
	// Ensure clean slate before test


	opLogger:=&OpLogger{}
	fp,err:=opLogger.rewriteOpLog()
	assert.Equal(t,nil,err)
	fp.Close()

	master := &Master{}
	opLogger, err = NewOpLogger(master)

	// Deferred cleanup
	defer func() {
		// Attempt to close the file if it's open
		if opLogger != nil && opLogger.currentOpLog != nil {
			closeErr := opLogger.currentOpLog.Close()
			if closeErr != nil {
				t.Logf("Failed to close file %s: %v", OpLogFileName, closeErr)
			}
		}
		removeErr := os.Remove(OpLogFileName)
		if removeErr != nil {
			t.Logf("Failed to remove file %s: %v", OpLogFileName, removeErr)
		}
	}()

	assert.NotEqual(t, nil, opLogger)
	assert.Equal(t, nil, err)

	err=opLogger.readOpLog()
	assert.Equal(t,nil,err)

	op:=Operation{
		Type: common.ClientMasterWriteRequestType,
		File:"test1",
		ChunkHandle: 1,
		NewFileName: "",
	}

	err=opLogger.writeToOpLog(op)
	assert.Equal(t,nil,err)
}


// Tests if writeToOpLog() writes and reads the OpLog properly
func TestReadAndWriteOpLog1(t *testing.T) {
	// Ensure clean slate before test


	opLogger:=&OpLogger{}
	fp,err:=opLogger.rewriteOpLog()
	assert.Equal(t,nil,err)
	fp.Close()

	master := &Master{
		FileMap: make(map[string][]int64),
	}
	opLogger, err = NewOpLogger(master)

	// Deferred cleanup
	defer func() {
		// Attempt to close the file if it's open
		if opLogger != nil && opLogger.currentOpLog != nil {
			closeErr := opLogger.currentOpLog.Close()
			if closeErr != nil {
				t.Logf("Failed to close file %s: %v", OpLogFileName, closeErr)
			}
		}
		removeErr := os.Remove(OpLogFileName)
		if removeErr != nil {
			t.Logf("Failed to remove file %s: %v", OpLogFileName, removeErr)
		}
	}()

	assert.NotEqual(t, nil, opLogger)
	assert.Equal(t, nil, err)

	err=opLogger.readOpLog()
	assert.Equal(t,nil,err)

	op:=Operation{
		Type: common.ClientMasterWriteRequestType,
		File:"test1",
		ChunkHandle: 1,
		NewFileName: "",
	}

	err=opLogger.writeToOpLog(op)
	assert.Equal(t,nil,err)

	err=opLogger.readOpLog()
	assert.Equal(t,nil,err)

	chunkHandles,chunksExist:=master.FileMap[op.File]
	assert.Equal(t,true,chunksExist)
	assert.Equal(t,1,len(chunkHandles))
	assert.Equal(t,op.ChunkHandle,chunkHandles[0])
}


// Tests if writeToOpLog() writes and reads the OpLog properly
func TestReadAndWriteOpLog2(t *testing.T) {
	// Ensure clean slate before test


	opLogger:=&OpLogger{}
	fp,err:=opLogger.rewriteOpLog()
	assert.Equal(t,nil,err)
	fp.Close()

	master := &Master{
		FileMap: make(map[string][]int64),
	}
	opLogger, err = NewOpLogger(master)

	// Deferred cleanup
	defer func() {
		// Attempt to close the file if it's open
		if opLogger != nil && opLogger.currentOpLog != nil {
			closeErr := opLogger.currentOpLog.Close()
			if closeErr != nil {
				t.Logf("Failed to close file %s: %v", OpLogFileName, closeErr)
			}
		}
		removeErr := os.Remove(OpLogFileName)
		if removeErr != nil {
			t.Logf("Failed to remove file %s: %v", OpLogFileName, removeErr)
		}
	}()

	assert.NotEqual(t, nil, opLogger)
	assert.Equal(t, nil, err)

	err=opLogger.readOpLog()
	assert.Equal(t,nil,err)

	op1:=Operation{
		Type: common.ClientMasterWriteRequestType,
		File:"test1",
		ChunkHandle: 1,
		NewFileName: "",
	}

	op2:=Operation{
		Type: common.ClientMasterDeleteRequestType,
		File:"test1",
		ChunkHandle: -1,
		NewFileName: "test1.deleted",
	}

	err=opLogger.writeToOpLog(op1)
	assert.Equal(t,nil,err)
	err=opLogger.writeToOpLog(op2)
	assert.Equal(t,nil,err)

	err=opLogger.readOpLog()
	assert.Equal(t,nil,err)

	_,chunksExist:=master.FileMap[op1.File]
	assert.Equal(t,false,chunksExist)

	deletedChunkHandles,deletedChunksExist:=master.FileMap[op2.NewFileName]
	assert.Equal(t,true,deletedChunksExist)
	assert.Equal(t,1,len(deletedChunkHandles))
	assert.Equal(t,op1.ChunkHandle,deletedChunkHandles[0])

}


