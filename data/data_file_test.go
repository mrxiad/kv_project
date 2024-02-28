package data

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	t.Log(os.TempDir())
}

func TestDataFile_Write(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 1)

	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	err = dataFile1.Write([]byte("hello world"))
	err = dataFile1.Write([]byte("hhhhh"))
	err = dataFile1.Sync()
	assert.Nil(t, err)

	//读取文件全部内容
	b := make([]byte, 100)
	n, err := dataFile1.IoManager.Read(b, 0)
	t.Log(string(b[:n]))
	assert.Nil(t, err)
}
