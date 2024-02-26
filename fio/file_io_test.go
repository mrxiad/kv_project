package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destoryFile(fileName string) {
	if os.Remove(fileName) != nil {
		panic("remove file failed")
	}
}

func TestNewFileIO(t *testing.T) {
	path := filepath.Join("./", "test.txt")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("./", "test.txt")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte("hello world"))
	assert.Nil(t, err)
	assert.Equal(t, 11, n)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("./", "test.txt")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	b := make([]byte, 5)
	n, err := fio.Read(b, 0)
	t.Log(string(b))
	assert.Nil(t, err)
	assert.Equal(t, 5, n)

	b = make([]byte, 5)
	n, err = fio.Read(b, 6)
	t.Log(string(b))
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("./", "test.txt")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}
