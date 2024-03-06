package index

import (
	"github.com/stretchr/testify/assert"
	"kv/data"
	"os"
	"path/filepath"
	"testing"
)

func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	_ = os.RemoveAll(path)
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPlusTree(path, false)

	pos := tree.Get([]byte("not exist"))
	assert.Nil(t, pos)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 1, Offset: 11})
	pos1 := tree.Get([]byte("aac"))
	assert.NotNil(t, pos1)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 1, Offset: 22})
	pos2 := tree.Get([]byte("aac"))
	assert.NotNil(t, pos2)
}

func TestBPlusTree_Size(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	_ = os.RemoveAll(path)
	_ = os.MkdirAll(path, os.ModePerm)
	t.Log(path)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPlusTree(path, false)
	assert.Equal(t, 0, tree.Size())
	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 1, Offset: 11})
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 12})
	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 1, Offset: 13})
	assert.Equal(t, 3, tree.Size())
}
