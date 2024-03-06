package index

import (
	"github.com/stretchr/testify/assert"
	"kv/data"
	"testing"
)

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 11})
	pos1 := art.Get([]byte("key-1"))
	assert.NotNil(t, pos1)

	// 获取不存在的数据
	pos2 := art.Get([]byte(""))
	assert.Nil(t, pos2)
	t.Log(pos2)

	// Put 已有的数据 更改其对应的 val
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 22})
	pos1 = art.Get([]byte("key-1"))
	assert.Equal(t, &data.LogRecordPos{Fid: 1, Offset: 22}, pos1)
	assert.NotNil(t, pos1)
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()
	assert.Equal(t, 0, art.Size())
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 1})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 2})
	art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 3})
	size := art.Size()
	assert.Equal(t, 3, size)
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()

	art.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 1})
	art.Put([]byte("adse"), &data.LogRecordPos{Fid: 1, Offset: 2})
	art.Put([]byte("bbde"), &data.LogRecordPos{Fid: 1, Offset: 3})
	art.Put([]byte("bade"), &data.LogRecordPos{Fid: 1, Offset: 4})

	iter := art.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
}
