package index

import (
	"github.com/stretchr/testify/assert"
	"kv/data"
	"testing"
)

func TestBtree_Put(t *testing.T) {
	bt := NewBTree()
	pos := &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	}
	res := bt.Put([]byte("name"), pos)

	assert.True(t, res)

	res = bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 2,
	})

	assert.True(t, res)
}

func TestBtree_Get(t *testing.T) {
	bt := NewBTree()
	pos := &data.LogRecordPos{
		Fid:    1,
		Offset: 2,
	}
	bt.Put([]byte("name"), pos)
	res := bt.Get([]byte("name"))
	t.Log(res)
	assert.Equal(t, pos, res)

	res = bt.Get([]byte("name1"))
	assert.Nil(t, res)
}

func TestBtree_Delete(t *testing.T) {
	bt := NewBTree()
	pos := &data.LogRecordPos{
		Fid:    1,
		Offset: 2,
	}
	bt.Put([]byte("name"), pos)

	res := bt.Delete([]byte("name"))
	assert.True(t, res)

	res = bt.Delete([]byte("name1"))
	assert.False(t, res)
}
