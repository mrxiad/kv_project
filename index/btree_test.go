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

func TestBtree_Iterator(t *testing.T) {
	btree := NewBTree()
	bti := btree.Iterator(false)
	assert.Equal(t, bti.Valid(), false)

	// 测试一条数据
	btree.Put([]byte("name1"), &data.LogRecordPos{Fid: 1, Offset: 1})
	bti = btree.Iterator(false)
	assert.Equal(t, bti.Valid(), true)
	assert.Equal(t, bti.Key(), []byte("name1"))
	assert.Equal(t, bti.Value(), &data.LogRecordPos{Fid: 1, Offset: 1})
	t.Log(bti.Key(), bti.Value())
	bti.Next()
	assert.Equal(t, bti.Valid(), false)

	//	测试多条数据
	btree.Put([]byte("name2"), &data.LogRecordPos{Fid: 1, Offset: 2})
	btree.Put([]byte("name3"), &data.LogRecordPos{Fid: 1, Offset: 3})
	bti = btree.Iterator(false)

	for bti.Rewind(); bti.Valid(); bti.Next() {
		t.Log(bti.Key(), bti.Value())
		assert.NotNil(t, bti.Key())
		assert.NotNil(t, bti.Value())
	}
	bti = btree.Iterator(true)
	for bti.Rewind(); bti.Valid(); bti.Next() {
		t.Log(bti.Key(), bti.Value())
		assert.NotNil(t, bti.Key())
		assert.NotNil(t, bti.Value())
	}

	// 测试seek
	bti = btree.Iterator(false)
	bti.Seek([]byte("z"))
	assert.Equal(t, bti.Valid(), false)
}
