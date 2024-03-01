package kv

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	if err != nil {
		t.Error(err)
	}
	defer destroyDB(db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestDB_NewIterator2(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	if err != nil {
		t.Error(err)
	}
	defer destroyDB(db)

	_ = db.Put([]byte("key1"), []byte("value1"))
	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	t.Log("key: ", string(iterator.Key()))
	value, err := iterator.Value()
	assert.Nil(t, err)
	assert.Equal(t, "value1", string(value))
}

func TestDB_NewIterator3(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	if err != nil {
		t.Error(err)
	}
	defer destroyDB(db)

	_ = db.Put([]byte("key1"), []byte("value1"))
	_ = db.Put([]byte("key2"), []byte("value2"))
	_ = db.Put([]byte("k"), []byte("value3"))
	_ = db.Put([]byte("key3"), []byte("value3"))
	iterator := db.NewIterator(DefaultIteratorOptions)

	//遍历所有数据
	iterator.Rewind()
	for iterator.Valid() {
		t.Log("key: ", string(iterator.Key()))
		value, err := iterator.Value()
		assert.Nil(t, err)
		t.Log("value: ", string(value))
		iterator.Next()
	}

	//反向迭代
	iterator = db.NewIterator(IteratorOptions{Reverse: true})
	iterator.Rewind()
	for iterator.Valid() {
		t.Log("key: ", string(iterator.Key()))
		value, err := iterator.Value()
		assert.Nil(t, err)
		t.Log("value: ", string(value))
		iterator.Next()
	}

	//指定perfix
	iterator = db.NewIterator(IteratorOptions{Prefix: []byte("key")})
	iterator.Rewind()
	for iterator.Valid() {
		t.Log("key: ", string(iterator.Key()))
		value, err := iterator.Value()
		assert.Nil(t, err)
		t.Log("value: ", string(value))
		iterator.Next()
	}
}
