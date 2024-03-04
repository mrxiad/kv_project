package kv

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func TestDB_NewWriteBatch(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		return
	}

	_, err = db.Get([]byte("key1"))
	assert.NotNil(t, err)

	//事务提交
	err = wb.Commit()
	assert.Nil(t, err)

	val, err := db.Get([]byte("key1"))
	assert.Nil(t, err)
	t.Log(string(val))
}

func TestWriteBatch_Put(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		return
	}

	_, err = db.Get([]byte("key1"))
	assert.NotNil(t, err)

	//事务提交
	err = wb.Commit()
	assert.Nil(t, err)

	val, err := db.Get([]byte("key1"))
	assert.Nil(t, err)
	t.Log(string(val))
}

func TestWriteBatch_Delete(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		return
	}

	_, err = db.Get([]byte("key1"))
	assert.NotNil(t, err)

	//事务提交（第一次）
	err = wb.Commit()
	assert.Nil(t, err)

	val, err := db.Get([]byte("key1"))
	assert.Nil(t, err)
	t.Log(string(val))

	wb = db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Delete([]byte("key1")) //删除	key1
	if err != nil {
		return
	}

	_, err = db.Get([]byte("key1")) //获取key1，此时应该找的到
	assert.Nil(t, err)
	//事务提交
	err = wb.Commit() //（第二次）
	//打印序列号
	t.Log(db.seqNo)

	assert.Nil(t, err)
	val, err = db.Get([]byte("key1"))
	assert.NotNil(t, err) //找不到对应的key

	err = db.Close()

	//再次打开数据库
	db, err = Open(opts)
	defer destroyDB(db)
	//再次获取key1，此时应该找不到
	val, err = db.Get([]byte("key1"))
	assert.NotNil(t, err) //找不到对应的key

}

func TestWriteBatch_Commit(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	keys := db.ListKeys()

	t.Log(keys)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	//写入很多数据
	for i := 0; i < 500000; i++ {
		err = wb.Put([]byte("key"+strconv.Itoa(i)), []byte("value"))
		assert.Nil(t, err)
		if i == 100000 {
			t.Log("100000")
		}
	}
	t.Log("put done")
	time.Sleep(10 * time.Second)
	//事务提交
	err = wb.Commit()
	assert.Nil(t, err)
}
