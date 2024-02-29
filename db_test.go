package kv

import (
	"github.com/stretchr/testify/assert"
	"kv/utils"
	"os"
	"testing"
)

// 测试完成之后销毁 DB 数据目录
func destroyDB(db *DB) {
	_ = db.Close()
	err := os.RemoveAll(db.options.DirPath)
	if err != nil {
		panic(err)
	}
}

func TestDB_Put(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常 Put 一条数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.重复 Put key 相同的数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	// 3.key 为空
	err = db.Put(nil, utils.RandomValue(24))
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4.value 为空
	err = db.Put(utils.GetTestKey(22), nil)
	assert.Nil(t, err)
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Equal(t, 0, len(val3))
	assert.Nil(t, err)

	// 6.重启后再 Put 数据
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val4 := utils.RandomValue(128)
	err = db2.Put(utils.GetTestKey(5555), val4)
	assert.Nil(t, err)
	val5, err := db2.Get(utils.GetTestKey(5555))
	assert.Nil(t, err)
	assert.Equal(t, val4, val5)
	err = db2.Close()
	assert.Nil(t, err)
}

func TestDB_Get(t *testing.T) {
	opts := DefaultOptions
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//1.正常读取一条数据
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.读取一个不存在的 key
	val2, err := db.Get([]byte("some key unknown"))
	assert.Nil(t, val2)
	assert.Equal(t, ErrKeyNotFound, err)

	//3.值被重复 Put 后在读取
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val3)

	//4.值被删除后再 Get
	err = db.Put(utils.GetTestKey(33), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(33))
	assert.Nil(t, err)
	val4, err := db.Get(utils.GetTestKey(33))
	t.Log(val4)

	// 6.重启后，前面写入的数据都能拿到
	//err = db.Close()
	//assert.Nil(t, err)
	//
	//// 重启数据库
	//db2, err := Open(opts)
	//val6, err := db2.Get(utils.GetTestKey(11))
	//assert.Nil(t, err)
	//assert.NotNil(t, val6)
	//assert.Equal(t, val1, val6)
	//
	//val7, err := db2.Get(utils.GetTestKey(22))
	//assert.Nil(t, err)
	//assert.NotNil(t, val7)
	//assert.Equal(t, val3, val7)
	//
	//val8, err := db2.Get(utils.GetTestKey(33))
	//assert.Equal(t, 0, len(val8))
	//assert.Equal(t, ErrKeyNotFound, err)
	//err = db2.Close()
	//assert.Nil(t, err)
}
