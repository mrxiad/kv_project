package kv

import (
	"kv/data"
	"kv/index"
	"sync"
)

/*
datafile --> IO操作
index    --> 内存操作(可以操作数据)
*/
type DB struct {
	mu         *sync.RWMutex
	activeFile *data.DataFile            // 当前活跃的数据文件, 用于写
	oldFiles   map[uint32]*data.DataFile // 旧的数据文件
	options    *Options                  // 数据库配置
	index      index.Indexer             // 内存索引
}

// Put 向数据库中存储key-value,key不可以为空
func (db *DB) Put(key []byte, value []byte) error {
	// 检查key是否为空
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造LogRecord
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加LogRecord到数据文件中
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.index.Put(key, pos); ok != true {
		return ErrIndexUpdateFailed
	}
	return nil
}

// Get  获取key的value
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	//找到内存索引信息
	logRecordPos := db.index.Get(key)
	//如果value不存在内存索引中，说明key不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	//根据文件id找到文件
	var dataFile *data.DataFile
	if logRecordPos.Fid == db.activeFile.FileId {
		dataFile = db.activeFile
	} else {
		dataFile = db.oldFiles[logRecordPos.Fid]
	}

	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	//此时找到了数据文件,需要读取数据
	logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	//如果是被删除的数据
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

// appendLogRecord 追加LogRecord到数据文件中,返回内存索引，用于快速返回写入的数据的位置
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断当前活跃文件是否存在，不存在的话初始化
	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	// 对要增加的数据进行编码
	encodedLogRecord, length := data.EncodeLogRecord(logRecord)

	// 准备写入数据
	//如果当前文件的写入位置加上要写入的数据长度大于文件的最大长度，那么就需要切换文件
	if db.activeFile.WriteOff+length > db.options.DataFileSize {
		//先持久化当前文件
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		//当前文件切换到旧文件
		db.oldFiles[db.activeFile.FileId] = db.activeFile
		//打开新的数据文件
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	//执行数据写入操作
	writeOff := db.activeFile.WriteOff //当前文件的写入位置
	if err := db.activeFile.Write(encodedLogRecord); err != nil {
		return nil, err
	}

	//根据用户配置决定是否持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	//更新当前文件的写入位置
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: uint32(writeOff),
	}

	db.activeFile.WriteOff += length //更新当前文件的写入位置，当作下一次写入的起始位置
	return pos, nil
}

// 设置当前活跃数据文件
// 访问此方法必须持有互斥锁
func (db *DB) setActiveFile() error {
	var initialFileID uint32 = 0 // 初始文件ID
	if db.activeFile != nil {
		initialFileID = db.activeFile.FileId + 1
	}
	// 打开数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileID)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}
