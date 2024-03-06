package kv

import (
	"encoding/binary"
	"kv/data"
	"kv/index"
	"sync"
	"sync/atomic"
)

const nonTranscationSeqNo uint64 = 0

var txnFinished = []byte("finished")

type WriteBatch struct {
	mu            *sync.Mutex
	db            *DB
	options       WriteBatchOptions
	pendingWrites map[string]*data.LogRecord // 待写入的数据
}

// NewWriteBatch 创建一个新的 WriteBatch
func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	if db.options.IndexType == index.BPTreeIndex && !db.seqNoFileExists && !db.isInitial {
		panic("cannot use write batch, seq no file not exists")
	}

	return &WriteBatch{
		mu:            new(sync.Mutex),
		db:            db,
		options:       opts,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 批量写数据
func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	//暂存数据
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Delete 删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	//数据不存在直接返回
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	//暂存数据
	logRecord := &data.LogRecord{
		Key:   key,
		Value: nil,
		Type:  data.LogRecordDeleted,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Commit 提交事务,将数据写入到磁盘，更新内存索引
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	//如果没有数据
	if len(wb.pendingWrites) == 0 {
		return nil
	}

	//数据量过大
	if len(wb.pendingWrites) > wb.options.MaxBatchNum {
		return ErrBatchTooLarge
	}

	//加锁保证事务提交串行化，防止并发写入
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	//获取事务id
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	//写入数据
	positions := make(map[string]*data.LogRecordPos)
	for _, logRecord := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(logRecord.Key, seqNo),
			Value: logRecord.Value,
			Type:  logRecord.Type,
		})
		if err != nil {
			return err
		}
		positions[string(logRecord.Key)] = logRecordPos
	}

	//写入末尾标识完成的数据，带key
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinished, seqNo),
		Type: data.LogRecordTxnFinished,
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	//根据配置决定是否持久化
	if wb.options.SyncWrites {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	//更新内存索引
	for _, logRecord := range wb.pendingWrites {
		if logRecord.Type == data.LogRecordDeleted {
			wb.db.index.Delete(logRecord.Key)
		}
		if logRecord.Type == data.LogRecordNormal {
			wb.db.index.Put(logRecord.Key, positions[string(logRecord.Key)])
		}
	}
	//清空数据
	wb.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

// 将key和序列号拼接
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq, seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey, seq)
	copy(encKey[n:], key)
	return encKey
}

// 解析序列号和key
func parseLogRecordKey(key []byte) (seqNo uint64, realKey []byte) {
	seqNo, n := binary.Uvarint(key)
	return seqNo, key[n:]
}
