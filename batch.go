package bitcask_go

import (
	"bitcask-go/data"
	"encoding/binary"
	"sort"
	"sync"
	"sync/atomic"
)

const nonTransactionSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin")

// WriteBatch 原子批量写数据、保证原子性
type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord // 暂存用户写入的数据
}

// NewWriteBatch 初始化 WriteBatch
func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	if db.options.IndexType == BPlusTree && !db.seqNoFileExists && !db.isInitial {
		panic("cannot use write batch, seq no file not exists")
	}
	return &WriteBatch{
		options:       opts,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 批量写数据
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 暂存 LogRecord
	logRecord := &data.LogRecord{Key: key, Value: value}
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

	// 数据不存在直接返回
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// 暂存 LogRecord
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Commit 提交事务 将暂存的数据写到数据文件，并更新内存索引
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 不存在缓存的数据 直接返回
	if len(wb.pendingWrites) == 0 {
		return nil
	}

	// 数据量过大 返回错误
	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchSize {
		return ErrExceedMaxBatchNum
	}

	slotsIdMap := make(map[uint32]struct{})
	for key := range wb.pendingWrites {
		slot := wb.db.hash([]byte(key))
		slotsIdMap[slot] = struct{}{}
	}

	//sort
	slots := make([]uint32, 0, len(slotsIdMap))
	for slot := range slotsIdMap {
		slots = append(slots, slot)
	}
	sort.Slice(slots, func(i, j int) bool {
		return slots[i] < slots[j]
	})

	// 加锁保证事务提交的串行化
	for _, slot := range slots {
		wb.db.mus[slot].Lock()
	}
	// 倒着释放锁
	defer func() {
		for i := len(slots) - 1; i >= 0; i-- {
			wb.db.mus[slots[i]].Unlock()
		}
	}()

	// 获取当前最新的事务序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	positions := make(map[string]*data.LogRecordPos)

	// 开始去写数据
	for _, record := range wb.pendingWrites {
		slot := wb.db.hash(record.Key)
		logRecordPos, err := wb.db.appendLogRecord(slot, &data.LogRecord{
			Key:   logRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = logRecordPos
	}

	// 写一条标识事务完成的数据,并且记录Num
	num := make([]byte, 8)
	binary.BigEndian.PutUint64(num, uint64(len(wb.pendingWrites)))

	finishedRecord := &data.LogRecord{
		Key:   txnFinKey,
		Value: num,
		Type:  data.LogRecordTxnFinished,
	}

	//找到最大fileId,写入事务完成的记录
	var maxFileId uint32 = 0
	var slotToWrite uint32
	for _, slot := range slots {
		if wb.db.activeFiles[slot].FileId > maxFileId {
			maxFileId = wb.db.activeFiles[slot].FileId
			slotToWrite = slot
		}
	}

	if _, err := wb.db.appendLogRecord(slotToWrite, finishedRecord); err != nil {
		return err
	} //添加一条记录标识事务完成

	// 根据配置去进行持久化
	if wb.options.SyncWrites {
		for _, slot := range slots {
			if err := wb.db.activeFiles[slot].Sync(); err != nil {
				return err
			}
		}
	}

	// 更新对应的内存索引(更新前保证数据写入日志文件成功)
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		var oldPos *data.LogRecordPos
		if record.Type == data.LogRecordNormal {
			oldPos = wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			oldPos, _ = wb.db.index.Delete(record.Key)
		}
		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 清空暂存的数据
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// key + Seq Number 编码
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

// 解析 LogRecord 的 Key，获取实际的 key 和事务序列号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
