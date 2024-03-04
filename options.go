package kv

import "kv/index"

type Options struct {
	DirPath      string          // 数据文件存储目录
	DataFileSize int64           // 数据文件大小
	SyncWrites   bool            // 每次写入是否同步
	IndexType    index.IndexType // 索引类型
}

var DefaultOptions = Options{
	DirPath:      "./tempDir",
	DataFileSize: 1 << 30, // 1G
	SyncWrites:   false,
	IndexType:    index.BTreeIndex,
}

type IteratorOptions struct {
	// 遍历前缀为指定值的 Key，默认为空
	Prefix  []byte
	Reverse bool
}

var DefaultIteratorOptions = IteratorOptions{
	// 遍历前缀为指定值的 Key，默认为空
	Prefix:  nil,
	Reverse: false,
}

type WriteBatchOptions struct {
	// 一个批次中最大的数据量
	MaxBatchNum int
	// 每一次事务提交时是否持久化
	SyncWrites bool
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 5000000,
	SyncWrites:  true,
}
