package kv

import "kv/index"

type Options struct {
	DirPath      string          // 数据文件存储目录
	DataFileSize int64           // 数据文件大小
	SyncWrites   bool            // 每次写入是否同步
	IndexType    index.IndexType // 索引类型
}
