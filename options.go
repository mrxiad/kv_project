package kv

type Options struct {
	DirPath      string // 数据文件存储目录
	DataFileSize int64  // 数据文件大小
	SyncWrites   bool   // 每次写入是否同步
}
