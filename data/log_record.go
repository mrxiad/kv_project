package data

// LogrecordPos 数据内存索引，主要描述了数据在磁盘上中的位置
type LogRecordPos struct {
	Fid    uint32 // 文件ID
	Offset uint32 // 文件偏移
}
