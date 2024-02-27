package data

type LogRecordType = byte

const (
	LogRecordNormal      LogRecordType = iota // 数据正常标记
	LogRecordDeleted                          // 数据删除标记
	LogRecordTxnFinished                      // 事务完成标记
)

// LogRecord 写入到数据文件的记录
// 之所以叫日志，是因为数据文件中的数据是追加写入的，类似日志的格式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos 数据内存索引，主要描述了数据在磁盘上中的位置
// 用于快速返回写入的数据的位置
type LogRecordPos struct {
	Fid    uint32 // 文件ID
	Offset uint32 // 文件偏移
}

// EncodeLogRecord 编码LogRecord，返回编码后的数据和数据长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}
