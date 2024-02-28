package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal      LogRecordType = iota // 数据正常标记
	LogRecordDeleted                          // 数据删除标记
	LogRecordTxnFinished                      // 事务完成标记
)

// MaxLogRecordSize crc ,type ,keySize ,valueSize
//
//	4  ,  1  ,    5   ,    5
const MaxLogRecordSize = binary.MaxVarintLen32*2 + 5

// LogRecord 写入到数据文件的记录
// 之所以叫日志，是因为数据文件中的数据是追加写入的，类似日志的格式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

type LogRecordHeader struct {
	Crc       uint32        // crc校验
	Type      LogRecordType // 记录类型
	KeySize   uint32        // key大小
	ValueSize uint32        // value大小
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

// 根据logRecord和头部的计算crc
func getLogRecordSRC(logRecord *LogRecord, header []byte) uint32 {
	return 0
}
