package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal      LogRecordType = iota // 数据正常标记
	LogRecordDeleted                          // 数据删除标记
	LogRecordTxnFinished                      // 事务完成标记
)

// MaxLogRecordHeaderSize crc ,type ,keySize ,valueSize
//
//	4  ,  1  ,    5   ,    5
const MaxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

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

// 暂存事务的结构
type TransactionRecord struct {
	LogRecord    *LogRecord
	LogRecordPos *LogRecordPos
}

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组及长度
// +-----------+------------+-------------+--------------+-----------+---------------+
// / crc 校验值 /  type 类型  /  key size   /  value size  /    key    /     value     /
// +-----------+------------+-------------+--------------+-----------+---------------+
//
//	4字节 		 1字节	     变长（最大5）	 变长（最大5）       变长			变长
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	header := make([]byte, MaxLogRecordHeaderSize)
	//crc最后算
	//type
	header[4] = byte(logRecord.Type)
	var index int = 5
	//key size
	index += binary.PutVarint(header[index:], int64(uint64(len(logRecord.Key))))
	//value size
	index += binary.PutVarint(header[index:], int64(uint64(len(logRecord.Value))))

	//此时index位置应该放入key
	var size = int64(index) + int64(len(logRecord.Key)+len(logRecord.Value))

	encByte := make([]byte, size)

	//拷贝header部分
	copy(encByte, header[:index])

	//fmt.Println("index:", index)
	//拷贝key和value
	copy(encByte[index:], logRecord.Key)
	copy(encByte[int64(index)+int64(len(logRecord.Key)):], logRecord.Value)

	//crc
	crc := crc32.ChecksumIEEE(encByte[4:])
	//存放到encByte的前四个字节中,小端序
	//fmt.Println(crc, index) //
	binary.LittleEndian.PutUint32(encByte, crc)
	return encByte, size
}

// DecodeLogRecordHeader 解码的时候，只解码头部信息，尽管传递整个数组，也只会解析前面的部分
func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	header := &LogRecordHeader{
		Crc:  binary.LittleEndian.Uint32(buf[:4]),
		Type: buf[4],
	}
	var index = 5
	//key size && value size 解析
	keySize, n := binary.Varint(buf[index:])
	if n <= 0 {
		return nil, 0
	}
	index += n
	valueSize, m := binary.Varint(buf[index:])
	if m <= 0 {
		return nil, 0
	}
	index += m
	header.KeySize = uint32(keySize)
	header.ValueSize = uint32(valueSize)

	return header, int64(index)
}

// GetLogRecordCRC 根据logRecord和头部的计算crc，header数组不包含crc,并且是正好的
func GetLogRecordCRC(logRecord *LogRecord, header []byte) uint32 {
	if logRecord == nil {
		return 0
	}
	// 初始化长度为0的切片，容量足以存储header, key和value
	buf := make([]byte, 0, len(header)+len(logRecord.Key)+len(logRecord.Value))
	buf = append(buf, header...)
	buf = append(buf, logRecord.Key...)
	buf = append(buf, logRecord.Value...)
	crc := crc32.ChecksumIEEE(buf)
	return crc
}
