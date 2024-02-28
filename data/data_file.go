package data

import (
	"fmt"
	"hash/crc32"
	"io"
	"kv/fio"
	"path/filepath"
)

const (
	DataFileSuffix = ".data"
)

// DataFile 用于执行IO操作，以及创建，保存文件相关
type DataFile struct {
	FileId    uint32        // 文件id
	WriteOff  int64         // 文件写到了哪个位置
	IoManager fio.IOManager // io 读写管理,用于操作数据读写
}

// OpenDataFile  初始化数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileSuffix)
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

// Sync 持久化当前文件
func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

// ReadLogRecord 读取offset位置的日志记录，返回日志记录和日志记录大小,(只读取一条记录）
func (df *DataFile) ReadLogRecord(offset uint32) (*LogRecord, int64, error) {
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}
	var headerByteSize int64 = MaxLogRecordSize

	// 如果文件大小小于offset+headerByteSize，说明文件已经读取完毕
	if fileSize < int64(offset)+headerByteSize {
		headerByteSize = fileSize - int64(offset)
	}

	// 读取头部信息
	headerByte, err := df.readNBytes(headerByteSize, int64(offset))
	if err != nil {
		return nil, 0, err
	}

	// 解码头部信息
	header, headerSize, _ := decodeLogRecordHeader(headerByte) //解析头部，获取头部信息和头部大小
	if header == nil {                                         // 读取到文件末尾
		return nil, 0, io.EOF
	}
	if header.KeySize == 0 || header.Crc == 0 {
		return nil, 0, io.EOF
	}

	keySize, valueSize := int64(header.KeySize), int64(header.ValueSize) // 获取key和value的大小
	var totalSize = headerSize + keySize + valueSize                     // 计算总大小

	var logRecord *LogRecord
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, int64(offset+uint32(headerSize)))
		if err != nil {
			return nil, 0, err
		}
		logRecord = &LogRecord{
			Key:   kvBuf[:keySize],
			Value: kvBuf[keySize:],
			Type:  header.Type,
		}
	}

	// 校验crc
	crc := getLogRecordSRC(logRecord, headerByte[crc32.Size:headerSize])
	if crc != header.Crc {
		return nil, 0, fmt.Errorf("crc校验失败")
	}

	return logRecord, totalSize, nil
}

// 读取指定位置的n个字节
func (df *DataFile) readNBytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := df.IoManager.Read(b, offset)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// 解码日志记录头部信息
func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64, error) {
	return nil, 0, nil
}
