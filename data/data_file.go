package data

import "kv/fio"

// DataFile 用于执行IO操作，以及创建，保存文件相关
type DataFile struct {
	FileId    uint32        // 文件id
	WriteOff  int64         // 文件写到了哪个位置
	IoManager fio.IOManager // io 读写管理,用于操作数据读写
}

// NewDataFile 初始化数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

// 持久化当前文件
func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

func (df *DataFile) ReadLogRecord(offset uint32) (*LogRecord, error) {
	return nil, nil
}
