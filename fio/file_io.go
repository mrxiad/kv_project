package fio

import "os"

type FileIO struct {
	fd *os.File // 文件描述符
}

func NewFileIOManager(fileName string) (*FileIO, error) {
	// 打开文件(如果文件不存在则创建), 读写模式, 文件权限
	fd, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, FilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

func (fio *FileIO) Read(b []byte, off int64) (int, error) {
	return fio.fd.ReadAt(b, off) // 从文件的给定位置读取数据(off是文件中的偏移量)
}

func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIO) Close() error {
	return fio.fd.Close()
}
