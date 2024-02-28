package fio

const (
	// DataFilePerm 文件权限
	DataFilePerm = 0644
)

// IOManager 抽象文件读写接口
type IOManager interface {
	/*
		此方法从文件的给定位置读取数据。
		它接收一个 byte 切片（用于存储读取的数据）和一个表示从文件开头计算的偏移量的 int64 类型的参数
		方法返回读取的字节数和可能发生的错误。
	*/
	Read([]byte, int64) (int, error)
	/*
		此方法向文件写入数据。
		它接收一个 byte 切片作为要写入的数据
		并返回写入的字节数和可能发生的错误
	*/
	Write([]byte) (int, error)
	/*
		同步方法确保所有缓冲的文件操作（如写入）都被实际写入底层存储设备。
		这是通过将文件的当前状态同步到磁盘来实现的，以确保数据的持久性。
		如果成功，返回 nil；否则返回错误。
	*/
	Sync() error
	/*
		关闭文件。调用此方法后，文件描述符被关闭，相关资源被释放。如果关闭成功，返回 nil；否则返回错误
	*/
	Close() error

	/*
		获取文件大小
	*/
	Size() (int64, error)
}

// NewIOManager 初始化IO方法
func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
