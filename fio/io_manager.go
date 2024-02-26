package fio

const (
	// 文件权限
	FilePerm = 0644
)

// 抽象文件读写接口
type IOManager interface {
	// 从文件的给定位置读取数据,读取到的数据在[]byte中
	Read([]byte, int64) (int, error)
	// 向文件的给定位置写入数据
	Write([]byte) (int, error)
	// 同步文件
	Sync() error
	// 关闭文件
	Close() error
}
