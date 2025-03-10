package bitcask_go

type Options struct {
	// 数据库数据目录
	DirPath string

	// 数据文件的大小
	DataFileSize int64

	// 每次写入是否持久化
	SyncWrites bool

	// 累计写到多少字节后进行持久化
	BytesPerSync uint

	// 索引类型
	IndexType IndexerType

	// 启动时是否使用 MMap 加载
	MMapAtStartup bool

	// 数据文件合并的阈值
	DataFileMergeRatio float32

	//hash槽的数量
	Slots int64
}

// IteratorOptions 索引迭代器配置项
type IteratorOptions struct {
	// 遍历前缀为指定值的 Key，默认为空
	Prefix []byte

	// 是否反向遍历，默认false是正向
	Reverse bool
}

// WriteBatchOptions 批量写配置项
type WriteBatchOptions struct {
	// 一个批次当中最大的数据量
	MaxBatchSize uint

	// 提交事务的时候，是否进行可持久化
	SyncWrites bool
}

type IndexerType = int8

const (
	// Btree 索引
	Btree IndexerType = iota + 1

	// ART Adaptive Radix Tree 自适应基数树索引
	ART

	// BPlusTree B+ 树索引，将索引存储到磁盘上
	BPlusTree
)

var DefaultOptions = Options{
	DirPath:            "./tmp/bitcask-go",
	DataFileSize:       256 * 1024 * 1024, // 256MB
	SyncWrites:         false,
	BytesPerSync:       0,
	IndexType:          ART,
	MMapAtStartup:      true,
	DataFileMergeRatio: 0.5,
	Slots:              4,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchSize: 10000,
	SyncWrites:   true,
}
