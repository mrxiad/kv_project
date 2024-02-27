# 笔记



## 各个类之间的关系

### 底层类


#### 数据类
```go
// 存储到文件的数据
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// 用于快速返回写入的数据的位置
type LogRecordPos struct {
	Fid    uint32 // 文件ID
	Offset uint32 // 文件偏移
}
```

#### 索引类：
_这个类操作_**_数据类_**
其中BTree是记录了每个key对应的最后一次存储value的文件位置

```go
//	接口
type Indexer interface {
    // Put 向索引中存储key对应的数据位置
    Put(key []byte, pos *data.LogRecordPos) bool
    // Get 从索引中获取key对应的数据位置
    Get(key []byte) *data.LogRecordPos
    // Delete 从索引中删除key对应的数据位置
    Delete(key []byte) bool
}

// 具体实现
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := Item{Key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(&it)
	bt.lock.Unlock()
	return true
}
```


#### IO类
```go
// 接口
type IOManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Sync() error
	Close() error
}

// 具体实现
type FileIO struct {
	fd *os.File // 文件描述符
}
```



### 次底层

#### 操作数据类
```go
// 用于执行IO操作，以及创建，保存文件相关
type DataFile struct {
	FileId    uint32        // 文件id
	WriteOff  int64         // 文件写到了哪个位置
	IoManager fio.IOManager // io 读写管理,用于操作数据读写
}
```


### 中层
#### 数据库引擎类
```go
/*
    datafile --> IO操作
    index    --> 内存操作(可以操作数据)
*/
type DB struct {
	mu         *sync.RWMutex
	activeFile *data.DataFile            // 当前活跃的数据文件, 用于写
	oldFiles   map[uint32]*data.DataFile // 旧的数据文件
	options    *Options                  // 数据库配置
	index      index.Indexer             // 内存索引
}
```







## 要点

- BTree记录了每一个key最后一次存储value的 **文件位置**，这允许快速检索而不需要遍历整个数据文件。
- 每次写入到文件的内容为（key-value-type）
- 这个版本每一次put，并不会直接`sync`，会先写到缓冲区里，这一步是IO操作

