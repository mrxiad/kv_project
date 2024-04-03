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





## DB启动流程

```go
// Open 打开存储引擎实例
func Open(options Options) (*DB, error) {
	// 校验用户传入的配置项
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 对目录进行校验，不存在就要创建
	if _, err := os.Stat(options.DirPath); err != nil {
		if os.IsNotExist(err) { //如果目录不存在
			if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
				return nil, err
			}
		} else { //如果目录存在，但是有其他错误
			return nil, err
		}
	}

	// 初始化数据库
	db := &DB{
		mu:       new(sync.RWMutex),
		oldFiles: make(map[uint32]*data.DataFile),
		options:  &options,
		index:    index.NewIndexer(options.IndexType),
	}

	// 加载数据文件，用于更新oldFiles和activeFile字段
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	//	加载内存索引,用于更新index,方便下一次写入
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}
```





## dataFile读取流程

> 注意，每次读取只读取了1条记录

```go
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
```





## 编码与解析

1. 编码的时候，crc最后计算，其中的key_size和value_size是变长的
2. 解码的时候，需要先解析出头部，然后根据头部的key_size和value_size来获取key和value，而且**传入解码的字节数组只许比头部长**，不允许比头部短！！
3. CRC的计算是通过`除了前四个字节（因为要存放crc）的所有内容计算的`，校验的参数，头部必须是刚刚好





### 每种文件的存储内容格式



> 需要保证key和value都是字节数组



#### 普通数据文件（.data)

```go
// 构造LogRecord
logRecord := &data.LogRecord{
	Key:   logRecordKeyWithSeq(key, nonTranscationSeqNo),
	Value: value,
	Type:  data.LogRecordNormal,
}

//添加
db.appendLogRecordWithLock(logRecord)

//内涵
EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) 
```

#### hint文件

```go
// WriteHintRecord 写入索引信息到 hint 文件中
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encRecord, _ := EncodeLogRecord(record)
	return df.Write(encRecord)
}
```

#### merge标识结束文件

```go
//一条record，		value存储的是merge文件中，最大的文件id+1
mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKye),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}
//编码
encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
```







## 锁部分

项目中的锁只存在于`索引`和`存储引擎`中

> 注意：底层方法不加锁，上层方法加锁

### 索引BTree

```go
Delete()
Iterator()
Put()
Size()
// 但是Get方法没有用锁，因为BTree的读取是安全的？？
```



### 存储引擎DB

> 存储引擎，每次存储数据的时候，都会调用索引BTree和文件IO，所以，这部分锁只需要加到文件IO上即可
>
> 注意防止死锁

```go
appendLogRecord()   Put操作调用了这个，所以Put不加锁
Close()
Fold()
Get()
Sync()
Iterator -> Value()
```





## 事务

> 这个代码进行了修改

```go
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error)
//注意，这里面的logRecord的key是  “realKey+序列号”
```



1.  事务类型，用到了db存储引擎，所以这个类是**上层**
2. commit 操作会生产seqNo，这个一定是递增的，因为seqNo是在加锁之后才会计算
3. 事务是先写入到“日志中”，再更新内存索引，**因为索引的value是pos**





## Merge流程



### 原存储引擎

1. 将当前活跃文件持久化，变成一个旧的文件，然后打开一个新的活跃文件。
2. 然后对于所有旧的文件进行持久化（另外一个存储引擎进行操作）




### 新存储引擎

1. 新建一个目录`./tempDir-merge`(原来的目录是`./tempDir`)
2. 存在的话，删除这个目录，然后创建这个目录
3. 在新建目录下创建`hint文件，存储索引`，这里的索引是不带事务的。
4. 初始化存储引擎（`mergeDB`），这里用到了open函数，但是open函数会用到新创建的目录，可能存在问题todo
5. 遍历原来的数据文件，将与内存索引一致的写到新的data（数据日志）文件中，并且更新hint（内存索引）文件。
6. 创建标识 merge 完成的文件，并写入”当前merge文件id的后一个id“
7. 这里没有对事务id进行查看，因为如果事务不成功，则内存索引不会更新，那此时写入到磁盘的日志就不会被利用到，但是在更新内存的时候出问题，就崩了



### 总结：

创建新的目录`/tempDir-merge`,新的文件`hint`和`merge`，以及`data`



## B+tree
B+tree默认会将key存储到一个文件中，所以key/pos内存索引不会通过hint文件加载，直接从指定的文件中加载key，所以不需要加载hint和加载没有经过merge的key。





