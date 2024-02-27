package kv

import (
	"io"
	"kv/data"
	"kv/index"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

/*
DB
datafile:IO操作
index:内存操作(可以操作数据)
*/
type DB struct {
	mu         *sync.RWMutex
	activeFile *data.DataFile            // 当前活跃的数据文件, 用于写
	oldFiles   map[uint32]*data.DataFile // 旧的数据文件
	options    *Options                  // 数据库配置
	index      index.Indexer             // 内存索引
	fileIds    []int                     //	文件id，之只可以在加载索引的时候使用，不可以在其他地方用
}

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

	// 加载数据文件，用于更新oldFiles和activeFile
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	//	加载内存索引,用于更新index,方便下一次写入
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// checkOptions 判断是否配置合法
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return ErrDataFileNotFound
	}
	if options.DataFileSize <= 0 {
		return ErrOptionsInvalid
	}
	return nil
}

// loadDataFiles 加载数据文件到内存中
func (db *DB) loadDataFiles() error {
	// 读取目录下的所有文件
	files, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	// 遍历文件,找到以.data结尾的数据文件
	for _, file := range files {
		if strings.HasSuffix(file.Name(), data.DataFileSuffix) {
			splitNames := strings.Split(file.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0]) //文件ID
			//数据目录被损坏了
			if err != nil {
				return err
			}
			fileIds = append(fileIds, fileId)
		}
	}
	// 对文件id进行排序
	sort.Ints(fileIds)
	// 保存文件id
	db.fileIds = fileIds

	// 逐个打开数据文件,存储到dataFile中
	for i, fileId := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fileId))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 { //最后一个文件是活跃文件
			db.activeFile = dataFile
		} else { //其他文件是旧文件
			db.oldFiles[uint32(fileId)] = dataFile
		}
	}
	return nil
}

// loadIndexFromDataFiles 从数据文件中加载索引，所以数据文件必须顺序读取
func (db *DB) loadIndexFromDataFiles() error {
	if len(db.fileIds) == 0 {
		return nil
	}
	// 遍历所有文件id，处理文件中的内容
	for i, fId := range db.fileIds {
		// 获取dataFile
		var fileId = uint32(fId)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.oldFiles[fileId]
		}

		//对于这个dataFile，将内容一个一个读取出来
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(uint32(offset))
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 构造内存索引并保存
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: uint32(offset),
			}

			if logRecord.Type == data.LogRecordDeleted {
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, logRecordPos)
			}

			offset += size
		}

		// 更新最后一个活跃文件，方便下一次写入
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	return nil
}

// Put 向数据库中存储key-value,key不可以为空
func (db *DB) Put(key []byte, value []byte) error {
	// 检查key是否为空
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造LogRecord
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加LogRecord到数据文件中
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.index.Put(key, pos); ok != true {
		return ErrIndexUpdateFailed
	}

	return nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//在索引中查找key是否存在
	if pos := db.index.Get(key); pos != nil {
		return nil
	}

	//构造logRecord，标识其是被删除的
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}

	// 写入到数据文件中
	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}
	//更新内存索引
	db.index.Delete(key)
	return nil
}

// Get  获取key的value
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	//找到内存索引信息
	logRecordPos := db.index.Get(key)
	//如果value不存在内存索引中，说明key不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	//根据文件id找到文件
	var dataFile *data.DataFile
	if logRecordPos.Fid == db.activeFile.FileId {
		dataFile = db.activeFile
	} else {
		dataFile = db.oldFiles[logRecordPos.Fid]
	}

	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	//此时找到了数据文件,需要读取数据
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	//如果是被删除的数据
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

// appendLogRecord 追加LogRecord到数据文件中,返回内存索引，用于快速返回写入的数据的位置
// 其中对于activeFile的WriteOff进行更新
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断当前活跃文件是否存在，不存在的话初始化
	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	// 对要增加的数据进行编码
	encodedLogRecord, length := data.EncodeLogRecord(logRecord)

	// 准备写入数据
	//如果当前文件的写入位置加上要写入的数据长度大于文件的最大长度，那么就需要切换文件
	if db.activeFile.WriteOff+length > db.options.DataFileSize {
		//先持久化当前文件
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		//当前文件切换到旧文件
		db.oldFiles[db.activeFile.FileId] = db.activeFile
		//打开新的数据文件
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	//执行数据写入操作
	writeOff := db.activeFile.WriteOff //当前文件的写入位置
	if err := db.activeFile.Write(encodedLogRecord); err != nil {
		return nil, err
	}

	//根据用户配置决定是否持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	//更新当前文件的写入位置
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: uint32(writeOff),
	}

	db.activeFile.WriteOff += length //更新当前文件的写入位置，当作下一次写入的起始位置
	return pos, nil
}

// 设置当前活跃数据文件
// 访问此方法必须持有互斥锁
func (db *DB) setActiveFile() error {
	var initialFileID uint32 = 0 // 初始文件ID
	if db.activeFile != nil {
		initialFileID = db.activeFile.FileId + 1
	}
	// 打开数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileID)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}
