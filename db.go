package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/fio"
	"bitcask-go/index"
	"bitcask-go/utils"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gofrs/flock"
)

const (
	seqNoKey      = "seq.no"
	fileLockName  = "flock"
	nextFileIdKey = "nextFile-id"
)

// DB bitcask 存储引擎实例
type DB struct {
	options Options
	fileIds []int // 文件 id，只能在加载索引的时候使用，不能在其他的地方更新使用

	index           index.Indexer // 内存索引(内部有锁)
	seqNo           uint64        // 事务序列号，全局递增
	isMerging       bool          // 是否正在 merge
	seqNoFileExists bool          // 存储事务序列号的文件是否存在
	isInitial       bool          // 是否第一次初始化数据目录
	fileLock        *flock.Flock  // 文件锁保证多进程之间的互斥
	bytesWrite      uint          // 累计写了多少个字节
	reclaimSize     int64         // 标识有多少数据是无效的

	nextFileId  atomic.Int64              //下一个活跃数据文件Id编号
	mus         []*sync.RWMutex           //锁，每个文件对应一个锁
	activeFiles []*data.DataFile          // 活跃文件，activeFiles[i]为第i个数据文件
	olderFiles  map[uint32]*data.DataFile // 旧的数据文件，key 为文件 id，value 为数据文件
}

// Stat 存储引擎统计信息
type Stat struct {
	KeyNum          uint  // key 的总数量
	DataFileNum     uint  // 数据文件的数量
	ReclaimableSize int64 // 可以进行 merge 回收的数据量 字节为单位
	DiskSize        int64 // 所占用磁盘空间的大小
}

// Open 打开 bitcask 存储引擎实例
func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial bool
	// 判断数据目录是否存在，不存在需要创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err = os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 判断当前数据目录是否正在使用
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName)) //创建文件
	hold, err := fileLock.TryLock()                                     //尝试上锁

	//log.Println(hold)
	if err != nil {
		return nil, err
	}

	if !hold {
		//不可以加锁，则返回错误
		return nil, ErrDatabaseIsUsing
	}

	// 空的文件目录
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		isInitial = true
	}

	// 初始化 DB 实例结构体
	db := &DB{
		options:     options,
		mus:         make([]*sync.RWMutex, options.Slots),
		activeFiles: make([]*data.DataFile, options.Slots),
		olderFiles:  make(map[uint32]*data.DataFile),
		index:       index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial:   isInitial,
		fileLock:    fileLock,
		nextFileId:  atomic.Int64{},
	}
	for i := range db.mus {
		db.mus[i] = new(sync.RWMutex)
	}

	// 加载 merge 数据目录，替换掉本目录下的旧数据文件
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 读取数据文件，加载到db中,更新fileIds，全部作为旧数据文件
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	// B+ 树索引不需要从数据文件中加载索引,其他的需要加载索引
	if options.IndexType != BPlusTree {
		// 从 hint 索引文件中加载索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}
		// 从数据文件中读取索引
		if err := db.loadIndexFromDataFile(); err != nil {
			return nil, err
		}
	}

	// 取出当前事务序列号
	if options.IndexType == BPlusTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
	}

	if err := db.loadNextFileId(); err != nil {
		return nil, err
	}
	return db, nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory, %v", err))
		}
	}()

	//锁全部
	for slot := range db.mus {
		db.mus[slot].RLock()
	}

	defer func() {
		for i := len(db.mus) - 1; i >= 0; i-- {
			db.mus[i].RUnlock()
		}
	}()

	// 关闭索引
	if err := db.index.Close(); err != nil {
		return err
	}

	// 保存当前事务序列号
	seqNoFile, err := data.OpenSeqNoFIle(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
		return err
	}

	// 保存 nextFileId
	nextFileIdFile, err := data.OpenNextFileIdFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record = &data.LogRecord{
		Key:   []byte(nextFileIdKey),
		Value: []byte(strconv.FormatUint(uint64(db.nextFileId.Load()), 10)),
	}
	encRecord, _ = data.EncodeLogRecord(record)
	if err := nextFileIdFile.Write(encRecord); err != nil {
		return err
	}
	if err := nextFileIdFile.Sync(); err != nil {
		return err
	}

	// 关闭活跃文件
	for _, file := range db.activeFiles {
		if file == nil {
			continue
		}
		_ = file.Sync()
		_ = file.Close()
	}

	// 关闭旧的数据文件
	for _, file := range db.olderFiles {
		_ = file.Close()
	}
	return nil
}

// SyncAll 数据的可持久化，所有活跃数据文件
func (db *DB) SyncAll() error {
	// 锁全部
	for slot := range db.mus {
		db.mus[slot].RLock()
	}

	defer func() {
		for i := len(db.mus) - 1; i >= 0; i-- {
			db.mus[i].RUnlock()
		}
	}()

	for slot := range db.activeFiles {
		if db.activeFiles[slot] == nil {
			continue
		}
		if err := db.activeFiles[slot].Sync(); err != nil {
			return err
		}
	}
	return nil
}

// Stat 返回数据库相关的统计信息
func (db *DB) Stat() *Stat {
	for slot := range db.mus {
		db.mus[slot].RLock()
	}

	defer func() {
		for i := len(db.mus) - 1; i >= 0; i-- {
			db.mus[i].Unlock()
		}
	}()

	var dataFiles = uint(len(db.olderFiles))
	if db.activeFiles[0] != nil {
		dataFiles += uint(len(db.activeFiles))
	}
	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size: %v", err))
	}
	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize, // todo
	}
}

// Backup 备份数据库，将数据文件拷贝到新的目录中
func (db *DB) Backup(dir string) error {
	//锁全部
	for slot := range db.mus {
		db.mus[slot].RLock()
	}

	defer func() {
		for i := len(db.mus) - 1; i >= 0; i-- {
			db.mus[i].RUnlock()
		}
	}()
	return utils.CopyDir(db.options.DirPath, dir, []string{fileLockName})
}

func (db *DB) hash(key []byte) uint32 {
	return utils.Hash(key) % uint32(db.options.Slots)
}

// Put 写入 key/value 数据，key 不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 序列号全局+1
	atomic.AddUint64(&db.seqNo, 1)

	// 构造 LogRecord 结构体
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, db.seqNo),
		Value: value,
		Type:  data.LogRecordTxnFinished, //注意,这个是finished,防止再写入一条数据
	}

	// hash
	slot := db.hash(key)

	// 追加写入到当前活跃文件中
	pos, err := db.appendLogRecordWithLock(slot, logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}

	return nil
}

// Delete 根据 key 删除对应的数据
func (db *DB) Delete(key []byte) error {
	// 判断 key 的有效性
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 检查 key 是否存在，如果不存在直接返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}
	// 序列号全局+1
	atomic.AddUint64(&db.seqNo, 1)

	// 构造 logRecord 信息，标识其是被删除的
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, db.seqNo),
		Type: data.LogRecordDeleted,
	}
	// hash
	slot := db.hash(key)

	// 写入到数据文件中
	pos, err := db.appendLogRecordWithLock(slot, logRecord)
	if err != nil {
		return nil
	}

	db.reclaimSize += int64(pos.Size)

	// 从内存索引中中删除对应的 key
	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

// Get 根据 key 读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// hash
	slot := db.hash(key)

	db.mus[slot].RLock()
	defer db.mus[slot].RUnlock()

	// 从内存的数据结构中取出 key 对应的索引信息
	logRecordPos := db.index.Get(key)
	// 如果 key 不在内存索引中，说明 key 不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// 从数据文件中获取value
	return db.getValueByPosition(slot, logRecordPos)
}

// ListKeys 获取数据库中的所有的 key(只操作内存索引，不需要加锁)
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	defer iterator.Close()
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx += 1
	}
	return keys
}

// Fold 获取所有的数据 并执行用户指定的操作fn
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	for slot := range db.mus {
		db.mus[slot].RLock()
	}

	//倒着defer
	defer func() {
		for i := len(db.mus) - 1; i >= 0; i-- {
			db.mus[i].RUnlock()
		}
	}()

	iterator := db.index.Iterator(false)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		key := iterator.Key()
		slot := db.hash(key)
		value, err := db.getValueByPosition(slot, iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

// getValueByPosition 根据索引信息读取数据
// 必须在上层加锁
func (db *DB) getValueByPosition(slot uint32, logRecordPos *data.LogRecordPos) ([]byte, error) {

	// 根据文件 id 找到对应的数据文件
	var dataFile *data.DataFile
	if db.activeFiles[slot] != nil && db.activeFiles[slot].FileId == logRecordPos.Fid {
		dataFile = db.activeFiles[slot]
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}
	// 数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移量读取对应的数据
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

// 追加数据到活跃文件中（上层不许加锁）
func (db *DB) appendLogRecordWithLock(slot uint32, logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mus[slot].Lock()
	defer db.mus[slot].Unlock()
	return db.appendLogRecord(slot, logRecord)
}

// 追加写入数据到活跃文件中(上层需要加锁)
func (db *DB) appendLogRecord(slot uint32, logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// 判断当前活跃文件是否存在，因为数据库在没有写入的时候是没有文件生成的
	// 如果为空则初始化文件
	if db.activeFiles[slot] == nil {
		if err := db.setActiveDataFile(slot); err != nil {
			return nil, err
		}
	}

	activeFile := db.activeFiles[slot]

	// 写入数据编码
	encRecord, size := data.EncodeLogRecord(logRecord)
	// 如果写入的数据已经达到了活跃文件的阈值，则关闭活跃文件，并打开新的文件
	if activeFile.WriteOff+size > db.options.DataFileSize {
		// 先将当前活跃文件进行持久化，保证已有的数据持久到磁盘当中
		if err := activeFile.Sync(); err != nil {
			return nil, err
		}

		// 将当前活跃文件转换为旧的数据文件
		db.olderFiles[activeFile.FileId] = activeFile

		// 打开新的数据文件
		if err := db.setActiveDataFile(slot); err != nil {
			return nil, err
		}
	}

	writeOff := activeFile.WriteOff
	if err := activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	db.bytesWrite += uint(size)
	// 根据用户配置决定是否持久化
	// 如果当前写入的字节数到达了用户的设置值
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}

	if needSync {
		if err := activeFile.Sync(); err != nil {
			return nil, err
		}
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	// 构造内存索引信息
	pos := &data.LogRecordPos{
		Fid:    activeFile.FileId,
		Offset: writeOff,
		Size:   uint32(size),
	}
	return pos, nil
}

// setActiveDataFile 为指定 slot 创建并设置新的活跃文件
// 注意：调用此方法前必须已经持有 db.mus[slot] 锁
func (db *DB) setActiveDataFile(slot uint32) error {
	newFileId := uint32(db.nextFileId.Load())
	// 打开新的数据文件，注意这里目录和 IO 模块根据你的具体实现可能有所不同
	dataFile, err := data.OpenDataFile(db.options.DirPath, newFileId, fio.StandardFIO)
	if err != nil {
		return err
	}

	// 更新活跃文件数组中的对应 slot
	db.activeFiles[slot] = dataFile
	db.nextFileId.Add(1)
	return nil
}

// 从磁盘加载数据文件
func (db *DB) loadDataFile() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	//遍历目录中的所有文件，找到所有以 .data 结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			// 0000.data 分隔
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			// 数据目录肯被损坏了
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件 id 进行排序，从小大大依次加载
	sort.Ints(fileIds)
	db.fileIds = fileIds

	// 遍历每个文件的id，打开对应的数据文件
	for _, fid := range fileIds {
		ioType := fio.StandardFIO
		if db.options.MMapAtStartup {
			ioType = fio.MemoryMap //内存映射，提高读取速度
		}
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), ioType)
		if err != nil {
			return err
		}
		db.olderFiles[uint32(fid)] = dataFile //所有都当数据文件处理
	}
	return nil
}

// 从数据文件中加载索引
// 遍历旧文件中的索引记录，并更新到内存索引中
// 从数据文件中加载索引
// 遍历旧文件中的索引记录，并更新到内存索引中
func (db *DB) loadIndexFromDataFile() error {
	// 没有文件，当前是空的数据库，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	// 查看是否发生过 merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid //最后一个文件id的下一个Id
	}

	// 更新内存索引，使用真实key来更新
	updateIndex := func(realKey []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var oldPos *data.LogRecordPos
		if typ == data.LogRecordDeleted {
			oldPos, _ = db.index.Delete(realKey)
			// 被删除的数据本身也是无效的，也要统计
			db.reclaimSize += int64(pos.Size)
		} else {
			oldPos = db.index.Put(realKey, pos)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 暂存事务数据
	transactionsRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo uint64 = nonTransactionSeqNo

	// 记录所有 key 对应的事务序列号，防止低事务序号更新高事务序号的数据
	keySeqMap := make(map[string]uint64)

	// 遍历索引文件id，处理文件中的记录
	for _, fid := range db.fileIds {
		var fileID = uint32(fid)
		// 如果比最近未参与 merge 的文件 id 更小，说明已经从 hint 文件中加载索引了
		if hasMerge && fileID < nonMergeFileId {
			continue
		}
		dataFile := db.olderFiles[fileID]

		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 构造内存索引保存的位置
			logRecordPos := &data.LogRecordPos{Fid: fileID, Offset: offset, Size: uint32(size)}

			// 解析 logRecord.Key，获得真实 key 和事务序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)

			// 如果不是事务完成的记录，暂存事务记录（使用真实 key）
			if !bytes.Equal(realKey, txnFinKey) {
				// 注意：此处不要修改 logRecord.Key，直接保留解析出来的 realKey 供后续更新索引时使用
				transactionsRecords[seqNo] = append(transactionsRecords[seqNo], &data.TransactionRecord{
					Record: logRecord,
					Pos:    logRecordPos,
				})
			}

			// 如果是事务完成记录，则可以更新内存索引
			if logRecord.Type == data.LogRecordTxnFinished {
				//获取value(本次事务所涉及的所有key数量),如果不一致则不允许更新
				num := binary.BigEndian.Uint64(logRecord.Value)
				if len(transactionsRecords[seqNo]) == int(num) {
					for _, txnRecord := range transactionsRecords[seqNo] {
						// 使用解析出的真实 key进行更新
						rk, _ := parseLogRecordKey(txnRecord.Record.Key)
						// 检查这个 key 是否已经用更大事务序号更新，如果是，则跳过
						if keySeq, ok := keySeqMap[string(rk)]; ok {
							if keySeq > seqNo {
								continue
							}
						}
						// 更新内存索引，使用真实 key进行更新
						updateIndex(rk, txnRecord.Record.Type, txnRecord.Pos)
						// 更新 key 对应的事务序列号
						keySeqMap[string(rk)] = seqNo
					}
					// 删除该事务序号下的记录
					delete(transactionsRecords, seqNo)
				}
			}

			//如果是删除记录,并且是单语句事务,则更新内存索引
			if logRecord.Type == data.LogRecordDeleted && !bytes.Equal(realKey, txnFinKey) {
				rk := realKey
				if keySeq, ok := keySeqMap[string(rk)]; ok {
					if keySeq > seqNo {
						continue
					}
				}
				// 更新内存索引，使用真实 key进行更新
				updateIndex(rk, logRecord.Type, logRecordPos)
				// 更新 key 对应的事务序列号
				keySeqMap[string(rk)] = seqNo
			}

			// 更新当前事务序号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			// 递增 offset，继续读取下一条记录
			offset += size
		}
	}

	// 更新全局事务序号
	db.seqNo = currentSeqNo
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("invalid merge ratio, must between 0 and 1")
	}
	return nil
}

func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}
	seqNoFile, err := data.OpenSeqNoFIle(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}

	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = seqNo
	db.seqNoFileExists = true

	return nil
}

func (db *DB) loadNextFileId() error {
	fileName := filepath.Join(db.options.DirPath, data.NextFileIdFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}
	nextFileIdFile, err := data.OpenNextFileIdFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := nextFileIdFile.ReadLogRecord(0)
	if err != nil {
		return err
	}

	nextFileId, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.nextFileId.Store(int64(nextFileId))
	return nil
}

// 将数据文件的 IO 类型设置为标准文件IO
func (db *DB) resetIoType(slot uint32) error {
	if db.activeFiles[slot] == nil {
		return nil
	}
	if err := db.activeFiles[slot].SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
		return err
	}

	for _, dataFile := range db.olderFiles {
		if err := dataFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
			return err
		}
	}
	return nil
}
