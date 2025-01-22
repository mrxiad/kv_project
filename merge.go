package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/utils"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKye = "merge.finished"
)

// Merge 清理无效数据、生成 Hint 文件，merge操作是不阻塞主协程
func (db *DB) Merge() error {
	for slot := range db.mus {
		db.mus[slot].RLock()
	}
	unlockAllFn := func() {
		for i := 0; i < len(db.mus); i++ {
			db.mus[i].RUnlock()
		}
	}
	// 如果 merge 正在进行中，则直接返回
	if db.isMerging {
		return ErrMergeIsProgress
	}

	// 查看可以 merge 的数据量是否达到了阈值
	totalSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		unlockAllFn()
		return err
	}

	//如果无效数据比总数据量不超过阈值
	if float32(db.reclaimSize)/float32(totalSize) < db.options.DataFileMergeRatio {
		unlockAllFn()
		return ErrMergeRatioUnreached
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 查看剩余空间容量是否可以容乃 merge 之后的数据量
	availableDiskSize, err := utils.AvailableDiskSize()
	if err != nil {
		unlockAllFn()
		return err
	}

	if uint64(totalSize-db.reclaimSize) >= availableDiskSize {
		unlockAllFn()
		return ErrNoEnoughSpaceForMerge
	}

	for slot := range db.activeFiles {
		if db.activeFiles[slot] == nil {
			continue
		}
		if err := db.activeFiles[slot].Sync(); err != nil {
			unlockAllFn()
			return err
		}
		// 将当前活跃数据文件转换为旧的数据文件
		db.olderFiles[db.activeFiles[slot].FileId] = db.activeFiles[slot]
	}

	// 取出所有需要 merge 的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	unlockAllFn() //解锁，此后可以进行写入
	if len(mergeFiles) == 0 {
		return nil
	}
	// 待 merge 的文件 从小大大排序，依次 merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	// 记录不需要参与Merge的最小文件Id
	nonMergeFileId := mergeFiles[len(mergeFiles)-1].FileId + 1

	mergePath := db.getMergePath() // 获取 merge 目录
	// 如果目录存在，说明发生过 merge 将其删除掉
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	// 新建一个 merge path 的目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	// 打开一个新的临时 bitcask 实例
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrites = false
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	// 打开 hint 文件 存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	// 遍历处理每个数据文件
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 解析拿到实际的 key
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey) // 获取pos
			// 和内存中的索引位置进行比较。如果有效则重新写入到新的数据文件中(由于事务更新是先写入到日志，再更新内存，即使挂了也没事
			// 如果内存没有写入这条key，所以下面的代码是不会执行的
			if logRecordPos != nil &&
				logRecordPos.Fid == dataFile.FileId &&
				logRecordPos.Offset == offset {
				// 不需要使用事务序列号 清除事务标记
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				logRecord.Type = data.LogRecordTxnFinished        //事务结束标志
				pos, err := mergeDB.appendLogRecord(0, logRecord) //mergeDB追加一条记录,只追加到第一个就可以
				if err != nil {
					return err
				}
				// 将当前位置索引写到 Hint 文件中去
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			// 增加 offset
			offset += size
		}
	}
	// sync 保证持久化
	if err := hintFile.Sync(); err != nil {
		return err
	} //只有这一个文件
	if err := mergeDB.activeFiles[0].Sync(); err != nil {
		return err
	}
	// 写标识 merge 完成的文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKye),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}

	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}

	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil
}

// 获取 merge 目录
func (db *DB) getMergePath() string {
	// 此处应使用 file 而非path
	// path 主要用于处理以斜杠(/)分隔的路径（Unix 风格），
	// 而 filepath 会根据运行程序的操作系统来处理路径
	//（例如，在 Windows 上使用反斜杠(\)）。如果您的代码是跨平台的
	// 建议始终使用 filepath 而不是 path。
	dir := filepath.Dir(filepath.Clean(db.options.DirPath))
	base := filepath.Base(db.options.DirPath)

	return filepath.Join(dir, base+mergeDirName)
}

// 加载 merge 数据目录
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	// merge 目录不存在的话 直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	//删除 merge 目录
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	// 读取 merge 目录下的所有文件
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 查找标识 merge 完成文件 判断 merge 是否处理完毕
	var mergeFinished bool
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName { // merge 完成文件
			mergeFinished = true
		}
		if entry.Name() == data.SeqNoFileName { // 事务序列号文件
			continue
		}
		// 文件锁目录跳过
		if entry.Name() == fileLockName {
			continue
		}
		mergeFileNames = append(mergeFileNames, entry.Name()) // 记录 merge 文件名
	}

	// 没有 merge 完成则直接返回
	if !mergeFinished {
		return nil
	}

	// 获取到下一个文件ID,因为文件是顺序编号的
	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	// 删除对应的数据文件(数据目录中以及被 merge完成的文件)
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.options.DirPath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// 将新的数据文件移动到数据目录中(merge后的文件)
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}
	return nil
}

// 获取 merge 完成的文件id的下一个
func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	// 打开 merge 完成文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	// 读取 merge 完成文件中的记录
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil
}

// 从 hint 文件中加载索引
func (db *DB) loadIndexFromHintFile() error {
	// 查看 hint 索引文件是否存在
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	//打开 hint 索引文件
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}
	// 读取文件中的索引
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// 解码 拿到实际的位置索引
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)
		offset += size
	}
	return nil
}
