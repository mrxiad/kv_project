package kv

import (
	"fmt"
	"io"
	"kv/data"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const MergeDirName = "-merge"
const mergeFinishedKye = "merge-finished"

// Merge 清理无效数据，
func (db *DB) Merge() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	if db.isMerging { //如果正在合并，直接返回
		db.mu.Unlock()
		return ErrMergeIsProcess
	}
	db.isMerging = true
	defer func() {
		db.isMerging = false
		db.mu.Unlock()
	}()
	//持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	//关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	//将当前活跃文件转化为旧的数据文件
	db.oldFiles[db.activeFile.FileId] = db.activeFile

	//打开新的活跃文件
	if err := db.setActiveFile(); err != nil {
		return err
	}
	nonMergeFileId := db.activeFile.FileId
	//取出所有需要合并的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.oldFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock()

	//从小到大排序
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergePath := db.getMergePath()
	//如果目录存在，删除
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	//创建目录
	if err := os.Mkdir(mergePath, os.ModePerm); err != nil {
		return err
	}

	//打开一个临时的存储引擎实例
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath //设置目录
	mergeOptions.SyncWrites = false

	mergeDB, err := Open(mergeOptions) //这里打开，会不会有问题？？因为这里的目录是mergePath
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
			logRecord, size, err := dataFile.ReadLogRecord(uint32(offset))
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 解析拿到实际的 key
			_, realKey := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey) // 从内存中获取索引----这个是最新版本的索引
			// 和内存中的索引位置进行比较。如果是最新的，就写入到新的数据文件中
			if logRecordPos != nil &&
				logRecordPos.Fid == dataFile.FileId &&
				logRecordPos.Offset == uint32(offset) {
				// 不需要使用事务序列号 清除事务标记
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTranscationSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
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
	}
	if err := mergeDB.Sync(); err != nil {
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

// "./tempDir",变为"./tempDir-merge"
func (db *DB) getMergePath() string {
	//获取合并文件的路径
	dir := path.Dir(path.Clean(db.options.DirPath))
	//获取文件名
	base := path.Base(db.options.DirPath)
	return filepath.Join(dir, base+MergeDirName)
}

// 加载 merge 数据目录
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()

	//merge目录不存在，直接返回
	if _, err := os.Stat(mergePath); err != nil {
		return nil
	}
	defer func() {
		// 删除 merge 目录
		_ = os.RemoveAll(mergePath)
	}()
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}
	//查询表示merge完成的文件，判断merge是否处理完了
	var mergeFinished bool
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
			break
		}
		if entry.Name() == data.SeqNoFileName {
			continue
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}
	//如果没有merge完成的文件，直接返回
	if !mergeFinished {
		return nil
	}
	//获取非merge文件id
	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	//删除旧的数据文件
	var fileId uint32
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := filepath.Join(db.options.DirPath, fmt.Sprintf("%09d", fileId)+data.DataFileSuffix)
		if err = os.Remove(fileName); err != nil {
			return err
		}
	}

	//将merge文件移动到数据目录
	for _, mergeFileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, mergeFileName)
		dstPath := filepath.Join(db.options.DirPath, mergeFileName)
		if err := os.Rename(srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

// 从最后一个文件中获取非merge文件id，需要传入目录
func (db *DB) getNonMergeFileId(mergePath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return 0, err
	}
	//读取merge完成文件
	mergeFinishedRecord, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	//解析出非merge文件id
	nonMergeFileId, err := strconv.Atoi(string(mergeFinishedRecord.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil
}

// 加载索引文件,这个函数更新了index
func (db *DB) loadIndexFromHintFile() error {
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)

	//查看hint文件是否存在
	if _, err := os.Stat(hintFileName); err != nil {
		return nil
	}
	//打开hint索引文件
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}
	//遍历hint文件
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(uint32(offset)) //读取一条记录
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		//解析出pos
		pos := data.DecodeLogRecordPos(logRecord.Value)
		//将key和pos存储到内存索引中
		db.index.Put(logRecord.Key, pos)
		offset += int64(size)
	}
	return nil
}
