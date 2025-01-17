package utils

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// DirSize 获取一个目录的大小
func DirSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// AvailableDiskSize 获取当前工作目录所在挂载点的剩余可用空间大小（字节）
func AvailableDiskSize() (uint64, error) {
	// 获取当前工作目录
	wd, err := os.Getwd()
	if err != nil {
		return 0, err
	}

	// 获取当前工作目录对应的挂载点路径
	// 简单处理：假设使用当前工作目录即可
	// 如果需要更精确的挂载点，可以用 filepath.VolumeName 或其他方式来获取
	rootPath := wd

	var stat syscall.Statfs_t
	// 调用 Statfs 来获取文件系统统计信息
	if err := syscall.Statfs(rootPath, &stat); err != nil {
		return 0, err
	}

	// 计算可用空间
	// 注意：stat.Bavail 表示非特权用户可用块数，
	// stat.Bsize 表示每个块的大小
	available := stat.Bavail * uint64(stat.Bsize)
	return available, nil
}

// CopyDir 拷贝数据目录
func CopyDir(src, dest string, exclude []string) error {
	// 目标目录不存在则创建
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.MkdirAll(dest, os.ModePerm); err != nil {
			return err
		}
	}

	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		fileName := strings.Replace(path, src, "", 1)
		if fileName == "" {
			return nil
		}

		for _, e := range exclude {
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}

		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dest, fileName), info.Mode())
		}

		data, err := os.ReadFile(filepath.Join(src, fileName))
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dest, fileName), data, info.Mode())
	})
}
