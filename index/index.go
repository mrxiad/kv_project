package index

import (
	"github.com/google/btree"
	"kv/data"
)

// Indexer 内存索引，用于根据key找到文件记录
type Indexer interface {
	// Put 向索引中存储key对应的数据位置
	Put(key []byte, pos *data.LogRecordPos) bool
	// Get 从索引中获取key对应的数据位置
	Get(key []byte) *data.LogRecordPos
	// Delete 从索引中删除key对应的数据位置
	Delete(key []byte) bool
}

type IndexType = int8

const (
	// BTreeIndex BTree 索引
	BTreeIndex IndexType = iota
)

func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case BTreeIndex:
		return NewBTree()
	default:
		panic("unknown index type")
	}
}

// Item :BTree 的元素
type Item struct {
	Key []byte
	pos *data.LogRecordPos
}

func (i *Item) Less(than btree.Item) bool {
	return string(i.Key) < string(than.(*Item).Key)
}
