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
	// Size 返回索引的大小
	Size() int
	// Iterator 返回一个新的迭代器
	Iterator(reverse bool) Iterator
}

type IndexType = int8

const (
	// BTreeIndex BTree 索引
	BTreeIndex IndexType = iota
	// ARTIndex ART 索引
	ARTIndex
	// BPTreeIndex B+Tree 索引
	BPTreeIndex
)

func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case BTreeIndex:
		return NewBTree()
	case ARTIndex:
		return NewART()
	case BPTreeIndex:
		return NewBPlusTree(dirPath, sync)
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

type Iterator interface {
	Rewind()                   //将迭代器指向最小的元素
	Next()                     //将迭代器指向下一个元素
	Seek(key []byte)           //将迭代器指向大于等于key的元素
	Key() []byte               //返回当前元素的key
	Value() *data.LogRecordPos //返回当前元素的value
	Close()                    //关闭迭代器，清空资源
	Valid() bool               //判断迭代器是否有效
}
