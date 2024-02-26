package index

import (
	"github.com/google/btree"
	"kv/data"
)

type Indexer interface {
	// 向索引中存储key对应的数据位置
	Put(key []byte, pos *data.LogRecordPos) bool
	// 从索引中获取key对应的数据位置
	Get(key []byte) *data.LogRecordPos
	// 从索引中删除key对应的数据位置
	Delete(key []byte) bool
}

type Item struct {
	Key []byte
	pos *data.LogRecordPos
}

func (i *Item) Less(than btree.Item) bool {
	return string(i.Key) < string(than.(*Item).Key)
}
