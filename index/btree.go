package index

import (
	"github.com/google/btree"
	"kv/data"
	"sync"
)

// BTree 索引实现
/*
	BTree 写操作需要加锁，读操作不需要加锁
*/
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := Item{Key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(&it)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := bt.tree.Get(&Item{Key: key})
	if it == nil {
		return nil
	}
	return it.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	bt.lock.Lock()
	oldItem := bt.tree.Delete(&Item{Key: key}) // 删除成功返回删除的Item，否则返回nil
	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}
