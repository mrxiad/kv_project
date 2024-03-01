package index

import (
	"github.com/google/btree"
	"kv/data"
	"sort"
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

func (bt *BTree) Size() int {
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return bt.tree.Len()
}
func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return bt.NewBTreeIterator(bt.tree, reverse)
}

type BTreeIterator struct {
	curIndex int     //当前元素的索引
	reverse  bool    //是否是逆序
	items    []*Item //元素数组
}

// NewBTreeIterator 创建一个BTree迭代器
func (bt *BTree) NewBTreeIterator(tree *btree.BTree, reverse bool) *BTreeIterator {
	var index int
	values := make([]*Item, tree.Len()) //获取所有的元素
	saveFunc := func(item btree.Item) bool {
		values[index] = item.(*Item)
		index++
		return true
	}
	if reverse {
		tree.Descend(saveFunc)
	} else {
		tree.Ascend(saveFunc)
	}
	return &BTreeIterator{
		curIndex: 0,
		reverse:  reverse,
		items:    values,
	}
}

func (bti *BTreeIterator) Rewind() {
	bti.curIndex = 0
}

func (bti *BTreeIterator) Next() {
	bti.curIndex++
}

func (bti *BTreeIterator) Prev() {
	bti.curIndex--
}

func (bti *BTreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.curIndex = sort.Search(len(bti.items), func(i int) bool {
			return string(bti.items[i].Key) <= string(key)
		})
	} else {
		bti.curIndex = sort.Search(len(bti.items), func(i int) bool {
			return string(bti.items[i].Key) >= string(key)
		})
	}
}

func (bti *BTreeIterator) Key() []byte {
	return bti.items[bti.curIndex].Key
}

func (bti *BTreeIterator) Value() *data.LogRecordPos {
	return bti.items[bti.curIndex].pos
}

func (bti *BTreeIterator) Close() {
	bti.curIndex = -1
	bti.items = nil
}

func (bti *BTreeIterator) Valid() bool {
	if bti.curIndex < 0 || bti.curIndex >= len(bti.items) {
		return false
	}
	return true
}
