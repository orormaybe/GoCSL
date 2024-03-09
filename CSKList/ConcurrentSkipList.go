package CSKList

import (
	"math/rand"
	"sync"
	"sync/atomic"
)

// ConcurrentSkipList 中涉及到三类锁
// deleteMutex：一把全局的读写锁；get、put 操作取读锁，实现共享；delete 操作取写锁，实现全局互斥
// keyMutex：每个 key 对应的一把互斥锁. 针对同一个 key 的 put 操作需要取 key 锁实现互斥
// nodeMutex：每个 node 对应的一把读写锁. 在 get 检索过程中，会逐层对左边界节点加读锁；put 在插入新节点过程中，会逐层对左边界节点加写锁.
type ConcurrentSkipList struct {
	// 跳表的头节点
	head *node
	// 当前跳表中存在的元素个数，通过 atomic.Int32 保证增减操作的原子性
	cap atomic.Int32
	// 只有删除操作取写锁，其他操作均取读锁
	// 通过该锁实现了删除操作的单独互斥处理
	DeleteMutex sync.RWMutex
	// 同一个 key 的 put 操作需要串行化，会基于 sync.Map 进行并发安全地存储管理
	keyToMutex sync.Map
	// 对象池，复用跳表中创建和删除的 node 结构，减轻 gc 压力
	nodesCache sync.Pool
	// 比较 node key 大小的规则，倘若 key1 < key2 返回 true，否则返回 false
	compareFunc func(key1, key2 any) bool
}

type node struct {
	key, val any
	nexts    []*node
	//nodeMutex
	sync.RWMutex
}

func NewConcurrentSkipList(compareFunc func(key1, key2 any) bool) *ConcurrentSkipList {
	return &ConcurrentSkipList{
		head: &node{
			nexts: make([]*node, 1),
		},
		nodesCache: sync.Pool{
			New: func() any {
				return &node{}
			},
		},
		compareFunc: compareFunc,
	}
}

func (c *ConcurrentSkipList) Get(key any) (any, bool) {
	c.DeleteMutex.RLock()
	defer c.DeleteMutex.RUnlock()
	cur := c.head
	for level := len(c.head.nexts) - 1; level >= 0; level-- {
		for cur.nexts[level] != nil && c.compareFunc(c.head.nexts[level].key, key) {
			cur = cur.nexts[level]
		}
		cur.RLock()
		defer cur.RUnlock()
		if cur.nexts[level] != nil && cur.nexts[level].key == key {
			return cur.nexts[level].val, true
		}
	}
	return -1, false
}

func (c *ConcurrentSkipList) Search(key any) *node {
	cur := c.head
	for level := len(c.head.nexts) - 1; level >= 0; level-- {
		for cur.nexts[level] != nil && c.compareFunc(cur.nexts[level].key, key) {
			cur = cur.nexts[level]
		}
		cur.RLock()
		defer cur.RUnlock()
		if cur.nexts[level] != nil && cur.nexts[level].key == key {
			return cur.nexts[level]
		}
	}
	return nil
}

func (c *ConcurrentSkipList) getKeyMutex(key any) *sync.Mutex {
	rawMutex, _ := c.keyToMutex.LoadOrStore(key, &sync.Mutex{})
	mutex, _ := rawMutex.(*sync.Mutex)
	return mutex
}

// 随机出新节点的最大层数索引
func (c *ConcurrentSkipList) randomLevel() int {
	var level int
	for rand.Intn(2) > 0 {
		level++
	}
	return level
}

func (c *ConcurrentSkipList) Put(key, val any) {
	c.DeleteMutex.RLock()
	defer c.DeleteMutex.RUnlock()

	keyMutex := c.getKeyMutex(key)
	keyMutex.Lock()
	defer keyMutex.Unlock()
	if tmpNode := c.Search(key); tmpNode != nil {
		tmpNode.val = val
		return
	}
	defer c.cap.Add(1)
	rLevel := c.randomLevel()
	newNode, _ := c.nodesCache.Get().(*node)
	newNode.key = key
	newNode.val = val
	newNode.nexts = make([]*node, rLevel+1)
	if rLevel > len(c.head.nexts)-1 {
		c.head.Lock()
		for rLevel > len(c.head.nexts)-1 {
			c.head.nexts = append(c.head.nexts, nil)
		}
		c.head.Unlock()
	}
	cur := c.head
	for level := len(c.head.nexts) - 1; level >= 0; level-- {
		for cur.nexts[level] != nil && c.compareFunc(c.head.nexts[level].key, key) {
			cur = cur.nexts[level]
		}
		cur.RLock()
		defer cur.RUnlock()
		newNode.nexts[level] = cur.nexts[level]
		cur.nexts[level] = newNode
	}
}

func (c *ConcurrentSkipList) Del(key any) {
	c.DeleteMutex.Lock()
	defer c.DeleteMutex.Unlock()
	var deleteNode *node
	cur := c.head
	for level := len(c.head.nexts) - 1; level >= 0; level-- {
		for cur.nexts[level] != nil && c.compareFunc(cur.nexts[level].key, key) {
			cur = cur.nexts[level]
		}
		if cur.nexts[level] == nil || (cur.nexts[level].key != key && !c.compareFunc(cur.nexts[level].key, key)) {
			continue
		}
		if deleteNode == nil {
			deleteNode = cur.nexts[level]
		}

		cur.nexts[level] = cur.nexts[level].nexts[level]

	}
	if deleteNode == nil {
		return
	}

	defer c.cap.Add(-1)
	deleteNode.nexts = nil
	c.nodesCache.Put(deleteNode)

	var dif int
	for level := len(c.head.nexts) - 1; level > 0 && c.head.nexts[level] == nil; level-- {
		dif++
	}
	c.head.nexts = c.head.nexts[:len(c.head.nexts)-dif]

}
