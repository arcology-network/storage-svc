package node

import (
	"math"
	"sync"

	"github.com/HPISTechnologies/common-lib/intl/common"
	"github.com/HPISTechnologies/common-lib/intl/types"
)

type Cache struct {
	cache map[uint64]*[]*types.AccountInfo
	//cacheHeight map[uint64]byte
	lock sync.RWMutex

	lanes int
}

func NewCache(lanes int) *Cache {

	return &Cache{
		cache: map[uint64]*[]*types.AccountInfo{},
		//cacheHeight: map[uint64]byte{},
		lanes: lanes,
	}

}
func (c *Cache) Empty(height uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.cache[height] = &[]*types.AccountInfo{}
}

func (c *Cache) SetStorageHash(height uint64, hash common.Hash) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	ins, ok := c.cache[height]
	if ok {
		c.setHash(ins, hash)
	}
	return nil
}

func (c *Cache) setHash(infs *[]*types.AccountInfo, hash common.Hash) {
	txLen := len(*infs)

	threads := c.lanes

	var step = int(math.Max(float64(txLen/threads), float64(txLen%threads)))
	wg := sync.WaitGroup{}

	for counter := 0; counter <= threads; counter++ {

		begin := counter * step
		end := int(math.Min(float64(begin+step), float64(txLen)))
		wg.Add(1)
		go func(begin int, end int) {

			for i := begin; i < end; i++ {
				(*infs)[i].Account.Root = hash
			}
			wg.Done()
		}(begin, end)

		if txLen == end {
			break
		}
	}
	wg.Wait()
}

func (c *Cache) Put(height uint64, accts *[]*types.AccountInfo) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.cache[height] = accts

	return nil
}

func (c *Cache) Get(height uint64) *[]*types.AccountInfo {
	c.lock.Lock()
	defer c.lock.Unlock()

	accts := c.cache[height]

	return accts
}

func (c *Cache) Remove(height uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.cache, height)

	return
}

func (c *Cache) IsExist(height uint64) bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.cache[height]; ok {
		return true
	}
	return false
}
