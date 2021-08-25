package node

import "sync"

type CacheHeight struct {
	cache map[uint64]byte
	lock  sync.RWMutex
}

func NewCacheHeight() *CacheHeight {

	return &CacheHeight{
		cache: map[uint64]byte{},
	}
}

func (c *CacheHeight) Delete(height uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.cache, height)
}
func (c *CacheHeight) Set(height uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.cache[height] = byte(1)
}
func (c *CacheHeight) Exist(height uint64) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, ok := c.cache[height]
	return ok
}
