package node

import (
	"sync"

	"github.com/arcology-network/common-lib/intl/common"
)

type CacheCode struct {
	cache map[uint64]*map[common.Address][]byte
	lock  sync.RWMutex
}

func NewCacheCode() *CacheCode {

	return &CacheCode{
		cache: map[uint64]*map[common.Address][]byte{},
	}

}
func (c *CacheCode) Empty(height uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.cache[height] = &map[common.Address][]byte{}
}

func (c *CacheCode) Put(height uint64, codes *map[common.Address][]byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.cache[height] = codes

	return nil
}

func (c *CacheCode) Get(height uint64) *map[common.Address][]byte {
	c.lock.Lock()
	defer c.lock.Unlock()

	codes := c.cache[height]

	return codes
}

func (c *CacheCode) Remove(height uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.cache, height)

	return
}

func (c *CacheCode) IsExist(height uint64) bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.cache[height]; ok {
		return true
	}
	return false
}
