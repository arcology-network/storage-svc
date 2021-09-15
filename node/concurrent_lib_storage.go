package node

import (
	"sync"

	"github.com/arcology-network/common-lib/intl/types"
)

type CacheCls struct {
	cache map[uint64]*[]*types.EuResult
	lock  sync.RWMutex
}

func NewCacheCls() *CacheCls {

	return &CacheCls{
		cache: map[uint64]*[]*types.EuResult{},
	}

}
func (c *CacheCls) Empty(height uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.cache[height] = &[]*types.EuResult{}
}

func (c *CacheCls) Put(height uint64, ers *[]*types.EuResult) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.cache[height] = ers

	return nil
}

func (c *CacheCls) Get(height uint64) *[]*types.EuResult {
	c.lock.Lock()
	defer c.lock.Unlock()

	ers := c.cache[height]

	return ers
}

func (c *CacheCls) Remove(height uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.cache, height)

	return
}

func (c *CacheCls) IsExist(height uint64) bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.cache[height]; ok {
		return true
	}
	return false
}
