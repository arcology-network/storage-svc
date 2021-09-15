package node

import (
	"sync"

	"github.com/arcology-network/common-lib/intl/common"
	"github.com/arcology-network/common-lib/intl/types"
)

type Apc struct {
	apc  map[common.Address]*types.Account
	lock sync.RWMutex
}

func NewApc() *Apc {

	return &Apc{
		apc: map[common.Address]*types.Account{},
	}

}

func (a *Apc) Add(addr common.Address, acct *types.Account) bool {
	a.lock.Lock()
	defer a.lock.Unlock()
	exist := false

	if _, ok := a.apc[addr]; ok {
		exist = true
	}
	a.apc[addr] = acct

	return exist
}

func (a *Apc) ToAccountInfos() *[]*types.AccountInfo {
	a.lock.Lock()
	defer a.lock.Unlock()

	accs := make([]*types.AccountInfo, len(a.apc))
	idx := 0
	for k, v := range a.apc {
		accs[idx] = &types.AccountInfo{
			Address: k,
			Account: *v,
		}
		idx++
	}

	return &accs
}

func (a *Apc) Get(addr common.Address) *types.Account {
	a.lock.Lock()
	defer a.lock.Unlock()
	return a.apc[addr]
}
