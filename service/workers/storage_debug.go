package workers

import (
	"github.com/arcology-network/common-lib/types"
	"github.com/arcology-network/component-lib/actor"
	storageTypes "github.com/arcology-network/storage-svc/service/types"
)

type StorageDebug struct {
	actor.WorkerThread
	datastore storageTypes.DB
}

//return a Subscriber struct
func NewStorageDebug(concurrency int, groupid string, ds storageTypes.DB) *StorageDebug {
	s := StorageDebug{}
	s.Set(concurrency, groupid)
	s.datastore = ds
	return &s
}

func (*StorageDebug) OnStart() {}
func (*StorageDebug) Stop()    {}

func (s *StorageDebug) OnMessageArrived(msgs []*actor.Message) error {
	for _, v := range msgs {
		switch v.Name {
		case actor.MsgExecutingLogs:
			executingLogs := v.Data.(types.ExecutingLogs)
			storageTypes.SaveExecutingLog(s.datastore, executingLogs.Txhash, &executingLogs)
		}
	}
	return nil
}
