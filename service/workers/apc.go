package workers

import (
	"github.com/arcology-network/common-lib/common"
	"github.com/arcology-network/common-lib/types"
	"github.com/arcology-network/component-lib/actor"
	"github.com/arcology-network/component-lib/storage"
	urlcommon "github.com/arcology-network/concurrenturl/v2/common"
	storageTypes "github.com/arcology-network/storage-svc/service/types"
)

type APC struct {
	storage.BasicDBOperation
}

func NewAPC() *APC {
	return &APC{}
}

func (apc *APC) PreCommit(euResults []*types.EuResult, height uint64) {
	apc.BasicDBOperation.PreCommit(euResults, height)
	keys, vals := apc.URL.Indexer().KVs()
	datas := make([][]byte, len(vals))
	encodeWorker := func(start, end, index int, args ...interface{}) {
		vals := args[0].([]interface{})[0].([]interface{})
		datas := args[0].([]interface{})[1].(*[][]byte)
		for i := start; i < end; i++ {
			(*datas)[i] = vals[i].(urlcommon.UnivalueInterface).Encode()
		}
	}
	common.ParallelWorker(len(vals), 4, encodeWorker, vals, &datas)
	apc.MsgBroker.Send(actor.MsgStateData, &storageTypes.StateData{
		Paths:  keys,
		Values: datas,
	})
}
