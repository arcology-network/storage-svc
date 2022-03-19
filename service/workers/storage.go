package workers

import (
	"math/big"
	"time"

	ethCommon "github.com/arcology-network/3rd-party/eth/common"
	ethTypes "github.com/arcology-network/3rd-party/eth/types"
	"github.com/arcology-network/common-lib/common"
	"github.com/arcology-network/common-lib/types"
	"github.com/arcology-network/component-lib/actor"
	"github.com/arcology-network/component-lib/log"
	storageTypes "github.com/arcology-network/storage-svc/service/types"
	"go.uber.org/zap"
)

type Storage struct {
	actor.WorkerThread
	// url       *ccurl.ConcurrentUrl
	// db        urlcommon.DatastoreInterface
	datastore storageTypes.DB
	caches    *storageTypes.LogCaches
	scanCache *storageTypes.ScanCache
}

//return a Subscriber struct
func NewStorage(concurrency int, groupid string, ds storageTypes.DB, caches *storageTypes.LogCaches, scanCache *storageTypes.ScanCache) *Storage {
	s := Storage{}
	s.Set(concurrency, groupid)
	/*
		db := cachedstorage.NewDataStore()
		s.db = db
		platform := urlcommon.NewPlatform()
		meta, _ := commutative.NewMeta(platform.Eth10())
		db.Inject(platform.Eth10(), meta)

		cdb := cachedstorage.NewConcurrentDB(db)

		s.url = ccurl.NewConcurrentUrl(cdb)
	*/
	s.caches = caches
	s.datastore = ds
	s.scanCache = scanCache
	return &s
}

func (*Storage) OnStart() {}
func (*Storage) Stop()    {}

func (s *Storage) OnMessageArrived(msgs []*actor.Message) error {
	var statedatas *storageTypes.StateData
	result := ""
	height := uint64(0)
	var receipts *[]*ethTypes.Receipt
	var block *types.MonacoBlock
	var exectime *types.StatisticalInformation
	spawnedRelations := []*types.SpawnedRelation{}
	inclusive := &types.InclusiveList{}

	for _, v := range msgs {
		switch v.Name {
		case actor.MsgStateData:
			height = v.Height
			statedatas = v.Data.(*storageTypes.StateData)
		case actor.MsgBlockCompleted:
			result = v.Data.(string)
		case actor.MsgParentInfo:
			parentinfo := v.Data.(*types.ParentInfo)
			isnil, err := s.IsNil(parentinfo, "parentinfo")
			if isnil {
				return err
			}
			storageTypes.SaveLastHash(s.datastore, parentinfo.ParentHash)
			storageTypes.SaveLastRoot(s.datastore, parentinfo.ParentRoot)
		case actor.MsgSelectedReceipts:
			receipts = v.Data.(*[]*ethTypes.Receipt)
		case actor.MsgPendingBlock:
			block = v.Data.(*types.MonacoBlock)
		case actor.MsgExecTime:
			exectime = v.Data.(*types.StatisticalInformation)
		case actor.MsgSpawnedRelations:
			spawnedRelations = v.Data.([]*types.SpawnedRelation)
		case actor.MsgConflictInclusive:
			inclusive = v.Data.(*types.InclusiveList)
		}
	}

	if actor.MsgBlockCompleted_Success == result {
		savet := time.Now()
		s.AddLog(log.LogLevel_Debug, ">>>>>>>>>>>>>>>>>>>>> storage start gather info", zap.Uint64("blockNo", height))

		if statedatas != nil {
			t := time.Now()
			keys := statedatas.Paths
			datas := statedatas.Values

			storageTypes.SetValues(s.datastore, keys, datas)
			s.AddLog(log.LogLevel_Debug, ">>>>>>>>>>>>>>>>>>>>> url commit", zap.Duration("time", time.Since(t)), zap.Int("paths", len(keys)))
		}

		if block != nil && block.Height > 0 {
			t0 := time.Now()
			storageTypes.SaveBlocks(s.datastore, block.Height, block)
			storageTypes.SaveBlockHashes(s.datastore, block)
			storageTypes.SaveBlockHashHeight(s.datastore, ethCommon.BytesToHash(block.Hash()), block.Height)
			s.AddLog(log.LogLevel_Debug, ">>>>>>>>>>>>>>>>>>>>> block save", zap.Duration("time", time.Since(t0)))

			s.scanCache.BlockReceived(block)
		}

		if receipts != nil {
			t0 := time.Now()
			relations := map[ethCommon.Hash]ethCommon.Hash{}
			for _, relation := range spawnedRelations {
				relations[relation.Txhash] = relation.SpawnedTxHash
			}

			conflictTxs := map[ethCommon.Hash]int{}
			if inclusive != nil {
				for i, hash := range inclusive.HashList {
					if !inclusive.Successful[i] {
						conflictTxs[*hash] = i
					}
				}
			}
			blockHash := block.Hash()
			failed := 0

			worker := func(start, end int, idx int, args ...interface{}) {
				relations := args[0].([]interface{})[0].(map[ethCommon.Hash]ethCommon.Hash)
				conflictTxs := args[0].([]interface{})[1].(map[ethCommon.Hash]int)
				receipts := args[0].([]interface{})[2].(*[]*ethTypes.Receipt)
				for i := start; i < end; i++ {
					txhash := (*receipts)[i].TxHash
					if spawnedHash, ok := relations[txhash]; ok {
						(*receipts)[i].SpawnedTxHash = spawnedHash
					}

					if _, ok := conflictTxs[txhash]; ok {
						(*receipts)[i].Status = 0
						(*receipts)[i].GasUsed = 0
					}

					if (*receipts)[i].Status == 0 {
						failed = failed + 1
					}

					(*receipts)[i].BlockHash = ethCommon.BytesToHash(blockHash)
					(*receipts)[i].BlockNumber = big.NewInt(int64(block.Height))
					(*receipts)[i].TransactionIndex = uint(i)

					for k := range (*receipts)[i].Logs {
						(*receipts)[i].Logs[k].BlockHash = (*receipts)[i].BlockHash
						(*receipts)[i].Logs[k].TxHash = (*receipts)[i].TxHash
						(*receipts)[i].Logs[k].TxIndex = (*receipts)[i].TransactionIndex
					}
					storageTypes.SaveReceipt(s.datastore, block.Height, txhash, (*receipts)[i])
				}
			}
			common.ParallelWorker(len(*receipts), s.Concurrency, worker, relations, conflictTxs, receipts)
			s.AddLog(log.LogLevel_Debug, ">>>>>>>>>>>>>>>>>>>>> receipt save", zap.Int("total", len(*receipts)), zap.Int("failed", failed), zap.Duration("time", time.Since(t0)))
			if len(*receipts) > 0 {
				s.caches.Add(height, *receipts)
			}
		}

		if exectime != nil && len(exectime.Key) > 0 {
			storageTypes.SaveStatisticInfos(s.datastore, height, exectime)
		}

		//last height
		storageTypes.SaveLatestHeight(s.datastore, height)
		s.AddLog(log.LogLevel_Info, "<<<<<<<<<<<<<<<<<<<<< storage gather info completed", zap.Duration("save time", time.Since(savet)), zap.Uint64("blockNo", height))

		s.MsgBroker.Send(actor.MsgLatestHeight, height)
	}

	return nil
}
