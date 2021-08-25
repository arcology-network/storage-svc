package node

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/HPISTechnologies/common-lib/extl/cli"
	cmn "github.com/HPISTechnologies/common-lib/extl/common"
	"github.com/HPISTechnologies/common-lib/extl/log"
	"github.com/HPISTechnologies/common-lib/intl/common"
	"github.com/HPISTechnologies/common-lib/intl/kafka"
	"github.com/HPISTechnologies/common-lib/intl/types"
	"github.com/HPISTechnologies/component-lib/intl/actor"
	"github.com/HPISTechnologies/component-lib/intl/component"

	"github.com/HPISTechnologies/eth-lib/ethdb"
	"github.com/HPISTechnologies/eth-lib/rlp"
	"github.com/spf13/viper"

	"github.com/HPISTechnologies/component-lib/intl/module"
)

type Storage struct {
	log log.Logger

	rawdb ethdb.Database

	//apc *Apc
	apc *types.Apc

	cache            *Cache
	cacheHeight      *CacheHeight
	chUpdate         chan uint64
	chanFinaApcOrder chan []*types.AccountInfo //receive accts
	pdcApc           *kafka.ComSender

	height uint64
	lanes  int

	//for log
	logSvc   component.LogModule
	sourceid string

	msgexch        component.MsgExchModule
	startReceiveAs time.Time
	pdcQuery       *kafka.ComSender

	chanCode        chan map[common.Address][]byte //receive codelist
	cacheCode       *CacheCode
	cacheCodeHeight *CacheHeight //map[uint64]byte
	chCodeUpdate    chan uint64

	chanFinaErs    chan []*types.EuResult
	cacheCls       *CacheCls
	cacheClsHeight *CacheHeight //map[uint64]byte
	chClsUpdate    chan uint64

	//isSubscribe bool

	allkeys []byte
}

// stop this node
func (s *Storage) Stop() {
	s.pdcApc.Stop()
	s.msgexch.Stop()
	s.pdcQuery.Stop()
	//s.apcReady.Stop()
}

func NewService(l log.Logger) *Storage {

	rank := viper.GetString("rank")

	rootDir := viper.GetString(cli.HomeFlag)

	dbpath := os.ExpandEnv(filepath.Join(rootDir, "storage", "chaindata"+rank))

	if err := cmn.EnsureDir(dbpath, 0777); err != nil {
		cmn.PanicSanity(err.Error())
	}
	lvdb, err := ethdb.NewLDBDatabase(dbpath, 512, 512, l)
	if err != nil {
		l.Error("storage-svc init err", "err", err)
		return nil
	}

	lanes := viper.GetInt("lanes")

	return &Storage{
		log:             l,
		rawdb:           lvdb,
		lanes:           lanes,
		cache:           NewCache(lanes),
		cacheCode:       NewCacheCode(), //      *CacheCode
		cacheCls:        NewCacheCls(),
		logSvc:          component.LogModule{},
		sourceid:        "storage",
		msgexch:         component.MsgExchModule{},
		cacheHeight:     NewCacheHeight(),
		chUpdate:        make(chan uint64, 2000),
		cacheCodeHeight: NewCacheHeight(), //map[uint64]byte{},
		chCodeUpdate:    make(chan uint64, 2000),
		cacheClsHeight:  NewCacheHeight(), //map[uint64]byte{},
		chClsUpdate:     make(chan uint64, 2000),
		apc:             types.NewApc(),
		allkeys:         []byte{},
	}
}

// start StateProc node
func (s *Storage) Start() error {

	mqaddr := viper.GetString("mqaddr")

	module.StartApc(s.log)

	topicGenesis := viper.GetString("genesis-apc")

	s.pdcApc = new(kafka.ComSender)

	if err := s.pdcApc.Start(mqaddr, topicGenesis, s.lanes); err != nil {
		panic(err)
	}

	topicQuery := viper.GetString("query-storage")
	s.pdcQuery = new(kafka.ComSender)
	if err := s.pdcQuery.Start(mqaddr, topicQuery, s.lanes); err != nil {
		panic(err)
	}

	s.msgexch.Start("storage-svc", s.lanes, s.receiveMsg)
	s.log.Info("msgexch started for send and receive msgs")

	s.logSvc.Start()
	s.log.Info("logSvc started")

	apcid := viper.GetString("svcid") + "-" + viper.GetString("insid")
	module.GetApceady(s.log, apcid, &s.msgexch).NoClb()

	s.log.Info("storage start subscribe chan...")
	collector := module.GetStateCollector(s.log)
	s.chanCode = collector.SubscribeCode()
	s.chanFinaApcOrder = collector.SubscribeApcOrder()
	s.chanFinaErs = collector.SubscribeEuResult() //  chan []*types.EuResult
	go s.receiveApcFromCollector()
	go s.receiveCodeFromCollector()
	go s.receiveClsFromCollector()

	s.loadApc()

	s.initCache()

	//start update thread
	go func() {
		for {
			height := <-s.chUpdate
			s.finallyCommit(height)
		}
	}()

	go func() {
		for {
			height := <-s.chCodeUpdate
			s.finallyCommitCode(height)
		}
	}()
	go func() {
		for {
			height := <-s.chClsUpdate
			s.finallyCommitCls(height)
		}
	}()
	//s.log.Info("svc started for write accounts into storage")

	return nil
}

// get saved parenthash and roothash from db,and send to exec-svc
func (s *Storage) initCache() error {
	lstHash := s.getData(lasthash)
	if lstHash == nil {
		lstHash = common.Hash{}.Bytes()
	}

	//s.msgexch.SendMsg(component.Msg_inithash, lstHash, 0)

	lstRoot := s.getData(lastroot)
	if lstRoot == nil {
		lstRoot = common.Hash{}.Bytes()
	}

	pareninfo := types.ParentInfo{
		ParentHash: common.BytesToHash(lstHash),
		ParentRoot: common.BytesToHash(lstRoot),
	}

	s.msgexch.SendMsg(component.Msg_Parentinfo, pareninfo.ToBytes(), 0)
	return nil

}

func (s *Storage) finallyCommit(height uint64) {

	s.log.Info("start write account info ", "height", height)

	ais := s.cache.Get(height)
	if ais == nil {
		return
	}

	s.log.Info("start save account datas ", "height", height)
	begintime0 := time.Now()
	s.UpdateAccounts(ais, height)
	s.logSvc.Log(component.LogId_storage_UpdateAccts,
		component.TimeLog_Type_Elapse,
		begintime0,
		time.Now(),
		fmt.Sprintf("updates=%d", len(*ais)),
		component.LogLevel_perfermance,
		height,
		s.sourceid)
	s.setData(lastheight, common.Uint64ToBytes(height))

	s.clearCache(height)

}

func (s *Storage) finallyCommitCode(height uint64) {

	s.log.Info("start write code info ", "height", height)

	codes := s.cacheCode.Get(height)
	if codes == nil {
		return
	}

	s.log.Info("start save code datas ", "height", height)
	begintime0 := time.Now()
	s.UpdateCodes(codes, height)
	s.logSvc.Log(component.LogId_storage_UpdateAccts,
		component.TimeLog_Type_Elapse,
		begintime0,
		time.Now(),
		fmt.Sprintf("updates=%d", len(*codes)),
		component.LogLevel_perfermance,
		height,
		s.sourceid)
	s.setData(lastheight, common.Uint64ToBytes(height))

	s.clearCacheCode(height)

}
func (s *Storage) finallyCommitCls(height uint64) {

	s.log.Info("start write concurrent-lib storage info ", "height", height)

	rsts := s.cacheCls.Get(height)
	if rsts == nil {
		return
	}

	s.log.Info("start save concurrent lib storage datas ", "height", height)
	begintime0 := time.Now()
	s.UpdateCls(rsts, height)
	s.logSvc.Log(component.LogId_storage_UpdateAccts,
		component.TimeLog_Type_Elapse,
		begintime0,
		time.Now(),
		fmt.Sprintf("updates=%d", len(*rsts)),
		component.LogLevel_perfermance,
		height,
		s.sourceid)
	s.UpdatePluginStorage(rsts, height)
	s.setData(lastheight, common.Uint64ToBytes(height))

	s.clearCacheCls(height)

}
func (s *Storage) clearCacheCls(height uint64) {

	s.cacheCls.Remove(height)
	//delete(s.cacheClsHeight, height)
	s.cacheClsHeight.Delete(height)
}
func (s *Storage) nilCacheCls(height uint64) {

	s.cacheCls.Empty(height)
	//s.cacheClsHeight[height] = byte(1)
	s.cacheClsHeight.Set(height)
}
func (s *Storage) clearCache(height uint64) {

	s.cache.Remove(height)
	s.cacheHeight.Delete(height)
	//delete(s.cacheHeight, height)
}
func (s *Storage) nilCache(height uint64) {

	s.cache.Empty(height)
	s.cacheHeight.Set(height)
	//s.cacheHeight[height] = byte(1)
}
func (s *Storage) clearCacheCode(height uint64) {

	s.cacheCode.Remove(height)
	//delete(s.cacheCodeHeight, height)
	s.cacheCodeHeight.Delete(height)
}
func (s *Storage) nilCacheCode(height uint64) {

	s.cacheCode.Empty(height)
	//s.cacheCodeHeight[height] = byte(1)
	s.cacheCodeHeight.Set(height)
}

// receive pack request ,start pack
func (s *Storage) receiveMsg(msgid int64, msgtype byte, data []byte, height uint64) error {
	// if !s.msgexch.CheckHeight(height, s.height, s.log, msgtype) {
	// 	return nil
	// }
	switch msgtype {
	case component.Msg_height:
		s.height = common.BytesToUint64(data)
		//s.msgexch.SetHeight(s.height)
	case component.Msg_block_Completed:
		result := string(data)
		s.log.Info("storage received Msg_block_Completed ", "height", s.height, "result", result)
		if actor.MsgBlockCompleted_Success == result {
			if s.cache.IsExist(s.height) {
				s.chUpdate <- s.height
			} else {
				//s.cacheHeight[s.height] = byte(1)
				s.cacheHeight.Set(s.height)
			}
			if s.cacheCode.IsExist(s.height) {
				s.chCodeUpdate <- s.height
			} else {
				//s.cacheCodeHeight[s.height] = byte(1)
				s.cacheCodeHeight.Set(s.height)
			}
			if s.cacheCls.IsExist(s.height) {
				s.chClsUpdate <- s.height
			} else {
				//s.cacheClsHeight[s.height] = byte(1)
				s.cacheClsHeight.Set(s.height)
			}
		} else {
			s.clearCache(s.height)
			s.clearCacheCode(s.height)
			s.clearCacheCls(s.height)
		}
	// case component.Msg_block_successful:
	// 	s.log.Info("storage received Msg_block_successful ", "height", s.height)

	// 	if s.cache.IsExist(s.height) {
	// 		s.chUpdate <- s.height
	// 	} else {
	// 		//s.cacheHeight[s.height] = byte(1)
	// 		s.cacheHeight.Set(s.height)
	// 	}
	// 	if s.cacheCode.IsExist(s.height) {
	// 		s.chCodeUpdate <- s.height
	// 	} else {
	// 		//s.cacheCodeHeight[s.height] = byte(1)
	// 		s.cacheCodeHeight.Set(s.height)
	// 	}
	// 	if s.cacheCls.IsExist(s.height) {
	// 		s.chClsUpdate <- s.height
	// 	} else {
	// 		//s.cacheClsHeight[s.height] = byte(1)
	// 		s.cacheClsHeight.Set(s.height)
	// 	}

	// case component.Msg_block_failed:
	// 	s.log.Info("storage received Msg_block_failed ", "height", s.height)
	// 	s.clearCache(s.height)
	// 	s.clearCacheCode(s.height)
	// 	s.clearCacheCls(s.height)
	// case component.Msg_height_empty:
	// 	s.nilCache(s.height)
	// 	s.nilCacheCode(s.height)
	// 	s.nilCacheCls(s.height)
	case component.Msg_Parentinfo:
		pareninfo := types.NewParentInfo(data)

		if pareninfo == nil {
			s.log.Error(" received  Msg_Parentinfo,but data len is err", "height", s.height, "data", data)
			return errors.New(" received  Msg_Parentinfo,but data len is err")
		}
		s.log.Info(" received  Msg_Parentinfo", "height", s.height)

		s.setData(lasthash, pareninfo.ParentHash.Bytes())
		s.setData(lastroot, pareninfo.ParentRoot.Bytes())

	// case component.Msg_initroot:
	// 	s.setData(lastroot, data)

	case component.Msg_apc_storageQuery:
		s.responseQuery(msgid, data)

	}
	return nil
}

// received concurrent-lib storage result list  from state-collector
func (s *Storage) receiveClsFromCollector() {
	for rsts := range s.chanFinaErs {
		s.log.Info("received euresult from state collector", "nums", len(rsts), "height", s.height)

		// s.cacheCls.Put(s.height, &rsts)
		// if s.cacheClsHeight.Exist(s.height) {
		// 	s.chClsUpdate <- s.height
		// }

	}
}

// received code list  from state-collector
func (s *Storage) receiveCodeFromCollector() {
	for codes := range s.chanCode {
		s.log.Info("received codes from state collector", "nums", len(codes), "height", s.height)
		// s.cacheCode.Put(s.height, &codes)
		// if s.cacheCodeHeight.Exist(s.height) {
		// 	s.chCodeUpdate <- s.height
		// }

	}
}

// received ordered accounts from state-collector
func (s *Storage) receiveApcFromCollector() {
	for apcOrder := range s.chanFinaApcOrder {
		s.log.Info("received order account info  from state collector", "nums", len(apcOrder), "height", s.height)

		// s.cache.Put(s.height, &apcOrder)
		// if s.cacheHeight.Exist(s.height) {
		// 	s.chUpdate <- s.height
		// }

	}
}

//response query ,return result
func (s *Storage) responseQuery(msgid int64, data []byte) error {
	queryType := data[0]
	switch queryType {
	case component.Q_lastState:
		addr := common.BytesToAddress(data[1:])

		s.log.Info("query addr", "addr", addr)

		acct, err := s.getLatestState(addr)
		if err != nil {
			s.log.Error("query last state err", "err", err, "address", addr)
			s.pdcQuery.SendRaw([]byte{}, msgid)
			return err
		}
		sendData, err := rlp.EncodeToBytes(*acct)
		if err != nil {
			s.log.Error("last state  encode err", "err", err, "address", addr)
			s.pdcQuery.SendRaw([]byte{}, msgid)
			return err
		}
		s.pdcQuery.SendRaw(sendData, msgid)
	case component.Q_lastCode:
		addr := common.BytesToAddress(data[1:])
		code, err := s.getLatestCode(addr)
		if err != nil {
			s.log.Error("query last code err", "err", err, "address", addr)
			s.pdcQuery.SendRaw([]byte{}, msgid)
			return err
		}
		s.pdcQuery.SendRaw(code.Code, msgid)
	case component.Q_lastPluginStorage:
		addrSpan := common.AddressLength + 1
		addr := common.BytesToAddress(data[1:addrSpan])
		key := data[addrSpan:]
		itemKey := getPluginStorageItemKey(true, addr, key, 0)
		storgs := s.getData(itemKey)
		if storgs == nil {
			s.log.Error("query last plugin storage err", "address", addr, "key", itemKey)
			s.pdcQuery.SendRaw([]byte{}, msgid)
			return nil
		}
		s.pdcQuery.SendRaw(storgs, msgid)
	}

	return nil
}

//PublishApc  publish into topic with geneisi block
func (s *Storage) PublishApc() error {

	begintime0 := time.Now()

	accs := s.apc.ToAccountInfos()

	if accs == nil {
		return nil
	}

	s.logSvc.Log(component.LogId_storage_gatherAccountFromApc,
		component.TimeLog_Type_Elapse,
		begintime0,
		time.Now(),
		fmt.Sprintf("updates=%d", len(*accs)),
		component.LogLevel_perfermance,
		s.height,
		s.sourceid)

	bysHeight := s.getData(lastheight)

	ais := &types.AccountState{
		AccountInfos: accs,
	}

	s.logSvc.Log(component.LogId_storage_startSendApc,
		component.TimeLog_Type_Happen,
		time.Now(),
		time.Now(),
		"",
		component.LogLevel_perfermance,
		s.height,
		s.sourceid)
	s.log.Info("start send apc datas", "nums", len(*ais.AccountInfos))
	return s.pdcApc.SendPack(ais, 0, common.BytesToUint64(bysHeight))

}

// get account info by address
func (s *Storage) GetAccount(key common.Address) (*types.Account, error) {
	act := s.apc.Get(key)
	if act != nil {
		return act, nil
	}
	lsstate, err := s.getLatestState(key)
	if err != nil {
		return nil, err
	}
	if lsstate == nil {
		return nil, nil
	}
	return &(lsstate.Acc), nil
}
func (s *Storage) GetCode(key common.Address) ([]byte, error) {

	lastCode, err := s.getLatestCode(key)
	if err != nil {
		return nil, err
	}
	if lastCode == nil {
		return nil, nil
	}
	return lastCode.Code, nil

}

func (s *Storage) existHeight(heights []uint64, thisheight uint64) bool {

	for i := len(heights) - 1; i >= 0; i-- {
		if heights[i] == thisheight {
			return true
		}
	}
	return false
}

func isMetaData(version types.Version) bool {
	b := version << 16
	return b == 0
}
func (s *Storage) UpdatePluginStorage(updates *[]*types.EuResult, blockheight uint64) error {
	if 1 == 1 {
		return nil
	}

	if updates == nil || len(*updates) == 0 {
		return nil
	}
	s.log.Info("write plugin storage  data into storage", "nums", len(*updates))

	for _, er := range *updates {
		if er == nil {
			continue
		}
		ethss := er.W.EthStorageWrites
		for add, eths := range ethss {
			//s.log.Info("Update plugin storage", "add", add, "ws", eths)

			addr := common.BytesToAddress([]byte(add))
			batch := s.rawdb.NewBatch()

			keys_last := [][]byte{}
			keys_history := [][]byte{}
			for k, v := range eths {
				//fmt.Printf("v=%v,bys=%v\n", v, v.Bytes())
				//latest
				lstKey := getPluginStorageItemKey(true, addr, k.Bytes(), blockheight)
				keys_last = append(keys_last, lstKey)
				batch.Put(lstKey, v.Bytes())
				//history
				hisKey := getPluginStorageItemKey(false, addr, k.Bytes(), blockheight)
				keys_history = append(keys_history, hisKey)
				batch.Put(hisKey, v.Bytes())
			}

			//latest
			lstEths, err := s.getLatestPluginStorage(addr)

			if err == nil && lstEths != nil {

				if !s.existHeight(lstEths.Height, blockheight) {
					lstEths.Height = append(lstEths.Height, blockheight)
				}

				keys := lstEths.Keys
				keys = createKeysArray(keys_last, keys)
				lstEths.Keys = keys

			} else {
				keys := [][]byte{}
				keys = createKeysArray(keys_last, keys)

				lstEths = &latestPluginStorage{
					Addr:   addr,
					Height: []uint64{blockheight},
					Keys:   keys,
				}
			}
			data, err := rlp.EncodeToBytes(*lstEths)
			if err != nil {
				s.log.Error("encode data save last plugin storage error", "err", err)
				return err
			}

			err = batch.Put(latestKey(StorageType_PluginStorage, addr), data)
			if err != nil {
				s.log.Error("save last plugin storage error", "err", err)
				return err
			}

			//write history storage

			hisEths, err := s.getHisPluginStorage(addr, blockheight)

			if err == nil && hisEths != nil {
				//lstCls.keys

				keys := hisEths.Keys
				keys = createKeysArray(keys_history, keys)
				hisEths.Keys = keys

			} else {
				keys := [][]byte{}
				keys = createKeysArray(keys_history, keys)
				hisEths = &historyPluginStorage{
					Addr: addr,
					Keys: keys,
				}
			}

			data, err = rlp.EncodeToBytes(*hisEths)
			if err != nil {
				s.log.Error("encode data save history plugin storage error", "err", err)
				return err
			}

			err = batch.Put(historyKeys(StorageType_PluginStorage, blockheight, addr), data)
			if err != nil {
				s.log.Error("save history plugin storage error", "err", err)
				return err
			}

			batch.Write()
		}
	}
	return nil
}
func (s *Storage) UpdateCls(updates *[]*types.EuResult, blockheight uint64) error {
	if 1 == 1 {
		return nil
	}

	if updates == nil || len(*updates) == 0 {
		return nil
	}
	s.log.Info("write concurrent-lib storage  data into storage", "nums", len(*updates))

	for _, er := range *updates {
		if er == nil {
			continue
		}

		wsm := er.W.ClibWrites
		for add, ws := range wsm {
			//s.log.Info("UpdateCls", "add", add, "ws", ws)

			addr := common.BytesToAddress([]byte(add))

			batch := s.rawdb.NewBatch()

			keys_last := [][]byte{}
			keys_history := [][]byte{}
			keys_meta := [][]byte{}
			for _, w := range ws.ArrayWrites {
				//latest
				lstKey := getClsItemArrayKey(true, addr, w.ID, w.Index, blockheight)
				keys_last = append(keys_last, lstKey)
				batch.Put(lstKey, w.Value)
				//history
				hisKey := getClsItemArrayKey(false, addr, w.ID, w.Index, blockheight)
				keys_history = append(keys_history, hisKey)
				batch.Put(hisKey, w.Value)
				//meta
				if isMetaData(w.Version) {
					//save metainfo
					meatkey := metaKey(cclDataType_Array, addr, w.ID)
					data, err := common.GobEncode(w)
					if err != nil {
						s.log.Error("encode send euresult ", "err", err)
						continue
					}
					keys_meta = append(keys_meta, meatkey)
					batch.Put(meatkey, data)
				}
			}

			for _, w := range ws.HashMapWrites {
				lstKey := getClsItemHashMapKey(true, addr, w.ID, w.Key, blockheight)
				keys_last = append(keys_last, lstKey)
				batch.Put(lstKey, w.Value)
				hisKey := getClsItemHashMapKey(false, addr, w.ID, w.Key, blockheight)
				keys_history = append(keys_history, hisKey)
				batch.Put(hisKey, w.Value)
				//meta
				if isMetaData(w.Version) {
					//save metainfo
					meatkey := metaKey(cclDataType_HashMap, addr, w.ID)

					keys_meta = append(keys_meta, meatkey)
					batch.Put(meatkey, []byte(w.Key))
				}
			}

			for _, w := range ws.CounterWrites {
				lstKey := getClsItemCounterKey(true, addr, w.ID, blockheight)
				keys_last = append(keys_last, lstKey)
				v := common.Uint64ToBytes(common.Int64ToUint64(w.Delta))
				batch.Put(lstKey, v)
				hisKey := getClsItemCounterKey(false, addr, w.ID, blockheight)
				keys_history = append(keys_history, hisKey)
				batch.Put(hisKey, v)
			}

			lstCls, err := s.getLatestCls(addr)

			if err == nil && lstCls != nil {

				if !s.existHeight(lstCls.Height, blockheight) {
					lstCls.Height = append(lstCls.Height, blockheight)
				}

				keys := lstCls.Keys
				keys = createKeysArray(keys_last, keys)
				lstCls.Keys = keys

				mkeys := lstCls.MetaKeys
				keys = createKeysArray(keys_meta, mkeys)
				lstCls.MetaKeys = mkeys

			} else {
				keys := [][]byte{}
				keys = createKeysArray(keys_last, keys)

				mkeys := [][]byte{}
				mkeys = createKeysArray(keys_meta, mkeys)

				lstCls = &latestCls{
					Addr:     addr,
					Height:   []uint64{blockheight},
					Keys:     keys,
					MetaKeys: mkeys,
				}
			}
			data, err := rlp.EncodeToBytes(*lstCls)
			if err != nil {
				s.log.Error("encode data save last concurrent-lib storage error", "err", err)
				return err
			}

			err = batch.Put(latestKey(StorageType_ConcurrentlibStorage, addr), data)
			if err != nil {
				s.log.Error("save last concurrent-lib storage error", "err", err)
				return err
			}

			//write history storage

			hisCls, err := s.getHisCls(addr, blockheight)

			if err == nil && hisCls != nil {

				keys := hisCls.Keys
				keys = createKeysArray(keys_history, keys)
				hisCls.Keys = keys

			} else {
				keys := [][]byte{}
				keys = createKeysArray(keys_history, keys)
				hisCls = &historyCls{
					Addr: addr,
					Keys: keys,
				}
			}

			data, err = rlp.EncodeToBytes(*hisCls)
			if err != nil {
				s.log.Error("encode data save history concurrent-lib storage error", "err", err)
				return err
			}

			err = batch.Put(historyKeys(StorageType_ConcurrentlibStorage, blockheight, addr), data)
			if err != nil {
				s.log.Error("save history concurrent-lib storage error", "err", err)
				return err
			}
			batch.Write()
		}
	}
	return nil

}

// items osf a not in b,add it into b
func createKeysArray(src [][]byte, dest [][]byte) [][]byte {
	for _, v := range src {
		if common.FindinArrays(dest, v) == -1 {
			dest = append(dest, v)
		}
	}
	return dest
}
func (s *Storage) UpdateCodes(updates *map[common.Address][]byte, blockheight uint64) error {
	if 1 == 1 {
		return nil
	}

	if updates == nil || len(*updates) == 0 {
		return nil
	}
	s.log.Info("write code  data into storage", "nums", len(*updates))

	batch := s.rawdb.NewBatch()

	for k, v := range *updates {
		if v == nil {
			continue
		}

		//s.log.Info("UpdateCodes", "k", k, "v", v)

		lstCode, err := s.getLatestCode(k)

		if err == nil && lstCode != nil {
			lstCode.Code = v
			lstCode.Height = append(lstCode.Height, blockheight)
		} else {
			lstCode = &latestCode{
				Addr:   k,
				Height: []uint64{blockheight},
				Code:   v,
			}
		}
		data, err := rlp.EncodeToBytes(*lstCode)
		if err != nil {
			s.log.Error("encode data save last code error", "err", err)
			return err
		}

		err = batch.Put(latestKey(StorageType_Code, k), data)
		if err != nil {
			s.log.Error("save last code error", "err", err)
			return err
		}

		//write history state

		err = batch.Put(historyKeys(StorageType_Code, blockheight, k), v)
		if err != nil {
			s.log.Error("save history code error", "err", err)
			return err
		}

	}
	return batch.Write()

}

//for test
func (s *Storage) getHistoryAcct(addr common.Address, heights []uint64) {
	for _, h := range heights {
		key := historyKeys(StorageType_Account, h, addr)
		data := s.getData(key)
		var acct *types.Account
		err := rlp.DecodeBytes(data, &acct)
		if err != nil {
			s.log.Error("decode err", "addr", addr, "height", h, "err", err)
		}
		s.log.Info("history account", "addr", addr, "height", h, "acct", acct)
	}

}

func (s *Storage) showWs(addr common.Address) {
	lcs, err := s.getLatestCls(addr)
	if err != nil {
		s.log.Error("get last cls err ", "err", err)
		return
	}
	if lcs == nil {
		s.log.Error("no cls ")
		return
	}
	s.log.Info("last cls", "addr", addr, "cls", lcs)

	for _, lkey := range lcs.Keys {
		data := s.getData(lkey)
		s.log.Info("last cls", "addr", addr, "key", lkey, "data", data)
	}

	//meta infos
	for _, lkey := range lcs.MetaKeys {
		data := s.getData(lkey)
		s.log.Info("last cls meta", "addr", addr, "key", lkey, "data", data)
	}

	for _, h := range lcs.Height {
		his, err := s.getHisCls(addr, h)
		if err != nil {
			s.log.Error("get history cls err ", "err", err)
			return
		}
		if his == nil {
			s.log.Error("no cls ")
			return
		}
		s.log.Info("history cls", "addr", addr, "height", h)

		for _, hkey := range his.Keys {
			data := s.getData(hkey)
			s.log.Info("history cls", "addr", addr, "hkey", hkey, "data", data)
		}
	}

}

//get all plugin storage
func (s *Storage) showPluginStorage(addr common.Address) {
	les, err := s.getLatestPluginStorage(addr)
	if err != nil {
		s.log.Error("get last plugin storages err ", "err", err)
		return
	}
	if les == nil {
		s.log.Error("no plugin storages ")
		return
	}
	s.log.Info("last plugin storage", "addr", addr, "les", les)

	for _, lkey := range les.Keys {
		data := s.getData(lkey)
		s.log.Info("last plugin storage", "addr", addr, "key", lkey, "data", data)
	}

	for _, h := range les.Height {
		his, err := s.getHisPluginStorage(addr, h)
		if err != nil {
			s.log.Error("get history plugin storage err ", "err", err)
			return
		}
		if his == nil {
			s.log.Error("no plugin storage ")
			return
		}
		s.log.Info("history plugin storage", "addr", addr, "height", h)

		for _, hkey := range his.Keys {
			data := s.getData(hkey)
			s.log.Info("history plugin storage", "addr", addr, "hkey", hkey, "data", data)
		}
	}

}

func (s *Storage) showCode(addr common.Address) {

	lcd, err := s.getLatestCode(addr)
	if err != nil {
		s.log.Error("get last code err ", "err", err)
		return
	}
	if lcd == nil {
		s.log.Error("no code ")
		return
	}

	s.log.Info("last code", "addr", addr, "code", lcd.Code)

	s.getHistoryCode(addr, lcd.Height)
}

func (s *Storage) getHistoryCode(addr common.Address, heights []uint64) {
	for _, h := range heights {
		key := historyKeys(StorageType_Code, h, addr)
		data := s.getData(key)
		s.log.Info("history account", "addr", addr, "height", h, "code", data)
	}

}

// write accountinfo into db
func (s *Storage) UpdateAccounts(updates *[]*types.AccountInfo, blockheight uint64) error {

	if 1 == 1 {
		return nil
	}

	if len(*updates) == 0 {
		return nil
	}

	s.log.Info("write data into storage", "nums", len(*updates))
	//s.setAllAddress(updates)

	batch := s.rawdb.NewBatch()

	for _, v := range *updates {
		if v == nil {
			continue
		}

		//s.log.Info("update account", "addr", v.Address, "acct", v.Account)
		//update apc

		if !s.apc.Add(v.Address, &v.Account) {
			s.addAllkeys(v.Address)
		}
		//write into storage
		lstate, err := s.getLatestState(v.Address)
		if err == nil && lstate != nil {

			lstate.Acc = v.Account
			lstate.Height = append(lstate.Height, blockheight)
		} else {
			lstate = &latestState{
				Addr:   v.Address,
				Height: []uint64{blockheight},
				Acc:    v.Account,
			}
		}
		//s.log.Info("update last account", "lstate", lstate)
		data, err := rlp.EncodeToBytes(*lstate)
		if err != nil {
			s.log.Error("encode data save last data error", "err", err)
			return err
		}

		err = batch.Put(latestKey(StorageType_Account, v.Address), data)

		if err != nil {
			s.log.Error("save last data error", "err", err)
			return err
		}

		//write history state

		data, err = rlp.EncodeToBytes(v.Account)
		if err != nil {
			s.log.Error("encode data for save history data error", "err", err)
			return err
		}

		err = batch.Put(historyKeys(StorageType_Account, blockheight, v.Address), data)
		if err != nil {
			s.log.Error("save history data error", "err", err)
			return err
		}

	}
	s.setAllAddress()
	return batch.Write()
}

// genesis block
func (s *Storage) genesis() error {

	initAccountGroup := viper.GetInt("eth-gn")

	initNums := initAccountGroup * 2

	begintime0 := time.Now()
	updates := make([]*types.AccountInfo, initNums)

	addrfile := viper.GetString("af")
	file_r, err := os.OpenFile(addrfile, os.O_RDWR, 0666)
	if err != nil {
		s.log.Error("Open read file error", "err", err)
		return err
	}
	defer file_r.Close()

	buf := bufio.NewReader(file_r)
	var lineindex int = 0

	for {
		if initNums >= 0 && lineindex >= initNums {

			break
		}

		line, err := buf.ReadString('\n')
		if err != nil {
			if err == io.EOF {

				s.log.Info("File exec finish", "filename", addrfile)
				break
			} else {
				s.log.Error(" read file error", "err", err)
				return err
			}
		}

		//s.log.Info("read addr from af", "addr", line)

		updates[lineindex] = &types.AccountInfo{
			Address: common.HexToAddress(line),
			Account: types.Account{
				Balance: new(big.Int).SetInt64(4278000000000000000),
			},
		}
		//s.log.Info("genesis addr", "addr", updates[lineindex].Address)
		lineindex++
	}

	s.logSvc.Log(component.LogId_storage_makeGenesis,
		component.TimeLog_Type_Elapse,
		//time.Now().Sub(begintime0),
		begintime0,
		time.Now(),
		"",
		component.LogLevel_perfermance,
		s.height,
		s.sourceid)
	s.log.Info("genesis alloc terminated", "nums", initNums)
	begintime1 := time.Now()

	s.logSvc.Log(component.LogId_storage_updateGenesis,
		component.TimeLog_Type_Elapse,

		begintime1,
		time.Now(),
		"",
		component.LogLevel_perfermance,
		s.height,
		s.sourceid)

	ais := &types.AccountState{
		AccountInfos: &updates,
		//Height:       0,
	}
	s.logSvc.Log(component.LogId_storage_startSendGenesis,
		component.TimeLog_Type_Happen,

		time.Now(),
		time.Now(),
		fmt.Sprintf("event=start send genesis accounts"),
		component.LogLevel_perfermance,
		s.height,
		s.sourceid)

	s.log.Info("start send genesis data", "nums", len(*ais.AccountInfos))
	s.pdcApc.SendPack(ais, 0, 0)

	s.UpdateAccounts(&updates, 0)

	return nil
}

// load apc at service init
func (s *Storage) loadApc() error {

	s.apc = types.NewApc()
	begintime0 := time.Now()
	addrs, err := s.getAllAddress()
	s.logSvc.Log(component.LogId_storage_LoadAllAcctsFromDb,
		component.TimeLog_Type_Elapse,

		begintime0,
		time.Now(),
		"",
		component.LogLevel_perfermance,
		s.height,
		s.sourceid)

	if err != nil {
		s.log.Error("load address err ", "err", err)

		s.genesis()
		return err
	}

	s.log.Info("load address from storage", "nums", len(addrs))
	counter := 0
	begintime1 := time.Now()
	for _, addr := range addrs {

		lst, err := s.getLatestState(addr)
		if err != nil {
			continue
		}
		s.apc.Add(addr, &(lst.Acc))

		//s.log.Info("latest acttinfo", "addr", addr, "acct", lst.Acc)

		//for test history
		// s.getHistoryAcct(addr, lst.Height)

		// s.showCode(addr)
		// s.showWs(addr)

		// s.showPluginStorage(addr)

		counter++
	}

	s.logSvc.Log(component.LogId_storage_LoadApcFromDb,
		component.TimeLog_Type_Elapse,

		begintime1,
		time.Now(),
		"",
		component.LogLevel_perfermance,
		s.height,
		s.sourceid)

	s.log.Info("init apc terminated", "acctnums", len(addrs))
	if counter == 0 {
		s.genesis()

	} else {
		s.PublishApc()
	}

	return nil
}

func (s *Storage) addAllkeys(addr common.Address) {
	s.allkeys = append(s.allkeys, addr.Bytes()...)
}
