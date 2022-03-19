package workers

import (
	"bufio"
	"io"

	"math/big"
	"os"
	"strings"
	"time"

	ethCommon "github.com/arcology-network/3rd-party/eth/common"
	ethTypes "github.com/arcology-network/3rd-party/eth/types"
	"github.com/arcology-network/common-lib/types"
	"github.com/arcology-network/component-lib/actor"
	"github.com/arcology-network/component-lib/log"
	ccurl "github.com/arcology-network/concurrenturl/v2"
	storageTypes "github.com/arcology-network/storage-svc/service/types"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	cachedstorage "github.com/arcology-network/common-lib/cachedstorage"
	urlcommon "github.com/arcology-network/concurrenturl/v2/common"
	"github.com/arcology-network/concurrenturl/v2/type/commutative"
	evmCommon "github.com/arcology-network/evm/common"
	adaptor "github.com/arcology-network/vm-adaptor/evm"
)

type Initializer struct {
	actor.WorkerThread
	datastore storageTypes.DB //urlcommon.DB
}

//return a Subscriber struct
func NewInitializer(concurrency int, groupid string, ds storageTypes.DB) *Initializer {
	in := Initializer{}
	in.Set(concurrency, groupid)
	in.datastore = ds
	return &in
}

func (i *Initializer) OnStart() {}
func (i *Initializer) Stop()    {}

func (i *Initializer) OnMessageArrived(msgs []*actor.Message) error {
	i.AddLog(log.LogLevel_Info, "storage initialize ", zap.String("send command", actor.MsgStartSub))
	i.MsgBroker.Send(actor.MsgStartSub, "")
	i.MsgBroker.Send(actor.MsgBlockCompleted, actor.MsgBlockCompleted_Success)

	lastHeight := storageTypes.GetLatestHeight(i.datastore)

	i.AddLog(log.LogLevel_Debug, "LoadData  lastHeight completed", zap.Uint64("lastHeight", lastHeight))
	i.LatestMessage.Height = lastHeight
	i.ChangeEnvironment(i.LatestMessage)
	i.MsgBroker.Send(actor.MsgLatestHeight, lastHeight)
	i.MsgBroker.Send(actor.MsgExecTime, &types.StatisticalInformation{
		Key:   "",
		Value: "",
	})
	i.MsgBroker.Send(actor.MsgPendingBlock, &types.MonacoBlock{
		Height:  0,
		Headers: [][]byte{},
		Txs:     [][]byte{},
	})
	i.MsgBroker.Send(actor.MsgSpawnedRelations, []*types.SpawnedRelation{})

	if lastHeight > 0 {
		i.LoadingParentInfo(lastHeight)
	}

	i.loadingAccountInfos(lastHeight)
	//i.MsgBroker.Send(actor.MsgClearCompletedReceipts, "true")
	//i.MsgBroker.Send(actor.MsgClearCompletedEuresults, "true")

	i.MsgBroker.Send(actor.MsgSelectedReceipts, &[]*ethTypes.Receipt{})

	return nil
}
func GetTransitions(db *cachedstorage.DataStore, address []ethCommon.Address, accounts []*types.Account) [][]byte {
	url := ccurl.NewConcurrentUrl(db)
	statedb := adaptor.NewStateDBV2(nil, db, url)
	statedb.Prepare(evmCommon.Hash{}, evmCommon.Hash{}, 0)
	for i, addr := range address {
		address := evmCommon.BytesToAddress(addr.Bytes())
		statedb.CreateAccount(address)
		statedb.SetBalance(address, accounts[i].Balance)
		statedb.SetNonce(address, accounts[i].Nonce)
	}
	_, transitions := url.ExportEncoded()
	return transitions
	// return ccurltype.Univalues(transitions)
}

// load apc at service init
func (i *Initializer) loadingAccountInfos(height uint64) error {
	GenesisApc := map[ethCommon.Address]*types.Account{}
	t0 := time.Now()

	i.genesis(&GenesisApc, height)
	i.AddLog(log.LogLevel_Info, "init genesis terminated", zap.Int("GenesisApc", len(GenesisApc)), zap.Duration("times", time.Now().Sub(t0)))

	transitionsData := make([][]byte, 0, len(GenesisApc)*10)
	db := cachedstorage.NewDataStore()
	platform := urlcommon.NewPlatform()
	meta, _ := commutative.NewMeta(platform.Eth10Account())
	db.Inject(platform.Eth10Account(), meta)

	batch := 10
	address := make([]ethCommon.Address, 0, batch)
	accounts := make([]*types.Account, 0, batch)
	idx := 0
	for addr, account := range GenesisApc {
		if idx%batch == 0 && idx > 0 {
			transitionsData = append(transitionsData, GetTransitions(db, address, accounts)...)
			address = make([]ethCommon.Address, 0, batch)
			accounts = make([]*types.Account, 0, batch)
		}
		address = append(address, addr)
		accounts = append(accounts, account)
		idx = idx + 1
	}

	if len(address) > 0 {
		transitionsData = append(transitionsData, GetTransitions(db, address, accounts)...)
	}

	euresults := make([]*types.EuResult, 1)
	euresults[0] = &types.EuResult{
		ID:          0,
		Transitions: transitionsData,
		H:           string(ethCommon.Hash{}.Bytes()),
	}

	i.AddLog(log.LogLevel_Info, "init genesis transitionsData", zap.Int("transitions Count", len(transitionsData)))
	i.LatestMessage.Height = height
	i.ChangeEnvironment(i.LatestMessage)
	i.MsgBroker.Send(actor.MsgInitExecuted, &euresults)
	i.MsgBroker.Send(actor.MsgConflictInclusive, &types.InclusiveList{})
	return nil
}

// genesis block
func (i *Initializer) genesis(GenesisApc *map[ethCommon.Address]*types.Account, height uint64) error {
	t0 := time.Now()
	addrfile := viper.GetString("af")
	file_r, err := os.OpenFile(addrfile, os.O_RDWR, 0666)
	if err != nil {
		i.AddLog(log.LogLevel_Error, "Open read file error", zap.String("err", err.Error()))
		return err
	}
	defer file_r.Close()

	buf := bufio.NewReader(file_r)
	var lineindex int = 0
	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				i.AddLog(log.LogLevel_Info, "File exec finish", zap.String("filename", addrfile))
				break
			} else {
				i.AddLog(log.LogLevel_Error, "read file error", zap.String("err", err.Error()))
				return err
			}
		}

		line = strings.TrimRight(line, "\n")
		strs := strings.Split(line, ",")
		balance, ok := new(big.Int).SetString(strs[2], 10)
		if !ok {
			balance = big.NewInt(0)
		}
		addr := ethCommon.HexToAddress(strs[1])
		(*GenesisApc)[addr] = &types.Account{
			Nonce:   1,
			Balance: balance,
		}
		lineindex++
	}

	i.AddLog(log.LogLevel_Info, "genesis alloc terminated", zap.Int("nums", lineindex), zap.Duration("times", time.Now().Sub(t0)))
	return nil
}

// get saved parenthash and roothash from db,and send to exec-svc
func (i *Initializer) LoadingParentInfo(height uint64) error {
	i.MsgBroker.Send(actor.MsgInitParentInfo, &types.ParentInfo{
		ParentHash: storageTypes.GetLastHash(i.datastore),
		ParentRoot: storageTypes.GetLastRoot(i.datastore),
	})
	return nil
}
