package workers

import (
	"bufio"
	"fmt"
	"io"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	ethCommon "github.com/arcology-network/3rd-party/eth/common"
	"github.com/arcology-network/common-lib/common"
	ccommon "github.com/arcology-network/common-lib/common"
	commerkle "github.com/arcology-network/common-lib/merkle"
	"github.com/arcology-network/common-lib/mhasher"
	"github.com/arcology-network/common-lib/types"
	"github.com/arcology-network/component-lib/storage"
	ccurl "github.com/arcology-network/concurrenturl/v2"
	urlcommon "github.com/arcology-network/concurrenturl/v2/common"
	urltype "github.com/arcology-network/concurrenturl/v2/type"
	"github.com/arcology-network/concurrenturl/v2/type/commutative"

	cachedstorage "github.com/arcology-network/common-lib/cachedstorage"
	"github.com/arcology-network/common-lib/mempool"
)

func TestGenesisEshing(t *testing.T) {
	GenesisApc := map[ethCommon.Address]*types.Account{}
	genesis(&GenesisApc, uint64(0), "af")

	db := cachedstorage.NewDataStore()
	platform := urlcommon.NewPlatform()
	meta, _ := commutative.NewMeta(platform.Eth10Account())
	db.Inject(platform.Eth10Account(), meta)

	transitionsData := make([][]byte, 0, len(GenesisApc)*10)
	//transitionsInit := make([]urlcommon.UnivalueInterface, 0, len(GenesisApc)*10)
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

	euresult := types.EuResult{
		ID:          0,
		Transitions: transitionsData,
	}
	txids, transitions := storage.GetTransitions([]*types.EuResult{&euresult})

	url := ccurl.NewConcurrentUrl(db)
	merkle := urltype.NewAccountMerkle(platform)

	url.Indexer().Import(transitions)
	merkle.Import(transitions)

	url.Precommit(txids)

	k, v := url.Indexer().KVs()
	fmt.Printf("==========================k:%v v:%v\n", len(k), len(v))
	merkle.Build(k, v)

	merkles := merkle.GetMerkles()
	fmt.Printf("merkles count : %v\n", len(*merkles))

	keys := make([]string, 0, len(*merkles))
	for p, _ := range *merkles {
		keys = append(keys, p)
	}

	sortedKeys, err := mhasher.SortStrings(keys)
	if err != nil {
		fmt.Printf("sort err : %v\n", err)
		return
	}

	rootDatas := make([][]byte, len(sortedKeys))
	worker := func(start, end, index int, args ...interface{}) {
		for i := start; i < end; i++ {
			if merkle, ok := (*merkles)[sortedKeys[i]]; ok {

				rootDatas[i] = merkle.GetRoot()
				fmt.Printf("merkles path found ------------------rootDatas[%v]:%x\n", i, rootDatas[i])
			}
		}
	}
	common.ParallelWorker(len(sortedKeys), 6, worker)
	for i, r := range rootDatas {
		fmt.Printf("merkle hash path : %v hash : %x \n", sortedKeys[i], r)
	}
	all := commerkle.NewMerkle(len(rootDatas), commerkle.Sha256)
	nodePool := mempool.NewMempool("nodes", func() interface{} {
		return commerkle.NewNode()
	})
	all.Init(rootDatas, nodePool)
	root := all.GetRoot()
	fmt.Printf("root=%x\n", root)
}

func TestGenesis(t *testing.T) {
	GenesisApc := map[ethCommon.Address]*types.Account{}

	genesis(&GenesisApc, uint64(0), "/home/weizp/work/af")

	//i.AddLog(log.LogLevel_Info, "init genesis terminated", zap.Int("GenesisApc", len(GenesisApc)), zap.Duration("times", time.Now().Sub(begintime1)))

	db := cachedstorage.NewDataStore()
	platform := urlcommon.NewPlatform()
	meta, _ := commutative.NewMeta(platform.Eth10Account())
	db.Inject(platform.Eth10Account(), meta)

	t4 := time.Now()
	transitionsData := make([][]byte, 0, len(GenesisApc)*10)
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

	euresult := &types.EuResult{
		ID:          0,
		Transitions: transitionsData,
	}
	fmt.Println("GetTransitions() ", len(transitionsData), " in :", time.Since(t4))

	startTime := time.Now()
	sendData, err := ccommon.GobEncode(euresult)
	if err != nil {
		fmt.Printf("err=%v\n", err)
	}
	fmt.Printf("euresult encode completed,tim=%v\n", time.Now().Sub(startTime))
	var msg types.EuResult
	startTime = time.Now()
	err = ccommon.GobDecode(sendData, &msg)
	fmt.Printf("euresult decode completed,tim=%v\n", time.Now().Sub(startTime))
}

// genesis block
func genesis(GenesisApc *map[ethCommon.Address]*types.Account, height uint64, filename string) error {

	//begintime0 := time.Now()
	//addrfile := viper.GetString(filename)
	file_r, err := os.OpenFile(filename, os.O_RDWR, 0666)
	if err != nil {
		//i.AddLog(log.LogLevel_Error, "Open read file error", zap.String("err", err.Error()))
		fmt.Printf("Open read file error=%v,filename=%v\n", err, filename)
		return err
	}
	defer file_r.Close()

	buf := bufio.NewReader(file_r)
	var lineindex int = 0
	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				//i.AddLog(log.LogLevel_Info, "File exec finish", zap.String("filename", addrfile))
				break
			} else {
				//i.AddLog(log.LogLevel_Error, "read file error", zap.String("err", err.Error()))
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

	//i.AddLog(log.LogLevel_Info, "genesis alloc terminated", zap.Int("nums", lineindex), zap.Duration("times", time.Now().Sub(begintime0)))

	return nil
}
