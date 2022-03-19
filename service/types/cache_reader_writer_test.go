package types

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	ethCommon "github.com/arcology-network/3rd-party/eth/common"
	ethRlp "github.com/arcology-network/3rd-party/eth/rlp"
	ethTypes "github.com/arcology-network/3rd-party/eth/types"
	cachedstorage "github.com/arcology-network/common-lib/cachedstorage"
	"github.com/arcology-network/common-lib/common"
	"github.com/arcology-network/common-lib/types"
	"github.com/arcology-network/concurrentlib"
	"github.com/arcology-network/concurrenturl/v2"
	urlcommon "github.com/arcology-network/concurrenturl/v2/common"
	commutative "github.com/arcology-network/concurrenturl/v2/type/commutative"
	evmcommon "github.com/arcology-network/evm/common"
	"github.com/arcology-network/vm-adaptor/evm"
)

// func initDb() *RedisDB {
// 	addrs := "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005,127.0.0.1:7006"
// 	//addrs := "127.0.0.1:6379"
// 	// addrs := "127.0.0.1:7001"
// 	return NewRedisDB(strings.Split(addrs, ","))
// }
func initDb() DB {
	//addrs := "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005,127.0.0.1:7006"
	//addrs := "127.0.0.1:6379"
	// addrs := "127.0.0.1:7001"
	return NewMemoryDB()
}
func TestBatch(t *testing.T) {
	db := initDb()
	keys := []string{"123", "456", "789"}
	vals := [][]byte{[]byte{11, 23, 45}, []byte{12, 34, 31}, []byte{90, 18, 19}}
	err := SetValues(db, keys, vals)
	if err != nil {
		fmt.Printf("err=%v\n", err)
		return
	}
	time.Sleep(10)
	values := GetValues(db, keys)
	fmt.Printf("values=%v\n", values)
	fmt.Printf("vals=%v\n", vals)
	if !reflect.DeepEqual(vals, values) {
		t.Error("Batch set get Error")
		return
	}
	fmt.Printf("values=%v\n", values)
}

func TestLastRoot(t *testing.T) {
	db := initDb()
	root := ethCommon.HexToHash("1459be9d87e5ce44b771914d17e2209f0fd35636051dc87216a050084a71b6a0")
	SaveLastRoot(db, root)

	saveRoot := GetLastRoot(db)
	if !reflect.DeepEqual(root, saveRoot) {
		t.Error("last Root set get Error")
	}
	fmt.Printf("values=%x\n", saveRoot)
}

func TestLastHash(t *testing.T) {
	db := initDb()
	hash := ethCommon.HexToHash("d7f041a84ec2cb8d9e298ab00bed55158e3f29f0020348842d0c3681e1faef89")
	SaveLastHash(db, hash)

	saveHash := GetLastHash(db)
	if !reflect.DeepEqual(hash, saveHash) {
		t.Error("last Root set get Error")
	}
	fmt.Printf("values=%x\n", saveHash)
}

func TestLastHeight(t *testing.T) {
	db := initDb()
	latestHeight := uint64(120)
	SaveLatestHeight(db, latestHeight)

	saveLatestHeight := GetLatestHeight(db)
	if !reflect.DeepEqual(latestHeight, saveLatestHeight) {
		t.Error("latest height set get Error")
	}
	fmt.Printf("latestHeight=%v\n", saveLatestHeight)
}

func TestStatisticalInformation(t *testing.T) {
	db := initDb()
	statisticInfo := types.StatisticalInformation{
		Key:      "starting",
		Value:    "ending",
		TimeUsed: time.Duration(130),
	}
	SaveStatisticInfos(db, uint64(120), &statisticInfo)
	saveStatisticInfo := GetStatisticInfos(db, uint64(120))
	if !reflect.DeepEqual(statisticInfo, *saveStatisticInfo) {
		t.Error("statisticInfo  set get Error")
	}
	fmt.Printf("saveStatisticInfo=%v\n", saveStatisticInfo)
}

func createTxs() ([][]byte, [][]byte) {
	lines := []string{
		"f8a780018502540be40094842d4bfdb1904503ac152483527f338cd5d9bcba80b84440c10f19000000000000000000000000ab01a3bfc5de6b5fc481e18f274adbdba9b111f000000000000000000000000000000000000000000000000000000002540be40026a083c4944d94240fec7bdaa9d533b6766242fe6010355f80aa4b9c043e93e5af11a02ffd7f2fd3329718b20ae58417580825b672f74dbdec4384edab57a3f8e2d25b,66812f708302896954212d553f4cf11dc8859e32cb3ae1decfd955aba6718312",
		"f8a780018502540be40094842d4bfdb1904503ac152483527f338cd5d9bcba80b84440c10f1900000000000000000000000021522c86a586e696961b68aa39632948d9f1117000000000000000000000000000000000000000000000000000000002540be40025a0e2e05bdab570985b5adb9b57aae8afb35254406c19fd1e27cf1ec4158dc43f89a04ac347443a700da3ec8130e0635f59d7a64138b628aaa20f27bb88e34394d549,714222414d021b26175afe55364239d7c12a8eb54e300b4256402bffea20da74",
		"f8a780018502540be40094842d4bfdb1904503ac152483527f338cd5d9bcba80b84440c10f19000000000000000000000000a75cd05bf16bbea1759de2a66c0472131bc5bd8d00000000000000000000000000000000000000000000000000000002540be40026a05cc67d5b6915c9315e2aee0fc3fe0d44206d3848ade512b60ec80c8fbb409df2a07b70dc5c41a60a97d40e6c43d3f905e280cda7eba71b100ab4be1d0dbba464ac,927cfa6decccd60f650e6566dc319240299ce3c08ff4afa1f75721e016407b57",
		"f8a780018502540be40094842d4bfdb1904503ac152483527f338cd5d9bcba80b84440c10f190000000000000000000000002c7161284197e40e83b1b657e98b3bb8ff3c90ed00000000000000000000000000000000000000000000000000000002540be40025a09d236b1bd534aa087eecc0848b58209143a32818ff613aa12b58f5163f50b116a048c54b59bc01374690fc8cfde4e73ce158ae4babf0992cccba54ae73a110f04d,f16a03e13c404247c0a0da9ceffd406d8d7b9bcde8626aadd83fdfdec70f55e6",
		"f8a780018502540be40094842d4bfdb1904503ac152483527f338cd5d9bcba80b84440c10f1900000000000000000000000057170608ae58b7d62dcdc3cbdb564c05ddbb7eee00000000000000000000000000000000000000000000000000000002540be40025a0f8740188f27ddcde631c1a64cfdf648ee8b3ccb89a26c29e6b0c6612bcefe3ada06eb8d7d3f3760f441d3e038e3cc6bdb4549225899bb2d57e2b55ef52f8057347,93b5d2eb7642b273e534aa0a72534026cc25a71506d7da31a693425a95c952b4",
	}
	txs := make([][]byte, len(lines))
	hashes := make([][]byte, len(lines))
	for i, line := range lines {
		str := strings.Split(line, ",")
		data, _ := hex.DecodeString(str[0])
		txs[i] = data
		hashes[i] = ethCommon.HexToHash(str[1]).Bytes()
	}
	return txs, hashes
}
func createBlock() (*types.MonacoBlock, [][]byte, [][]byte) {
	txs, hashes := createTxs()
	txss := make([][]byte, len(txs))
	for i, tx := range txs {
		txx := make([]byte, len(tx)+1)
		txx[0] = types.AppType_Eth
		copy(txx[1:], tx)
		txss[i] = txx
	}
	return &types.MonacoBlock{
		Height:  uint64(110),
		Headers: [][]byte{[]byte{12, 34, 56, 78, 90}},
		Txs:     txss,
	}, txs, hashes
}
func TestGatherTxs(t *testing.T) {
	block, txs, hashes := createBlock()
	_, newhashes, txReals := gatherTxs(block)
	if !reflect.DeepEqual(txs, txReals) {
		t.Error("txs set get Error")
	}
	hhs := make([][]byte, len(newhashes))
	for i, hh := range newhashes {
		hhs[i] = hh.Bytes()
	}
	if !reflect.DeepEqual(hhs, hashes) {
		t.Error("hashes set get Error")
	}
}

func TestBlock(t *testing.T) {
	db := initDb()
	block, txs, hashes := createBlock()
	SaveBlockHashes(db, block)
	hashestrs := make([]string, len(hashes))
	for i, hash := range hashes {
		hashestrs[i] = fmt.Sprintf("%x", hash)
	}
	savedHashes := GetBlockHashes(db, block.Height)
	if !reflect.DeepEqual(hashestrs, savedHashes) {
		t.Error("hashes set get Error")
	}
	for i, hash := range hashes {
		tx := GetTransactionByHash(db, ethCommon.BytesToHash(hash))
		txdata, err := ethRlp.EncodeToBytes(tx)
		if err != nil {
			t.Error("tx encode Error")
		}
		if !reflect.DeepEqual(txdata, txs[i]) {
			t.Error("tx set get Error")
		}
	}

	SaveBlocks(db, block.Height, block)
	savedBlock := GetBlocks(db, block.Height)
	if !reflect.DeepEqual(*savedBlock, *block) {
		t.Error("block set get Error")
	}
}

func TestReceipt(t *testing.T) {
	db := initDb()
	log := &ethTypes.Log{
		Address: ethCommon.HexToAddress("66812f708302896954212d553f4cf11dc8859e32"),
		Topics: []ethCommon.Hash{
			ethCommon.HexToHash("1459be9d87e5ce44b771914d17e2209f0fd35636051dc87216a050084a71b620"),
		},
		Data:        []byte{34, 56, 23},
		BlockNumber: uint64(110),
		TxHash:      ethCommon.HexToHash("1459be9d87e5ce44b771914d17e2209f0fd35636051dc87216a050084a71b6a0"),
		TxIndex:     uint(2),
		BlockHash:   ethCommon.HexToHash("1459be9d87e5ce44b771914d17e2209f0fd35636051dc87216a050084a71b560"),
		Index:       uint(0),
		Removed:     true,
	}

	receipt := ethTypes.Receipt{
		PostState:         []byte{1, 2, 3, 4},
		Status:            uint64(0),
		Logs:              []*ethTypes.Log{log},
		CumulativeGasUsed: uint64(100),
		TxHash:            ethCommon.HexToHash("1459be9d87e5ce44b771914d17e2209f0fd35636051dc87216a050084a71b6a0"),
		ContractAddress:   ethCommon.HexToAddress("66812f708302896954212d553f4cf11dc8859e32"),
		GasUsed:           uint64(2100),
		SpawnedTxHash:     ethCommon.HexToHash("d7f041a84ec2cb8d9e298ab00bed55158e3f29f0020348842d0c3681e1faef89"),
	}
	SaveReceipt(db, uint64(110), receipt.TxHash, &receipt)
	savedReceipt, height := GetReceipt(db, receipt.TxHash)
	if !reflect.DeepEqual(*savedReceipt, receipt) {
		t.Error("receipt set get Error")
	}
	if !reflect.DeepEqual(uint64(110), height) {
		t.Error("receipt height set get Error")
	}
}

func TestExecutingLog(t *testing.T) {
	db := initDb()
	logs := types.ExecutingLogs{
		Txhash: ethCommon.HexToHash("1459be9d87e5ce44b771914d17e2209f0fd35636051dc87216a050084a71b6a0"),
		Logs: []types.ExecutingLog{
			{
				Key:   "starting",
				Value: "true",
			},
			{
				Key:   "ending",
				Value: "false",
			},
		},
	}
	logstr, err := logs.Marshal()
	if err != nil {
		t.Error("ExecutingLog Marshal Error")
		return
	}
	SaveExecutingLog(db, logs.Txhash, &logs)
	savedLogs := GetExecutingLog(db, logs.Txhash)
	if !reflect.DeepEqual(logstr, savedLogs) {
		t.Error("ExecutingLog set get Error")
	}
}

func TestBlockHashHeight(t *testing.T) {
	db := initDb()
	hash := ethCommon.HexToHash("1459be9d87e5ce44b771914d17e2209f0fd35636051dc87216a050084a71b6a0")
	height := uint64(110)
	SaveBlockHashHeight(db, hash, height)
	savedHeight := GetBlockHeightByHash(db, hash)
	if !reflect.DeepEqual(height, savedHeight) {
		t.Error("height index set get Error")
	}
}

func TestCoinbase(t *testing.T) {
	db := initDb()
	coinbase := ethCommon.HexToAddress("66812f708302896954212d553f4cf11dc8859e32")

	SaveCoinbase(db, coinbase)
	savedCoinbase := GetCoinbase(db)
	if !reflect.DeepEqual(coinbase, savedCoinbase) {
		t.Error("coinbase set get Error")
	}
}

func TestAccountState(t *testing.T) {
	db := initDb()

	datastore := cachedstorage.NewDataStore()
	meta, _ := commutative.NewMeta(urlcommon.NewPlatform().Eth10Account())
	datastore.Inject(urlcommon.NewPlatform().Eth10Account(), meta)
	url := concurrenturl.NewConcurrentUrl(datastore)

	accounthex := "66812f708302896954212d553f4cf11dc8859e32"

	account := evmcommon.HexToAddress(accounthex)
	statedb := evm.NewStateDBV2(nil, datastore, url)
	statedb.Prepare(evmcommon.Hash{}, evmcommon.Hash{}, 1)
	statedb.CreateAccount(account)

	statedb.SetBalance(account, big.NewInt(120))
	statedb.SetNonce(account, uint64(101))
	statedb.SetCode(account, []byte{23, 45, 23, 45, 11, 23, 45, 66, 77, 33, 77})
	hashhex := "1459be9d87e5ce44b771914d17e2209f0fd35636051dc87216a050084a71b6a0"
	hash := evmcommon.HexToHash(hashhex)
	hash1 := evmcommon.HexToHash("3359be9d87e5ce44b771914d17e2209f0fd35636051dc87216a050084a71b611")
	statedb.SetState(account, hash, hash1)
	_, transitions := url.Export(true)
	t.Log("\n" + FormatTransitions(transitions))
	url.Import(transitions)
	url.PostImport()
	url.Precommit([]uint32{1})

	keys, vals := url.Indexer().KVs()
	datas := make([][]byte, len(vals))

	encodeWorker := func(start, end, index int, args ...interface{}) {
		vals := args[0].([]interface{})[0].([]interface{})
		datas := args[0].([]interface{})[1].(*[][]byte)
		for i := start; i < end; i++ {
			(*datas)[i] = vals[i].(urlcommon.UnivalueInterface).Encode()
		}
	}
	common.ParallelWorker(len(vals), 4, encodeWorker, vals, &datas)

	SetValues(db, keys, datas)

	balance := GetBalance(db, accounthex)
	if !reflect.DeepEqual(balance, big.NewInt(120)) {
		t.Error("balance set get Error")
	}
	nonce := GetNonce(db, accounthex)
	if !reflect.DeepEqual(nonce, int64(1)) {
		t.Error("nonce set get Error")
	}
	code := GetCode(db, accounthex)
	if !reflect.DeepEqual(code, []byte{23, 45, 23, 45, 11, 23, 45, 66, 77, 33, 77}) {
		t.Error("code set get Error")
	}
	states := GetStorage(db, accounthex, hashhex)
	if !reflect.DeepEqual(states, hash1.Bytes()) {
		t.Error("storage set get Error")
	}
}

type txContext struct {
	height *big.Int
	index  uint32
}

func (context *txContext) GetHeight() *big.Int {
	return context.height
}

func (context *txContext) GetIndex() uint32 {
	return context.index
}

func TestContainer(t *testing.T) {
	db := initDb()

	datastore := cachedstorage.NewDataStore()
	meta, _ := commutative.NewMeta(urlcommon.NewPlatform().Eth10Account())
	datastore.Inject(urlcommon.NewPlatform().Eth10Account(), meta)
	url := concurrenturl.NewConcurrentUrl(datastore)

	account1 := types.Address("contractAddress1")
	account2 := types.Address("contractAddress2")
	arrayID1 := "arrayID1"
	arrayID2 := "arrayID2"
	map1 := "mapID1"
	map2 := "mapID2"
	queue1 := "queueID1"
	queue2 := "queueID2"

	array := concurrentlib.NewFixedLengthArray(url, &txContext{index: 1})
	if !array.Create(account1, arrayID1, concurrentlib.DataTypeUint256, 11) {
		t.Error("Failed to create array11.")
	}

	value1 := ethCommon.BytesToHash([]byte{1}).Bytes()
	if !array.SetElem(account1, arrayID1, 1, value1, concurrentlib.DataTypeUint256) {
		t.Error("Failed to set element on array11.")
	}
	if !array.Create(account1, arrayID2, concurrentlib.DataTypeUint256, 12) {
		t.Error("Failed to create array12.")
	}
	value2 := ethCommon.BytesToHash([]byte{2}).Bytes()
	if !array.SetElem(account1, arrayID2, 2, value2, concurrentlib.DataTypeUint256) {
		t.Error("Failed to set element on array12.")
	}
	if !array.Create(account2, arrayID1, concurrentlib.DataTypeUint256, 21) {
		t.Error("Failed to create array21.")
	}
	value3 := ethCommon.BytesToHash([]byte{3}).Bytes()
	if !array.SetElem(account2, arrayID1, 3, value3, concurrentlib.DataTypeUint256) {
		t.Error("Failed to set element on array21.")
	}
	if !array.Create(account2, arrayID2, concurrentlib.DataTypeUint256, 22) {
		t.Error("Failed to create array22.")
	}
	value4 := ethCommon.BytesToHash([]byte{4}).Bytes()
	if !array.SetElem(account2, arrayID2, 4, value4, concurrentlib.DataTypeUint256) {
		t.Error("Failed to set element on array22.")
	}

	sm := concurrentlib.NewSortedMap(url, &txContext{index: 1})
	if !sm.Create(account1, map1, concurrentlib.DataTypeUint256, concurrentlib.DataTypeUint256) {
		t.Error("Failed to create map11.")
	}
	if !sm.SetValue(account1, map1, []byte("key11"), []byte("value11"), concurrentlib.DataTypeUint256, concurrentlib.DataTypeUint256) {
		t.Error("Failed to set value on map11.")
	}
	if !sm.Create(account1, map2, concurrentlib.DataTypeUint256, concurrentlib.DataTypeUint256) {
		t.Error("Failed to create map12.")
	}
	if !sm.SetValue(account1, map2, []byte("key12"), []byte("value12"), concurrentlib.DataTypeUint256, concurrentlib.DataTypeUint256) {
		t.Error("Failed to set value on map12.")
	}
	if !sm.Create(account2, map1, concurrentlib.DataTypeUint256, concurrentlib.DataTypeUint256) {
		t.Error("Failed to create map21.")
	}
	if !sm.SetValue(account2, map1, []byte("key21"), []byte("value21"), concurrentlib.DataTypeUint256, concurrentlib.DataTypeUint256) {
		t.Error("Failed to set value on map21.")
	}
	if !sm.Create(account2, map2, concurrentlib.DataTypeUint256, concurrentlib.DataTypeUint256) {
		t.Error("Failed to create map22.")
	}
	if !sm.SetValue(account2, map2, []byte("key22"), []byte("value22"), concurrentlib.DataTypeUint256, concurrentlib.DataTypeUint256) {
		t.Error("Failed to set value on map22.")
	}

	queueContext := txContext{height: new(big.Int).SetUint64(100), index: 1}

	queue := concurrentlib.NewQueue(url, &queueContext)
	if !queue.Create(account1, queue1, concurrentlib.DataTypeUint256) {
		t.Error("Failed to create queue11.")
	}
	elem11 := []byte("queue11elem1")
	if !queue.Push(account1, queue1, elem11, concurrentlib.DataTypeUint256) {
		t.Error("Failed to push element1 to queue11.")
	}
	elem12 := []byte("queue11elem2")
	if !queue.Push(account1, queue1, elem12, concurrentlib.DataTypeUint256) {
		t.Error("Failed to push element2 to queue11.")
	}
	if value, ok := queue.Pop(account1, queue1, concurrentlib.DataTypeUint256); !ok || !bytes.Equal(value, elem11) {
		t.Error("Failed to pop element from queue11.")
	}

	key := getStoredKey(&queueContext, 1)

	if !queue.Create(account1, queue2, concurrentlib.DataTypeUint256) {
		t.Error("Failed to create queue12.")
	}
	if !queue.Create(account2, queue1, concurrentlib.DataTypeUint256) {
		t.Error("Failed to create queue21.")
	}
	elem21 := []byte("queue21elem1")
	if !queue.Push(account2, queue1, elem21, concurrentlib.DataTypeUint256) {
		t.Error("Failed to push element1 to queue21.")
	}
	elem22 := []byte("queue21elem2")
	if !queue.Push(account2, queue1, elem22, concurrentlib.DataTypeUint256) {
		t.Error("Failed to push element2 to queue21.")
	}
	if value, ok := queue.Pop(account2, queue1, concurrentlib.DataTypeUint256); !ok || !bytes.Equal(value, elem21) {
		t.Error("Failed to pop element from queue21.")
	}
	key1 := getStoredKey(&queueContext, 3)

	if !queue.Create(account2, queue2, concurrentlib.DataTypeUint256) {
		t.Error("Failed to create queue22.")
	}

	_, transitions := url.Export(true)
	t.Log("\n" + FormatTransitions(transitions))

	url.Import(transitions)
	url.PostImport()
	url.Precommit([]uint32{1})

	keys, vals := url.Indexer().KVs()
	datas := make([][]byte, len(vals))

	encodeWorker := func(start, end, index int, args ...interface{}) {
		vals := args[0].([]interface{})[0].([]interface{})
		datas := args[0].([]interface{})[1].(*[][]byte)
		for i := start; i < end; i++ {
			(*datas)[i] = vals[i].(urlcommon.UnivalueInterface).Encode()
		}
	}
	common.ParallelWorker(len(vals), 4, encodeWorker, vals, &datas)

	SetValues(db, keys, datas)
	data := GetContainerArray(db, "contractAddress1", arrayID1, 1)
	if !reflect.DeepEqual(value1, data) {
		t.Error("container array set get Error")
	}
	data = GetContainerArray(db, "contractAddress2", arrayID2, 4)
	if !reflect.DeepEqual(value4, data) {
		t.Error("container array set get Error")
	}

	data = GetContainerMap(db, "contractAddress1", map1, []byte("key11"))
	if !reflect.DeepEqual([]byte("value11"), data) {
		t.Error("container map set get Error")
	}

	data = GetContainerMap(db, "contractAddress2", map2, []byte("key22"))
	if !reflect.DeepEqual([]byte("value22"), data) {
		t.Error("container map set get Error")
	}

	data = GetContainerQueue(db, "contractAddress1", queue1, []byte(key))
	if !reflect.DeepEqual(elem12, data) {
		t.Error("container queue set get Error")
	}

	data = GetContainerQueue(db, "contractAddress2", queue1, []byte(key1))
	if !reflect.DeepEqual(elem22, data) {
		t.Error("container queue set get Error")
	}

}

func getStoredKey(context *txContext, count uint32) string {
	b := make([]byte, 8, 16)
	binary.BigEndian.PutUint64(b, context.GetHeight().Uint64())

	bi := make([]byte, 4)
	binary.BigEndian.PutUint32(bi, context.GetIndex())
	b = append(b, bi...)

	bc := make([]byte, 4)
	binary.BigEndian.PutUint32(bc, count)
	b = append(b, bc...)

	return string(b)
}
