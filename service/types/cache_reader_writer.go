package types

import (
	"fmt"
	"math/big"

	ethCommon "github.com/arcology-network/3rd-party/eth/common"
	ethRlp "github.com/arcology-network/3rd-party/eth/rlp"
	ethTypes "github.com/arcology-network/3rd-party/eth/types"
	"github.com/arcology-network/common-lib/common"
	"github.com/arcology-network/common-lib/types"
	urlcommon "github.com/arcology-network/concurrenturl/v2/common"
	cctype "github.com/arcology-network/concurrenturl/v2/type"
	"github.com/arcology-network/concurrenturl/v2/type/commutative"
	"github.com/arcology-network/concurrenturl/v2/type/noncommutative"
)

func SetValues(ds DB, keys []string, values [][]byte) error {
	return ds.BatchSet(keys, values)
}
func GetValues(ds DB, keys []string) [][]byte {
	values, err := ds.BatchGet(keys)
	if err != nil {
		return [][]byte{}
	}
	return values
}

func GetLastRoot(ds DB) ethCommon.Hash {
	data, err := ds.Get(getLastRootPath())
	if err != nil {
		return ethCommon.Hash{}
	}
	return ethCommon.BytesToHash(data)
}

func SaveLastRoot(ds DB, hash ethCommon.Hash) {
	ds.Set(getLastRootPath(), hash.Bytes())
}

func GetLastHash(ds DB) ethCommon.Hash {
	data, err := ds.Get(getLastHashPath())
	if err != nil {
		return ethCommon.Hash{}
	}
	return ethCommon.BytesToHash(data)
}
func SaveLastHash(ds DB, hash ethCommon.Hash) {
	ds.Set(getLastHashPath(), hash.Bytes())
}

func GetLatestHeight(ds DB) uint64 {
	data, err := ds.Get(getLatestHeightPath())
	if err != nil {
		return 0
	}
	return common.BytesToUint64(data)
}

func SaveLatestHeight(ds DB, height uint64) {
	ds.Set(getLatestHeightPath(), common.Uint64ToBytes(height))
}

func GetStatisticInfos(ds DB, height uint64) *types.StatisticalInformation {
	statisticalInformation := types.StatisticalInformation{}
	data, err := ds.Get(getStatisticInfosPath(height))
	if err != nil {
		return nil
	}
	statisticalInformation.Decode(data)
	return &statisticalInformation
}

func SaveStatisticInfos(ds DB, height uint64, statisticInfo *types.StatisticalInformation) {
	ds.Set(getStatisticInfosPath(height), statisticInfo.EncodeToBytes())
}

func GetBlockHashes(ds DB, height uint64) []string {
	datas, err := ds.Get(getBlockHashesPath(height))
	if err != nil {
		return []string{}
	}

	counter := len(datas) / ethCommon.HashLength
	hashes := make([]string, counter)
	for i := range hashes {
		data := datas[i*ethCommon.HashLength : (i+1)*ethCommon.HashLength]
		hashes[i] = fmt.Sprintf("%x", data)
	}
	return hashes
}

func gatherTxs(block *types.MonacoBlock) ([]byte, []ethCommon.Hash, [][]byte) {
	if block == nil || len(block.Txs) == 0 {
		return nil, nil, nil
	}
	counter := len(block.Txs)
	if counter == 0 {
		return nil, nil, nil
	}
	hashs := make([]byte, counter*ethCommon.HashLength)
	rawhashs := make([]ethCommon.Hash, counter)
	txReals := make([][]byte, counter)
	common.ParallelWorker(counter, nthread, hashWorker, block.Txs, &hashs, &rawhashs, &txReals)
	return hashs, rawhashs, txReals
}
func hashWorker(start, end, idx int, args ...interface{}) {
	txs := args[0].([]interface{})[0].([][]byte)
	datas := args[0].([]interface{})[1].(*[]byte)
	hashes := args[0].([]interface{})[2].(*[]ethCommon.Hash)
	txReals := args[0].([]interface{})[3].(*[][]byte)
	for i := start; i < end; i++ {
		tx := txs[i]
		if len(tx) == 0 {
			continue
		}
		txType := tx[0]
		txReal := tx[1:]
		switch txType {
		case types.AppType_Eth:
			otx := new(ethTypes.Transaction)
			if err := ethRlp.DecodeBytes(txReal, otx); err != nil {
				continue
			}

			txhash := ethCommon.RlpHash(otx)
			(*hashes)[i] = txhash
			(*txReals)[i] = txReal
			copy((*datas)[i*ethCommon.HashLength:], txhash.Bytes())
		}
	}
}

func SaveBlockHashes(ds DB, block *types.MonacoBlock) {
	datas, rawhashs, txReals := gatherTxs(block)
	if datas != nil {
		ds.Set(getBlockHashesPath(block.Height), datas)
	}
	for i, hash := range rawhashs {
		ds.Set(getHashtransactionsPath(hash), txReals[i])
	}
}

func GetTransactionByHash(ds DB, hash ethCommon.Hash) *ethTypes.Transaction {
	data, err := ds.Get(getHashtransactionsPath(hash))
	if err != nil {
		return nil
	} else {
		otx := new(ethTypes.Transaction)
		if err := ethRlp.DecodeBytes(data, otx); err != nil {
			return nil
		}
		return otx
	}
}

func GetBlocks(ds DB, height uint64) *types.MonacoBlock {
	data, err := ds.Get(getBlocksPath(height))
	if err != nil {
		return nil
	} else {
		block := types.MonacoBlock{}
		err := block.GobDecode(data)
		if err != nil {
			return nil
		}
		return &block
	}
}

func SaveBlocks(ds DB, height uint64, block *types.MonacoBlock) {
	data, err := block.GobEncode()
	if err != nil {
		return
	}
	ds.Set(getBlocksPath(height), data)
}

func GetReceipt(ds DB, hash ethCommon.Hash) (*ethTypes.Receipt, uint64) {
	datas, err := ds.Get(getReceiptsPath(hash))
	if err != nil || len(datas) == 0 {
		return &ethTypes.Receipt{}, 0
	} else {
		height := common.BytesToUint64(datas[0:8])
		var receipt ethTypes.Receipt
		if err := common.GobDecode(datas[8:], &receipt); err != nil {
			return nil, 0
		}
		return &receipt, height
	}
}

func SaveReceipt(ds DB, height uint64, hash ethCommon.Hash, receipt *ethTypes.Receipt) {
	saveData := common.Uint64ToBytes(height)
	receiptRaw, err := common.GobEncode(*receipt)
	if err != nil {
		return
	}
	saveData = append(saveData, receiptRaw...)
	ds.Set(getReceiptsPath(hash), saveData)
}

func GetExecutingLog(ds DB, hash ethCommon.Hash) string {
	data, err := ds.Get(getExecutinglogPath(hash))
	if err != nil {
		return ""
	} else {
		return string(data)
	}
}

func SaveExecutingLog(ds DB, hash ethCommon.Hash, logs *types.ExecutingLogs) {
	logstr, err := logs.Marshal()
	if err != nil {
		return
	}
	ds.Set(getExecutinglogPath(hash), []byte(logstr))
}

func GetBlockHeightByHash(ds DB, hash ethCommon.Hash) uint64 {
	data, err := ds.Get(getBlockHashHeightPath(hash))
	if err != nil {
		return 0
	} else {
		return common.BytesToUint64(data)
	}
}

func SaveBlockHashHeight(ds DB, hash ethCommon.Hash, height uint64) {
	ds.Set(getBlockHashHeightPath(hash), common.Uint64ToBytes(height))
}

func GetCoinbase(ds DB) ethCommon.Address {
	data, err := ds.Get(getCoinbasePath())
	if err != nil {
		return ethCommon.Address{}
	} else {
		return ethCommon.BytesToAddress(data)
	}
}
func SaveCoinbase(ds DB, addr ethCommon.Address) {
	ds.Set(getCoinbasePath(), addr.Bytes())
}

func GetBalance(ds DB, addr string) *big.Int {
	key := getBalancePath(addr)
	data, err := ds.Get(key)
	if err != nil {
		return big.NewInt(0)
	} else {
		balance := (&cctype.Univalue{}).Decode(data).(urlcommon.UnivalueInterface)
		return balance.Value().(*commutative.Balance).Value().(*big.Int)
	}
}
func GetNonce(ds DB, addr string) int64 {
	data, err := ds.Get(getNoncePath(addr))
	if err != nil {
		return 0
	} else {
		nonce := (&cctype.Univalue{}).Decode(data).(urlcommon.UnivalueInterface)
		return nonce.Value().(*commutative.Int64).Value().(int64)
	}
}
func GetCode(ds DB, addr string) []byte {
	data, err := ds.Get(getCodePath(addr))
	if err != nil {
		return []byte{}
	} else {
		code := (&cctype.Univalue{}).Decode(data).(urlcommon.UnivalueInterface)
		return code.Value().(*noncommutative.Bytes).Data()
	}
}

func GetStorage(ds DB, addr, key string) []byte {
	data, err := ds.Get(getStorageKeyPath(addr, key))
	if err != nil {
		return []byte{}
	} else {
		storage := (&cctype.Univalue{}).Decode(data).(urlcommon.UnivalueInterface)
		return storage.Value().(*noncommutative.Bytes).Data()
	}
}

func GetContainerArray(ds DB, addr, id string, idx int) []byte {
	data, err := ds.Get(getContainerArrayPath(addr, id, idx))
	if err != nil {
		return []byte{}
	} else {
		univalue := (&cctype.Univalue{}).Decode(data).(urlcommon.UnivalueInterface)
		return univalue.Value().(*noncommutative.Bytes).Data()
	}
}

func GetContainerMap(ds DB, addr, id string, key []byte) []byte {
	data, err := ds.Get(getContainerMapPath(addr, id, key))
	if err != nil {
		return []byte{}
	} else {
		univalue := (&cctype.Univalue{}).Decode(data).(urlcommon.UnivalueInterface)
		return univalue.Value().(*noncommutative.Bytes).Data()
	}
}

func GetContainerQueue(ds DB, addr, id string, key []byte) []byte {
	data, err := ds.Get(getContainerQueuePath(addr, id, key))
	if err != nil {
		return []byte{}
	} else {
		univalue := (&cctype.Univalue{}).Decode(data).(urlcommon.UnivalueInterface)
		return univalue.Value().(*noncommutative.Bytes).Data()
	}
}
