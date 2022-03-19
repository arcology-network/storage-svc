package types

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	ethCommon "github.com/arcology-network/3rd-party/eth/common"
	urlcommon "github.com/arcology-network/concurrenturl/v2/common"
	cctype "github.com/arcology-network/concurrenturl/v2/type"
	"github.com/arcology-network/concurrenturl/v2/type/commutative"
	"github.com/arcology-network/concurrenturl/v2/type/noncommutative"
)

const (
	nthread = 4
)

var BASE_URL string

func init() {
	BASE_URL = urlcommon.NewPlatform().Eth10()
}

func getBalancePath(addr string) string {
	paths, _, _ := urlcommon.NewPlatform().Builtin(BASE_URL, addr)
	return paths[3]
}
func getNoncePath(addr string) string {
	paths, _, _ := urlcommon.NewPlatform().Builtin(BASE_URL, addr)
	return paths[2]
}
func getCodePath(addr string) string {
	paths, _, _ := urlcommon.NewPlatform().Builtin(BASE_URL, addr)
	return paths[1]
}

func getStorageKeyPath(addr, key string) string {
	paths, _, _ := urlcommon.NewPlatform().Builtin(BASE_URL, addr)
	if !strings.HasPrefix(key, "0x") {
		key = "0x" + key
	}
	return paths[7] + key
}

func getContainerValuePath(addr, id string, key interface{}) string {
	paths, _, _ := urlcommon.NewPlatform().Builtin(BASE_URL, addr)
	return fmt.Sprintf("%s%v", paths[6]+id+"/", key)
}
func getContainerStoredKey(key []byte) string {
	return "$" + hex.EncodeToString(key)
}
func getContainerArrayPath(addr, id string, idx int) string {
	return getContainerValuePath(addr, id, idx)
}
func getContainerMapPath(addr, id string, key []byte) string {
	return getContainerValuePath(addr, id, getContainerStoredKey(key))
}
func getContainerQueuePath(addr, id string, key []byte) string {
	return getContainerValuePath(addr, id, getContainerStoredKey(key))
}

func getLastHashPath() string {
	return BASE_URL + "lastHash/"
}

func getLastRootPath() string {
	return BASE_URL + "lastRoot/"
}

func getLatestHeightPath() string {
	return BASE_URL + "latestHeight/"
}

func getStatisticInfosPath(height uint64) string {
	return BASE_URL + "statisticInfos/" + HeightToStr(height)
}

func getBlockHashesPath(height uint64) string {
	return BASE_URL + "blockHashes/" + HeightToStr(height)
}

func getBlocksPath(height uint64) string {
	return BASE_URL + "blocks/" + HeightToStr(height)
}

func getReceiptsPath(hash ethCommon.Hash) string {
	return BASE_URL + "receipts/" + HashToStr(hash)
}

func getExecutinglogPath(hash ethCommon.Hash) string {
	return BASE_URL + "executingLogs/" + HashToStr(hash)
}

func getBlockHashHeightPath(hash ethCommon.Hash) string {
	return BASE_URL + "blockhashHeights/" + HashToStr(hash)
}

func getHashtransactionsPath(hash ethCommon.Hash) string {
	return BASE_URL + "hashtransactions/" + HashToStr(hash)
}

func getCoinbasePath() string {
	return BASE_URL + "coinbase"
}

func HeightToStr(height uint64) string {
	return fmt.Sprintf("%v", height)
}

func HashToStr(hash ethCommon.Hash) string {
	return fmt.Sprintf("%x", hash.Bytes())
}

func FormatTransitions(transitions []urlcommon.UnivalueInterface) string {
	var str string
	for _, t := range transitions {
		str += fmt.Sprintf("[%v:%v,%v,%v,%v]%s%s\n", t.(*cctype.Univalue).GetTx(), t.(*cctype.Univalue).Reads(), t.(*cctype.Univalue).Writes(), t.(*cctype.Univalue).Preexist(), t.(*cctype.Univalue).Composite(), *(t.(*cctype.Univalue).GetPath()), formatValue(t.(*cctype.Univalue).Value()))
	}
	return str
}
func formatValue(value interface{}) string {
	switch value.(type) {
	case *commutative.Meta:
		meta := value.(*commutative.Meta)
		var str string
		str += "{"
		for i, k := range meta.PeekKeys() {
			str += k
			if i != len(meta.PeekKeys())-1 {
				str += ", "
			}
		}
		str += "}"
		if len(meta.PeekAdded()) != 0 {
			str += " + {"
			for i, k := range meta.PeekAdded() {
				str += k
				if i != len(meta.PeekAdded())-1 {
					str += ", "
				}
			}
			str += "}"
		}
		if len(meta.PeekRemoved()) != 0 {
			str += " - {"
			for i, k := range meta.PeekRemoved() {
				str += k
				if i != len(meta.PeekRemoved())-1 {
					str += ", "
				}
			}
			str += "}"
		}
		return str
	case *noncommutative.Int64:
		return fmt.Sprintf(" = %v", int64(*value.(*noncommutative.Int64)))
	case *noncommutative.Bytes:
		return fmt.Sprintf(" = %v", value.(*noncommutative.Bytes).Data())
	case *commutative.Balance:
		v := value.(*commutative.Balance).Value()
		d := value.(*commutative.Balance).GetDelta()
		return fmt.Sprintf(" = %v + %v", v.(*big.Int).Uint64(), d.(*big.Int).Int64())
	case *commutative.Int64:
		v := value.(*commutative.Int64).Value()
		d := value.(*commutative.Int64).GetDelta()
		return fmt.Sprintf(" = %v + %v", v, d)
	}
	return ""
}
