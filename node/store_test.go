package node

import (
	"bufio"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arcology/common-lib/extl/cli"
	cmn "github.com/arcology/common-lib/extl/common"
	"github.com/arcology/common-lib/extl/log"
	"github.com/arcology/common-lib/intl/common"
	"github.com/arcology/common-lib/intl/toolkit"
	"github.com/arcology/common-lib/intl/types"
	etypes "github.com/arcology/eth-lib/core/types"
	"github.com/arcology/eth-lib/ethdb"
	"github.com/arcology/eth-lib/rlp"
	"github.com/spf13/viper"
)

func Test_savePlugStorage(t *testing.T) {

	l := log.NewTMLogger(log.NewSyncWriter(os.Stdout))

	rank := "22"

	rootDir := viper.GetString(cli.HomeFlag)

	dbpath := os.ExpandEnv(filepath.Join(rootDir, "storage", "chaindata"+rank))

	if err := cmn.EnsureDir(dbpath, 0777); err != nil {
		cmn.PanicSanity(err.Error())
	}
	lvdb, err := ethdb.NewLDBDatabase(dbpath, 512, 512, l)
	if err != nil {
		l.Error("storage-svc init err", "err", err)
		return
	}

	s := Storage{
		log:     l,
		rawdb:   lvdb,
		allkeys: []byte{},
	}
	//-----------------------------------------------------------------------------

	addrs := getAddrs(20)

	tofLen := 1
	txs := make([]*etypes.Transaction, tofLen)
	hashs := make([]*common.Hash, tofLen)
	nonce := uint64(1)
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	var gaslimit uint64 = 1000000
	idx := 0
	i := idx + 10
	for {
		if idx >= tofLen {
			break
		}

		addrFrom := addrs[i]      //common.BigToAddress(big.NewInt(int64(i)))
		addrTo := addrs[i+tofLen] //common.BigToAddress(big.NewInt(int64(i + tofLen)))

		var payload []byte = []byte{}

		for j := 0; j < random.Intn(100); j++ {
			payload = append(payload, byte(j))
		}

		tx := etypes.NewTransactionTS(
			0,
			nonce,
			*addrFrom,
			*addrTo,
			big.NewInt(random.Int63n(10000)),
			gaslimit,
			big.NewInt(2100),
			payload)
		txs[idx] = tx
		hash := toolkit.RlpHash(tx)
		hashs[idx] = &hash
		idx++
		i++
	}

	ers := make([]*types.EuResult, tofLen)

	//nilroot := common.Hash{}
	actCostgas := uint64(21000)
	for i, tx := range txs {

		ethStorageWrites := map[types.Address]map[common.Hash]common.Hash{}

		addrsFrom := types.Address(tx.From().Bytes())
		ethstorage := map[common.Hash]common.Hash{}
		for j := 0; j < 10; j++ {
			has := common.BigToHash(big.NewInt(int64(j)))
			//fmt.Printf("hash=%v\n", has)
			ethstorage[has] = has
		}
		ethStorageWrites[addrsFrom] = ethstorage
		s.allkeys = append(s.allkeys, tx.From().Bytes()...)

		// addrsTo := types.Address(tx.To().Bytes())
		// ethstorageTo := map[common.Hash]common.Hash{}
		// for j := 10; j < 20; j++ {
		// 	has := common.BigToHash(big.NewInt(int64(j)))
		// 	ethstorageTo[has] = has
		// }
		// ethStorageWrites[addrsTo] = ethstorageTo

		// s.allkeys = append(s.allkeys, tx.To().Bytes()...)

		//fmt.Printf("ethStorageWrites=%v\n", ethStorageWrites)

		w := types.Writes{
			EthStorageWrites: ethStorageWrites,
		}

		er := types.EuResult{
			H:       *hashs[i],
			Status:  etypes.ReceiptStatusSuccessful,
			GasUsed: actCostgas,
			W:       &w,
		}
		ers[i] = &er

	}

	//--------------------------------------------------------------------------------------
	s.UpdatePluginStorage(&ers, 0)
	s.setAllAddress()
	addrss, err := s.getAllAddress()
	if err != nil {
		fmt.Printf("get all address err=%v\n", err)
		return
	}
	for _, addrr := range addrss {
		s.showPluginStorage(addrr)
	}
}

func getAddrs(maxnums int) []*common.Address {
	addrs := []*common.Address{}
	fmt.Println("start get address from file")
	addrfile := "/home/wei/work/af"
	file_r, err := os.OpenFile(addrfile, os.O_RDWR, 0666)
	if err != nil {
		fmt.Println("Open read file error!", err)
		return nil
	}
	defer file_r.Close()
	buf := bufio.NewReader(file_r)
	lineCounter := 0
	for {
		if lineCounter >= maxnums {
			break
		}

		line, err := buf.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Println("File exec finish!")
				break
			} else {
				fmt.Println("Read file error!", err)
				return nil
			}
		}
		//fmt.Printf("line=%v\n", line)
		addr := common.HexToAddress(line)
		addrs = append(addrs, &addr)
		lineCounter++
	}
	fmt.Printf("get address %d,start send transactions at %v\n", lineCounter, time.Now())

	return addrs
}
func Test_rlpAccount(t *testing.T) {
	acct := types.Account{
		Nonce:   uint64(12),
		Balance: new(big.Int).SetInt64(4278000000000000000),
	}
	bys, err := rlp.EncodeToBytes(acct)
	if err != nil {
		fmt.Printf("err=%v\n", err)
		return
	}
	fmt.Printf("bys=%v\n", bys)

	var acc *types.Account
	err = rlp.DecodeBytes(bys, &acc)
	if err != nil {
		fmt.Printf("err=%v\n", err)
		return
	}
	fmt.Printf("acc=%v\n", acc)
}

func Test_rlplstAcct(t *testing.T) {
	acct := types.Account{
		Nonce:   uint64(12),
		Balance: new(big.Int).SetInt64(4278000000000000000),
	}
	ls := latestState{
		Addr: common.BigToAddress(big.NewInt(1)),
		Acc:  acct,
	}
	bys, err := rlp.EncodeToBytes(ls)
	if err != nil {
		fmt.Printf("err=%v\n", err)
		return
	}
	fmt.Printf("bys=%v\n", bys)

	var acc *latestState
	err = rlp.DecodeBytes(bys, &acc)
	if err != nil {
		fmt.Printf("err=%v\n", err)
		return
	}
	fmt.Printf("acc=%v\n", acc)

}

func Test_moveleft(t *testing.T) {
	var a uint64 = 20
	b := a << 16
	fmt.Printf("a=%b,b=%b\n", a, b)
}
