package workers

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/arcology-network/common-lib/types"
	"github.com/arcology-network/component-lib/rpc"

	"github.com/arcology-network/component-lib/ethrpc"
	evmCommon "github.com/arcology-network/evm/common"
	evmTypes "github.com/arcology-network/evm/core/types"
	"github.com/smallnest/rpcx/client"
)

var xclient client.XClient

func init() {
	xclient = rpc.InitZookeeperRpcClient("storage", []string{"127.0.0.1:2181"})
}

func TestBlockNumber(t *testing.T) {
	request := types.QueryRequest{
		QueryType: types.QueryType_BlockNumber,
	}
	response := types.QueryResult{}

	err := xclient.Call(context.Background(), "Query", &request, &response)
	if err != nil {
		fmt.Printf("err=%v\n", err)
		return
	}
	fmt.Printf("blocknumber=%v\n", response.Data.(uint64))
}

func TestGetTransactionCount(t *testing.T) {
	request := types.QueryRequest{
		QueryType: types.QueryType_TransactionCount,
		Data: types.RequestParameters{
			Number:  int64(2),
			Address: evmCommon.HexToAddress("ab01a3bfc5de6b5fc481e18f274adbdba9b111f0"),
		},
	}
	response := types.QueryResult{}

	err := xclient.Call(context.Background(), "Query", &request, &response)
	if err != nil {
		fmt.Printf("err=%v\n", err)
		return
	}
	fmt.Printf("TransactionCount=%v\n", response.Data.(uint64))
}

func TestGetCode(t *testing.T) {
	request := types.QueryRequest{
		QueryType: types.QueryType_Code,
		Data: types.RequestParameters{
			Number:  int64(10),
			Address: evmCommon.HexToAddress("fbc451fbd7e17a1e7b18347337657c1f2c52b631"),
		},
	}
	response := types.QueryResult{}

	err := xclient.Call(context.Background(), "Query", &request, &response)
	if err != nil {
		fmt.Printf("err=%v\n", err)
		return
	}
	fmt.Printf("Code=%v\n", response.Data.([]byte))
}
func TestGetBalanceEth(t *testing.T) {
	request := types.QueryRequest{
		QueryType: types.QueryType_Balance_Eth,
		Data: &types.RequestParameters{
			Number:  int64(10),
			Address: evmCommon.HexToAddress("057f6a43175a7be4c770e45825658f936cbda423"),
		},
	}
	response := types.QueryResult{}

	err := xclient.Call(context.Background(), "Query", &request, &response)
	if err != nil {
		fmt.Printf("err=%v\n", err)
		return
	}
	fmt.Printf("Balance=%v\n", response.Data.(*big.Int))
}

func TestGetStorage(t *testing.T) {
	request := types.QueryRequest{
		QueryType: types.QueryType_Storage,
		Data: types.RequestStorage{
			Number:  int64(10),
			Address: evmCommon.HexToAddress("fbc451fbd7e17a1e7b18347337657c1f2c52b631"),
			Key:     "0000000000000000000000000000000000000000000000000000000000000003",
		},
	}
	response := types.QueryResult{}

	err := xclient.Call(context.Background(), "Query", &request, &response)
	if err != nil {
		fmt.Printf("err=%v\n", err)
		return
	}
	fmt.Printf("Storage=%v\n", response.Data.([]byte))
}

func TestGetReceipt(t *testing.T) {
	request := types.QueryRequest{
		QueryType: types.QueryType_Receipt_Eth,
		Data:      evmCommon.HexToHash("0x07b14158146fccf1ba801971b9fa395f0d4704310007b9c5cba13c54c9402a3a"),
	}
	response := types.QueryResult{}

	err := xclient.Call(context.Background(), "Query", &request, &response)
	if err != nil {
		fmt.Printf("err=%v\n", err)
		return
	}
	fmt.Printf("Receipt=%v\n", response.Data.(*evmTypes.Receipt))
}

func TestGetTransaction(t *testing.T) {
	request := types.QueryRequest{
		QueryType: types.QueryType_Transaction,
		Data:      evmCommon.HexToHash("0xad4de8a8d8201bdc3b10c2c5453f9d254e902a5b77da67e586547ca21f3cd90f"),
	}
	response := types.QueryResult{}

	err := xclient.Call(context.Background(), "Query", &request, &response)
	if err != nil {
		fmt.Printf("err=%v\n", err)
		return
	}
	fmt.Printf("Transaction=%v\n", response.Data.(*ethrpc.RPCTransaction))
}

func TestGetBlock(t *testing.T) {
	fullTx := false
	request := types.QueryRequest{
		QueryType: types.QueryType_Block_Eth,
		Data: &types.RequestBlockEth{
			Number: int64(24),
			FullTx: fullTx,
		},
	}
	response := types.QueryResult{}

	err := xclient.Call(context.Background(), "Query", &request, &response)
	if err != nil {
		fmt.Printf("err=%v\n", err)
		return
	}

	block := response.Data.(*ethrpc.RPCBlock)
	fmt.Printf("block header=%v\n", *block.Header)
	if fullTx {
		for _, tx := range block.Transactions {
			fmt.Printf("transaction=%v\n", tx.(*evmTypes.Transaction))
		}
	} else {
		for _, tx := range block.Transactions {
			fmt.Printf("transaction hash=%v\n", tx.(evmCommon.Hash))
		}
	}
}
