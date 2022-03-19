package workers

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/arcology-network/common-lib/types"
)

func TestLatestHeight(t *testing.T) {
	request := types.QueryRequest{
		QueryType: types.QueryType_LatestHeight,
	}
	response := types.QueryResult{}

	err := xclient.Call(context.Background(), "Query", &request, &response)
	if err != nil {
		fmt.Printf("err=%v\n", err)
		return
	}
	fmt.Printf("latest height=%v\n", response.Data.(int))
}

func TestGetNonce(t *testing.T) {
	request := types.QueryRequest{
		QueryType: types.QueryType_Nonce,
		Data: types.RequestBalance{
			Height:  2,
			Address: "f3f4c7ffb78b501aa44ded632260bf49ad762b13",
		},
	}
	response := types.QueryResult{}

	err := xclient.Call(context.Background(), "Query", &request, &response)
	if err != nil {
		fmt.Printf("err=%v\n", err)
		return
	}
	fmt.Printf("nonce=%v\n", response.Data.(uint64))
}

func TestGetBalance(t *testing.T) {
	request := types.QueryRequest{
		QueryType: types.QueryType_Balance,
		Data: types.RequestBalance{
			Height:  2,
			Address: "f3f4c7ffb78b501aa44ded632260bf49ad762b13",
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

func TestGetRawBlock(t *testing.T) {
	request := types.QueryRequest{
		QueryType: types.QueryType_RawBlock,
		Data:      uint64(20),
	}
	response := types.QueryResult{}

	err := xclient.Call(context.Background(), "Query", &request, &response)
	if err != nil {
		fmt.Printf("err=%v\n", err)
		return
	}
	fmt.Printf("RawBlock=%v\n", response.Data.(*types.MonacoBlock))
}

func TestGetAmmoliteBlock(t *testing.T) {
	getTransactions := true
	request := types.QueryRequest{
		QueryType: types.QueryType_Block,
		Data: &types.RequestBlock{
			Height:       15,
			Transactions: getTransactions,
		},
	}
	response := types.QueryResult{}

	err := xclient.Call(context.Background(), "Query", &request, &response)
	if err != nil {
		fmt.Printf("err=%v\n", err)
		return
	}
	fmt.Printf("ammolite block=%v\n", response.Data.(types.Block))
}

func TestGetContainer(t *testing.T) {
	request := types.QueryRequest{
		QueryType: types.QueryType_Container,
		Data: types.RequestContainer{
			Height:  10,
			Address: "",
			Id:      "",
			Style:   "",
			Key:     "",
		},
	}
	response := types.QueryResult{}

	err := xclient.Call(context.Background(), "Query", &request, &response)
	if err != nil {
		fmt.Printf("err=%v\n", err)
		return
	}
	fmt.Printf("container=%v\n", response.Data.([]byte))
}

func TestGetReceipts(t *testing.T) {
	request := types.QueryRequest{
		QueryType: types.QueryType_Receipt,
		Data: types.RequestReceipt{
			Hashes:             []string{"4e5561643a98fa4c9f5963e1f52e7da823564fd3897255790947cc1f8ff23778"},
			ExecutingDebugLogs: true,
		},
	}
	response := types.QueryResult{}

	err := xclient.Call(context.Background(), "Query", &request, &response)
	if err != nil {
		fmt.Printf("err=%v\n", err)
		return
	}
	receipts := response.Data.([]*types.Receipt)
	for i := range receipts {
		fmt.Printf("receipt=%v\n", *receipts[i])
	}

}
