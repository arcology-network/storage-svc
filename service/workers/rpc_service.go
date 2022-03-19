package workers

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	ethCommon "github.com/arcology-network/3rd-party/eth/common"
	ethRlp "github.com/arcology-network/3rd-party/eth/rlp"
	ethTypes "github.com/arcology-network/3rd-party/eth/types"
	"github.com/arcology-network/common-lib/types"
	"github.com/arcology-network/component-lib/actor"
	mainCfg "github.com/arcology-network/component-lib/config"
	"github.com/arcology-network/component-lib/ethrpc"
	"github.com/arcology-network/component-lib/log"
	evm "github.com/arcology-network/evm"
	evmCommon "github.com/arcology-network/evm/common"
	evmTypes "github.com/arcology-network/evm/core/types"
	storageTypes "github.com/arcology-network/storage-svc/service/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

var (
	receiptRequest = promauto.NewSummary(prometheus.SummaryOpts{
		Name: "storage_receipt_request_process_seconds",
		Help: "The response time of the receipt request.",
	})
)

type RpcService struct {
	actor.WorkerThread
	lastHeight uint64
	datastore  storageTypes.DB //ccurlcommon.DB
	chainID    *big.Int
	caches     *storageTypes.LogCaches
}

//return a Subscriber struct
func NewRpcService(concurrency int, groupid string, ds storageTypes.DB, caches *storageTypes.LogCaches) *RpcService {
	rs := RpcService{}
	rs.Set(concurrency, groupid)
	rs.datastore = ds
	rs.caches = caches

	return &rs
}

func (rs *RpcService) OnStart() {
	rs.chainID = mainCfg.MainConfig.ChainId
}
func (*RpcService) Stop() {}

func (rs *RpcService) OnMessageArrived(msgs []*actor.Message) error {
	for _, v := range msgs {
		switch v.Name {
		case actor.MsgLatestHeight:
			rs.lastHeight = v.Data.(uint64)
		}
	}
	return nil
}

func (rs *RpcService) Query(ctx context.Context, request *types.QueryRequest, response *types.QueryResult) error {
	switch request.QueryType {
	case types.QueryType_LatestHeight:
		response.Data = int(rs.lastHeight)
	case types.QueryType_Nonce:
		request := request.Data.(types.RequestBalance)
		response.Data = uint64(storageTypes.GetNonce(rs.datastore, request.Address))
	case types.QueryType_Balance:
		request := request.Data.(types.RequestBalance)
		response.Data = storageTypes.GetBalance(rs.datastore, request.Address)

	case types.QueryType_RawBlock:
		queryHeight := request.Data.(uint64)
		block := storageTypes.GetBlocks(rs.datastore, queryHeight)
		if block == nil {
			return errors.New("block is nil")
		}
		response.Data = block
	case types.QueryType_Block:
		blockRequest := request.Data.(*types.RequestBlock)
		if blockRequest == nil {
			return errors.New("query params is nil")
		}
		var queryHeight uint64
		if blockRequest.Height < 0 {
			queryHeight = rs.lastHeight
		} else {
			queryHeight = uint64(blockRequest.Height)
		}

		block := storageTypes.GetBlocks(rs.datastore, queryHeight)
		if block == nil {
			return errors.New("block is nil")
		}
		statisticInfo := storageTypes.GetStatisticInfos(rs.datastore, queryHeight)
		if statisticInfo == nil {
			return errors.New("statisticInfo is nil")
		}

		coinbase := ""
		gasUsed := big.NewInt(0)
		hash := ""
		timestamp := 0
		height := 0
		if len(block.Headers) > 0 {
			data := block.Headers[0]
			var header ethTypes.Header
			err := ethRlp.DecodeBytes(data[1:], &header)

			if err != nil {
				rs.AddLog(log.LogLevel_Error, "block header decode err", zap.String("err", err.Error()))
				return err
			}
			coinbase = header.Coinbase.String()
			gasUsed = big.NewInt(int64(header.GasUsed))
			hash = fmt.Sprintf("%x", header.Hash())
			timestamp = int(header.Time.Int64())
			height = int(block.Height)
		}

		queryBlock := types.Block{
			Height:    height,
			Hash:      hash,
			Coinbase:  coinbase,
			Number:    len(block.Txs),
			ExecTime:  statisticInfo.TimeUsed,
			GasUsed:   gasUsed,
			Timestamp: timestamp,
		}
		if blockRequest.Transactions {
			hashes := storageTypes.GetBlockHashes(rs.datastore, queryHeight)
			if hashes != nil {
				queryBlock.Transactions = hashes
			}
		}
		response.Data = queryBlock

	case types.QueryType_Container:
		request := request.Data.(types.RequestContainer)
		key := string(ethCommon.Hex2Bytes(request.Key))
		data := []byte{}
		switch request.Style {
		case types.ConcurrentLibStyle_Array:
			idx, err := strconv.Atoi(key)
			if err != nil {
				return err
			}
			data = storageTypes.GetContainerArray(rs.datastore, request.Address, request.Id, idx)
		case types.ConcurrentLibStyle_Map:
			data = storageTypes.GetContainerMap(rs.datastore, request.Address, request.Id, []byte(key))
		case types.ConcurrentLibStyle_Queue:
			data = storageTypes.GetContainerQueue(rs.datastore, request.Address, request.Id, []byte(key))
		}
		if data == nil {
			return errors.New("load history state err")
		}
		response.Data = data
	case types.QueryType_Receipt:
		start := time.Now()
		requestReceipt := request.Data.(*types.RequestReceipt)
		if requestReceipt == nil {
			return nil
		}
		hashes := requestReceipt.Hashes

		receipts := make([]*types.Receipt, 0, len(hashes))
		for _, hash := range hashes {
			txhash := ethCommon.HexToHash(hash)
			receipt, receiptHeight := storageTypes.GetReceipt(rs.datastore, txhash)
			if receiptHeight == 0 {
				continue
			}
			logs := make([]*types.Log, len(receipt.Logs))
			for j, log := range receipt.Logs {
				topics := make([]string, len(log.Topics))
				for k, topic := range log.Topics {
					topics[k] = fmt.Sprintf("%x", topic.Bytes())
				}
				logs[j] = &types.Log{
					Address:     fmt.Sprintf("%x", log.Address.Bytes()),
					Topics:      topics,
					Data:        fmt.Sprintf("%x", log.Data),
					BlockNumber: log.BlockNumber,
					TxHash:      fmt.Sprintf("%x", log.TxHash.Bytes()),
					TxIndex:     log.TxIndex,
					BlockHash:   fmt.Sprintf("%x", log.BlockHash.Bytes()),
					Index:       log.Index,
				}
			}

			receiptNew := &types.Receipt{
				Status:          int(receipt.Status),
				ContractAddress: fmt.Sprintf("%x", receipt.ContractAddress),
				GasUsed:         big.NewInt(int64(receipt.GasUsed)),
				Logs:            logs,
				Height:          int(receiptHeight),
				SpawnedTxHash:   fmt.Sprintf("%x", receipt.SpawnedTxHash),
			}
			if requestReceipt.ExecutingDebugLogs {
				receiptNew.ExecutingLogs = storageTypes.GetExecutingLog(rs.datastore, txhash)
			}
			receipts = append(receipts, receiptNew)

		}

		receiptRequest.Observe(time.Now().Sub(start).Seconds())
		response.Data = receipts

	//------------------------------------------------for Ethereum rpc api-----------------------------------------
	case types.QueryType_BlockNumber:
		response.Data = rs.lastHeight
	case types.QueryType_TransactionCount:
		request := request.Data.(*types.RequestParameters)
		response.Data = uint64(storageTypes.GetNonce(rs.datastore, fmt.Sprintf("%x", request.Address.Bytes())))
	case types.QueryType_Code:
		request := request.Data.(*types.RequestParameters)
		response.Data = storageTypes.GetCode(rs.datastore, fmt.Sprintf("%x", request.Address.Bytes()))
	case types.QueryType_Balance_Eth:
		request := request.Data.(*types.RequestParameters)
		response.Data = storageTypes.GetBalance(rs.datastore, fmt.Sprintf("%x", request.Address.Bytes()))
	case types.QueryType_Storage:
		request := request.Data.(*types.RequestStorage)
		response.Data = storageTypes.GetStorage(rs.datastore, fmt.Sprintf("%x", request.Address.Bytes()), request.Key)
	case types.QueryType_Receipt_Eth:
		hash := request.Data.(evmCommon.Hash)
		receipt, height := storageTypes.GetReceipt(rs.datastore, ethCommon.BytesToHash(hash.Bytes()))
		if receipt == nil || height == 0 {
			response.Data = nil
			return errors.New("receipt not found")
		}
		receiptRet := evmTypes.Receipt{
			Type:              receipt.Type,
			PostState:         receipt.PostState,
			Status:            receipt.Status,
			CumulativeGasUsed: receipt.CumulativeGasUsed,
			Bloom:             evmTypes.Bloom(receipt.Bloom),
			TxHash:            evmCommon.Hash(receipt.TxHash),
			ContractAddress:   evmCommon.Address(receipt.ContractAddress),
			GasUsed:           receipt.GasUsed,
			BlockHash:         evmCommon.Hash(receipt.BlockHash),
			BlockNumber:       receipt.BlockNumber,
			TransactionIndex:  receipt.TransactionIndex,
		}
		logs := make([]*evmTypes.Log, len(receipt.Logs))
		for i, log := range receipt.Logs {
			topics := make([]evmCommon.Hash, len(log.Topics))
			for j, topic := range log.Topics {
				topics[j] = evmCommon.BytesToHash(topic.Bytes())
			}
			logs[i] = &evmTypes.Log{
				Address:     evmCommon.BytesToAddress(log.Address.Bytes()),
				Topics:      topics,
				Data:        log.Data,
				BlockNumber: log.BlockNumber,
				TxHash:      evmCommon.BytesToHash(log.TxHash.Bytes()),
				TxIndex:     log.TxIndex,
				BlockHash:   evmCommon.BytesToHash(log.BlockHash.Bytes()),
				Index:       log.Index,
				Removed:     log.Removed,
			}
		}
		receiptRet.Logs = logs
		response.Data = &receiptRet
	case types.QueryType_Transaction:
		hash := request.Data.(evmCommon.Hash)
		transaction, err := rs.getTransaction(ethCommon.BytesToHash(hash.Bytes()))
		if err != nil {
			return err
		}
		response.Data = *transaction
	case types.QueryType_Block_Eth:
		request := request.Data.(*types.RequestBlockEth)
		queryHeight := rs.getQueryHeight(request.Number)

		rpcBlock, err := rs.getRpcBlock(queryHeight, request.FullTx)
		if err != nil {
			return err
		}
		response.Data = rpcBlock
	case types.QueryType_BlocByHash:
		request := request.Data.(*types.RequestBlockEth)
		height := storageTypes.GetBlockHeightByHash(rs.datastore, ethCommon.BytesToHash(request.Hash.Bytes()))
		rpcBlock, err := rs.getRpcBlock(height, request.FullTx)
		if err != nil {
			return err
		}
		response.Data = rpcBlock
	case types.QueryType_Logs:
		request := request.Data.(*evm.FilterQuery)
		response.Data = rs.caches.Query(*request)
	case types.QueryType_TxNumsByHash:
		hash := request.Data.(evmCommon.Hash)
		height := storageTypes.GetBlockHeightByHash(rs.datastore, ethCommon.BytesToHash(hash.Bytes()))
		response.Data = rs.getBlockTxs(height)
	case types.QueryType_TxNumsByNumber:
		number := request.Data.(int64)
		height := rs.getQueryHeight(number)
		response.Data = rs.getBlockTxs(height)
	case types.QueryType_TxByHashAndIdx:
		request := request.Data.(*types.RequestBlockEth)
		height := storageTypes.GetBlockHeightByHash(rs.datastore, ethCommon.BytesToHash(request.Hash.Bytes()))
		hashstr := storageTypes.GetBlockHashes(rs.datastore, height)

		if request.Index >= len(hashstr) || request.Index < 0 {
			return errors.New("iddex not found")
		}
		transaction, err := rs.getTransaction(ethCommon.HexToHash(hashstr[request.Index]))
		if err != nil {
			return err
		}
		response.Data = *transaction
	case types.QueryType_TxByNumberAndIdx:
		request := request.Data.(*types.RequestBlockEth)
		height := rs.getQueryHeight(request.Number)
		hashstr := storageTypes.GetBlockHashes(rs.datastore, height)

		if request.Index >= len(hashstr) || request.Index < 0 {
			return errors.New("iddex not found")
		}
		transaction, err := rs.getTransaction(ethCommon.HexToHash(hashstr[request.Index]))
		if err != nil {
			return err
		}
		response.Data = *transaction
	}

	return nil
}
func (rs *RpcService) getTransaction(hash ethCommon.Hash) (*ethrpc.RPCTransaction, error) {
	receipt, _ := storageTypes.GetReceipt(rs.datastore, hash)
	tx := storageTypes.GetTransactionByHash(rs.datastore, hash)
	msg, err := tx.AsMessage(ethTypes.NewEIP155Signer(rs.chainID))
	if err != nil {
		return nil, err
	}
	transactionIndex := uint64(receipt.TransactionIndex)
	v, s, r := tx.RawSignatureValues()
	return &ethrpc.RPCTransaction{
		BlockHash:        evmCommon.Hash(receipt.BlockHash),
		BlockNumber:      receipt.BlockNumber,
		From:             evmCommon.Address(msg.From()),
		Gas:              receipt.GasUsed,
		GasPrice:         tx.GasPrice(),
		Hash:             evmCommon.Hash(receipt.TxHash),
		Input:            msg.Data(),
		Nonce:            tx.Nonce(),
		To:               (*evmCommon.Address)(msg.To()),
		TransactionIndex: &transactionIndex,
		Value:            msg.Value(),
		Type:             uint64(receipt.Type),
		V:                v,
		S:                s,
		R:                r,
	}, nil
}
func (rs *RpcService) getQueryHeight(number int64) uint64 {
	queryHeight := uint64(0)
	if number < 0 {
		switch number {
		case ethrpc.BlockNumberLatest:
			queryHeight = rs.lastHeight
		case ethrpc.BlockNumberPending:
			queryHeight = rs.lastHeight
		case ethrpc.BlockNumberEarliest:
			queryHeight = uint64(0)
		default:
			queryHeight = rs.lastHeight
		}
	} else {
		queryHeight = uint64(number)
	}
	return queryHeight
}
func (rs *RpcService) getBlockTxs(height uint64) int {
	block := storageTypes.GetBlocks(rs.datastore, height)
	txnums := 0
	if block != nil {
		txnums = len(block.Txs)

	}
	return txnums
}
func (rs *RpcService) getRpcBlock(height uint64, fulltx bool) (*ethrpc.RPCBlock, error) {
	block := storageTypes.GetBlocks(rs.datastore, height)

	header := evmTypes.Header{}
	for i := range block.Headers {
		if block.Headers[i][0] != types.AppType_Eth {
			continue
		}

		ethheader := ethTypes.Header{}
		err := ethRlp.DecodeBytes(block.Headers[i][1:], &ethheader)
		if err != nil {
			rs.AddLog(log.LogLevel_Error, "block header decode err", zap.String("err", err.Error()))
			return nil, err
		}
		header = evmTypes.Header{
			ParentHash:  evmCommon.Hash(ethheader.ParentHash),
			Number:      ethheader.Number,
			Time:        ethheader.Time.Uint64(),
			Difficulty:  ethheader.Difficulty,
			Coinbase:    evmCommon.Address(ethheader.Coinbase),
			Root:        evmCommon.Hash(ethheader.Root),
			GasUsed:     ethheader.GasUsed,
			TxHash:      evmCommon.Hash(ethheader.TxHash),
			ReceiptHash: evmCommon.Hash(ethheader.ReceiptHash),
			GasLimit:    ethheader.GasLimit,
		}
	}

	rpcBlock := ethrpc.RPCBlock{
		Header: &header,
	}
	hashstr := storageTypes.GetBlockHashes(rs.datastore, block.Height)

	if fulltx {
		transactions := make([]interface{}, len(hashstr))
		for i := range hashstr {
			tx := storageTypes.GetTransactionByHash(rs.datastore, ethCommon.HexToHash(hashstr[i]))
			if tx.To() == nil {
				transactions[i] = evmTypes.NewContractCreation(tx.Nonce(), tx.Value(), tx.Gas(), tx.GasPrice(), tx.Data())
			} else {
				transactions[i] = evmTypes.NewTransaction(tx.Nonce(), evmCommon.Address(*tx.To()), tx.Value(), tx.Gas(), tx.GasPrice(), tx.Data())
			}
		}
		rpcBlock.Transactions = transactions
	} else {
		hashes := make([]interface{}, len(hashstr))
		for i := range hashstr {
			hashes[i] = evmCommon.HexToHash(hashstr[i])
		}
		rpcBlock.Transactions = hashes
	}
	return &rpcBlock, nil
}
