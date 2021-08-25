package node

import (
	"github.com/HPISTechnologies/common-lib/intl/common"
	"github.com/HPISTechnologies/common-lib/intl/types"
	"github.com/HPISTechnologies/eth-lib/rlp"
)

//--------------------------------------------------------------------------------
// get all addrs at init
func (s *Storage) getAllAddress() ([]common.Address, error) {
	data, err := s.rawdb.Get(allkeys)

	if err != nil {
		//fmt.Println(err)
		return nil, err
	}
	s.allkeys = data
	//types.AddressLength
	addrCounts := len(data) / types.AddressLength
	addrs := make([]common.Address, addrCounts)
	for i := 0; i < addrCounts; i++ {
		addData := data[i*types.AddressLength : (i+1)*types.AddressLength]
		addrs[i] = common.BytesToAddress(addData)
	}

	return addrs, nil
}

// save all address into db as one key
func (s *Storage) setAllAddress() error {

	return s.rawdb.Put(allkeys, s.allkeys)
}

// get last state info
func (s *Storage) getLatestState(key common.Address) (*latestState, error) {

	enc, err := s.rawdb.Get(latestKey(StorageType_Account, key))
	if err != nil {
		//fmt.Printf("-----getLastState err:%v\n", err)
		return nil, err
	}
	//fmt.Printf("enc=%x\n", enc)
	if len(enc) == 0 {
		return nil, nil
	}

	var data latestState
	if err := rlp.DecodeBytes(enc, &data); err != nil {
		return nil, err
	}

	return &data, nil
}

// get last code info
func (s *Storage) getLatestCode(key common.Address) (*latestCode, error) {

	enc, err := s.rawdb.Get(latestKey(StorageType_Code, key))
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	var data latestCode
	if err := rlp.DecodeBytes(enc, &data); err != nil {
		return nil, err
	}

	return &data, nil
}

// get last concurrent-lib strorage info
func (s *Storage) getLatestCls(key common.Address) (*latestCls, error) {

	enc, err := s.rawdb.Get(latestKey(StorageType_ConcurrentlibStorage, key))
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}

	var data latestCls
	if err := rlp.DecodeBytes(enc, &data); err != nil {
		return nil, err
	}

	return &data, nil
}

// get last plugin strorage info
func (s *Storage) getLatestPluginStorage(key common.Address) (*latestPluginStorage, error) {

	enc, err := s.rawdb.Get(latestKey(StorageType_PluginStorage, key))
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}

	var data latestPluginStorage
	if err := rlp.DecodeBytes(enc, &data); err != nil {
		return nil, err
	}

	return &data, nil
}

// get history concurrent-lib strorage info
func (s *Storage) getHisCls(key common.Address, height uint64) (*historyCls, error) {

	enc, err := s.rawdb.Get(historyKeys(StorageType_ConcurrentlibStorage, height, key))
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}

	var data historyCls
	if err := rlp.DecodeBytes(enc, &data); err != nil {
		return nil, err
	}

	return &data, nil
}

// get history plugin strorage info
func (s *Storage) getHisPluginStorage(key common.Address, height uint64) (*historyPluginStorage, error) {

	enc, err := s.rawdb.Get(historyKeys(StorageType_PluginStorage, height, key))
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}

	var data historyPluginStorage
	if err := rlp.DecodeBytes(enc, &data); err != nil {
		return nil, err
	}

	return &data, nil
}

func (s *Storage) setData(key, data []byte) error {
	return s.rawdb.Put(key, data)
}
func (s *Storage) getData(key []byte) []byte {
	enc, err := s.rawdb.Get(key)
	if err != nil {
		return nil
	}
	if len(enc) == 0 {
		return nil
	}
	return enc
}

var (
	latestStatePrefix  = []byte("l")       // lastStatePrefix + account address -> current state of account
	historyStatePrefix = []byte("h")       // historyStatePrefix + account address+ height (uint64 big endian)  -> history state of account
	allkeys            = []byte("allkeys") //save all address
	lastheight         = []byte("height")
	lasthash           = []byte("hash")
	lastroot           = []byte("root")

	//code prefix
	latestCodePrefix  = []byte("lc") // lastCodePrefix + account address -> code
	historyCodePrefix = []byte("hc") // historyCodePrefix + account address+ height (uint64 big endian)  -> history code

	//concurrentlib storage
	latestClsPrefix  = []byte("lsc") // lastClsPrefix + account address -> val   //all last keys and heights
	historyClsPrefix = []byte("hsc") // historyClsPrefix + account address+ height (uint64 big endian)  -> val  //all current keys and heights

	// lastClsPrefix +cclDataType_Array         + account address +id  +index-> val
	//  lastClsPrefix +cclDataType_HashMap+ account address +id  +key    -> val
	//  lastClsPrefix +cclDataType_Counter   + account address +id  -> val
	//latestClsItemPrefix = []byte("lsci") //last  keys items prefix

	//  historyClsItemPrefix + height (uint64 big endian)+cclDataType_Array           + account address +id  +index-> val
	//  historyClsItemPrefix + height (uint64 big endian)+cclDataType_HashMap  + account address +id  +key    -> val
	//  historyClsItemPrefix + height (uint64 big endian)+cclDataType_Counter     + account address +id  -> val
	historyClsItemPrefix = []byte("hsci") //hsitory keys items prefix

	//meta data key prefix
	metaPrefix = []byte("m")

	//plugin-storage
	latestPluginStoragePrefix  = []byte("les") // latestPluginStoragePrefix + account address -> val   //all last keys and heights
	historyPluginStoragePrefix = []byte("hes") // historyPluginStoragePrefix + account address+ height (uint64 big endian)  -> val  //all current keys and heights

	latestPluginStorageItemPrefix  = []byte("lesi") // latestPluginStorageItemPrefix + account address +key -> val
	historyPluginStorageItemPrefix = []byte("hesi") //historyPluginStorageItemPrefix + height (uint64 big endian)+ account address +key

)

const (
	cclDataType_Array   = byte(0)
	cclDataType_HashMap = byte(1)
	cclDataType_Counter = byte(2)

	StorageType_Account              = byte(0)
	StorageType_Code                 = byte(1)
	StorageType_ConcurrentlibStorage = byte(2)
	StorageType_PluginStorage        = byte(3)
)

// get metadata key
//metaPrefix +cclDataType_Array          + account address +id
//metaPrefix +cclDataType_HashMap  + account address +id
func metaKey(dataType byte, addr common.Address, id string) []byte {
	keys := []byte{}
	keys = append(keys, metaPrefix...)
	keys = append(keys, dataType)
	keys = append(keys, addr.Bytes()...)
	keys = append(keys, id...)
	return keys
}

// get last  keys
// latestStatePrefix + account address -> val
// latestCodePrefix + account address -> val
// latestClsPrefix + account address -> val   //all last keys and heights
// latestEthPrefix + account address -> val
func latestKey(elementType byte, addr common.Address) []byte {

	keys := []byte{}
	switch elementType {
	case StorageType_Account:
		keys = append(keys, latestStatePrefix...)
	case StorageType_Code:
		keys = append(keys, latestCodePrefix...)
	case StorageType_ConcurrentlibStorage:
		keys = append(keys, latestClsPrefix...)
	case StorageType_PluginStorage:
		keys = append(keys, latestPluginStoragePrefix...)
	}
	keys = append(keys, addr.Bytes()...)
	return keys

}

// get history keys
// historyCodePrefix + account address+ height (uint64 big endian)
// historyStatePrefix + account address+ height (uint64 big endian)
// historyClsPrefix + account address+ height (uint64 big endian)  -> val  //all current keys and heights
// historyEthPrefix + account address+ height (uint64 big endian)  -> val
func historyKeys(elementType byte, height uint64, addr common.Address) []byte {
	keys := []byte{}

	switch elementType {
	case StorageType_Account:
		keys = append(keys, historyStatePrefix...)
	case StorageType_Code:
		keys = append(keys, historyCodePrefix...)
	case StorageType_ConcurrentlibStorage:
		keys = append(keys, historyClsPrefix...)
	case StorageType_PluginStorage:
		keys = append(keys, historyPluginStoragePrefix...)
	}

	keys = append(keys, addr.Bytes()...)
	keys = append(keys, common.Uint64ToBytes(uint64(height))...)
	return keys

}

//get last ccls item key for array
// 	lastClsPrefix                                                                           +cclDataType_Array         + account address +id  +index-> val
//  historyClsItemPrefix + height (uint64 big endian)+cclDataType_Array         + account address +id  +index-> val
func getClsItemArrayKey(islast bool, addr common.Address, id string, idx int, height uint64) []byte {

	keys := []byte{}
	if islast {
		keys = append(keys, latestClsPrefix...)
	} else {
		keys = append(keys, historyClsItemPrefix...)
		keys = append(keys, common.Uint64ToBytes(height)...)
	}

	keys = append(keys, cclDataType_Array)
	keys = append(keys, addr.Bytes()...)
	keys = append(keys, id...)
	keys = append(keys, common.Uint64ToBytes(uint64(idx))...)
	return keys
}

//get last ccls item key for hashmap
// 	lastClsPrefix                                                                           +cclDataType_HashMap + account address  +id  +key    -> val
//  historyClsItemPrefix + height (uint64 big endian)+cclDataType_HashMap  + account address +id  +key    -> val
func getClsItemHashMapKey(islast bool, addr common.Address, id string, key string, height uint64) []byte {

	keys := []byte{}
	if islast {
		keys = append(keys, latestClsPrefix...)
	} else {
		keys = append(keys, historyClsItemPrefix...)
		keys = append(keys, common.Uint64ToBytes(height)...)
	}

	keys = append(keys, cclDataType_HashMap)
	keys = append(keys, addr.Bytes()...)
	keys = append(keys, id...)
	keys = append(keys, key...)
	return keys
}

//get last ccls item key for counter
//  lastClsPrefix                                                                           +cclDataType_Counter     + account address +id  -> val
//  historyClsItemPrefix + height (uint64 big endian)+cclDataType_Counter     + account address +id  -> val
func getClsItemCounterKey(islast bool, addr common.Address, id string, height uint64) []byte {

	keys := []byte{}
	if islast {
		keys = append(keys, latestClsPrefix...)
	} else {
		keys = append(keys, historyClsItemPrefix...)
		keys = append(keys, common.Uint64ToBytes(height)...)
	}

	keys = append(keys, cclDataType_Counter)
	keys = append(keys, addr.Bytes()...)
	keys = append(keys, id...)
	return keys
}

// latestPluginStorageItemPrefix  = []byte("lesi") // latestPluginStorageItemPrefix + account address +key -> val
// 	historyPluginStorageItemPrefix = []byte("hesi") //historyPluginStorageItemPrefix + height (uint64 big endian)+ account address +key
func getPluginStorageItemKey(isLast bool, addr common.Address, key []byte, height uint64) []byte {
	keys := []byte{}
	if isLast {
		keys = append(keys, latestPluginStorageItemPrefix...)
		keys = append(keys, addr.Bytes()...)
		keys = append(keys, key...)
	} else {
		keys = append(keys, historyPluginStorageItemPrefix...)
		keys = append(keys, common.Uint64ToBytes(height)...)
		keys = append(keys, addr.Bytes()...)
		keys = append(keys, key...)
	}
	return key
}

//current state will be asved in db
type latestState struct {
	Addr   common.Address //this is key of lastState
	Height []uint64       //block height of change
	Acc    types.Account
}

type latestCode struct {
	Addr   common.Address //this is key of lastState
	Height []uint64       //block height of change
	Code   []byte
}

// last concurrent-lib sorage
type latestCls struct {
	Addr     common.Address //this is key of lastState
	Height   []uint64       //block height of change
	Keys     [][]byte       //allkeys of this account at last height
	MetaKeys [][]byte       //all metakeys
}

// history concurrent-lib sorage
type historyCls struct {
	Addr common.Address //this is key of current
	Keys [][]byte       //allkeys of this account at current height
}

// last plugin sorage
type latestPluginStorage struct {
	Addr   common.Address //this is key of lastState
	Height []uint64       //block height of change
	Keys   [][]byte       //allkeys of this account at last height
}

// history plugin sorage
type historyPluginStorage struct {
	Addr common.Address //this is key of current
	Keys [][]byte       //allkeys of this account at current height
}
