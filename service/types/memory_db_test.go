package types

import (
	"fmt"
	"reflect"
	"testing"
)

func TestMemoryDB(t *testing.T) {
	db := NewMemoryDB()

	key := "d7f041a84ec2cb8d9e298ab00bed55158e3f29f0020348842d0c3681e1faef89"
	address := "66812f708302896954212d553f4cf11dc8859e32"
	db.Set(key, []byte(address))
	addr, err := db.Get(key)
	if err != nil {
		t.Error(" get Error")
		return
	}
	if !reflect.DeepEqual(addr, []byte(address)) {
		t.Error("set get Error")
		return
	}

	batchNum := 3
	keys := make([]string, batchNum)
	for i := 0; i < batchNum; i++ {
		keys[i] = fmt.Sprintf("%v_%v", key, i)
	}

	datas := make([][]byte, batchNum)
	for i := 0; i < batchNum; i++ {
		addr := fmt.Sprintf("%v_%v", address, i)
		datas[i] = []byte(addr)
	}
	db.BatchSet(keys, datas)
	vals, err := db.BatchGet(keys)
	if err != nil {
		t.Error(" batch get Error")
		return
	}
	if !reflect.DeepEqual(datas, vals) {
		t.Error("set get Error")
		return
	}
}
