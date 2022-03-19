package types

import (
	"encoding/hex"
	"fmt"
	"testing"
	"time"
)

func BenchmarkRedis(b *testing.B) {
	db := initDb()
	data, _ := hex.DecodeString("1459be9d87e5ce44b771914d17e2209f0fd35636051dc87216a050084a71b6a0")
	//data := ethCommon.HexToHash("1459be9d87e5ce44b771914d17e2209f0fd35636051dc87216a050084a71b6a0").Bytes()
	key := "d7f041a84ec2cb8d9e298ab00bed55158e3f29f0020348842d0c3681e1faef89"
	address := "66812f708302896954212d553f4cf11dc8859e32"
	keypath := getStorageKeyPath(address, key)
	//keypath := "d7f041a84ec2cb8d9e298ab00bed55158e3f29f0020348842d0c3681e1faef89"
	size := 100000
	keys := make([]string, size)

	for i := range keys {
		keys[i] = fmt.Sprintf("%v%v", keypath, i)
	}

	starttime := time.Now()

	for i := 0; i < size; i++ {
		db.Set(keys[i], data)
	}

	fmt.Printf("Set %v time used: %v\n", size, time.Since(starttime))

	starttime = time.Now()

	for i := 0; i < size; i++ {
		db.Get(keys[i])
	}

	fmt.Printf("Get %v time used: %v\n", size, time.Since(starttime))
}
