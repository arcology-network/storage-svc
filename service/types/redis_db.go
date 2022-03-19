package types

import (
	"context"
	"time"

	"github.com/arcology-network/common-lib/common"
	"github.com/go-redis/redis/v8"
)

type RedisDB struct {
	client *redis.Client
	// client *redis.ClusterClient
	ctx context.Context
}

func NewRedisDB(addrs []string) *RedisDB {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addrs[0],
		Password: "", // no password set
		DB:       0,  // use default DB

		PoolSize:    50,
		PoolTimeout: 30 * time.Second,
	})

	// rdb := redis.NewClusterClient(&redis.ClusterOptions{
	// 	Addrs:        addrs,
	// 	DialTimeout:  100 * time.Microsecond,
	// 	ReadTimeout:  100 * time.Microsecond,
	// 	WriteTimeout: 100 * time.Microsecond,

	// 	PoolSize:    50,
	// 	PoolTimeout: 30 * time.Second,
	// })
	status := rdb.Ping(context.TODO())
	if status.Err() != nil {
		panic(status.Err().Error())
	}
	return &RedisDB{
		client: rdb,
		ctx:    context.TODO(),
	}
}

func (rdb *RedisDB) Set(key string, value []byte) error {
	return rdb.client.Set(rdb.ctx, key, value, 0).Err()
}

func (rdb *RedisDB) Get(key string) ([]byte, error) {
	return rdb.client.Get(rdb.ctx, key).Bytes()
}

func (rdb *RedisDB) BatchGet(keys []string) ([][]byte, error) {
	counter := len(keys)
	results := make([][]byte, counter)
	eers := make([]error, counter)
	getWorker := func(start, end, idx int, args ...interface{}) {
		ks := args[0].([]interface{})[0].([]string)
		vs := args[0].([]interface{})[1].(*[][]byte)
		es := args[0].([]interface{})[2].(*[]error)
		for i := start; i < end; i++ {
			data, err := rdb.client.Get(rdb.ctx, ks[i]).Bytes()
			if err != nil {
				(*es)[i] = err
				continue
			}
			(*vs)[i] = data
		}
	}
	//nthread := rdb.client.Options().PoolSize
	common.ParallelWorker(counter, nthread, getWorker, keys, &results, &eers)
	return results, nil
}
func (rdb *RedisDB) BatchSet(keys []string, datas [][]byte) error {
	counter := len(keys)
	eers := make([]error, counter)
	setWorker := func(start, end, idx int, args ...interface{}) {
		ks := args[0].([]interface{})[0].([]string)
		vs := args[0].([]interface{})[1].([][]byte)
		es := args[0].([]interface{})[2].(*[]error)
		for i := start; i < end; i++ {
			(*es)[i] = rdb.client.Set(rdb.ctx, ks[i], vs[i], 0).Err()
		}
	}
	//nthread := rdb.client.Options().PoolSize
	common.ParallelWorker(counter, nthread, setWorker, keys, datas, &eers)
	for i := range eers {
		if eers[i] != nil {
			return eers[i]
		}
	}
	return nil
}
