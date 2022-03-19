package types

import (
	"context"
	"time"

	"github.com/chasex/redis-go-cluster"
)

type RedisClusterDB struct {
	client redis.Cluster
	ctx    context.Context
}

func NewRedisClusterDB(addrs []string) *RedisClusterDB {
	cluster, err := redis.NewCluster(
		&redis.Options{
			StartNodes:   addrs,
			ConnTimeout:  50 * time.Millisecond,
			ReadTimeout:  50 * time.Millisecond,
			WriteTimeout: 50 * time.Millisecond,
			KeepAlive:    16,
			AliveTime:    60 * time.Second,
		})
	if err != nil {
		panic(err)
	}
	return &RedisClusterDB{
		client: cluster,
		ctx:    context.TODO(),
	}
}
func (rdb *RedisClusterDB) Set(key string, value []byte) error {
	_, err := rdb.client.Do("SET", key, value)
	return err
}

func (rdb *RedisClusterDB) Get(key string) ([]byte, error) {
	reply, err := rdb.client.Do("GET", key)
	return reply.([]byte), err
}
func (rdb *RedisClusterDB) BatchGet(keys []string) ([][]byte, error) {
	return nil, nil
}
func (rdb *RedisClusterDB) BatchSet(keys []string, datas [][]byte) error {
	return nil
}
