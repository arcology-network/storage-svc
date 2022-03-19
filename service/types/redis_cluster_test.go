package types

import (
	"testing"
)

func TestRedisCLuster(t *testing.T) {
	/*
		addrs := "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005,127.0.0.1:7006"

		cluster, err := redis.NewCluster(
			&redis.Options{
				StartNodes:   strings.Split(addrs, ","),
				ConnTimeout:  50 * time.Millisecond,
				ReadTimeout:  50 * time.Millisecond,
				WriteTimeout: 50 * time.Millisecond,
				KeepAlive:    16,
				AliveTime:    60 * time.Second,
			})

		if err != nil {
			log.Fatalf("redis.New error: %s", err.Error())
		}

		cluster.Do("SET", "aaa", "1212122")

		data, err := cluster.Do("GET", "aaa")
		if err != nil {
			fmt.Printf("err=%v\n", err)
			return
		}
		fmt.Printf("data=%v\n", data)
	*/
}
