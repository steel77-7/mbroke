package utils

import (
	"context"

	"github.com/mbroke/types"
	"github.com/redis/go-redis/v9"
)

var Redis = redis.NewClient(&redis.Options{
	Addr:     "localhost:6379",
	PoolSize: 200,
	Password: "",
	DB:       0,
	Protocol: 2,
})

var CTX = context.Background()

func Redis_init() {
	err := Redis.XGroupCreateMkStream(CTX, "ingest:primary", "primary", "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		panic(err)
	}

	err2 := Redis.XGroupCreateMkStream(CTX, "ingest:dead_end", "primary", "0").Err()
	if err2 != nil && err2.Error() != "BUSYGROUP Consumer Group name already exists" {
		panic(err2)
	}
}

var Ingest_channel = make(chan types.Job, 1000)

var Worker_channel = make(chan types.Job, 1000)

var Retry_channel = make(chan *types.Worker, 1000)
