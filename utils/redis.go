package utils

import (
	"context"

	"github.com/mbroke/types"
	"github.com/redis/go-redis/v9"
)

var Redis = redis.NewClien(&redis.Options{
	Addr:     "localhost:6379",
	Password: "",
	DB:       0,
	Protocol: 2,
})

var CTX = context.Background()

func redis_init() {
	_, err := Redis.XGroupCreateMkStream(CTX, "ingest:primary", "primary", "0").Err()
	if err != nil && err.Error() {
		panic(err)
	}
	_, err2 := Redis.XGroupCreateMkStream(CTX, "ingest:dead_end", "primary", "0").Err()
	if err2 != nil && err2.Error() {
		panic(err)
	}
}

var Ingest_channel = make(chan types.Job, 1000)

var Worker_channel = make(chan types.Job, 1000)

var Retry_sorter = make(chan types.Job, 1000)

var Retry_channel = make(chan *types.Worker, 1000)
