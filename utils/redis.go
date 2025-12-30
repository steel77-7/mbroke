package utils

import (
	"context"

	"github.com/mbroke/types"
	"github.com/redis/go-redis/v9"
)

var Redis = redis.NewClient(&redis.Options{
	Addr:     "localhost:6379",
	Password: "",
	DB:       0,
	Protocol: 2,
})

var CTX = context.Background()

func Redis_init() {
	// 1. Create primary group
	// Added "0" as the 4th argument
	err := Redis.XGroupCreateMkStream(CTX, "ingest:primary", "primary", "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		panic(err)
	}

	// 2. Create dead_end group
	err2 := Redis.XGroupCreateMkStream(CTX, "ingest:dead_end", "primary", "0").Err()
	if err2 != nil && err2.Error() != "BUSYGROUP Consumer Group name already exists" {
		panic(err2) // Fixed: was panicking with 'err' instead of 'err2'
	}
}

var Ingest_channel = make(chan types.Job, 1000)

var Worker_channel = make(chan types.Job, 1000)

var Retry_channel = make(chan *types.Worker, 1000)
