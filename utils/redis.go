package utils

import (
	"context"

	"github.com/mbroke/types"
	"github.com/redis/go-redis/v9"
)

var Redis *redis.Client
var CTX = context.Background()

func Redis_init() {
	Redis = redis.NewClient(&redis.Options{
		Addr:     RedConf.Addr,
		PoolSize: RedConf.PoolSize,
		Password: RedConf.Password,
		DB:       RedConf.DB,
		Protocol: RedConf.Protocol,
	})
	err := Redis.XGroupCreateMkStream(CTX, Conf.StreamName, Conf.ConsumerGroupName, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		panic(err)
	}

	err2 := Redis.XGroupCreateMkStream(CTX, Conf.DeadLetterName, Conf.ConsumerGroupName, "0").Err()
	if err2 != nil && err2.Error() != "BUSYGROUP Consumer Group name already exists" {
		panic(err2)
	}
}

var Ingest_channel = make(chan types.Job, 10000)
var Worker_inquiry_channel = make(chan string, 10000)
var Worker_feeder_channel = make(chan types.WorkerFeeding, 10000)

var Worker_channel = make(chan types.Job, 10000)
var IngesterChannel = make(chan types.Job, 100000)
var Dead_letter_channel = make(chan types.Job, 10000)
var ACK_channel = make(chan string, 10000)
var Del_channel = make(chan string, 10000)
