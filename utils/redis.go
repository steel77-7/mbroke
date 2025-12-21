package utils

import (
	"log"
	"broker/types"
	"sync"
	"github.com/redis/go-redis/v9"
)


Redis := redis.NewClien(&redis.Options{
	Addr:"localhost:6379" ,
	Password:"",
	DB:0,
	Protocol : 2
})

CTX := context.Background()

Ingest_channel := make (chan types.Job , 1000)

Worker_channel := make(chan types.Job , 1000)
