package utils

import (
	"broker/types"
	"log"

	"github.com/redis/go-redis/v9"
)

var stream string = "ingest:primary"

func Feed() {
	log.SetPrefix("Error in Feeder: ")
	log.SetFlags(0)
	var job Job
	args := &redis.XAddArgs{
		Stream: "ingest:primary",
		MaxLen: 20000,
		Values: job,
	}
	for {
		job = <-Ingest_channel //blocking
		res, err := Redis.XAdd(CTX, args).Result()
		if err != nil {
			log.Print("Error in adding the job:" + err)
			continue
		}
	}
}

func Feed_to_broker() {
	log.SetPrefix("Error in Feed_to_broker: ")
	log.SetFlags(0)
	args := &redis.XReadArgs{
		Streams: []string{stream, "0"},
		Count:   100,
	}
	var job types.Job
	for {
		res, err := Redis.XRead(CTX, args).Result()
		if err != nil {
			log.Print("Coudn't read values from redis [Feed to the broker]")
			continue
		}
		for _, msg := range res.Messages {
			job = types.Job{
				ID:    msg.Values["id"].(string),
				Worker_id :,
				Count: msg.Values["count"].(int),
				Data:  msg.Values["data"].(string),
				Max:   msg.Values["max"].(int),
			}
			Worker_channel <- job
		}
	}
}
