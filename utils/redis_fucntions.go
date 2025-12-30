package utils

import (
	"log"

	"github.com/mbroke/types"
	"github.com/redis/go-redis/v9"
)

var stream string = "ingest:primary"

// func Feed() {
// 	log.SetPrefix("Error in Feeder: ")
// 	log.SetFlags(0)
// 	var job types.Job
// 	args := &redis.XAddArgs{
// 		Stream: "ingest:primary",
// 		Consumer:  "primary",

// 		MaxLen: 20000,
// 		Values: job,
// 	}
// 	for {
// 		job = <-Ingest_channel //blocking
// 		res, err := Redis.XAdd(CTX, args).Result()
// 		if err != nil {
// 			log.Print("Error in adding the job:" + err)
// 			continue
// 		}
// 	}
// }

func Feed() {
	var job types.Job
	for {
		job = <-Ingest_channel
		tbs := map[string]interface{}{
			"id":   job.ID,
			"data": job.Data,
		}
		args := &redis.XAddArgs{
			Stream: "ingest:primary",
			MaxLen: 20000,
			Values: tbs,
		}
		_, err := Redis.XAdd(CTX, args).Result()
		if err != nil {
			log.Print("Error in adding the job: %v", err)
			continue
		}
		log.Print("Job added")
	}
}

func Feed_to_broker() {

	//var job types.Job
	for {
		args := &redis.XReadGroupArgs{
			Streams:  []string{stream, ">"},
			Group:    "primary",
			Consumer: "",
			Count:    100,
		}
		res, err := Redis.XReadGroup(CTX, args).Result()
		if err != nil {
			log.Print("Coudn't read values from redis [Feed to the broker]:%v ", err)
			continue
		}
		// for _, msg := range res.Messages {
		// 	job = types.Job{
		// 		ID: msg.Values["id"].(string),
		// 		//	Status: false,
		// 		Worker: msg.Values["worker"].(string),
		// 		Count:  msg.Values["count"].(int),
		// 		Data:   msg.Values["data"].(string),
		// 		Max:    msg.Values["max"].(int),
		// 	}
		// 	Worker_channel <- job
		// }

		for _, xStream := range res {
			for _, msg := range xStream.Messages {

				job := types.Job{
					ID:   msg.Values["id"].(string),
					Data: msg.Values["data"].(string),
				}
				Worker_channel <- job
			}
		}
	}
}

// slow down
// this function will cause redundancy or infinte repeats | all will be dead letter queued if filtering is not used
func Retry() {
	for {
		job := <-Retry_channel
		pending, err := Redis.XPendingExt(CTX, &redis.XPendingExtArgs{
			Stream: "ingest-primary",
			Group:  "primary",
			Start:  job.Job_id,
			End:    job.Job_id,
			Count:  1,
		}).Result()
		if err != nil {
			log.Print("Couldn'nt fetch the info for the pending job")
			continue
		}
		res, err := Redis.XRange(CTX, "ingest:primary", pending[0].ID, pending[0].ID).Result()
		if err != nil {
			log.Print("couldnt not fetch the prev job")
			continue
		}
		//checking for dead end
		if pending[0].RetryCount > 5 {
			_, err := Redis.XAdd(CTX, &redis.XAddArgs{
				Stream: "ingest:dead_end",
				Values: res[0].Values,
				MaxLen: 10000,
			}).Result()
			if err != nil {
				log.Print("Couldn'nt add the job to the dead letter queue")
				continue
			}
		}

		Worker_channel <- types.Job{
			ID:   res[0].Values["id"].(string),
			Data: res[0].Values["data"].(string),
		}
		// _, err2 := Redis.XClaim(CTX, &redis.XClaimArgs{
		// 	Stream:   "ingest:primary",
		// 	Group:    "primary",
		// 	Consumer: uuid.New(),
		// 	Messages: []string{job.Job_id},
		// }).Result()

		// if err2 != nil {
		// 	log.Print("Couldn'nt claim the job while retrying")
		// 	continue
	}
}

// func Map_cleanup() {
// 	// var start string = "-"
// 	// var batch_size int64 = 100
// 	for {
// 		pending, err := Redis.XPendingExt(CTX, &redis.XPendingExtArgs{
// 			Stream: "ingest:primary",
// 			Group:  "primary",
// 			Start:  "-",
// 			End:    "+",
// 			Count:  1,

// 		}).Result()
// 		if err!= nil{
// 			log.Print("COulnt fecth the pedning in the Map_ cleanup")
// 			continue
// 		}
// 		if

// 	}
// }
