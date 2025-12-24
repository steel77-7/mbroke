package utils

import (
	"log"

	"github.com/google/uuid"
	"github.com/mbroke/types"
	"github.com/redis/go-redis/v9"
)

var stream string = "ingest:primary"

func Feed() {
	log.SetPrefix("Error in Feeder: ")
	log.SetFlags(0)
	var job types.Job
	args := &redis.XAddArgs{
		Stream: "ingest:primary",
		//	Consumer:  "primary",
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
	args := &redis.XReadGroupArgs{
		Streams:  []string{stream, "0"},
		Consumer: "primary:worker",
		//	Consumer: "primary",
		Count: 100,
	}
	var job types.Job
	for {
		res, err := Redis.XReadGroup(CTX, args).Result()
		if err != nil {
			log.Print("Coudn't read values from redis [Feed to the broker]")
			continue
		}
		for _, msg := range res.Messages {
			job = types.Job{
				ID:     msg.Values["id"].(string),
				Status: false,
				Count:  msg.Values["count"].(int),
				Data:   msg.Values["data"].(string),
				Max:    msg.Values["max"].(int),
			}
			Worker_channel <- job
		}
	}
}

// func Retry() {
// 	// constantly
// 	for {
// 		pendingInfo, err := Redis.XPendingExt(CTX, &redis.XPendingExtArgs{
// 			Stream: "ingest:primary",
// 			Group:  "primary",
// 			Start:  "-",
// 			End:    "+",
// 			Count:  10,
// 		}).Result()
// 		if err != nil {
// 			log.Print("Error in fetching the peding list in retry")
// 			continue
// 		}
// 		for _, p := range pendingInfo {
// 			if p.RetryCount > 5 {
// 				//implement the dead end queue logic here
// 				tbs, _ := Redis.XRange(CTX, "ingest:primary", p.ID, p.ID).Result()
// 				if err != nil {
// 					log.Print("Document could not be fetched")
// 					continue
// 				}
// 				_, err := Redis.XAdd(CTX, &redis.XAddArgs{
// 					Stream: "ingest:dead_end",
// 					Values: tbs.Values,
// 					MaxLen: 10000,
// 				}).Result()
// 				if err != nil {
// 					log.Print("Document could not be fetched")
// 					continue
// 				}
// 			}
// 			_, err := Redis.XRange(CTX, "ingest:primary", p.ID, p.ID).Result()
// 			if err != nil {
// 				log.Print("Document could not be fetched")
// 				continue
// 			}

// 			claim, err := Redis.XClaim(CTX, &redis.XClaimArgs{
// 				Stream:   "ingest:primary",
// 				Group:    "primary",
// 				Consumer: uuid.New(),
// 				Messages: []string{p.ID},
// 			}).Result()

// 			if err != nil {
// 				log.Print("Could not add the")
// 				continue
// 			}
// 			if len(claim) > 0 {
// 				Worker_channel <- claim[0].Values
// 			}

// 		}
// 	}
// }
//
//

// slow down
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
		//checking for dead end
		if pending.RetryCount > 5 {
			_, err := Redis.XAdd(CTX, &redis.XAddArgs{
				Stream: "ingest:dead_end",
				Values: pending.Values,
				MaxLen: 10000,
			}).Result()
			if err != nil {
				log.Print("Couldn'nt add the job to the dead letter queue")
				continue
			}
		}
		_, err2 := Redis.XClaim(CTX, &redis.XClaimArgs{
			Stream:   "ingest:primary",
			Group:    "primary",
			Consumer: uuid.New(),
			Messages: []string{job.Job_id},
		}).Result()

		if err2 != nil {
			log.Print("Couldn'nt claim the job while retrying")
			continue
		}
	}
}
