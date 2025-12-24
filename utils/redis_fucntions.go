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

// slow down
// this function will cause redundancy or infinte repeats | all will be dead letter queued if filtering is not used
func Retry() {
	for {
		// set := make(map[]string struct{})
		// for value := range Retry_channel{
		// 	set[value]
		// }
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

func Retry_filter() {
	//filter the retry channel so that no duplication occurs
	set := make(map[string]struct{})
	for {
		set[<-Retry_sorter] = struct{}{}
	}
}
