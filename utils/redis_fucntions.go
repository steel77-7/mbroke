package utils

import (
	"log"

	"github.com/mbroke/types"
	"github.com/redis/go-redis/v9"
)

var stream string = "ingest:primary"

func Feed(job types.Job) { //this will be in the ingest
	//var job types.Job
	//	job = <-Ingest_channel
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
	}
	log.Print("Job added")
}

// func Feed_to_broker() { //this will be in the worker feeding

// 	//var job types.Job
// 	for {
// 		args := &redis.XReadGroupArgs{
// 			Streams:  []string{stream, ">"},
// 			Group:    "primary",
// 			Consumer: "",
// 			Count:    100,
// 		}
// 		res, err := Redis.XReadGroup(CTX, args).Result()
// 		if err != nil {
// 			log.Print("Coudn't read values from redis [Feed to the broker]:%v ", err)
// 			continue
// 		}

// 		for _, xStream := range res {
// 			for _, msg := range xStream.Messages {

//					job := types.Job{
//						ID:   msg.Values["id"].(string),
//						Data: msg.Values["data"].(string),
//					}
//					Worker_channel <- job
//				}
//			}
//		}
//	}
//

func ACK(id string) bool {
	if err := Redis.XAck(CTX, stream, "primary", id).Err(); err != nil {
		if err := Redis.XDel(CTX, stream, id); err != nil {
			return true
		}
	}

	return false
}
func Feed_to_worker(id string) *redis.XMessage { //this will be in the worker feeding
	log.Print("Worker id: ", id)
	to_claim, err := Redis.XPendingExt(CTX, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  "primary",
		//	Consumer: id,
		Start: "-",
		End:   "+",
		Count: 10,
	}).Result()
	if err == nil && len(to_claim) > 0 {
		//check if worker is alive or not

		for _, p := range to_claim {
			_, ok := Worker_map.List[p.Consumer]
			if !ok {
				log.Print("Pending job")
				claimed, err := Redis.XClaim(CTX, &redis.XClaimArgs{
					Stream:   stream,
					Group:    "primary",
					Consumer: id,
					Messages: []string{p.ID},
				}).Result()
				if err != nil {
					log.Fatal("COuldnt claim the job")
				}
				if len(claimed) > 0 {
					return &claimed[0]
				}
			}
		}
	}

	if err != nil {
		log.Print("Coudn't read values from redis [Feed to the broker]:%v ", err)
		log.Fatal("crased in feed to worker")
	}

	args := &redis.XReadGroupArgs{
		Streams:  []string{stream, ">"},
		Group:    "primary",
		Consumer: id,
		Count:    1,
	}
	res, err := Redis.XReadGroup(CTX, args).Result()
	if err != nil {
		log.Print("Coudn't read values from redis [Feed to the broker]:%v ", err)
		log.Fatal("crased in feed to worker")
	}
	if err != nil || len(res) == 0 || len(res[0].Messages) == 0 {
		return nil
	}
	log.Print("NEw job")
	return &res[0].Messages[0]
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
	}
}
