package utils

import (
	"log"

	"github.com/mbroke/types"
	"github.com/redis/go-redis/v9"
)

var stream string = "ingest:primary"

func Feed(job types.Job) { //this will be in the ingest
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

func ACK(id string) bool {
	log.Print("ACKINg the job")
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
			Worker_map.Mu.Lock()
			val, ok := Worker_map.List[p.Consumer]
			Worker_map.Mu.Unlock()

			if (!ok) || (ok && val.Job_id != p.ID) {
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
	res, err1 := Redis.XReadGroup(CTX, args).Result()
	if err1 != nil {
		log.Print("Coudn't read values from redis [Feed to the broker]: ", err)
		log.Fatal("crased in feed to worker")
	}
	if err1 != nil || len(res) == 0 || len(res[0].Messages) == 0 {
		return nil
	}
	log.Print("New job")
	return &res[0].Messages[0]
}
