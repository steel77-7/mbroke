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
		MaxLen: 2000000,
		Values: tbs,
	}
	_, err := Redis.XAdd(CTX, args).Result()
	if err != nil {
		log.Print("Error in adding the job: %v", err)
	}
}

func ACK(ids []string) bool {
	if err := Redis.XAck(CTX, stream, "primary", ids...).Err(); err != nil {
		for _, id := range ids {
			if err := Redis.XDel(CTX, stream, id); err != nil {
				return true
			}
		}
	}
	return false
}

// runs in the background  to ack jobs
func Acker() {
	for {
		if len(ACK_channel) > 100 {
			var tp []string
			for {
				select {
				case id := <-ACK_channel:
					tp = append(tp, id)
				default:
					ACK(tp)
				}
			}
		}
	}
}
func Feed_to_worker(id string) *redis.XMessage { //this will be in the worker feeding

	to_claim, err := Redis.XPendingExt(CTX, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  "primary",
		//	Consumer: id,
		Start: "-",
		End:   "+",
		Count: 500,
	}).Result()

	new, errn := Redis.XRead(CTX, &redis.XReadArgs{
		Streams: []string{"ingest:primary", "0"},
		Count:   1,
		Block:   1,
	}).Result()
	if errn != nil && errn != redis.Nil {
		log.Print("error in fetching the data from redis:", errn)
		return nil
	}
	if err == nil && len(to_claim) > 200 || (err == nil && len(new) == 0) {
		for _, p := range to_claim {
			if p.RetryCount > 5 {
				tp, err := Redis.XRange(CTX, stream, p.ID, p.ID).Result()
				if err != nil {
					log.Print("Couldnt push into the dead end queue: ", err)
				}
				if len(tp) == 0 {
					ACK_channel <- p.ID
				} else {
					_, err1 := Redis.XAdd(CTX, &redis.XAddArgs{
						Stream: "ingest:dead_end",
						Values: tp[0].Values,
					}).Result()
					if err1 != nil {
						log.Print("Couldnt push into the dead end queue: ", err)
					}
					_, err2 := Redis.XDel(CTX, stream, p.ID).Result()
					if err2 != nil {
						log.Print("Couldnt push into the dead end queue: ", err)
					}
				}
				continue
				//
			}
			Worker_map.Mu.Lock()
			val, ok := Worker_map.List[p.Consumer]
			Worker_map.Mu.Unlock()

			if (!ok) || (ok && val.Job_id != p.ID) {
				claimed, err := Redis.XClaim(CTX, &redis.XClaimArgs{
					Stream:   stream,
					Group:    "primary",
					Consumer: id,
					Messages: []string{p.ID},
				}).Result()
				if err != nil {
					log.Print("COuldnt claim the job")
					return nil
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
		Block:    0,
	}
	res, err1 := Redis.XReadGroup(CTX, args).Result()
	if err1 != nil {
		log.Print("Coudn't read values from redis [Feed to the broker]: ", err)
		log.Fatal("crased in feed to worker")
	}
	if err1 != nil || len(res) == 0 || len(res[0].Messages) == 0 {
		return nil
	}
	return &res[0].Messages[0]
}
