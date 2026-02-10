package utils

import (
	"fmt"
	"log"
	"time"

	"github.com/mbroke/types"
	"github.com/redis/go-redis/v9"
)

var stream string = "ingest:primary"
var IDLE_TIME time.Duration = 1000 * time.Millisecond

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
	if err := Redis.XAck(CTX, stream, "primary", ids...).Err(); err == nil {
		for _, id := range ids {
			if err := Redis.XDel(CTX, stream, id); err != nil {
				return true
			}
		}
	}
	return false
}

func Del_consumer(ids []string) {

	for _, id := range ids {
		_, err := Redis.XGroupDelConsumer(CTX, stream, "primary", id).Result()
		if err != nil {
			log.Print("Consumer not deleted", err)
		}
		log.Print("worker deleted")
	}
}

func Consumer_deleter() {
	for {
		if len(Del_channel) > 5 {
			var tp []string
			for {
				select {
				case id := <-Del_channel:
					tp = append(tp, id)
				default:
					Del_consumer(tp)
				}
			}
		}
	}
}

func Acker() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var batch []string

	for {
		select {
		case id := <-ACK_channel:
			batch = append(batch, id)

		case <-ticker.C:
			if len(batch) > 0 {
				ACK(batch)
				batch = nil
			}
		}
	}
}

func Feed_to_worker(id string) *redis.XMessage { //this will be in the worker feeding

	to_claim, err := Redis.XPendingExt(CTX, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  "primary",
		Idle:   1000 * time.Millisecond,
		Start:  "-",
		End:    "+",
		Count:  10,
	}).Result()

	//log.Print("1")

	for _, p := range to_claim {
		fmt.Print(len(to_claim))
		if p.RetryCount > 5 {
			log.Print("dead lettered ")
			tp, err := Redis.XRange(CTX, stream, p.ID, p.ID).Result()
			if err != nil {
				log.Print("Couldnt push into the dead end queue: ", err)
			}
			if len(tp) == 0 {
				log.Print(tp)
				continue
			}
			ACK_channel <- p.ID //ack it first

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
			continue
		}

		Worker_map.Mu.Lock()
		val, ok := Worker_map.List[p.Consumer]
		Worker_map.Mu.Unlock()

		if (!ok) || (ok && val.Job_id != p.ID) {
			if (p.Idle * time.Duration(p.RetryCount)) > (time.Duration(p.RetryCount) * IDLE_TIME) {
				claimed, err := Redis.XClaim(CTX, &redis.XClaimArgs{
					Stream:   stream,
					Group:    "primary",
					Consumer: id,
					Messages: []string{p.ID},
				}).Result()
				log.Print("pending")
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
		//log.Fatal("crased in feed to worker")
		return nil
	}
	//log.Print("3")

	args := &redis.XReadGroupArgs{
		Streams:  []string{stream, ">"},
		Group:    "primary",
		Consumer: id,
		Count:    1,
		Block:    100 * time.Millisecond,
	}
	res, err1 := Redis.XReadGroup(CTX, args).Result()
	if err1 != nil {
		//log.Print("Coudn't read values from redis [Feed to the broker]: ", err)
		//	log.Fatal("crased in feed to worker")
		return nil
	}
	//log.Print("4")
	//log.Print("new ")
	if err1 != nil || len(res) == 0 || len(res[0].Messages) == 0 {
		return nil
	}
	return &res[0].Messages[0]
}
