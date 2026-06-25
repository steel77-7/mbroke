package utils

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/mbroke/types"
	"github.com/redis/go-redis/v9"
)

// Ingester go routine for pushing jobs into redis
func StartIngester() {
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()

		batch := make([]types.Job, 0, 5000)

		flush := func() {
			if len(batch) == 0 {
				return
			}
			//batching the jobs to reduce the network overhead
			pipe := Redis.Pipeline()

			for _, job := range batch {
				pipe.XAdd(CTX, &redis.XAddArgs{
					Stream: Conf.StreamName,
					//	MaxLen: Conf.MaxLen,
					Approx: true,
					Values: map[string]interface{}{
						"metadata": job.Metadata,
						"data":     job.Data,
					},
				})
			}

			_, err := pipe.Exec(CTX)
			if err != nil {
				log.Printf("pipeline flush failed: %v", err)
			}

			batch = batch[:0]
		}

		for {
			select {
			case job := <-IngesterChannel:
				batch = append(batch, job)

				if len(batch) >= 1000 {
					flush()
				}

			case <-ticker.C:
				flush()
			}
		}
	}()
}

func ACK(ids []string) bool {
	if _, err := Redis.XAck(CTX, Conf.StreamName, Conf.ConsumerGroupName, ids...).Result(); err == nil {
		Redis.XDel(CTX, Conf.StreamName, ids...)
		return true
	} else {
		log.Print(err)
		return false
	}

}

// acknowledgements are batched as well
func Acker() {
	ticker := time.NewTicker(2000 * time.Millisecond)
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

func Del_consumer(ids []string) {
	pipe := Redis.Pipeline()
	for _, consumer := range ids {
		pipe.XGroupDelConsumer(CTX, Conf.StreamName, Conf.ConsumerGroupName, consumer)
	}
	_, err := pipe.Exec(CTX)
	if err != nil {
		log.Print("Couldnt delte the consuemr")
	}

}

func Consumer_deleter() {
	ticker := time.NewTicker(700 * time.Millisecond)
	defer ticker.Stop()
	var batch []string

	for {
		select {
		case id := <-Del_channel:
			batch = append(batch, id)

		case <-ticker.C:
			if len(batch) > 0 {
				Del_consumer(batch)
				batch = nil
			}
		}
	}
}

// scans the pending list entries to contantly find jobs with :
// 1. expired leases to deliver them to new workers
// 2. dead jobs that need to be pushed into the dead letter queue
func Pending_jobs() {
	for {
		id := <-Worker_inquiry_channel
		var claim []string

		to_claim, err := Redis.XPendingExt(CTX, &redis.XPendingExtArgs{
			Stream: Conf.StreamName,
			Group:  Conf.ConsumerGroupName,
			Idle:   time.Duration(Conf.IdleTime) * time.Second,
			Start:  "-",
			End:    "+",
			Count:  Conf.BatchSize,
		}).Result()
		//	log.Print("len:", len(to_claim))
		if err != nil {
			log.Print("Couldnt fetch pending jobs")
			Worker_inquiry_channel <- id
			continue
		}
		if len(to_claim) == 0 {
			Worker_inquiry_channel <- id
			continue
		}

		pipe := Redis.Pipeline()
		hasDeadJobs := false
		for _, job := range to_claim {
			//log.Print("Retry count:", job.RetryCount)
			if job.RetryCount <= Conf.RetryCount {
				claim = append(claim, job.ID)
			} else {
				log.Print("Getting job for dead letter:", job.ID)
				log.Print("Retry count:", job.RetryCount)
				pipe.XRange(CTX, Conf.StreamName, job.ID, job.ID)
				hasDeadJobs = true
			}
		}

		if hasDeadJobs {
			cmds, err1 := pipe.Exec(CTX)
			if err1 != nil {
				log.Print("Couldnt fetch dead jobs", err1)
				time.Sleep(10 * time.Millisecond)
				Worker_inquiry_channel <- id
				continue
			}

			deadPipe := Redis.Pipeline()
			hasDeadOps := false
			for _, cmd := range cmds {
				msgs, _ := cmd.(*redis.XMessageSliceCmd).Result()
				if len(msgs) > 0 {
					for _, msg := range msgs {
						deadPipe.XAdd(CTX, &redis.XAddArgs{
							Stream: Conf.DeadLetterName,
							Values: msg.Values,
						})
						deadPipe.XAck(CTX, Conf.StreamName, Conf.ConsumerGroupName, msg.ID)
						deadPipe.XDel(CTX, Conf.StreamName, msg.ID)
						hasDeadOps = true
					}
				}
			}
			if hasDeadOps {
				_, dead_err := deadPipe.Exec(CTX)
				if dead_err != nil {
					log.Fatal("Yeah this happened", dead_err)
				}
			}
		}

		if len(claim) == 0 {
			time.Sleep(10 * time.Millisecond)
			Worker_inquiry_channel <- id
			continue
		}

		jobs, _ := Redis.XClaim(CTX, &redis.XClaimArgs{
			Stream:   Conf.StreamName,
			Group:    Conf.ConsumerGroupName,
			Consumer: id,
			Messages: claim,
		}).Result()
		claim = nil
		Worker_feeder_channel <- types.WorkerFeeding{Data: jobs, ID: id}
	}
}

// for retrieving new jobs from the stream and assigning them to a  consumer
func Feed_to_worker() {
	var id string
	for {

		id = <-Worker_inquiry_channel

		args := &redis.XReadGroupArgs{
			Streams:  []string{Conf.StreamName, ">"},
			Group:    Conf.ConsumerGroupName,
			Consumer: id,
			Count:    Conf.BatchSize,
		}
		var messages []redis.XMessage
		res, err1 := Redis.XReadGroup(CTX, args).Result()
		if err1 != nil || len(res) == 0 {
			Worker_inquiry_channel <- id
			continue
		}
		for _, s := range res {
			for _, mess := range s.Messages {
				messages = append(messages, mess)
			}
		}
		Worker_feeder_channel <- types.WorkerFeeding{Data: messages, ID: id}

	}

}

// ----------------------------
// ----------------------------
// Worker tracking functions
// ----------------------------
// ----------------------------
func Add_to_set(id string) {
	currTime := float64(time.Now().Unix() + 10)
	_, err := Redis.ZAdd(CTX, Conf.SetName, redis.Z{Member: id, Score: currTime}).Result()
	if err != nil {
		log.Print("[ADD TO SET FUNCTION] Couldnt add to the set ")
		return
	}

}

func Update_score(id string, time float64) {
	_, err := Redis.ZAdd(CTX, Conf.SetName, redis.Z{Member: id, Score: time}).Result()
	if err != nil {
		log.Print("[UPDATE SCORE] Couldnt UPDATE  the set ")
		return
	}
}

func Present_in_set(id string) bool {
	_, err := Redis.ZScore(CTX, Conf.SetName, id).Result()
	if errors.Is(err, redis.Nil) {
		return false
	} else if err != nil {
		log.Print("[PRESENT IN SET] ", err)
		return false
	}
	return true

}
func Remove_from_set(id string) {
	_, err := Redis.ZRem(CTX, Conf.SetName, id).Result()
	if err != nil {
		log.Print("[REMOVE FORM SET]", err)
		return
	}
}
func Fetch_dead_workers(time float64) []string {
	res, err := Redis.ZRangeByScore(CTX, Conf.SetName, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%f", time)}).Result()
	if err != nil {
		log.Print("[IN THE FETCH DEAD WORKERs] COULDNT FETCH WORKER", err)
		return make([]string, 0)
	}
	return res

}

func Check_worker_if_present(id string) bool {
	exists, err := Redis.SIsMember(CTX, Conf.SetName, id).Result()
	if err != nil {
		log.Print("[CHECK WORKER IN SET] some error", err)
		return false
	}
	return exists
}

func Worker_map_len() int64 {
	count, err := Redis.SCard(CTX, Conf.SetName).Result()
	if err != nil {
		log.Print("[WORKER MAP LEN] OCuldnt findn the length")
		return -1
	}
	return count
}
