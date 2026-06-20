package utils

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/mbroke/types"
	"github.com/redis/go-redis/v9"
)

var setName string = "workerset"

func Feed(job types.Job) { //this will be in the ingest
	log.Print("adding")
	tbs := map[string]interface{}{
		"metadata": job.Metadata,
		"data":     job.Data,
	}
	args := &redis.XAddArgs{
		Stream: Conf.StreamName,
		MaxLen: Conf.MaxLen,
		Approx: true,
		Values: tbs,
	}
	_, err := Redis.XAdd(CTX, args).Result()
	if err != nil {
		log.Print("Error in adding the job: %v", err)
	}
}

func StartIngester() {
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()

		batch := make([]types.Job, 0, 5000)

		flush := func() {
			if len(batch) == 0 {
				return
			}

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
	log.Print("int he ack ")

	if count, err := Redis.XAck(CTX, Conf.StreamName, Conf.ConsumerGroupName, ids...).Result(); err == nil {
		// for _, id := range ids {
		// 	Redis.XDel(CTX, Conf.StreamName, id)
		// 	return true
		// }
		log.Print("Count:  of ack : ", count)
		Redis.XDel(CTX, Conf.StreamName, ids...)
		return true
	} else {
		log.Print(err)
		return false
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

func Acker() {
	ticker := time.NewTicker(700 * time.Millisecond)
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
		Stream: Conf.StreamName,
		Group:  Conf.ConsumerGroupName,
		Idle:   time.Duration(Conf.IdleTime) * time.Millisecond,
		Start:  "-",
		End:    "+",
		Count:  100,
	}).Result()

	//log.Print("1")

	for _, p := range to_claim {
		if p.RetryCount > Conf.RetryCount {
			tp, err := Redis.XRange(CTX, Conf.StreamName, p.ID, p.ID).Result()
			if err != nil {
				log.Print("Couldnt push into the dead end queue: ", err)
			}
			if len(tp) == 0 {
				continue
			}
			//ACK_channel <- p.ID //ack it first

			_, err1 := Redis.XAdd(CTX, &redis.XAddArgs{
				Stream: Conf.DeadLetterName,
				Values: tp[0].Values,
			}).Result()
			if err1 != nil {
				log.Print("Couldnt push into the dead end queue: ", err)
			}

			_, err2 := Redis.XDel(CTX, Conf.StreamName, p.ID).Result()
			if err2 != nil {
				log.Print("Couldnt push into the dead end queue: ", err)
			}
			//Add_to_queue(meta.ID)
			continue
		}

		//alternative :
		ok := Present_in_set(p.Consumer)
		has_job := Fetch_worker(p.Consumer)
		if (!ok) || (ok && len(has_job) > 0) || (ok && has_job["job_id"] != p.ID) {
			if p.Idle > time.Duration(Conf.IdleTime) {
				claimed, err := Redis.XClaim(CTX, &redis.XClaimArgs{
					Stream:   Conf.StreamName,
					Group:    Conf.ConsumerGroupName,
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
		// if (!ok) || (ok && val.Job_id != p.ID) { //problem is here......too much duplication
		// 	if (p.Idle * time.Duration(p.RetryCount)) > (time.Duration(p.RetryCount) * time.Duration(Conf.IdleTime)) {
		// 		claimed, err := Redis.XClaim(CTX, &redis.XClaimArgs{
		// 			Stream:   Conf.StreamName,
		// 			Group:    Conf.ConsumerGroupName,
		// 			Consumer: id,
		// 			Messages: []string{p.ID},
		// 		}).Result()
		// 		if err != nil {
		// 			log.Print("COuldnt claim the job")
		// 			return nil
		// 		}
		// 		if len(claimed) > 0 {
		// 			return &claimed[0]
		// 		}
		// 	}
		// }
	}

	if err != nil {
		log.Printf("Coudn't read values from redis [Feed to the broker]:%v ", err)
		//log.Fatal("crased in feed to worker")
		return nil
	}
	//this is for the new messages
	args := &redis.XReadGroupArgs{
		Streams:  []string{Conf.StreamName, ">"},
		Group:    Conf.ConsumerGroupName,
		Consumer: id,
		Count:    1,
		Block:    100 * time.Millisecond,
	}
	res, err1 := Redis.XReadGroup(CTX, args).Result()
	// if err1 != nil {

	// 	return nil
	// }
	if err1 != nil || len(res) == 0 || len(res[0].Messages) == 0 {
		return nil
	}
	return &res[0].Messages[0]
}

func Add_into_dict(data types.Metadata) error {
	err := Redis.HSet(CTX, data.ID, "id", data.ID, "state", strconv.FormatBool(data.State), "url", data.Url).Err()
	if err != nil {
		return err
	}
	return nil
}

func Add_to_queue(key string) error {
	err := Redis.LPush(CTX, Conf.ResQueue, key).Err()
	if err != nil {
		return err
	}
	return nil
}

// func Reply_to_producer() {

// 	client := &http.Client{
// 		Timeout: 5 * time.Second,
// 	}

// 	for {
// 		result, err := Redis.BRPop(CTX, 0, Conf.ResQueue).Result()
// 		if err != nil {
// 			log.Print(err)
// 			continue
// 		}

// 		jobID := result[1]
// 		res, err := Redis.HGetAll(CTX, jobID).Result()
// 		if err != nil {
// 			log.Print(err)
// 			continue
// 		}
// 		log.Print(res)
// 		var m types.Metadata
// 		err = Redis.HGetAll(CTX, jobID).Scan(&m)
// 		if err != nil {
// 			log.Print("scan error:", err)
// 			continue
// 		}
// 		log.Print(m)
// 		body, _ := json.Marshal(m)

// 		mac := hmac.New(sha256.New, []byte(Conf.Hmac))
// 		mac.Write(body)
// 		signature := hex.EncodeToString(mac.Sum(nil))

// 		req, err := http.NewRequest("POST", m.Url, bytes.NewBuffer(body))
// 		if err != nil {
// 			log.Print("request error:", err)
// 			continue
// 		}
// 		req.Header.Set("Content-Type", "application/json")
// 		req.Header.Set("X-Signature", signature)
// 		req.Header.Set("X-Timestamp", time.Now().UTC().Format(time.RFC3339))

// 		resp, err := client.Do(req)
// 		if err != nil {
// 			log.Print("webhook failed:", err)
// 			continue
// 		}
// 		resp.Body.Close()

// 		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
// 			log.Print("bad status:", resp.StatusCode)
// 			continue
// 		}

// 		Redis.Del(CTX, jobID)
// 	}
// }

func Add_to_set(id string) {
	currTime := float64(time.Now().Unix() + 10)
	_, err := Redis.ZAdd(CTX, setName, redis.Z{Member: id, Score: currTime}).Result()
	if err != nil {
		log.Print("[ADD TO SET FUNCTION] Couldnt add to the set ")
		return
	}

}

func Update_score(id string, time float64) {
	_, err := Redis.ZAdd(CTX, setName, redis.Z{Member: id, Score: time}).Result()
	if err != nil {
		log.Print("[UPDATE SCORE] Couldnt UPDATE  the set ")
		return
	}
}

func Present_in_set(id string) bool {
	_, err := Redis.ZScore(CTX, setName, id).Result()
	if errors.Is(err, redis.Nil) {
		return false
	} else if err != nil {
		log.Print("[PRESENT IN SET] ", err)
		return false
	}
	return true

}
func Remove_from_set(id string) {
	log.Print("Removed from the set :", id)

	_, err := Redis.ZRem(CTX, setName, id).Result()
	if err != nil {
		log.Print("[REMOVE FORM SET]", err)
		return
	}
}
func Fetch_dead_workers(time float64) []string {
	res, err := Redis.ZRangeByScore(CTX, setName, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%f", time)}).Result()
	if err != nil {
		log.Print("[IN THE FETCH DEAD WORKERs] COULDNT FETCH WORKER", err)
		return make([]string, 0)
	}
	return res

}

func Add_to_map(worker *types.Worker) {
	err := Redis.HSet(CTX, "worker:"+(worker.ID), worker).Err()
	if err != nil {
		log.Print("[ADD TO MAP] Cannot add to map : ", worker.ID, "\n", err)
		return
	}
	log.Print("WORker added to the map : ", worker.ID)
}
func Remove_worker_from_map(id string) error {
	log.Print("Removed from the map :", id)
	_, err := Redis.Del(CTX, "worker:"+id).Result()
	if err != nil {
		log.Print("[REMOVE FROM WORKER MAP] Couldnt remove the worker: ", err)
		return err
	}
	return nil
}
func Check_worker_if_present(id string) bool {
	exists, err := Redis.SIsMember(CTX, setName, id).Result()
	if err != nil {
		log.Print("[CHECK WORKER IN SET] some error", err)
		return false
	}
	return exists
}

func Worker_map_len() int64 {
	count, err := Redis.SCard(CTX, setName).Result()
	if err != nil {
		log.Print("[WORKER MAP LEN] OCuldnt findn the length")
		return -1
	}
	return count
}

func Fetch_worker(id string) map[string]string {
	res, err := Redis.HGetAll(CTX, "worker:"+id).Result()
	if err != nil {
		log.Print("[IN THE FETCH WORKER] Couldnt fetch the worker")
		return nil
	}
	return res
}
