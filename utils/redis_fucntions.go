package utils

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/mbroke/types"
	"github.com/redis/go-redis/v9"
)

var setName string = "workerset"

func Feed(job types.Job) { //this will be in the ingest
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

func ACK(ids []string) bool {
	if err := Redis.XAck(CTX, Conf.StreamName, Conf.ConsumerGroupName, ids...).Err(); err == nil {
		for _, id := range ids {
			val, errr := Redis.XRange(CTX, Conf.StreamName, id, id).Result()
			if errr != nil {
				log.Print("int he ack ", err)
			}
			if len(val) == 0 {
				continue
			}
			m := val[0].Values["metadata"]
			raw, ok := m.(string)
			if !ok {
				return false
			}
			var meta types.Metadata
			err1 := json.Unmarshal([]byte(raw), &meta)
			if err1 != nil {
				return false
			}
			err2 := Redis.HSet(CTX, meta.ID, "state", true).Err()
			if err2 != nil {
				return false
			}
			//log.Print("int hre acker", meta.ID)

			Add_to_queue(meta.ID)
			Redis.XDel(CTX, Conf.StreamName, id)

			return true

		}
	}
	return false
}

func Del_consumer(ids []string) {

	for _, id := range ids {
		_, err := Redis.XGroupDelConsumer(CTX, Conf.StreamName, Conf.ConsumerGroupName, id).Result()
		if err != nil {
			log.Print("Consumer not deleted", err)
		}
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
	ticker := time.NewTicker(100 * time.Millisecond)
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
		Count:  10,
	}).Result()

	//log.Print("1")

	for _, p := range to_claim {
		//fmt.Print(to_claim)
		if p.RetryCount > Conf.RetryCount {
			tp, err := Redis.XRange(CTX, Conf.StreamName, p.ID, p.ID).Result()
			if err != nil {
				log.Print("Couldnt push into the dead end queue: ", err)
			}
			if len(tp) == 0 {
				//	log.Print(tp)
				continue
			}
			ACK_channel <- p.ID //ack it first

			_, err1 := Redis.XAdd(CTX, &redis.XAddArgs{
				Stream: Conf.DeadLetterName,
				Values: tp[0].Values,
			}).Result()
			if err1 != nil {
				log.Print("Couldnt push into the dead end queue: ", err)
			}

			val, _ := Redis.XRange(CTX, Conf.StreamName, id, id).Result()
			if len(val) == 0 {
				return nil
			}
			m := val[0].Values["metadata"]
			raw, ok := m.(string)
			if !ok {
				return nil
			}
			var meta types.Metadata
			errr := json.Unmarshal([]byte(raw), &meta)
			if errr != nil {
				log.Print(errr)
				return nil
			}
			err3 := Redis.HSet(CTX, meta.ID, "state", false).Err()
			if err3 != nil {
				log.Print(err3)

				return nil
			}
			_, err2 := Redis.XDel(CTX, Conf.StreamName, p.ID).Result()
			if err2 != nil {
				log.Print("Couldnt push into the dead end queue: ", err)
			}
			Add_to_queue(meta.ID)
			continue
		}
		//shifting the reliance form the worker map to the redis ....cause the worker map is slow
		//
		Worker_map.Mu.Lock()
		val, ok := Worker_map.List[p.Consumer]
		Worker_map.Mu.Unlock()
		//current consumer kaam kar raha hai and healthy heartbeats bhej raha hai .....but might be a zombie
		// establish a lease
		if (!ok) || (ok && val.Job_id != p.ID) { //problem is here......too much duplication
			if (p.Idle * time.Duration(p.RetryCount)) > (time.Duration(p.RetryCount) * time.Duration(Conf.IdleTime)) {
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
	}

	if err != nil {
		log.Print("Coudn't read values from redis [Feed to the broker]:%v ", err)
		//log.Fatal("crased in feed to worker")
		return nil
	}

	args := &redis.XReadGroupArgs{
		Streams:  []string{Conf.StreamName, ">"},
		Group:    Conf.ConsumerGroupName,
		Consumer: id,
		Count:    1,
		Block:    100 * time.Millisecond,
	}
	res, err1 := Redis.XReadGroup(CTX, args).Result()
	if err1 != nil {

		return nil
	}
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

func Reply_to_producer() {

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	for {
		result, err := Redis.BRPop(CTX, 0, Conf.ResQueue).Result()
		if err != nil {
			log.Print(err)
			continue
		}

		jobID := result[1]
		res, err := Redis.HGetAll(CTX, jobID).Result()
		if err != nil {
			log.Print(err)
			continue
		}
		log.Print(res)
		var m types.Metadata
		err = Redis.HGetAll(CTX, jobID).Scan(&m)
		if err != nil {
			log.Print("scan error:", err)
			continue
		}
		log.Print(m)
		body, _ := json.Marshal(m)

		mac := hmac.New(sha256.New, []byte(Conf.Hmac))
		mac.Write(body)
		signature := hex.EncodeToString(mac.Sum(nil))

		req, err := http.NewRequest("POST", m.Url, bytes.NewBuffer(body))
		if err != nil {
			log.Print("request error:", err)
			continue
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Signature", signature)
		req.Header.Set("X-Timestamp", time.Now().UTC().Format(time.RFC3339))

		resp, err := client.Do(req)
		if err != nil {
			log.Print("webhook failed:", err)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Print("bad status:", resp.StatusCode)
			continue
		}

		Redis.Del(CTX, jobID)
	}
}

func Add_to_set(id string) {
	currTime := float64(time.Now().Unix() + 10)
	_, err := Redis.ZAdd(CTX, setName, redis.Z{Member: id, Score: currTime}).Result()
	if err != nil {
		log.Print("[ADD TO SET FUNCTION] Couldnt add to the set ")
		return
	}

}

func Fetch_dead_workers(time float64) []string {
	res, err := Redis.ZRangeByScore(CTX, setName, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%f", time)}).Result()
	if err != nil {
		log.Fatal("[IN THE FETCH DEAD WORKERs] COULDNT FETCH WORKER", err)
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
