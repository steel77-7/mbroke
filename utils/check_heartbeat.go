package utils

import (
	"log"
	"sync"
	"time"

	"github.com/mbroke/types"
	"github.com/redis/go-redis/v9"
)

type work_map struct {
	mu   sync.Mutex
	List map[string]*types.Worker
}

var Worker_map work_map

func Check_hearbeat() {
	log.SetPrefix("[Error in heartbeat]: ")
	log.SetFlags(0)
	for {
		for key, value := range Worker_map.List {
			if time.Now().UTC().UnixMilli()-value.Last_ping > 10000 {
				res, err := Redis.XPending(CTX, &redis.XPendingExtArgs{
					Stream: "ingest:primary",
					Group:  "primary",
					Start:  "-",
					End:    "+",
					Count:  10,
				}).Result()

				if err != nil {
					log.Print("in the check heartbeat utils" + err)
					continue
				}

				Retry_channel <- Worker_map.List[key]
				delete(Worker_map.List, key)

				//implement logic for
			}
		}
	}
}
