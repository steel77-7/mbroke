package utils

import (
	"log"
	"sync"
	"time"

	"github.com/mbroke/types"
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
			if time.Now().UTC().UnixMilli()-value.Last_ping >= 10000 {
				//worker id mismatch issue
				res, err := Redis.XRange(CTX, value.Job_id, value.Job_id, 1)
				if err != nil {
					log.Print("Job not coulnt not be fetched")
					continue
				}
				if len(res) != 0
					Retry_sorter <- Worker_map.List[key]

				delete(Worker_map.List, key)

			}
		}
	}
}
