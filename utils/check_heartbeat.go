package utils

import (
	"log"
	"time"

	"github.com/mbroke/types"
)

type work_map struct {
	//	Mu   sync.Mutex
	List map[string]*types.Worker
}

var Worker_map work_map = work_map{
	List: make(map[string]*types.Worker),
}

func Check_hearbeat() {
	log.SetPrefix("[Error in heartbeat]: ")
	log.SetFlags(0)
	for {
		log.Print("Workermap .leng:", len(Worker_map.List))
		log.Print("WorkerChannel .leng:", len(Worker_channel))

		for key, value := range Worker_map.List {
			if time.Now().UTC().UnixMilli()-value.Last_ping >= 10000 {
				//worker id mismatch issue
				res, err := Redis.XRange(CTX, "ingest_primary", value.Job_id, value.Job_id).Result()
				if err != nil {
					log.Print("Job not coulnt not be fetched")
					continue
				}
				if len(res) != 0 {
					Retry_channel <- Worker_map.List[key]
				}

				delete(Worker_map.List, key)

			}
		}
	}
}
