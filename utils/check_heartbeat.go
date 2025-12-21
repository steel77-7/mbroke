package utils

import (
	"log"
	"sync"
	"time"

	"github.com/mbroke/types"
)

type work_map struct {
	mu   sync.Mutex
	list map[string]*types.Worker
}

var Worker_map work_map

func Check_hearbeat() {
	log.SetPrefix("[Error in heartbeat]: ")
	log.SetFlags(0)
	for {
		for key, value := range Worker_map.list {
			if time.Now().UTC().UnixMilli()-value.Last_ping > 10000 {
				delete(Worker_map.list, key)
				//implement logic for res[ushing the request]
			}
		}
	}
}
