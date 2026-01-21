package utils

import (
	"sync"
	"time"

	"github.com/mbroke/types"
)

var Worker_map work_map = work_map{
	Mu:   &sync.RWMutex{},
	List: make(map[string]*types.Worker),
}

// not checked yet
func Check_heartbeat() {
	for {
		time.Sleep(time.Duration(1) * time.Second)

		//i have to make it retry
		if len(Worker_map.List) == 0 {
			continue
		}
		//log.Print("lub dub")
		Worker_map.Mu.Lock()
		for key, value := range Worker_map.List {
			if time.Now().UTC().UnixMilli()-value.Last_ping >= 10000 {
				//this will automatically label it as a workerless job and then it will eventually be executed
				delete(Worker_map.List, key)

			}
		}
		Worker_map.Mu.Unlock()
	}
}
