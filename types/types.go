package types

import (
	"encoding/json"
	"sync"

	"github.com/redis/go-redis/v9"
	// "github.com/mbroke/types"
)

type Job struct {
	Metadata string `json:"metadata"`
	Data     string `json:"data"`
}

type JobInfo struct {
	ID   string `json:"id" redis:"id"`
	Data string `json:"data" redis:"data"`
}


type Ack_request struct {
	ID  string `json:"id"`
	ACK bool   `json:"ack"`
}
type WorkerFeeding struct {
	ID   string           `json:"id"`
	Data []redis.XMessage `json:"data"`
}
type Worker struct {
	ID        string `json:"id" redis:"id"`
	Job_id    string `json:"job_id" redis:"job_id"`
	Last_ping int64  `json:"last_ping" redis:"last_ping"`
}

type Heartbeat struct {
	ID string `json:"id"`
}
type Metadata struct {
	ID    string `redis:"id" json:"id"`
	Url   string `redis:"url" json:"url"`
	State bool   `redis:"state" json:"state"`
}
type Job_req struct {
	//ID   string          `json:"id"`
	Metadata json.RawMessage `json:"metadata"`
	Data     json.RawMessage `json:"data"`
}

type Message struct {
	Length   uint32
	Msg_type byte
	Payload  []byte
}

type Work_map struct {
	Mu   *sync.RWMutex
	List map[string]*Worker
}
