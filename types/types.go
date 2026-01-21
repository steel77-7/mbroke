package types

type Job struct {
	ID   string `json:"id"`
	Data string `json:"data"`
}

type Ack_request struct {
	ID  string `json:"id"`
	ACK bool   `json:"ack"`
}

type Worker struct { //for teh heartbeat
	ID        string `json:"id"`
	Job_id    string `json:"job_id"`
	Last_ping int64  `json:"last_ping"`
}

type Heartbeat struct {
	ID string `json:"id"`
}
