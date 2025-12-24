package types

type Job struct {
	ID string `json:"id"`
	//	Worker *Worker `json:"worker"`
	Status bool   `json:"status"`
	Count  int    `json:"count"`
	Data   string `json:"data"`
	Max    int    `json:"max"`
}

type JobTbs struct {
}

type ErrorResponse struct {
}

type Worker struct { //for teh heartbeat
	ID     string `json:"id"`
	Job_id string `json:"job_id"`
	//Last_ping time.Time
	Last_ping int64 `json:"last_ping"`
}

type Heartbeat struct {
	ID string `json:"is"`
}
