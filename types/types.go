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
	ID     string
	Job_id string
	//Last_ping time.Time
	Last_ping int64
}

type Heartbeat struct {
	ID string `json:"is"`
}
