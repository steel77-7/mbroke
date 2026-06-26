// --------------------------------------
// Adaptive lease timing based on the batch size of teh
package utils

import (
	"sync"
	"sync/atomic"
	"time"
)

type Lease struct {
	t time.Time
}

var SentTime sync.Map

var Time_channel = make(chan time.Duration)
var LeaseVar atomic.Int64

const (
	MinLease = 1 * time.Second
	MaxLease = 30 * time.Second
)

func Lease_routine() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	var processingTime time.Duration
	var avg time.Duration = 20 * time.Millisecond
	for {
		select {
		case <-ticker.C:
			{
				LeaseVar.Store(int64(LeaseCalculator(avg)))
			}
		case processingTime = <-Time_channel:
			{
				avg = (avg*9 + processingTime) / 10
			}
		}
	}
}

func LeaseCalculator(avg time.Duration) time.Duration {
	var res time.Duration = time.Duration(float64(avg)*float64(Conf.BatchSize)*2) * time.Second
	if res < MinLease {
		res = MinLease
	}
	if res > MaxLease {
		res = MaxLease
	}
	return res
}
