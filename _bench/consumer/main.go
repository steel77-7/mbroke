package main

import (
	"consumer/socket"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

var (
	totalProcessed atomic.Int64
	totalNacked    atomic.Int64
	totalCrashes   atomic.Int64
	totalReconnects atomic.Int64
	activeWorkers  atomic.Int64
)

// runWorker connects and keeps reconnecting on crash
func runWorker(id int) {
	for {
		activeWorkers.Add(1)

		client := socket.NewBrokerClient(id)
		if client == nil {
			log.Fatalf("[worker-%d] failed to create broker client", id)
		}

		if err := client.Connect(); err != nil {
			activeWorkers.Add(-1)
			log.Printf("[worker-%d] connect failed: %v — retrying", id, err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// block until this client dies
		<-client.Done()

		// collect stats from the dead client
		p, n, c := client.Stats()
		totalProcessed.Add(p)
		totalNacked.Add(n)
		totalCrashes.Add(c)
		activeWorkers.Add(-1)

		totalReconnects.Add(1)

		// small delay before reconnect
		time.Sleep(500 * time.Millisecond)
	}
}

func statsLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		fmt.Printf(
			"\r[consumers] active: %-4d | processed: %-8d | nacked: %-6d | crashes: %-4d | reconnects: %-4d",
			activeWorkers.Load(),
			totalProcessed.Load(),
			totalNacked.Load(),
			totalCrashes.Load(),
			totalReconnects.Load(),
		)
	}
}

func main() {
	const numWorkers = 1 // scaled by docker-compose replicas or Makefile

	go statsLoop()

	for i := 0; i < numWorkers; i++ {
		go runWorker(i)
	}

	select {}
}
