package producer

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

var jobsProduced atomic.Int64

func makeBody(id int64) []byte {
	return []byte(fmt.Sprintf(
		`{"metadata":{"id":"job-%d","url":"https://example.com","state":false},"data":{"number":%d,"test":true}}`,
		id,
		id,
	))
}

func sendJob(client *http.Client, url string, id int64) error {

	body := makeBody(id)

	resp, err := client.Post(
		url,
		"application/json",
		bytes.NewReader(body),
	)

	if err != nil {
		return err
	}

	resp.Body.Close()

	return nil
}

func loadEnv() {
	if err := godotenv.Load("producer/.env"); err != nil {
		log.Fatalf("failed to load .env: %v", err)
	}
}

func newRedisClient() *redis.Client {

	db, _ := strconv.Atoi(os.Getenv("REDIS_DB"))
	protocol, _ := strconv.Atoi(os.Getenv("REDIS_PROTOCOL"))
	poolSize, _ := strconv.Atoi(os.Getenv("REDIS_POOL_SIZE"))

	return redis.NewClient(&redis.Options{
		Addr:         os.Getenv("REDIS_ADDR"),
		Password:     os.Getenv("REDIS_PASSWORD"),
		DB:           db,
		Protocol:     protocol,
		PoolSize:     poolSize,
		MinIdleConns: 2,
	})
}

// statsLogger runs in a separate goroutine and periodically logs:
//   - redis ingestion rate (entries added to stream per second)
//   - redis consumption rate (entries read by group per second)
//   - peak ingestion and consumption rates
//   - stream length
//   - truly active workers (consumers with idle < 30s)
//   - number of jobs currently in processing (pending)
func statsLogger(rdb *redis.Client, streamName, groupName string) {

	ctx := context.Background()

	const interval = 250 * time.Millisecond
	const perSecScale = int64(time.Second / interval) // 4

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var prevEntriesAdded int64
	var prevStreamLen int64

	var peakIngestion int64
	var peakConsumption int64

	const activeIdleThreshold = 30_000 // 30 seconds in ms

	for range ticker.C {

		// ingestion rate via XINFO STREAM → EntriesAdded
		var ingestionRate int64
		var streamLen int64

		streamInfo, err := rdb.XInfoStream(ctx, streamName).Result()
		if err == nil {
			curEntries := streamInfo.EntriesAdded
			if prevEntriesAdded > 0 {
				ingestionRate = (curEntries - prevEntriesAdded) * perSecScale
			}
			prevEntriesAdded = curEntries
			streamLen = streamInfo.Length
		}

		// consumption rate = ingestion - stream length change
		// (jobs removed from stream per second)
		var consumptionRate int64
		if prevStreamLen > 0 {
			streamDelta := (streamLen - prevStreamLen) * perSecScale
			consumptionRate = ingestionRate - streamDelta
			if consumptionRate < 0 {
				consumptionRate = 0
			}
		}
		prevStreamLen = streamLen

		// active workers via XINFO CONSUMERS — only count those with idle < threshold
		var activeWorkers int
		var pendingJobs int64

		consumers, err := rdb.XInfoConsumers(ctx, streamName, groupName).Result()
		if err == nil {
			for _, c := range consumers {
				pendingJobs += c.Pending
				if c.Idle.Milliseconds() < activeIdleThreshold {
					activeWorkers++
				}
			}
		}

		// track peaks
		if ingestionRate > peakIngestion {
			peakIngestion = ingestionRate
		}
		if consumptionRate > peakConsumption {
			peakConsumption = consumptionRate
		}

		fmt.Printf(
			"\r[stats] in/s: %-6d (peak: %-6d) | out/s: %-6d (peak: %-6d) | stream: %-8d | workers: %-4d | pending: %-6d",
			ingestionRate, peakIngestion,
			consumptionRate, peakConsumption,
			streamLen,
			activeWorkers,
			pendingJobs,
		)
	}
}

func Producer() {

	loadEnv()

	ingestURL := os.Getenv("INGEST_URL")
	streamName := os.Getenv("STREAM_NAME")
	groupName := os.Getenv("CONSUMER_GROUP_NAME")

	// redis client for monitoring
	rdb := newRedisClient()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("failed to connect to redis: %v", err)
	}

	fmt.Println("[producer] connected to redis for monitoring")

	// start the stats logger in a separate thread (goroutine)
	go statsLogger(rdb, streamName, groupName)

	// http client for producing jobs
	tr := &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 1000,
		MaxConnsPerHost:     1000,
		IdleConnTimeout:     90 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}

	client := &http.Client{
		Transport: tr,
		Timeout:   5 * time.Second,
	}

	const workers = 150

	for i := 0; i < workers; i++ {

		go func() {
			for c := 0; c < 10000; c++ {

				id := jobsProduced.Add(1)

				if err := sendJob(client, ingestURL, id); err != nil {
					continue
				}
			}
		}()
	}

	select {}
}
