# mbroke

`mbroke` is a distributed, high-throughput message broker written in Go. It uses Redis Streams as a durable, persistent source of truth storage, while the broker manages scheduling, leasing, acknowledgements, worker discovery, retries, and dead-letter recovery.

By offloading queue persistence to Redis Streams and managing scheduling, worker lifecycles, and failure recovery inside a stateful Go broker, `mbroke` bridges the gap between raw persistence and reliable distributed orchestration.

Workers communicate with the broker via a custom, lightweight binary protocol over stateful TCP sockets, achieving low-latency job delivery and rapid fault detection.

---

## Motivation

Redis Streams provide an excellent foundation for append-only log data structures, but building a production-grade distributed work queue directly on top of them presents significant challenges:

1. **Active Polling Overhead**: Running multiple workers that poll Redis Streams via `XREADGROUP` or `XPENDING` causes significant CPU and network load on the Redis cluster.
2. **Coarse Connection Lifecycle Tracking**: Redis has no concept of application-level worker health. If a worker process silently crashes or hangs (zombie state), its lease remains active until manually reclaimed, delaying job reprocessing.
3. **Static Visibility Leases**: Traditional message queues use static visibility timeouts. If set too short, jobs are processed twice; if set too long, failed jobs sit idle for minutes before being retried.
4. **Network and Command Overhead**: Issuing an `XADD` for every single job ingestion and an `XACK`/`XDEL` for every single completion introduces massive TCP roundtrip overhead.

`mbroke` solves these problems by placing a stateful, in-memory broker in front of Redis:
* **Ingestion and ACK pipelining** buffers write operations to minimize Redis command counts.
* **Stateful TCP connections** with low-overhead binary framing enable the broker to actively push jobs to workers and monitor their lifecycles.
* **Active client heartbeat tracking** via a Redis sorted set allows the broker to detect crashed or disconnected workers in under 10 seconds.
* **Dynamic, adaptive lease estimation** calculates the rolling average processing time to dynamically shrink failover times for fast tasks and expand them for slow ones.

---

## High-Level Architecture

```mermaid
graph TD
    Producer[Producer] -->|HTTP POST| IngestEndpoint[HTTP Ingestion Endpoint]
    IngestEndpoint -->|Go Channel| Broker[Broker]
    Broker -->|Pipelined XAdd| RedisStreams[(Redis Streams)]

    Workers[TCP-Connected Workers] -->|TCP PULL| Queue[In-memory Scheduling Queue]
    Queue -->|De-queue Worker ID| Broker
    Broker -->|XReadGroup / XClaim| RedisStreams
    Broker -->|TCP PULL Payload| Workers

    Workers -->|TCP TACK| AckProc[ACK Processing]
    AckProc -->|Batched XAck & XDel| RedisStreams

    Broker -.->|Processing Latency| LeaseMgr[Lease Management]
    LeaseMgr -.->|Adaptive Timeout| DLQ[Dead-Letter Recovery]
    RedisStreams -->|Idle Past Timeout| DLQ
    
    DLQ -->|Retry <= Limit: Reclaim| Queue
    DLQ -->|Retry > Limit: Move to DLQ Stream| RedisStreams
```

### Components and Flow Description

* **Producer**: Sends jobs containing payload data and metadata over HTTP to the broker.
* **HTTP Ingestion Endpoint**: Exposes the `/ingest` REST endpoint. It routes incoming payloads directly into an in-memory buffer (`IngesterChannel`) to keep client response times minimal.
* **Broker**: Coordinates the background loops for batching, scheduling, TCP connection handling, heartbeats, and acknowledgements.
* **Redis Streams**: Acts as the single source of durable truth for job state, using consumer groups to distribute and partition workload.
* **In-memory Scheduling Queue**: Buffers worker inquiry identifiers (`Worker_inquiry_channel`) to map ready TCP workers to available jobs without continuously polling Redis.
* **TCP-Connected Workers**: Process tasks delivered via a stateful TCP connection using a custom binary frame protocol.
* **Lease Management**: Dynamically computes processing leases (`LeaseVar`) based on worker performance feedback to minimize failover detection times.
* **ACK Processing**: Batches completed task identifiers and issues pipeline calls (`XAck` & `XDel`) to mark jobs as done and clear them from Redis.
* **Dead-Letter Recovery**: Identifies repeatedly failing jobs, isolates them by writing to a dedicated dead-letter stream, and deletes them from the primary stream.

---

## Internal Workflows

### 1. Ingestion: Producer → Broker → Redis Stream
Producers submit JSON payloads to the broker's HTTP server (`POST /ingest`).
1. The ingest handler parses the payload and enqueues it to `IngesterChannel` (buffered up to 100,000 jobs).
2. The `StartIngester` goroutine drains this channel.
3. To eliminate network overhead, it accumulates jobs and flushes them to Redis via a pipeline (`XAdd`) in batches of **1,000 jobs** or every **5 milliseconds**.

### 2. In-Memory Broker Scheduler
Jobs are scheduled reactively to prevent polling loops:
1. When a worker is idle, it sends a TCP `PULL` frame to the broker.
2. The broker registers the worker's availability by placing its UUID in the `Worker_inquiry_channel`.
3. Two scheduling loops watch this channel:
   * **`Feed_to_worker`**: Reads new (`>`) jobs from the primary Redis Stream via `XReadGroup` and passes them to the worker feeder.
   * **`Pending_jobs`**: Scans the stream's pending list entries (`XPendingExt`) for expired leases, claims them via `XClaim`, and redirects them for reprocessing.
4. The **`Worker_feeder`** retrieves the message from the queue, records its dispatch timestamp, and pushes the payload directly down the worker's TCP socket.

### 3. Adaptive Lease Management
Traditional visibility timeouts are static. `mbroke` implements a dynamic feedback loop:
1. When a worker completes a job, it sends a `TACK` frame containing the job ID.
2. The broker calculates the processing duration: `duration = time.Since(SentTime[jobID])` and forwards it to the `Time_channel`.
3. The `Lease_routine` reads processing times and computes a rolling average:
   $$\text{avg} = \frac{9 \times \text{avg} + \text{duration}}{10}$$
4. Every second, the visibility timeout is updated dynamically:
   $$\text{LeaseVar} = \max(\min(2 \times \text{avg} \times \text{BatchSize}, \text{MaxLease}), \text{MinLease})$$
   * *`MinLease` = 1 second*
   * *`MaxLease` = 30 seconds*
5. This dynamic lease duration is passed directly to the `Pending_jobs` reclaimer for accurate, adaptive recovery times.

### 4. Batched ACK Processing
Just as job ingestion is batched, acknowledgements are aggregated to prevent Redis write congestion:
1. When a task is acknowledged (`TACK`), its job ID is sent to the `ACK_channel`.
2. The `Acker` routine collects these IDs and issues a batched Redis command pipeline every **2,000 milliseconds**.
3. It performs an `XAck` to acknowledge the consumer group, followed by `XDel` to remove the payload from the stream. Deleting processed messages keeps Redis memory consumption strictly bounded.

### 5. Dead-Letter Recovery (DLQ)
If a task repeatedly fails or causes worker crashes:
1. The `Pending_jobs` loop inspects the delivery count of pending jobs fetched via `XPendingExt`.
2. If a job's retry count exceeds the configured `RETRY_COUNT` (default: 3):
   * It is read from the stream via `XRange`.
   * It is written to the dead-letter stream specified by `DEAD_LETTER_NAME` (default: `dead_letter`).
   * It is removed from the primary stream via `XAck` and `XDel`.

### 6. Worker Discovery and Heartbeats
Active worker tracking prevents scheduling jobs to disconnected or frozen workers:
1. Upon connecting, the worker issues a `CONNECT` frame containing the shared `SECRET`.
2. Upon authentication, the broker registers the worker with a unique UUID and adds it to a Redis sorted set (`workerset`) with a score set to `time.Now().Unix() + 10`.
3. The worker sends periodic `HEARTBEAT` frames (every 100ms in benchmarks) containing its next expected score. The broker updates the sorted set with this value.
4. A background loop (`check_heartbeat`) scans the sorted set every **2 seconds** using `ZRangeByScore` to identify workers that have missed their heartbeat window.
5. If a worker is deemed dead, its connection is closed, it is removed from the active client map and the sorted set, and its leased jobs are left to be reclaimed by `Pending_jobs`.

---

## TCP Worker Protocol Reference

Workers communicate with the broker over a custom, lightweight binary protocol designed for speed and low CPU overhead. 

### Message Frame Format

All TCP messages are framed with a 5-byte header followed by a variable-length payload:
* **Length** (4 Bytes / Big-Endian): The total length of the remaining frame (`1 + len(Payload)`).
* **MsgType** (1 Byte): Code representing the command type.
* **Payload** (N Bytes): Raw byte sequence matching the message type format.

### Message Types
* **`CONNECT`** (Value: `0`, Worker $\rightarrow$ Broker): The authentication shared secret string.
* **`HEARTBEAT`** (Value: `1`, Worker $\leftrightarrow$ Broker): String representation of the client's current Unix timestamp + 10s. Broker replies with `0` if worker is unknown/rejected.
* **`TACK`** (Value: `2`, Worker $\leftrightarrow$ Broker): Client sends the Job ID string to acknowledge completion, or `"dack"` to NACK. Broker replies with `1` on success, `0` on rejection.
* **`PULL`** (Value: `3`, Worker $\leftrightarrow$ Broker): Client sends empty payload to request jobs. Broker responds with a `PULL` frame containing a JSON array of `[]types.JobInfo`.
* **`INVALID`** (Value: `4`, Broker $\rightarrow$ Worker): Sent by the broker when an invalid message type is received. Returns `0`.

---

## Configuration Reference

The broker reads configuration parameters from environment variables (or a local `.env` file).

### Broker & Queue Variables
* `PORT` (Default: `8080`): Port for HTTP ingestion and administration endpoint.
* `TCP_SERVER_PORT` (Default: `9000`): Port for the stateful TCP worker server.
* `STREAM_NAME` (Default: `ingest:primary`): Redis Stream name used for durable job storage.
* `CONSUMER_GROUP_NAME` (Default: `primary`): Redis Stream Consumer Group name.
* `DEAD_LETTER_NAME` (Default: `dead_letter`): Redis Stream name for dead-lettered jobs.
* `BATCH_SIZE` (Default: `100`): Max number of jobs dispatched per worker poll.
* `RETRY_COUNT` (Default: `3`): Maximum processing attempts before dead-lettering.
* `SECRET` (Default: `secret`): Shared authentication secret required for workers.
* `SET_NAME` (Default: `workerset`): Key name for the Redis sorted set tracking worker heartbeats.
* `MAX_LEN` (Default: `2000000`): Maximum length configuration defined in client structs.

### Redis Connection Variables
* `REDIS_ADDR` (Default: `redis:6379`): Redis server address.
* `REDIS_POOL_SIZE` (Default: `10`): Redis client connection pool size.
* `REDIS_PASSWORD` (Default: *Empty*): Redis password.
* `REDIS_DB` (Default: `0`): Redis Database index.
* `REDIS_PROTOCOL` (Default: `2`): Redis connection protocol (`2` or `3`).

---

## Performance & Runtime Statistics

The statistics logs represent the real-time operational state of the broker under load.

### Startup Initialization

When launching the broker, the system starts HTTP listeners, registers the `/ingest` routing, launches the TCP server on port `9000`, and starts background goroutines for ingesting, claiming, and auditing workers.

![Startup Logs](screenshots/screenshot-2026-06-26_20.27.54.png)
*Figure 1: Broker startup logs displaying successful TCP and HTTP server initialization.*

---

### High-Throughput Ingestion (Producers Active, Workers Offline)

During the pure ingestion phase, producers write to the `/ingest` HTTP endpoint while no workers are connected (`workers = 0`).

![Ingestion Statistics](screenshots/screenshot-2026-06-26_20.28.24.png)
*Figure 2: Ingestion statistics showing a peak throughput of ~58k jobs/second with no connected workers while Redis stream length continuously increases.*

* **Analysis**: Ingestion throughput consistently runs between **50k and 58k jobs/sec**. This demonstrates the efficiency of the broker's in-memory buffering channel coupled with Redis pipeline writes, bypassing single-item roundtrip network overhead.

---

### Concurrent Job Scheduling & Leasing

When consumers are introduced to the cluster, the broker schedules jobs concurrently.

![Dispatch and Scheduling](screenshots/screenshot-2026-06-26_20.30.51.png)
*Figure 3: Scheduling stats showing ~250 workers and a dispatch peak of ~35k jobs/second while pending leases fluctuate as workers process jobs.*

* **Analysis**: With **250 active workers**, dispatch peaks at roughly **35.5k jobs/sec**. The `pending` lease count fluctuates rapidly as workers acquire and acknowledge tasks concurrently.

---

### Queue Draining

This phase showcases workers consuming the remaining queue backlog after producers stop sending jobs.

![Queue Draining Statistics](screenshots/screenshot-2026-06-26_20.36.58.png)
*Figure 4: Queue draining process. Shows workers draining the queue. The broker dispatches jobs in bursts while pending jobs decrease until the stream is empty.*

* **Analysis**: Jobs are dispatched in bursts (`out/s` spikes to ~35k, then drops to 0, while `pending` jobs step down in blocks). The broker processes worker inquiries in batch iterations. Pending leases decrease rapidly as workers resolve tasks, successfully emptying the stream to `0` without leaking stale leases.

---

### Automatic Recovery of Failed Jobs (Dead-Letter Queue)

If a task repeatedly fails or causes worker crashes, the broker isolates the job without interrupting the rest of the queue.

![Dead-Letter Recovery in Redis](screenshots/image.png)
*Figure 5: Redis console showing isolated dead-lettered jobs in the dead_letter stream after exceeding the maximum retry threshold.*

* **Failed Job Detection**: The reclaimer loop tracks the delivery count of each pending job.
* **Expired Leases**: When a job remains in the pending state longer than the computed dynamic visibility lease, it is scanned and claimed by the reclaimer loop.
* **Dead-Letter Isolation**: If a job's delivery count exceeds `RETRY_COUNT` (default: 3), it is extracted from the primary stream, written to the `dead_letter` stream, and deleted from the primary stream. This prevents bad payloads from blocking subsequent jobs.
* **Continuous Processing**: Workers continue processing the remaining valid items in the queue without manual intervention or broker downtime.

---

## Installation & Usage

### Prerequisites
* Go 1.22 or higher
* Redis 6.2 or higher (Redis 7.0+ recommended)

### Clone the Repository
```bash
git clone https://github.com/mbroke/mbroke.git
cd mbroke
```

### Running the Broker

1. **Configure Environment**: Create a `.env` file in the root directory:
   ```env
   PORT=8080
   TCP_SERVER_PORT=9000
   STREAM_NAME=ingest:primary
   DEAD_LETTER_NAME=dead_letter
   SECRET=your_auth_secret
   REDIS_ADDR=127.0.0.1:6379
   BATCH_SIZE=100
   RETRY_COUNT=3
   ```
2. **Start the Broker**:
   ```bash
   go run main.go
   ```

### Running the Benchmarks

The repository provides benchmarking tools to validate throughput and chaos resilience.

#### 1. Running the Worker Client
The benchmark worker simulates production conditions, complete with random network drops, processing delays, zombie states, and garbage data injection.
```bash
cd _bench/consumer
# Ensure .env is configured with correct SECRET and BROKER_URL
go run main.go
```

#### 2. Running the Job Producer
The producer injects synthetic workloads at maximum throughput and runs the monitoring thread.
```bash
cd _bench/producer
go run producer.go
```

---

## Project Structure

```
.
├── main.go               # Broker entry point; initializes server and background loops
├── routes
│   └── ingest.go         # HTTP POST /ingest handler
├── types
│   └── types.go          # Shared Structs (Job, Worker, Messages, Protocol Frames)
├── utils
│   ├── config.go         # Environment configuration parser & defaults
│   ├── lease.go          # Adaptive lease calculations and background routine
│   ├── redis.go          # Redis client wrapper & global communication channels
│   ├── redis_fucntions.go# Redis Stream interactions, Ingestion, Reclaimer, DLQ, ZSet
│   └── worker_handler.go # TCP Server, protocol frame encoder/decoder, heartbeats
├── _bench                # Ingestion and Worker benchmarks for throughput testing
│   ├── consumer          # Simulated TCP worker suite with chaos generation
│   └── producer          # HTTP ingestion load tester & stats monitoring
└── Dockerfile            # Container definition for the broker
```

---

## Design Decisions & Rationale

* **Raw TCP Sockets for Workers**: Using heavier protocols like HTTP/1.1 introduces standard header structures and connection framing overhead. A raw TCP socket with a custom 5-byte header allows parsing at wire-speed with near-zero allocation overhead.
* **In-Memory Go Channels for Buffering**: The broker acts as an asynchronous buffer. Discarding synchronous database writes in favor of a buffered Go channel (`IngesterChannel`) allows HTTP producers to get instant `201 Created` responses, deferring Redis writing to optimized batch workers.
* **Redis Sorted Set for Worker Heartbeats**: Storing worker scores (`Timestamp + 10s`) inside a sorted set allows the broker to perform dead-worker scans using a single $O(\log N + M)$ range command (`ZRangeByScore`), avoiding costly iterations over large active client maps.
* **Lease-Deletion Model for GC**: Instead of keeping processed jobs in the stream history, `mbroke` issues an `XDel` immediately after `XAck`. This turns the Redis Stream into a strict FIFO ring-buffer, preventing infinite memory leaks and keeping Redis RAM footprint constant.
