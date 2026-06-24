package socket

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/joho/godotenv"
)

type JobInfo struct {
	ID   string `json:"id"`
	Data string `json:"data"`
}

type Ack_request struct {
	ID  string `json:"id"`
	ACK bool   `json:"ack"`
}

type MessageType uint8

const (
	CONNECT   MessageType = 0
	HEARTBEAT MessageType = 1
	TACK      MessageType = 2
	PULL      MessageType = 3
	INVALID   MessageType = 4
)

type Message struct {
	Length  uint32
	MsgType MessageType
	Payload []byte
}

// chaos knobs — tune these to make it more or less brutal
// const (
// 	// crash probabilities (per event)
// 	crashOnJobProb  = 0.03 // 3% chance of crashing while processing a job
// 	crashRandomProb = 0.01 // 1% chance per heartbeat tick of random crash
// 	zombieProb      = 0.02 // 2% chance of becoming a zombie (stop responding but keep conn open)
// 	halfWriteProb   = 0.02 // 2% chance of writing a partial message then dying
// 	garbageProb     = 0.02 // 2% chance of sending garbage bytes
// 	nackProb        = 0.15 // 15% chance of NACKing a job (sending "0" TACK)
// 	slowProcessProb = 0.10 // 10% chance of very slow processing (2-5 seconds)
// 	doubleAckProb   = 0.03 // 3% chance of sending duplicate TACK
// 	wrongTypeProb   = 0.02 // 2% chance of sending a message with wrong type byte

//	// timing
//	heartbeatInterval = 3 * time.Second
//	minProcessTime    = 5 * time.Millisecond
//	maxProcessTime    = 100 * time.Millisecond
//	slowProcessMin    = 2 * time.Second
//	slowProcessMax    = 5 * time.Second
//	zombieLifetime    = 30 * time.Second // how long a zombie holds the connection
//	reconnectDelay    = 500 * time.Millisecond
//
// )
const (
	// crash probabilities (per event)
	crashOnJobProb  = 0 // 3% chance of crashing while processing a job
	crashRandomProb = 0 // 1% chance per heartbeat tick of random crash
	zombieProb      = 0 // 2% chance of becoming a zombie (stop responding but keep conn open)
	halfWriteProb   = 0 // 2% chance of writing a partial message then dying
	garbageProb     = 0 // 2% chance of sending garbage bytes
	nackProb        = 0 // 15% chance of NACKing a job (sending "0" TACK)
	slowProcessProb = 0 // 10% chance of very slow processing (2-5 seconds)
	doubleAckProb   = 0 // 3% chance of sending duplicate TACK
	wrongTypeProb   = 0 // 2% chance of sending a message with wrong type byte

	// timing
	heartbeatInterval = 100 * time.Millisecond
	minProcessTime    = 10 * time.Millisecond
	maxProcessTime    = 10 * time.Millisecond
	slowProcessMin    = 100 * time.Millisecond
	slowProcessMax    = 100 * time.Millisecond
	zombieLifetime    = 0 * time.Second // how long a zombie holds the connection
	reconnectDelay    = 500 * time.Millisecond
)

type BrokerClient struct {
	conn      net.Conn
	brokerIP  string
	port      string
	secret    string
	queue     chan Message
	pullReady chan struct{}
	done      chan struct{}
	closeOnce int32
	id        int // for logging

	// stats
	jobsProcessed atomic.Int64
	jobsNacked    atomic.Int64
	crashes       atomic.Int64
}

func NewBrokerClient(id int) *BrokerClient {
	_ = godotenv.Load()

	brokerIP := os.Getenv("BROKER_URL")
	port := os.Getenv("TCP_SERVER_PORT")
	secret := os.Getenv("SECRET")

	if brokerIP == "" || port == "" {
		return nil
	}

	return &BrokerClient{
		brokerIP:  brokerIP,
		port:      port,
		secret:    secret,
		queue:     make(chan Message, 256),
		pullReady: make(chan struct{}, 1),
		done:      make(chan struct{}),
		id:        id,
	}
}

func (b *BrokerClient) Connect() error {
	conn, err := net.Dial("tcp", b.brokerIP+":"+b.port)
	if err != nil {
		return err
	}

	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	b.conn = conn

	connectMsg := Message{
		Length:  uint32(len(b.secret) + 1),
		MsgType: CONNECT,
		Payload: []byte(b.secret),
	}

	if err := b.Send(connectMsg); err != nil {
		return err
	}

	go b.ReadLoop()
	go b.Writer()
	go b.PullLoop()
	go b.HeartbeatLoop()
	go b.ChaosTimer()

	// signal first pull immediately
	b.signalPull()

	return nil
}

func (b *BrokerClient) signalPull() {
	select {
	case b.pullReady <- struct{}{}:
	default:
	}
}

func (b *BrokerClient) logf(format string, args ...any) {
	prefix := fmt.Sprintf("[worker-%d] ", b.id)
	log.Printf(prefix+format, args...)
}

// HeartbeatLoop sends periodic heartbeats with the current timestamp + 10s
func (b *BrokerClient) HeartbeatLoop() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ts := float64(time.Now().Unix() + 10)
			payload := strconv.FormatFloat(ts, 'f', 0, 64)

			select {
			case b.queue <- Message{
				Length:  uint32(1 + len(payload)),
				MsgType: HEARTBEAT,
				Payload: []byte(payload),
			}:
			case <-b.done:
				return
			}

		case <-b.done:
			return
		}
	}
}

// ChaosTimer randomly kills the connection after some time
func (b *BrokerClient) ChaosTimer() {
	// random lifetime between 5-60 seconds
	lifetime := time.Duration(5+rand.IntN(55)) * time.Second

	select {
	case <-time.After(lifetime):
		if rand.Float64() < crashRandomProb*10 { // ~10% chance at lifetime expiry
			b.logf("CHAOS: random crash after %v", lifetime)
			b.crashes.Add(1)
			b.crashHard()
		}
	case <-b.done:
		return
	}
}

// PullLoop sends PULL requests when signaled
func (b *BrokerClient) PullLoop() {
	pullMsg := Message{Length: 1, MsgType: PULL}

	for {
		select {
		case <-b.pullReady:
			select {
			case b.queue <- pullMsg:
			case <-b.done:
				return
			}
		case <-b.done:
			return
		}
	}
}

func (b *BrokerClient) ReadLoop() {
	var header [5]byte

	for {
		_, err := io.ReadFull(b.conn, header[:])
		if err != nil {
			b.Close()
			return
		}

		length := binary.BigEndian.Uint32(header[:4])
		msgType := MessageType(header[4])

		if length < 1 {
			b.Close()
			return
		}

		var payload []byte

		if length > 1 {
			payload = make([]byte, length-1)

			_, err = io.ReadFull(b.conn, payload)
			if err != nil {
				b.Close()
				return
			}
		}

		b.MessageHandler(Message{
			Length:  length,
			MsgType: msgType,
			Payload: payload,
		})
	}
}

func (b *BrokerClient) MessageHandler(data Message) {
	switch data.MsgType {

	case HEARTBEAT:
		if string(data.Payload) == "0" {
			b.logf("HEARTBEAT rejected by broker — we're marked dead")
			b.Close()
			return
		}

	case TACK:
		if string(data.Payload) == "0" {
			b.logf("TACK rejected by broker — we're marked dead")
			b.Close()
			return
		}

	case PULL:
		if len(data.Payload) == 0 {
			b.signalPull()
			return
		}

		payloadStr := string(data.Payload)
		if payloadStr == "0" || payloadStr == "" {
			b.signalPull()
			return
		}

		var jobs []JobInfo
		if err := json.Unmarshal(data.Payload, &jobs); err != nil {
			b.logf("failed to unmarshal jobs array: %v (payload: %s)", err, payloadStr)
			b.signalPull()
			return
		}

		if len(jobs) == 0 {
			b.signalPull()
			return
		}

		for _, job := range jobs {
			b.processJob(job)
		}
		b.signalPull()

	default:
		b.logf("unknown message type %d", data.MsgType)
	}
}

func (b *BrokerClient) processJob(job JobInfo) {

	// === CHAOS: crash mid-job (before ACK) ===
	if rand.Float64() < crashOnJobProb {
		b.logf("CHAOS: crash mid-job (no ACK sent)")
		b.crashes.Add(1)
		b.crashHard()
		return
	}

	// === CHAOS: become a zombie ===
	if rand.Float64() < zombieProb {
		b.logf("CHAOS: becoming zombie — holding connection open, not responding")
		b.crashes.Add(1)
		// just sleep holding the connection, then die
		time.Sleep(zombieLifetime)
		b.crashHard()
		return
	}

	// === simulate processing time ===
	if rand.Float64() < slowProcessProb {
		// very slow processing
		delay := slowProcessMin + time.Duration(rand.Int64N(int64(slowProcessMax-slowProcessMin)))
		time.Sleep(delay)
	} else {
		// normal processing
		delay := minProcessTime + time.Duration(rand.Int64N(int64(maxProcessTime-minProcessTime)))
		time.Sleep(delay)
	}

	// === CHAOS: send garbage bytes ===
	if rand.Float64() < garbageProb {
		b.logf("CHAOS: sending garbage bytes")
		garbage := make([]byte, 10+rand.IntN(50))
		for i := range garbage {
			garbage[i] = byte(rand.IntN(256))
		}
		b.conn.Write(garbage)
		b.crashHard()
		return
	}

	// === CHAOS: half-written message then crash ===
	if rand.Float64() < halfWriteProb {
		b.logf("CHAOS: half-written TACK then crash")
		// write just the length header, then die
		var partial [3]byte
		binary.BigEndian.PutUint16(partial[:2], 2)
		b.conn.Write(partial[:])
		b.crashHard()
		return
	}

	// === CHAOS: send wrong message type ===
	if rand.Float64() < wrongTypeProb {
		b.logf("CHAOS: sending message with wrong type byte")
		wrongType := MessageType(10 + rand.IntN(240)) // some random invalid type
		b.queue <- Message{
			Length:  uint32(1 + len(job.ID)),
			MsgType: wrongType,
			Payload: []byte(job.ID),
		}
		return
	}

	// === decide ACK or NACK ===
	ackPayload := job.ID
	if rand.Float64() < nackProb {
		ackPayload = "dack"
		b.jobsNacked.Add(1)
	} else {
		b.jobsProcessed.Add(1)
	}

	b.queue <- Message{
		Length:  uint32(1 + len(ackPayload)),
		MsgType: TACK,
		Payload: []byte(ackPayload),
	}

	// === CHAOS: duplicate TACK ===
	if rand.Float64() < doubleAckProb {
		b.logf("CHAOS: sending duplicate TACK")
		b.queue <- Message{
			Length:  uint32(1 + len(ackPayload)),
			MsgType: TACK,
			Payload: []byte(ackPayload),
		}
	}
}

func (b *BrokerClient) Writer() {
	buf := make([]byte, 0, 512)

	for {
		select {
		case msg := <-b.queue:
			start := time.Now()
			buf = buf[:5]
			binary.BigEndian.PutUint32(buf[:4], msg.Length)
			buf[4] = byte(msg.MsgType)
			buf = append(buf, msg.Payload...)

			total := 0
			for total < len(buf) {
				n, err := b.conn.Write(buf[total:])
				if err != nil {
					b.Close()
					return
				}
				total += n
			}
			log.Print("send:", time.Since(start))

		case <-b.done:
			return
		}
	}
}

// Send is used only for the initial CONNECT message.
func (b *BrokerClient) Send(msg Message) error {
	header := [5]byte{}
	binary.BigEndian.PutUint32(header[:4], msg.Length)
	header[4] = byte(msg.MsgType)

	packet := append(header[:], msg.Payload...)

	total := 0
	for total < len(packet) {
		n, err := b.conn.Write(packet[total:])
		if err != nil {
			return err
		}
		total += n
	}

	return nil
}

// crashHard closes the connection abruptly without cleanup
func (b *BrokerClient) crashHard() {
	if b.conn != nil {
		// set linger to 0 to force RST instead of graceful FIN
		if tc, ok := b.conn.(*net.TCPConn); ok {
			tc.SetLinger(0)
		}
		b.conn.Close()
	}
	// signal done to stop all goroutines
	b.Close()
}

func (b *BrokerClient) Close() {
	if atomic.CompareAndSwapInt32(&b.closeOnce, 0, 1) {
		b.logf("disconnected (processed: %d, nacked: %d, crashes: %d)",
			b.jobsProcessed.Load(), b.jobsNacked.Load(), b.crashes.Load())
		close(b.done)

		if b.conn != nil {
			_ = b.conn.Close()
		}
	}
}

func (b *BrokerClient) Stats() (processed, nacked, crashes int64) {
	return b.jobsProcessed.Load(), b.jobsNacked.Load(), b.crashes.Load()
}

func (b *BrokerClient) Done() <-chan struct{} {
	return b.done
}
