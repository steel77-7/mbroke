package socket

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync/atomic"

	"github.com/joho/godotenv"
)

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

type BrokerClient struct {
	conn      net.Conn
	brokerIP  string
	port      string
	secret    string
	queue     chan Message
	pullReady chan struct{}
	done      chan struct{}
	closeOnce int32
}

func NewBrokerClient() *BrokerClient {
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
	}
}

func (b *BrokerClient) Connect() error {
	conn, err := net.Dial("tcp", b.brokerIP+":"+b.port)
	if err != nil {
		return err
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

// PullLoop sends PULL requests as fast as the broker can serve them.
// Instead of polling on a timer, it waits for a signal that we're ready for the next job.
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
		// heartbeat ack — no action needed

	case TACK:
		// task ack from broker — no action needed

	case PULL:
		payload := string(data.Payload)

		if payload == "0" || payload == "" {
			// no job available, pull again immediately
			b.signalPull()
			return
		}

		// process job inline (no goroutine overhead — ReadLoop is the worker)
		b.processJob(payload)

	default:
		log.Printf("unknown message type %d", data.MsgType)
	}
}

func (b *BrokerClient) processJob(job string) {
	// send ACK immediately
	b.queue <- Message{
		Length:  2,
		MsgType: TACK,
		Payload: []byte("1"),
	}

	// ready for next job
	b.signalPull()
}

func (b *BrokerClient) Writer() {
	// pre-allocate a write buffer to avoid allocations per send
	buf := make([]byte, 0, 512)

	for {
		select {
		case msg := <-b.queue:
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

func (b *BrokerClient) Close() {
	if atomic.CompareAndSwapInt32(&b.closeOnce, 0, 1) {
		fmt.Println("disconnected from broker")
		close(b.done)

		if b.conn != nil {
			_ = b.conn.Close()
		}
	}
}
