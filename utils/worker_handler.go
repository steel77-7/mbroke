package utils

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mbroke/types"
)

type Message_type int

const (
	CONNECT = iota
	HEARTBEAT
	TACK
	PULL
	INVALID
)

// local structs for worker communication and custom protocol
type Client struct {
	conn      net.Conn
	id        string
	ready     bool
	last_ping time.Time
	auth      bool
	mu        *sync.Mutex
	quitch    chan struct{}
}

type Server struct {
	Addr     string
	Listener net.Listener
	quitch   chan struct{}
	mu       *sync.Mutex
	clients  map[string]*Client
}

// ---------------------
// Worker TCP communication server
// ---------------------
func NewServer(addr string) *Server {
	return &Server{
		Addr:    addr,
		clients: make(map[string]*Client),
		mu:      &sync.Mutex{},
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		log.Print("Error in starting the tcp server: ", ln)
		return err
	}
	s.Listener = ln
	log.Print("server started")
	defer ln.Close()
	go s.accept_loop()
	go s.Worker_feeder()
	go s.check_heartbeat()
	<-s.quitch
	return nil
}

func (s *Server) accept_loop() {
	for {
		conn, err := s.Listener.Accept()

		if err != nil {
			log.Print("Couldnt accept connection :", err)
			continue
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
		}
		go s.read_loop(conn)
	}
}

func (s *Server) send(w io.Writer, kind byte, payload []byte) error {
	length := uint32(1 + len(payload))
	var header [5]byte
	binary.BigEndian.PutUint32(header[:], length)
	header[4] = kind
	if _, err := w.Write(header[:]); err != nil {
		return err
	}

	if len(payload) > 0 {
		_, err := w.Write(payload)
		return err
	}
	return nil
}

// worker connection are accepted here
func (s *Server) read_loop(conn net.Conn) {
	var len_buf [4]byte
	if _, err := io.ReadFull(conn, len_buf[:]); err != nil {
		log.Print()
		return
	}
	length := binary.BigEndian.Uint32(len_buf[:])
	if length < 1 {
		log.Print("Coudlnt establish connection [LENGTH IS 0] ")
		return
	}

	type_buf := make([]byte, 1)
	if _, err := io.ReadFull(conn, type_buf[:]); err != nil {
		log.Print("couldnt read the type:", err)
		return
	}

	if Message_type(type_buf[0]) != CONNECT {
		log.Print("IDK TF IS WRONG HERE //// or someone malicious")
		return
	}

	payload_len := int(length - 1)
	payload := make([]byte, payload_len)

	if _, err := io.ReadFull(conn, payload[:]); err != nil {
		log.Print("couldnt read the payload:", err)
		return
	}

	if string(payload) != Conf.Secret {
		conn.Close()
		log.Print("Connection closed")
		return
	}
	id := fmt.Sprint(uuid.New())

	s.mu.Lock()
	c := &Client{
		conn:   conn,
		id:     id,
		ready:  true,
		mu:     &sync.Mutex{},
		quitch: make(chan struct{}, 2),
	}
	s.clients[id] = c
	s.mu.Unlock()
	Add_to_set(id)

	for {
		s.mu.Lock()
		val, ok := s.clients[id]
		s.mu.Unlock()
		if ok {
			select {
			case <-val.quitch:
				{

					return
				}
			default:
				{
					c.message_handler()
				}
			}
		} else {
			log.Print("disconnecting (client unregistered)")
			return
		}
	}
}

// Workers are delivered jobs here
// Pushing them into queues
// Workers poll the server to retrieve jobs
func (s *Server) Worker_feeder() {

	var payload types.WorkerFeeding
	for {

		payload = <-Worker_feeder_channel

		id := payload.ID

		ok := Present_in_set(id)
		s.mu.Lock()
		val, _ := s.clients[id]
		s.mu.Unlock()

		if ok {
			var jobIDs []string
			var jobs []types.JobInfo
			for _, msg := range payload.Data {
				jobIDs = append(jobIDs, msg.ID)
				dataVal, _ := msg.Values["data"].(string)
				jobs = append(jobs, types.JobInfo{
					ID:   msg.ID,
					Data: dataVal,
				})
			}

			tbs, _ := json.Marshal(jobs)
			val.mu.Lock()
			err := val.send(PULL, tbs)
			if err != nil {
				log.Print("couldnt send the pull res", err)
			}

			val.mu.Unlock()

		}

	}
}

// checks if a worker is alive or not
// if not then job is assigned to another worker
// if yes then worker continues to process the job until its lease expires
func (s *Server) check_heartbeat() {

	for {
		time.Sleep(time.Duration(2) * time.Second)

		var dead_workers []string = Fetch_dead_workers(float64(time.Now().Unix()))
		for _, dead := range dead_workers {

			Remove_from_set(dead)
			s.mu.Lock()
			val, o := s.clients[dead]
			s.mu.Unlock()

			if !o {
				Del_channel <- dead
				continue
			}
			val.mu.Lock()
			val.conn.Close()
			val.quitch <- struct{}{}
			s.mu.Lock()

			delete(s.clients, dead)
			s.mu.Unlock()

			val.mu.Unlock()
			Del_channel <- dead
		}

	}
}

func (client *Client) send(kind byte, payload []byte) error {
	length := uint32(1 + len(payload))
	var header [5]byte
	var w io.Writer = client.conn
	binary.BigEndian.PutUint32(header[0:4], length)
	header[4] = kind
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		_, err := w.Write(payload)
		return err
	}
	return nil
}

// custom protocol for reading messages
func (client *Client) read_message() (msg types.Message, err error) {

	var len_buf [4]byte
	var r io.Reader = client.conn

	if _, err := io.ReadFull(r, len_buf[:]); err != nil {
		return types.Message{}, err
	}
	length := binary.BigEndian.Uint32(len_buf[:])
	if length < 1 {
		return types.Message{}, fmt.Errorf("invalid length")
	}

	msgTypeBuf := make([]byte, 1)
	if _, err = io.ReadFull(r, msgTypeBuf); err != nil {
		return types.Message{}, err
	}
	msg_type := msgTypeBuf[0]

	msg = types.Message{
		Length:   length,
		Msg_type: msg_type,
	}

	payload_len := length - 1
	if payload_len > 0 {
		payload := make([]byte, payload_len)
		if _, err := io.ReadFull(r, payload[:]); err != nil {
			return types.Message{}, err
		}
		msg.Payload = payload
	}

	return msg, nil
}

func (client *Client) message_handler() {
	msg, err := client.read_message()
	if err != nil {
		client.conn.Close()
		client.quitch <- struct{}{}
		return
	}

	switch Message_type(msg.Msg_type) {
	case CONNECT:
		{
			client.auth = true
			client.ready = true
		}
	case HEARTBEAT:
		{
			ok := Present_in_set(client.id)
			now, _ := strconv.ParseFloat(string(msg.Payload), 64)
			if !ok {
				client.mu.Lock()
				err := client.send(HEARTBEAT, []byte("0"))
				if err != nil {
					log.Print("couldnt send the heartbeat res")
				}
				client.mu.Unlock()

				break
			}
			Update_score(client.id, now)
		}

	case TACK:
		{
			ok := Present_in_set(client.id)
			if !ok {
				client.mu.Lock()

				err := client.send(TACK, []byte("0"))
				if err != nil {
					log.Print("couldnt send data")
				}
				client.mu.Unlock()
				return
			}

			jobID := string(msg.Payload)
			if jobID != "" {
				ACK_channel <- jobID
			}
			client.mu.Lock()

			err := client.send(TACK, []byte("1"))
			if err != nil {
				log.Print("couldnt send the tack res")
			}
			client.mu.Unlock()

		}
	case PULL:
		{
			Worker_inquiry_channel <- client.id
		}
	default:
		{
			client.mu.Lock()

			err := client.send(INVALID, []byte("0"))
			if err != nil {
				log.Print("couldnt send the invalid res")
			}
			client.mu.Unlock()

		}
	}
}
