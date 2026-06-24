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

type Client struct { //the workers
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

// the problem is here
// jaha jaha pe worker map use hua hai waha pe redis lgwa de
// dhang se krna aur
var Worker_map types.Work_map = types.Work_map{
	Mu:   &sync.RWMutex{},
	List: make(map[string]*types.Worker),
}

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

		//log.Print("new")
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

// [....][.][,,,,,,,,]protocol ka format
func (s *Server) read_loop(conn net.Conn) {
	var len_buf [4]byte
	//	var r io.Reader
	if _, err := io.ReadFull(conn, len_buf[:]); err != nil {
		log.Print()
		return
	}
	length := binary.BigEndian.Uint32(len_buf[:])
	if length < 1 {
		log.Print("Coudlnt establish connection [LENGTH IS 0] ")
		return
	}
	//log.Print("the length: ", string(len_buf[:]))

	type_buf := make([]byte, 1)
	if _, err := io.ReadFull(conn, type_buf[:]); err != nil {
		log.Print("couldnt read the type:", err)
		return
	}
	//log.Print("the type: ", type_buf[:])
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
	//	log.Print("paylaod: ", string(payload))
	if string(payload) != Conf.Secret {
		conn.Close()
		log.Print("Connection closed")
		return
	}
	id := fmt.Sprint(uuid.New())
	//log.Print(id)
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
	//log.Print("reaached the server registeration")
	for {
		s.mu.Lock()
		val, ok := s.clients[id]
		s.mu.Unlock()
		if ok {
			select {
			case <-val.quitch:
				{
					//log.Print("disconneting")
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
func (s *Server) Worker_feeder() {
	// ticker := time.NewTicker(10 * time.Millisecond)
	// defer ticker.Stop()
	var payload types.WorkerFeeding
	for {
		//log.Print("Worker_feeder_len:", len(Worker_feeder_channel))

		//start := time.Now()
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
			// new_worker := &types.Worker{
			// 	ID:        id,
			// 	Job_id:    strings.Join(jobIDs, ","),
			// 	Last_ping: time.Now().UTC().UnixMilli(),
			// }
			//Add_to_map(new_worker)
			tbs, _ := json.Marshal(jobs)
			//log.Print("job , ", string(tbs))
			val.mu.Lock()
			err := val.send(PULL, tbs)
			if err != nil {
				log.Print("couldnt send the pull res", err)
			}
			//log.Print("send:", time.Since(start))
			//go val.send(PULL, []byte(tbs))
			val.mu.Unlock()

		}

	}
}

func (s *Server) check_heartbeat() {

	for {
		time.Sleep(time.Duration(1) * time.Second)

		var dead_workers []string = Fetch_dead_workers(float64(time.Now().Unix()))
		for _, dead := range dead_workers {
			//kickj from the map
			// from the client thing
			// clear the job for next pull
			//	Remove_worker_from_map(dead)
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
			//Del_consumer([]string{dead})
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

func (client *Client) read_message() (msg types.Message, err error) {
	//log.Print("read message")
	//start := time.Now()

	var len_buf [4]byte
	var r io.Reader = client.conn

	if _, err := io.ReadFull(r, len_buf[:]); err != nil { ////here
		//	log.Print("it closed alrweadt")
		return types.Message{}, err
	}
	length := binary.BigEndian.Uint32(len_buf[:])
	if length < 1 {
		return types.Message{}, fmt.Errorf("invalid length")
	}
	//	start := time.Now()

	msgTypeBuf := make([]byte, 1)
	if _, err = io.ReadFull(r, msgTypeBuf); err != nil {
		return types.Message{}, err
	}
	msg_type := msgTypeBuf[0]
	//	log.Print("send:", time.Since(start))

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
	// duration := time.Since(start)
	// if duration > 10*time.Millisecond {
	// 	log.Println("send", duration)
	// }
	return msg, nil
}
func (client *Client) message_handler() {

	msg, err := client.read_message()
	if err != nil {
		////	log.Printf("Read error on client %s: %v", client.id, err)
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
			//log.Print("heartbeat")

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
			//log.Print("TACK")
			ok := Present_in_set(client.id)
			if !ok {
				client.mu.Lock()

				err := client.send(TACK, []byte("0"))
				if err != nil {
					log.Print("couldnt send data")
				}
				client.mu.Unlock()

				break
			}

			jobID := string(msg.Payload)
			if jobID != "" {
				log.Print("id :", jobID)
				ACK_channel <- jobID
			}

			//remove the worker from the map as well if they dopnt have job
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

			// job := Feed_to_worker(client.id)
			// if job == nil {
			// 	client.send(PULL, []byte("0"))
			// 	break
			// }
			// worker := &types.Worker{
			// 	ID:        client.id,
			// 	Job_id:    job.ID,
			// 	Last_ping: time.Now().UTC().UnixMilli(),
			// }
			// Add_to_map(worker)
			// tbs, _ := json.Marshal(job.Values["data"])
			// err := client.send(PULL, []byte(tbs))
			// if err != nil {
			// 	log.Print("couldnt send the pull res", err)
			// }

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
