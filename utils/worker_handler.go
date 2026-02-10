package utils

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
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

type Message struct {
	length   uint32
	msg_type byte
	payload  []byte
}

type work_map struct {
	Mu   *sync.RWMutex
	List map[string]*types.Worker
}

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
	defer ln.Close()
	go s.accept_loop()

	go s.check_heartbeat()
	<-s.quitch
	return nil
}

func (s *Server) accept_loop() {

	for {
		conn, err := s.Listener.Accept()
		log.Print("new")
		if err != nil {
			log.Print("Couldnt accept connection :", err)
			continue
		}
		go s.read_loop(conn)
	}
}

func (s *Server) send(w io.Writer, kind byte, payload []byte) error {
	length := uint32(1 + len(payload))
	var header [5]byte
	//	var w io.Writer = client.conn
	binary.BigEndian.PutUint32(header[:], length)
	header[4] = kind
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	//	log.Print("sending: ", kind)
	//	log.Print("length:", length)
	if len(payload) > 0 {
		_, err := w.Write(payload)
		return err
	}
	return nil
}

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

	type_buf := make([]byte, 1)
	if _, err := io.ReadFull(conn, type_buf[:]); err != nil {
		log.Print("couldnt read the type:", err)
		return
	}
	if Message_type(type_buf[0]) != CONNECT {
		log.Print("IDK TF IS WRONG HERE //// or someone malicious")
		return
	}

	//log.Print("messgae type 1: ", type_buf[0], Message_type(type_buf[0]))

	payload_len := int(length - 1)
	payload := make([]byte, payload_len)
	if _, err := io.ReadFull(conn, payload[:]); err != nil {
		log.Print("couldnt read the payload:", err)
		return
	}
	//log.Print(payload)
	// log.Print("CLient joined payoad:", string(msg.payload))
	// log.Print("CLient joined length:", msg.length)
	// log.Print("CLient joined type :", msg.msg_type)
	if string(payload) != os.Getenv("SECRET") {
		conn.Close()
		return
	}
	id := fmt.Sprint(uuid.New())
	//log.Print(id)
	s.mu.Lock()
	s.clients[id] = &Client{
		conn:   conn,
		id:     id,
		ready:  true,
		mu:     s.mu,
		quitch: make(chan struct{}, 2),
	}
	s.mu.Unlock()

	for {
		//log.Print("read loop")
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
					val.message_handler()
				}
			}
		}
	}
}

func (s *Server) check_heartbeat() {

	for {
		//log.Print("heartbeat")
		time.Sleep(time.Duration(1) * time.Second)

		//i have to make it retry
		if len(Worker_map.List) == 0 {
			continue
		}
		//log.Print("lub dub")
		Worker_map.Mu.Lock()
		for key, value := range Worker_map.List {
			if time.Now().UTC().UnixMilli()-value.Last_ping >= 10000 {
				delete(Worker_map.List, key)

				val, o := s.clients[key]
				if !o {
					break
				}
				val.mu.Lock()
				val.conn.Close()
				val.quitch <- struct{}{}
				delete(s.clients, key)
				val.mu.Unlock()

				Del_consumer([]string{key})
			}
		}
		Worker_map.Mu.Unlock()
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

func (client *Client) read_message() (msg Message, err error) {
	var len_buf [4]byte
	var r io.Reader
	r = client.conn
	if _, err := io.ReadFull(r, len_buf[:]); err != nil {
		return Message{}, err
	}

	length := binary.BigEndian.Uint32(len_buf[:])
	if length < 1 {
		err = fmt.Errorf("invalid length")
		return Message{}, err
	}

	msgTypeBuf := make([]byte, 1)
	//	log.Print("messgae type: ", Message_type(msgTypeBuf[0]))

	if _, err = io.ReadFull(r, msgTypeBuf); err != nil {
		return Message{}, err
	}

	msg_type := msgTypeBuf[0]

	payload_len := length - 1
	if payload_len > 0 {
		payload := make([]byte, payload_len)
		if _, err := io.ReadFull(r, payload[:]); err != nil {
			return Message{}, err
		}
		msg = Message{
			length:   length,
			msg_type: msg_type,
			payload:  payload,
		}
		return msg, err
	}
	return Message{}, err
}

func (client *Client) message_handler() {
	//	log.Print("message ahdnler")
	msg, err := client.read_message()
	if err != nil {
		if err == io.EOF {
			client.conn.Close()
			log.Printf("Connection to the client %d is closed", client.id)
			client.quitch <- struct{}{}
			return
		}
		log.Print("couldnt parse the message", err)
		return
	}
	//log.Print("type:", msg.msg_type)
	//log.Print("length:", msg.length)
	//log.Print("payload:", string(msg.payload))

	switch Message_type(msg.msg_type) {
	case CONNECT:
		{
			client.auth = true
			client.ready = true
		}
	case HEARTBEAT:
		{
			//log.Print("heartbeat")
			val, ok := Worker_map.List[client.id]
			if !ok {
				err := client.send(HEARTBEAT, []byte("0"))
				if err != nil {
					log.Print("couldnt send the heartbeat res")
				}
				break
			}
			Worker_map.Mu.Lock()
			val.Last_ping = time.Now().UTC().UnixMilli()
			Worker_map.Mu.Unlock()

		}

	case TACK:
		{
			//	log.Print("TACK")

			Worker_map.Mu.Lock()
			worker, ok := Worker_map.List[client.id]
			if !ok {
				err := client.send(TACK, []byte("0"))
				if err != nil {
					log.Print("couldnt send data")
				}
				break
			}
			Worker_map.Mu.Unlock()
			if string(msg.payload) == "1" {
				//log.Print("ack")
				ACK_channel <- worker.Job_id
			}
			err := client.send(TACK, []byte("1"))
			if err != nil {
				log.Print("couldnt send the tack res")
			}

		}
	case PULL:
		{
			//log.Print("PULL")
			job := Feed_to_worker(client.id)
			//log.Print("asking")
			if job == nil {
				client.send(PULL, []byte("0"))
				break
			}
			Worker_map.Mu.Lock()
			Worker_map.List[client.id] = &types.Worker{
				ID:        client.id,
				Job_id:    job.ID,
				Last_ping: time.Now().UTC().UnixMilli(),
			}
			Worker_map.Mu.Unlock()
			tbs, _ := json.Marshal(job.Values["data"])
			err := client.send(PULL, []byte(tbs))
			if err != nil {
				log.Print("couldnt send the pull res", err)
			}
		}
	default:
		{
			//log.Print("Invalid messgae type")
			err := client.send(INVALID, []byte("0"))
			if err != nil {
				log.Print("couldnt send the invalid res")
			}
		}
	}
}
