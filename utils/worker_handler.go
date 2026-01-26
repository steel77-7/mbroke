package utils

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

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
	//	id       string
	//
	// err      error
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
	defer ln.Close()
	s.accept_loop()
	s.check_heartbeat()
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
		go s.read_loop(conn)
	}
}

func (s *Server) send(w io.Writer, kind byte, payload []byte) error {
	length := uint32(1 + len(payload))
	var header [5]byte
	//	var w io.Writer = client.conn
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
	payload_len := int(length - 1)
	payload := make([]byte, payload_len)
	if _, err := io.ReadFull(conn, payload[:]); err != nil {
		log.Print("couldnt read the payload:", err)
		return
	}
	s.clients[string(payload)] = &Client{
		conn:  conn,
		id:    string(payload),
		ready: true,
		mu:    s.mu,
	}

	// if Message_type(msg[0]) == CONNECT {
	// 	s.clients[msg[1:len(msg)]] = &Client{
	// 		conn:  conn,
	// 		id:    msg[1:len(msg)],
	// 		ready: true,
	// 		mu:    s.mu,
	// 	}
	// }
	for {
		// new_msg, err := reader.ReadString('\n')
		// if err != nil {
		// 	log.Print("faulyt message: ", err)
		// 	continue
		// }
		// val, ok := s.clients[msg[1:len(msg)]]
		// if ok {
		// 	val.message_handler(new_msg)
		// }
		val, ok := s.clients[string(payload)]
		if ok {
			val.message_handler()
		}

	}
}

func (s *Server) check_heartbeat() {
	for {
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
	if _, err = io.ReadFull(r, msgTypeBuf); err != nil {
		return Message{}, err
	}

	msg_type := msgTypeBuf[0]

	payload_len := int(length - 1)
	if payload_len > 0 {
		payload := make([]byte, payload_len)
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
	msg, err := client.read_message()
	if err != nil {
		log.Print("couldnt parse the message")
		return
	}
	switch Message_type(msg.msg_type) {
	case CONNECT:
		{
			//auth function here
			client.auth = true
			client.ready = true
		}
	case HEARTBEAT:
		{
			val, ok := Worker_map.List[client.id]
			if !ok {
				client.send(byte(HEARTBEAT), []byte("0"))
				break
			}
			val.Last_ping = time.Now().UTC().UnixMilli()
		}

	case TACK:
		{
			mess := "1"
			if string(msg.payload) == "0" {
				mess = "0"
			}
			Worker_map.Mu.Lock()
			worker, _ := Worker_map.List[client.id]
			Worker_map.Mu.Unlock()
			ACK_channel <- worker.Job_id
			client.send(byte(TACK), []byte(mess))
		}
	case PULL:
		{
			job := Feed_to_worker(client.id)
			Worker_map.Mu.Lock()
			Worker_map.List[client.id] = &types.Worker{
				ID:        client.id,
				Job_id:    job.ID,
				Last_ping: time.Now().UTC().UnixMilli(),
			}
			Worker_map.Mu.Unlock()
			tbs, _ := json.Marshal(job.Values["data"])
			client.send(PULL, []byte(tbs))
		}
	default:
		{
			log.Print("Invalid messgae type")
			client.send(INVALID, []byte("0"))
		}
	}
}
