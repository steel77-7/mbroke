package utils

import (
	"bufio"
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
	//err      error
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
}

type Server struct {
	Addr     string
	Listener net.Listener
	quitch   chan struct{}
	mu       sync.Mutex
	clients  map[string]*Client
}

func NewServer(addr string) *Server {
	return &Server{
		Addr:    addr,
		clients: make(map[string]*Client),
		mu:      sync.Mutex{},
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		log.Print("Error in starting the tcp server: ", ln)
		return err
	}
	defer ln.Close()
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
		//yaha pe auth hogi
		//but abhi ke liye auth logic ke alava aur dekhte hai
		//if a worker does not send connect for a very long time them disconnect them //checked via the ready ...if not ready for a very logn time then disconnect

	}
}

func (s *Server) read_loop(conn net.Conn) {
	reader := bufio.NewReader(conn)
	msg, err := reader.ReadString('\n')
	if err != nil {
		log.Print("COUldnt not read anything")
		conn.Close()
		return
	}

	if Message_type(msg[0]) == CONNECT {
		s.clients[msg[1:len(msg)]] = &Client{
			conn:  conn,
			id:    msg[1:len(msg)],
			ready: true,
		}
	}
	for {
		new_msg, err := reader.ReadString('\n')
		if err != nil {
			log.Print("faulyt message: ", err)
			continue
		}
		val, ok := s.clients[msg[1:len(msg)]]
		if ok {
			val.message_handler(new_msg)
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
func (client *Client) read_message() (msg Message, err error) {
	var len_buf [4]byte
	var r io.Reader

	if _, err := io.ReadFull(r, len_buf[:]); err != nil {
		return Message{}, err
	}

	length := binary.BigEndian.Uint32(len_buf[:])
	if length < 1 {
		err = fmt.Errorf("invalid length")
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

func (client *Client) message_handler(message string) {
	id := message[0:32] //just for exmaple  rn

	switch Message_type(message[0]) {
	case CONNECT:
		{
			//auth function here
			client.auth = true
			client.ready = true
		}
	case HEARTBEAT:
		{
			val, ok := Worker_map.List[id]
			if !ok {
				client.conn.Write([]byte(fmt.Sprintf("%d%d", HEARTBEAT, 0)))
				break
			}
			val.Last_ping = time.Now().UTC().UnixMilli()
		}
	case TACK:
		{
			mess := "1"
			if message[32:33] == "0" {
				mess = "0"
			}
			Worker_map.Mu.Lock()
			worker, _ := Worker_map.List[id]
			Worker_map.Mu.Unlock()
			ACK_channel <- worker.Job_id
			client.send(Message_type(TACK), mess)
		}
	case PULL:
		{
			job := Feed_to_worker(id)
			Worker_map.Mu.Lock()
			Worker_map.List[id] = &types.Worker{
				ID:        id,
				Job_id:    job.ID,
				Last_ping: time.Now().UTC().UnixMilli(),
			}
			Worker_map.Mu.Unlock()
			tbs, _ := json.Marshal(job.Values["data"])
			client.send(PULL, string(tbs))
		}
	default:
		{
			log.Print("Invalid messgae type")
			client.send(INVALID, "0")
		}
	}
}
