package dstore

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/spindle"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
)

const (
	spindleLockName = "dstorespindlelock"

	CmdLeader = "LEADER" // are you the leader? reply: ACK
	CmdWrite  = "WRITE"  // write key/value "WRITE base64(payload)"
	CmdAck    = "ACK"
)

var (
	ErrNotRunning = fmt.Errorf("dstore: not running")
)

type cmd_t struct {
	Ctrl string // WRITE|ACK
	Data []byte // LogItem if WRITE, id if ACK
}

type KeyValue struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Timestamp time.Time `json:"timestamp"` // ignored in Put()
}

// LogItem represents an item in our log.
type LogItem struct {
	Id        string
	Key       string
	Value     string
	Leader    string
	Timestamp time.Time // ignored in Put()
}

// Store is our main distributed, append-only log storage object.
type Store struct {
	group         string // fleet's name
	id            string // this instance's unique id
	spannerClient *spanner.Client
	*spindle.Lock                        // handles our distributed lock
	lockTable     string                 // spindle lock table
	lockName      string                 // spindle lock name
	logTable      string                 // append-only log table
	queue         map[string]chan []byte // for tracking outgoing/incoming messages
	writeTimeout  int64                  // Put() timeout
	active        int32                  // 1=running, 0=off
	mtx           sync.Mutex             // local lock

	logger *log.Logger // can be silenced by `log.New(ioutil.Discard, "", 0)`
}

// String returns some friendly information.
func (s *Store) String() string {
	return fmt.Sprintf("name:%v/%s spindle:%v;%v;%v",
		s.group,
		s.id,
		s.spannerClient.DatabaseName(),
		s.lockTable,
		s.logTable,
	)
}

// Run starts the main handler. It blocks until 'ctx' is cancelled,
// optionally sending a error message to 'done' when finished.
func (s *Store) Run(ctx context.Context, done ...chan error) error {
	var err error
	defer func(e *error) {
		if len(done) > 0 {
			done[0] <- *e
		}
	}(&err)

	// Some housekeeping.
	if s.spannerClient == nil {
		err = fmt.Errorf("dstore: Spanner client cannot be nil")
		return err
	}

	for _, v := range []struct {
		name string
		val  string
	}{
		{"SpindleTable", s.lockTable},
		{"SpindleLockName", s.lockName},
		{"LogTable", s.logTable},
	} {
		if v.val == "" {
			err = fmt.Errorf("dstore: %v cannot be empty", v.name)
			return err
		}
	}

	// Setup our server for leader communication.
	host := s.id + ":8080"
	addr, err := net.ResolveTCPAddr("tcp4", host)
	if err != nil {
		s.logger.Printf("ResolveTCPAddr failed: %v", err)
		return err
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		s.logger.Printf("ResolveTCPAddr failed: %v", err)
		return err
	}

	s.logger.Printf("tcp: listen on %v", host)
	handleConn := func(conn net.Conn) {
		defer conn.Close()
		for {
			buffer, err := bufio.NewReader(conn).ReadString('\n')
			if err != nil {
				s.logger.Println("client left")
				return
			}

			msg := buffer[:len(buffer)-1]
			s.logger.Printf("message: %v", msg)

			switch {
			case strings.HasPrefix(msg, CmdLeader):
				if ldr, _ := s.HasLock(); ldr {
					reply := fmt.Sprintf("%v\n", CmdAck)
					conn.Write([]byte(reply))
				} else {
					conn.Write([]byte("\n"))
					return
				}
			case strings.HasPrefix(msg, CmdWrite):
				if ldr, _ := s.HasLock(); ldr {
					payload := strings.Split(msg, " ")[1]
					decoded, _ := base64.StdEncoding.DecodeString(payload)
					var kv KeyValue
					json.Unmarshal(decoded, &kv)
					s.Put(ctx, kv, true)
					reply := fmt.Sprintf("%v\n", CmdAck)
					conn.Write([]byte(reply))
					return
				} else {
					conn.Write([]byte("\n"))
					return
				}
			default:
				s.logger.Println("not supported")
				return
			}
		}
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				s.logger.Printf("listener.Accept failed: %v", err)
				return
			}

			go handleConn(conn)
		}
	}()

	// Make sure we close our listener upon termination.
	tcpctx := context.WithValue(ctx, struct{}{}, nil)
	go func() {
		<-tcpctx.Done()
		listener.Close()
	}()

	s.Lock = spindle.New(
		s.spannerClient,
		s.lockTable,
		fmt.Sprintf("dstore/spindle/lockname/%v", s.group),
		spindle.WithDuration(30000), // 30s duration
		spindle.WithId(s.id),
	)

	spindledone := make(chan error, 1)
	spindlectx, cancel := context.WithCancel(context.Background())
	s.Lock.Run(spindlectx, spindledone)
	defer func() {
		cancel()      // stop spindle;
		<-spindledone // and wait
	}()

	atomic.StoreInt32(&s.active, 1)
	defer atomic.StoreInt32(&s.active, 0)

	<-ctx.Done() // wait for termination
	return nil
}

// Get reads a key (or keys) from Store.
// 0 (default) = latest only
// -1 = all (latest to oldest, [0]=latest)
// -2 = oldest version only
// >0 = items behind latest; 3 means latest + 2 versions behind, [0]=latest
func (s *Store) Get(ctx context.Context, key string, limit ...int64) ([]KeyValue, error) {
	defer func(begin time.Time) {
		s.logger.Printf("[Get] duration=%v", time.Since(begin))
	}(time.Now())

	ret := []KeyValue{}
	query := `select key, value, timestamp
from ` + s.logTable + `
where key = @key and timestamp is not null
order by timestamp desc limit 1`

	if len(limit) > 0 {
		switch {
		case limit[0] > 0:
			query = `"select key, value, timestamp
from ` + s.logTable + `
where key = @key and timestamp is not null
order by timestamp desc limit ` + fmt.Sprintf("%v", limit[0])
		case limit[0] == -1:
			query = `"select key, value, timestamp
from ` + s.logTable + `
where key = @key and timestamp is not null
order by timestamp desc`
		case limit[0] == -2:
			query = `"select key, value, timestamp
from ` + s.logTable + `
where key = @key and timestamp is not null
order by timestamp limit 1`
		}
	}

	stmt := spanner.Statement{SQL: query, Params: map[string]interface{}{"key": key}}
	iter := s.spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return ret, err
		}

		var li LogItem
		err = row.ToStruct(&li)
		if err != nil {
			return ret, err
		}

		ret = append(ret, KeyValue{
			Key:       li.Key,
			Value:     li.Value,
			Timestamp: li.Timestamp,
		})
	}

	return ret, nil
}

// Put saves a key/value to Store.
func (s *Store) Put(ctx context.Context, kv KeyValue, direct ...bool) error {
	if atomic.LoadInt32(&s.active) != 1 {
		return ErrNotRunning
	}

	defer func(begin time.Time) {
		s.logger.Printf("[Put] duration=%v", time.Since(begin))
	}(time.Now())

	id := uuid.NewString()
	var tmpdirect bool
	if len(direct) > 0 {
		tmpdirect = direct[0]
	}

	leader, _ := s.HasLock()
	if tmpdirect || leader {
		s.logger.Printf("leader: direct write: %+v", kv)
		_, err := s.spannerClient.Apply(ctx, []*spanner.Mutation{
			spanner.InsertOrUpdate(s.logTable,
				[]string{"id", "key", "value", "leader", "timestamp"},
				[]interface{}{id, kv.Key, kv.Value, s.id, spanner.CommitTimestamp},
			),
		})

		return err
	}

	// For non-leaders, we confirm the leader via spindle, and if so, ask leader to
	// to the write for us.
	ldrIp, err := s.Leader()
	if err != nil {
		return err
	}

	if ldrIp == "" {
		return fmt.Errorf("no leader available, try again")
	}

	s.logger.Printf("[%v] leader is %v, send confirm", s.id, ldrIp)
	addr, err := net.ResolveTCPAddr("tcp4", ldrIp+":8080")
	if err != nil {
		s.logger.Printf("ResolveTCPAddr failed: %v", err)
		return err
	}

	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		s.logger.Printf("DialTCP failed: %v", err)
		return err
	}

	defer conn.Close()
	msg := fmt.Sprintf("%v\n", CmdLeader)
	_, err = conn.Write([]byte(msg)) // confirm leader, expect ACK
	if err != nil {
		s.logger.Printf("Write failed: %v", err)
		return err
	}

	buffer, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		s.logger.Printf("ReadString failed: %v", err)
		return err
	}

	msg = buffer[:len(buffer)-1]
	s.logger.Printf("reply1: %v", msg)

	switch {
	case strings.HasPrefix(msg, CmdAck):
		b, _ := json.Marshal(kv)
		encoded := base64.StdEncoding.EncodeToString(b)
		msg = fmt.Sprintf("%v %v\n", CmdWrite, encoded)
		_, err = conn.Write([]byte(msg)) // actual write request, expect ACK
		if err != nil {
			s.logger.Printf("Write failed: %v", err)
			return err
		}

		buffer, err = bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			s.logger.Printf("ReadString failed: %v", err)
			return err
		}

		msg = buffer[:len(buffer)-1]
		s.logger.Printf("reply2: %v", msg)
		switch {
		case strings.HasPrefix(msg, CmdAck):
			s.logger.Printf("yay!")
		default:
			s.logger.Printf("tsk tsk")
			return fmt.Errorf("Put failed")
		}
	}

	return nil
}

// Config is our configuration to New().
type Config struct {
	// Required: the group name this instance belongs to.
	// NOTE: Will also be used as PubSub topic name, so naming conventions apply.
	// See https://cloud.google.com/pubsub/docs/admin#resource_names
	GroupName       string
	Id              string          // optional: this instance's unique id, will generate uuid if empty
	SpannerClient   *spanner.Client // required: Spanner client, project is implicit, will not use 'PubSubProject'
	SpindleTable    string          // required: table name for *spindle.Lock
	SpindleLockName string          // optional: "dstorespindlelock" by default
	LogTable        string          // required: table name for the append-only storage
	WriteTimeout    int64           // optional: wait time (in ms) for Put(), default is 5000ms
	Logger          *log.Logger     // optional: can be silenced by `log.New(ioutil.Discard, "", 0)`
}

// New creates an instance of Store.
func New(cfg Config) *Store {
	s := &Store{
		group:         cfg.GroupName,
		id:            cfg.Id,
		spannerClient: cfg.SpannerClient,
		lockTable:     cfg.SpindleTable,
		lockName:      cfg.SpindleLockName,
		logTable:      cfg.LogTable,
		queue:         make(map[string]chan []byte),
		writeTimeout:  cfg.WriteTimeout,
		logger:        cfg.Logger,
	}

	if s.id == "" {
		s.id = uuid.NewString()
	}

	if s.lockName == "" {
		s.lockName = spindleLockName
	}

	if s.writeTimeout == 0 {
		s.writeTimeout = 5000
	}

	if s.logger == nil {
		prefix := fmt.Sprintf("[dstore/%v] ", s.id)
		s.logger = log.New(os.Stdout, prefix, log.LstdFlags)
	}

	return s
}
