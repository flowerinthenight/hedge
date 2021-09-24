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
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/spindle"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
)

const (
	CmdLeader = "LEADER" // are you the leader? reply: ACK
	CmdWrite  = "WRITE"  // write key/value "WRITE base64(payload)"
	CmdAck    = "ACK"
)

var (
	ErrNotRunning = fmt.Errorf("dstore: not running")
	ErrNoLeader   = fmt.Errorf("dstore: no leader available")
)

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
	hostPort      string // host:port
	spannerClient *spanner.Client
	*spindle.Lock        // handles our distributed lock
	lockTable     string // spindle lock table
	lockName      string // spindle lock name
	logTable      string // append-only log table
	writeTimeout  int64  // Put() timeout
	active        int32  // 1=running, 0=off

	logger *log.Logger // can be silenced by `log.New(ioutil.Discard, "", 0)`
}

// String implements the Stringer interface.
func (s *Store) String() string {
	return fmt.Sprintf("hostport:%s spindle:%v;%v;%v",
		s.hostPort,
		s.spannerClient.DatabaseName(),
		s.lockTable,
		s.logTable,
	)
}

// Run starts the main handler. It blocks until 'ctx' is cancelled,
// optionally sending an error message to 'done' when finished.
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
	addr, err := net.ResolveTCPAddr("tcp4", s.hostPort)
	if err != nil {
		s.logger.Printf("ResolveTCPAddr failed: %v", err)
		return err
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		s.logger.Printf("ResolveTCPAddr failed: %v", err)
		return err
	}

	defer listener.Close()
	s.logger.Printf("tcp: listen on %v", s.hostPort)

	handleConn := func(conn net.Conn) {
		defer conn.Close()
		for {
			buffer, err := bufio.NewReader(conn).ReadString('\n')
			if err != nil {
				s.logger.Println("no client")
				return
			}

			msg := buffer[:len(buffer)-1]
			s.logger.Printf("message: %v", msg)

			switch {
			case strings.HasPrefix(msg, CmdLeader): // confirm leader only
				if hl, _ := s.HasLock(); hl {
					conn.Write([]byte(s.buildAckReply(nil)))
				} else {
					conn.Write([]byte("\n"))
					return // not leader, done
				}
			case strings.HasPrefix(msg, CmdWrite+" "): // actual write
				reply := s.buildAckReply(nil)
				if hl, _ := s.HasLock(); hl {
					payload := strings.Split(msg, " ")[1]
					decoded, err := base64.StdEncoding.DecodeString(payload)
					if err != nil {
						reply = s.buildAckReply(err)
					} else {
						var kv KeyValue
						err = json.Unmarshal(decoded, &kv)
						if err != nil {
							reply = s.buildAckReply(err)
						} else {
							err = s.Put(ctx, kv, true)
							reply = s.buildAckReply(err)
						}
					}
				} else {
					reply = fmt.Sprintf("\n") // not leader
				}

				conn.Write([]byte(reply))
				return
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

	s.Lock = spindle.New(
		s.spannerClient,
		s.lockTable,
		fmt.Sprintf("dstore/spindle/lockname/%v", s.lockName),
		spindle.WithDuration(30000), // 30s duration
		spindle.WithId(s.hostPort),
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
// limit = 0 --> (default) latest only
// limit = -1 --> all (latest to oldest, [0]=latest)
// limit = -2 --> oldest version only
// limit > 0 --> items behind latest; 3 means latest + 2 versions behind, [0]=latest
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

	var err error
	var tmpdirect, hl bool
	if len(direct) > 0 {
		tmpdirect = direct[0]
	} else {
		hl, _ = s.HasLock()
	}

	if tmpdirect || hl {
		b, _ := json.Marshal(kv)
		s.logger.Printf("leader: direct write: %v", string(b))
		_, err := s.spannerClient.Apply(ctx, []*spanner.Mutation{
			spanner.InsertOrUpdate(s.logTable,
				[]string{"id", "key", "value", "leader", "timestamp"},
				[]interface{}{uuid.NewString(), kv.Key, kv.Value, s.hostPort, spanner.CommitTimestamp},
			),
		})

		return err
	}

	// For non-leaders, we confirm the leader via spindle, and if so, ask leader to do the
	// actual write for us. Let's do a couple retries up to spindle's timeout.
	var conn net.Conn
	var leaderAddr string
	var confirmed bool
	for i := 0; i < 15; i++ {
		err = func() error {
			leaderAddr, err = s.Leader()
			if err != nil {
				return err
			}

			if leaderAddr == "" {
				return ErrNoLeader
			}

			s.logger.Printf("current leader is %v, confirm", leaderAddr)
			addr, err := net.ResolveTCPAddr("tcp4", leaderAddr)
			if err != nil {
				s.logger.Printf("ResolveTCPAddr failed: %v", err)
				return err
			}

			conn, err = net.DialTCP("tcp", nil, addr)
			if err != nil {
				s.logger.Printf("DialTCP failed: %v", err)
				return err
			}

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
			s.logger.Printf("reply[1/2]: %v", msg)
			if !strings.HasPrefix(msg, CmdAck) {
				return fmt.Errorf("not really leader")
			}

			confirmed = true
			return nil
		}()

		if err == nil {
			break
		}

		time.Sleep(time.Second * 2)
	}

	if conn != nil {
		defer conn.Close()
	}

	if !confirmed {
		return ErrNoLeader
	}

	b, _ := json.Marshal(kv)
	encoded := base64.StdEncoding.EncodeToString(b)
	msg := fmt.Sprintf("%v %v\n", CmdWrite, encoded)
	_, err = conn.Write([]byte(msg)) // actual write request, expect ACK
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
	s.logger.Printf("reply[2/2]: %v", msg)

	switch {
	case strings.HasPrefix(msg, CmdAck):
		ss := strings.Split(msg, " ")
		if len(ss) > 1 { // failed
			decoded, _ := base64.StdEncoding.DecodeString(ss[1])
			return fmt.Errorf(string(decoded))
		}
	default:
		return ErrNoLeader
	}

	return nil
}

func (s *Store) buildAckReply(err error) string {
	if err != nil {
		ee := base64.StdEncoding.EncodeToString([]byte(err.Error()))
		return fmt.Sprintf("%v %v\n", CmdAck, ee)
	} else {
		return fmt.Sprintf("%v\n", CmdAck)
	}
}

// Config is our configuration to New().
type Config struct {
	HostPort        string          // required: serves as the instance's unique id, should be ip:port
	SpannerClient   *spanner.Client // required: Spanner client connection
	SpindleTable    string          // required: table name for *spindle.Lock
	SpindleLockName string          // required: spindle's lock name; should be the same for the group
	LogTable        string          // required: table name for the append-only storage
	WriteTimeout    int64           // optional: wait time (in ms) for Put(), default is 5000ms
	Logger          *log.Logger     // optional: can be silenced by `log.New(ioutil.Discard, "", 0)`
}

// New creates an instance of Store.
func New(cfg Config) *Store {
	s := &Store{
		hostPort:      cfg.HostPort,
		spannerClient: cfg.SpannerClient,
		lockTable:     cfg.SpindleTable,
		lockName:      cfg.SpindleLockName,
		logTable:      cfg.LogTable,
		writeTimeout:  cfg.WriteTimeout, // not used at the moment
		logger:        cfg.Logger,
	}

	if s.writeTimeout == 0 {
		s.writeTimeout = 5000
	}

	if s.logger == nil {
		prefix := fmt.Sprintf("[dstore/%v] ", s.hostPort)
		s.logger = log.New(os.Stdout, prefix, log.LstdFlags)
	}

	return s
}
