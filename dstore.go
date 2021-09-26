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
	CmdLeader = "LEADER" // for leader confirmation, reply="ACK"
	CmdWrite  = "WRITE"  // write key/value, fmt="WRITE base64(payload)"
	CmdAck    = "ACK"    // leader reply, fmt="ACK"|"ACK base64(err)"
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
	hostPort      string          // this instance's id; address:port
	spannerClient *spanner.Client // both for spindle and dstore
	lockTable     string          // spindle lock table
	lockName      string          // spindle lock name
	lockTimeout   int64           // spindle's lock lease duration in ms
	logTable      string          // append-only log table

	*spindle.Lock             // handles our distributed lock
	active        int32       // 1=running, 0=off
	logger        *log.Logger // can be silenced by `log.New(ioutil.Discard, "", 0)`
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
		s.logger.Printf("ListenTCP failed: %v", err)
		return err
	}

	defer listener.Close()
	s.logger.Printf("tcp: listen on %v", s.hostPort)

	handleConn := func(conn net.Conn) {
		defer conn.Close()
		for {
			msg, err := s.recv(conn)
			if err != nil {
				s.logger.Printf("recv failed: %v", err)
				return
			}

			switch {
			case strings.HasPrefix(msg, CmdLeader): // confirm leader only
				reply := s.buildAckReply(nil)
				if hl, _ := s.HasLock(); !hl {
					reply = "\n"
				}

				conn.Write([]byte(reply))
				return // always end confirm
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
					reply = "\n" // not leader, possible even if confirmed
				}

				conn.Write([]byte(reply))
				return // always end write
			default:
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
		spindle.WithDuration(s.lockTimeout),
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

// Put saves a key/value to Store. This call will try to block, at least roughly until spindle's
// timeout, to wait for the leader's availability to do actual writes before returning.
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
	var confirmed bool
	first := make(chan struct{}, 1)
	first <- struct{}{} // immediately the first time
	tcnt, tlimit := int64(0), (s.lockTimeout/2000)*2
	ticker := time.NewTicker(time.Second * 2) // processing can be more than this
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return context.Canceled
		case <-first:
		case <-ticker.C:
		}

		err = func() error {
			timeout := time.Second * 5
			leader, err := s.Leader()
			if err != nil {
				return err
			}

			if leader == "" {
				return ErrNoLeader
			}

			s.logger.Printf("current leader is %v, confirm", leader)
			lconn, err := net.DialTimeout("tcp", leader, timeout)
			if err != nil {
				s.logger.Printf("DialTimeout failed: %v", err)
				return err
			}

			defer lconn.Close()
			reply, err := s.send(lconn, fmt.Sprintf("%v\n", CmdLeader))
			if err != nil {
				s.logger.Printf("send failed: %v", err)
				return err
			}

			s.logger.Printf("reply[1/2]: %v", reply)
			if !strings.HasPrefix(reply, CmdAck) {
				return ErrNoLeader
			}

			// Create a new connection to the confirmed leader.
			conn, err = net.DialTimeout("tcp", leader, timeout)
			if err != nil {
				s.logger.Printf("DialTimeout failed: %v", err)
				return err
			}

			confirmed = true
			return nil
		}()

		tcnt++
		if err == nil || (tcnt > tlimit) {
			break
		}
	}

	if conn != nil {
		defer conn.Close()
	}

	if !confirmed {
		return ErrNoLeader
	}

	b, _ := json.Marshal(kv)
	enc := base64.StdEncoding.EncodeToString(b)
	reply, err := s.send(conn, fmt.Sprintf("%v %v\n", CmdWrite, enc))
	if err != nil {
		s.logger.Printf("send failed: %v", err)
		return err
	}

	s.logger.Printf("reply[2/2]: %v", reply)

	switch {
	case strings.HasPrefix(reply, CmdAck):
		ss := strings.Split(reply, " ")
		if len(ss) > 1 { // failed
			dec, _ := base64.StdEncoding.DecodeString(ss[1])
			return fmt.Errorf(string(dec))
		}
	default:
		return ErrNoLeader
	}

	return nil
}

func (s *Store) send(conn net.Conn, msg string) (string, error) {
	_, err := conn.Write([]byte(msg))
	if err != nil {
		s.logger.Printf("Write failed: %v", err)
		return "", err
	}

	return s.recv(conn)
}

func (s *Store) recv(conn net.Conn) (string, error) {
	buffer, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		s.logger.Printf("ReadString failed: %v", err)
		return "", err
	}

	reply := buffer[:len(buffer)-1]
	return reply, nil
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
	SpindleTimeout  int64           // optional: spindle's lease duration in ms, default to 30s, min=2s
	LogTable        string          // required: table name for the append-only storage
	Logger          *log.Logger     // optional: can be silenced by `log.New(ioutil.Discard, "", 0)`
}

// New creates an instance of Store.
func New(cfg Config) *Store {
	s := &Store{
		hostPort:      cfg.HostPort,
		spannerClient: cfg.SpannerClient,
		lockTable:     cfg.SpindleTable,
		lockName:      cfg.SpindleLockName,
		lockTimeout:   cfg.SpindleTimeout,
		logTable:      cfg.LogTable,
		logger:        cfg.Logger,
	}

	switch {
	case s.lockTimeout == 0:
		s.lockTimeout = 30000 // default
	case s.lockTimeout < 2000:
		s.lockTimeout = 2000 // minimum
	}

	if s.logger == nil {
		prefix := fmt.Sprintf("[dstore/%v] ", s.hostPort)
		s.logger = log.New(os.Stdout, prefix, log.LstdFlags)
	}

	return s
}
