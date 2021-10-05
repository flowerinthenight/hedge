package hedge

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
	CmdLeader     = "LDR" // for leader confirmation, reply="ACK"
	CmdWrite      = "PUT" // write key/value, fmt="PUT base64(payload)"
	CmdSend       = "SND" // member to leader, fmt="SND base64(payload)"
	CmdPing       = "HEY" // heartbeat to indicate availability, fmt="HEY [id]"
	CmdMembers    = "MEM" // members info from leader to all, fmt="MEM base64(JSON(members))"
	CmdBroadcast  = "ALL" // broadcast to all, fmt="ALL base64(payload)"
	CmdAck        = "ACK" // generic reply, fmt="ACK"|"ACK base64(err)"|"ACK base64(JSON(members))"
	CmdSemaphore  = "SEM" // create semaphore, fmt="SEM {name} {limit}"
	CmdSemAcquire = "SEA" // acquire semaphore, fmt="SEA {name} {caller}"
	CmdSemRelease = "SER" // release semaphore, fmt="SER {name} {caller}"
)

var (
	ErrNotRunning = fmt.Errorf("hedge: not running")
	ErrNoLeader   = fmt.Errorf("hedge: no leader available")
	ErrNoHandler  = fmt.Errorf("hedge: no message handler")
)

// FnLeaderHandler represents a leader node's message handler for messages
// sent from members. 'data' is any arbitrary data you provide using the
// 'WithLeaderHandler' option. 'msg' is the message sent from the calling
// member. The returning []byte will serve as the handler's reply.
//
// Typical use case would be:
// 1) Any node (including the leader) calls the Send(...) API.
// 2) The current leader handles the call by reading the input.
// 3) Leader will then call FnLeaderHandler, passing the arbitrary data
//    along with the message.
// 4) FnLeaderHandler will process the data as leader, then returns the
//    reply to the calling member.
type FnLeaderHandler func(data interface{}, msg []byte) ([]byte, error)

// FnBroadcastHandler represents a node's message handler for broadcast
// messages from anybody in the group, including the leader. Arguments
// and return values are basically the same as FnLeaderHandler.
type FnBroadcastHandler func(data interface{}, msg []byte) ([]byte, error)

// KeyValue is for Put()/Get() callers.
type KeyValue struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Timestamp time.Time `json:"timestamp"` // read-only, populated when Get()
}

// LogItem represents an item in our log.
type LogItem struct {
	Id        string
	Key       string
	Value     string
	Leader    string
	Timestamp time.Time
}

type Option interface {
	Apply(*Op)
}

type withDuration int64

func (w withDuration) Apply(o *Op) { o.lockTimeout = int64(w) }

// WithDuration sets Op's internal spindle object's lease duration.
// Defaults to 30s when not set. Minimum value is 2s.
func WithDuration(v int64) Option { return withDuration(v) }

type withLeaderHandler struct {
	d interface{}
	h FnLeaderHandler
}

func (w withLeaderHandler) Apply(o *Op) {
	o.fnLdrData = w.d
	o.fnLeader = w.h
}

// WithLeaderHandler sets the node's callback function when it is the current
// leader and when members send messages to it using the Send(...) API. Any
// arbitrary data represented by 'd' will be passed to the callback 'h' every
// time it is called. See 'FnLeaderHandler' type definition for more details.
func WithLeaderHandler(d interface{}, h FnLeaderHandler) Option {
	return withLeaderHandler{d, h}
}

type withBroadcastHandler struct {
	d interface{}
	h FnBroadcastHandler
}

func (w withBroadcastHandler) Apply(o *Op) {
	o.fnBcData = w.d
	o.fnBroadcast = w.h
}

// WithBroadcastHandler sets the node's callback function for broadcast messages
// from anyone in the group using the Broadcast(...) API. Any arbitrary data
// represented by 'd' will be passed to the callback 'h' every time it is called.
// See 'FnBroadcastHandler' type definition for more details.
func WithBroadcastHandler(d interface{}, h FnBroadcastHandler) Option {
	return withBroadcastHandler{d, h}
}

type withLogger struct{ l *log.Logger }

func (w withLogger) Apply(o *Op) { o.logger = w.l }

// WithLogger sets Op's logger object. Can be silenced by setting
// v to `log.New(ioutil.Discard, "", 0)`.
func WithLogger(v *log.Logger) Option { return withLogger{v} }

// Op is our main instance for hedge operations.
type Op struct {
	hostPort      string          // this instance's id; address:port
	spannerClient *spanner.Client // both for spindle and hedge
	lockTable     string          // spindle lock table
	lockName      string          // spindle lock name
	lockTimeout   int64           // spindle's lock lease duration in ms
	logTable      string          // append-only log table

	fnLeader    FnLeaderHandler    // leader message handler
	fnLdrData   interface{}        // arbitrary data passed to fnLeader
	fnBroadcast FnBroadcastHandler // broadcast message handler
	fnBcData    interface{}        // arbitrary data passed to fnBroadcast

	*spindle.Lock                     // handles our distributed lock
	members       map[string]struct{} // key=id
	mtx           sync.Mutex          // local mutex
	active        int32               // 1=running, 0=off
	logger        *log.Logger         // internal logger
}

// String implements the Stringer interface.
func (o *Op) String() string {
	return fmt.Sprintf("hostport:%s;spindle:%v;%v;%v",
		o.hostPort,
		o.spannerClient.DatabaseName(),
		o.lockTable,
		o.logTable,
	)
}

// Run starts the main handler. It blocks until 'ctx' is cancelled,
// optionally sending an error message to 'done' when finished.
func (o *Op) Run(ctx context.Context, done ...chan error) error {
	var err error
	defer func(e *error) {
		if len(done) > 0 {
			done[0] <- *e
		}
	}(&err)

	// Some housekeeping.
	if o.spannerClient == nil {
		err = fmt.Errorf("hedge: Spanner client cannot be nil")
		return err
	}

	for _, v := range []struct {
		name string
		val  string
	}{
		{"SpindleTable", o.lockTable},
		{"SpindleLockName", o.lockName},
		{"LogTable", o.logTable},
	} {
		if v.val == "" {
			err = fmt.Errorf("hedge: %v cannot be empty", v.name)
			return err
		}
	}

	// Setup our server for leader communication.
	addr, err := net.ResolveTCPAddr("tcp4", o.hostPort)
	if err != nil {
		o.logger.Printf("ResolveTCPAddr failed: %v", err)
		return err
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		o.logger.Printf("ListenTCP failed: %v", err)
		return err
	}

	defer listener.Close()
	o.logger.Printf("tcp: listen on %v", o.hostPort)

	handleConn := func(conn net.Conn) {
		defer conn.Close()
		for {
			msg, err := o.recv(conn)
			if err != nil {
				o.logger.Printf("recv failed: %v", err)
				return
			}

			switch {
			case strings.HasPrefix(msg, CmdLeader): // confirm leader only
				reply := o.buildAckReply(nil)
				if hl, _ := o.HasLock(); !hl {
					reply = "\n"
				}

				conn.Write([]byte(reply))
				return // always end LDR
			case strings.HasPrefix(msg, CmdWrite+" "): // actual write
				reply := o.buildAckReply(nil)
				if hl, _ := o.HasLock(); hl {
					payload := strings.Split(msg, " ")[1]
					decoded, _ := base64.StdEncoding.DecodeString(payload)
					var kv KeyValue
					err = json.Unmarshal(decoded, &kv)
					if err != nil {
						reply = o.buildAckReply(err)
					} else {
						err = o.Put(ctx, kv, true)
						reply = o.buildAckReply(err)
					}
				} else {
					reply = "\n" // not leader, possible even if previously confirmed
				}

				conn.Write([]byte(reply))
				return // always end PUT
			case strings.HasPrefix(msg, CmdSend+" "): // Send(...) API
				reply := base64.StdEncoding.EncodeToString([]byte(ErrNoLeader.Error())) + "\n"
				if hl, _ := o.HasLock(); hl {
					reply = base64.StdEncoding.EncodeToString([]byte(ErrNoHandler.Error())) + "\n"
					if o.fnLeader != nil {
						payload := strings.Split(msg, " ")[1]
						decoded, _ := base64.StdEncoding.DecodeString(payload)
						r, e := o.fnLeader(o.fnLdrData, decoded) // call leader handler
						if e != nil {
							reply = base64.StdEncoding.EncodeToString([]byte(e.Error())) + "\n"
						} else {
							br := base64.StdEncoding.EncodeToString([]byte(""))
							if r != nil {
								br = base64.StdEncoding.EncodeToString(r)
							}

							// The final correct reply format.
							reply = fmt.Sprintf("%v %v\n", CmdAck, br)
						}
					}
				}

				conn.Write([]byte(reply))
				return // always end SND
			case msg == CmdPing: // leader asking if we are online
				reply := o.buildAckReply(nil)
				conn.Write([]byte(reply))
				return
			case strings.HasPrefix(msg, CmdPing+" "): // heartbeat
				o.addMember(strings.Split(msg, " ")[1])
				reply := o.encodeMembers() + "\n"
				conn.Write([]byte(reply))
				return
			case strings.HasPrefix(msg, CmdMembers+" "): // broadcast online members
				payload := strings.Split(msg, " ")[1]
				decoded, _ := base64.StdEncoding.DecodeString(payload)
				var m map[string]struct{}
				json.Unmarshal(decoded, &m)
				m[o.hostPort] = struct{}{} // just to be sure
				o.setMembers(m)            // then replace my records
				o.logger.Printf("members=%v", len(o.getMembers()))
				reply := o.buildAckReply(nil)
				conn.Write([]byte(reply))
				return
			default:
				return // close conn
			}
		}
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				o.logger.Printf("listener.Accept failed: %v", err)
				return
			}

			go handleConn(conn)
		}
	}()

	// Setup and start our internal spindle object.
	o.Lock = spindle.New(
		o.spannerClient,
		o.lockTable,
		fmt.Sprintf("hedge/spindle/lockname/%v", o.lockName),
		spindle.WithDuration(o.lockTimeout),
		spindle.WithId(o.hostPort),
	)

	spindleDone := make(chan error, 1)
	spindleCtx, cancel := context.WithCancel(context.Background())
	o.Lock.Run(spindleCtx, spindleDone)
	defer func() {
		cancel()      // stop spindle;
		<-spindleDone // and wait
	}()

	// Start active members tracking goroutine.
	o.members[o.hostPort] = struct{}{}
	mbchkDone := make(chan error, 1)
	mbchkCtx := context.WithValue(ctx, struct{}{}, nil)
	first := make(chan struct{}, 1)
	first <- struct{}{} // immediately the first time
	ticker := time.NewTicker(time.Millisecond * time.Duration(o.lockTimeout))
	defer func() {
		ticker.Stop()
		<-mbchkDone
	}()

	go func() {
		var active int32
		ensureMembers := func() {
			atomic.StoreInt32(&active, 1)
			defer atomic.StoreInt32(&active, 0)
			var w sync.WaitGroup
			allm := o.getMembers()
			todel := make(chan string, len(allm))
			for k := range allm {
				w.Add(1)
				go func(id string) {
					var rmid string
					defer func(rm *string) {
						todel <- rmid
						w.Done()
					}(&rmid)

					timeout := time.Second * 5
					conn, err := net.DialTimeout("tcp", id, timeout)
					if err != nil {
						o.logger.Printf("DialTimeout failed: %v", err)
						rmid = id // delete this
						return
					}

					r, err := o.send(conn, CmdPing+"\n")
					if err != nil {
						o.logger.Printf("[leader] send failed: %v", err)
						rmid = id // delete this
						return
					}

					if r != CmdAck {
						o.logger.Printf("[leader] reply failed: %v", r)
						rmid = id // delete this
					}
				}(k)
			}

			w.Wait()
			for range allm {
				rm := <-todel
				if rm != "" {
					o.logger.Printf("[leader] delete %v", rm)
					o.delMember(rm)
				}
			}

			for k := range o.getMembers() {
				w.Add(1)
				go func(id string) {
					defer w.Done()
					timeout := time.Second * 5
					conn, err := net.DialTimeout("tcp", id, timeout)
					if err != nil {
						o.logger.Printf("[leader] DialTimeout failed: %v", err)
						return
					}

					defer conn.Close()
					msg := fmt.Sprintf("%v %v\n", CmdMembers, o.encodeMembers())
					_, err = o.send(conn, msg)
					if err != nil {
						o.logger.Printf("[leader] send failed: %v", err)
					}
				}(k)
			}

			w.Wait()
		}

		var hbactive int32
		heartbeat := func() {
			atomic.StoreInt32(&hbactive, 1)
			defer atomic.StoreInt32(&hbactive, 0)
			lconn, err := o.getLeaderConn(ctx)
			if err != nil {
				o.logger.Printf("getLeaderConn failed: %v", err)
				return
			}

			defer lconn.Close()
			msg := fmt.Sprintf("%v %v\n", CmdPing, o.hostPort)
			r, err := o.send(lconn, msg)
			if err != nil {
				o.logger.Printf("send failed: %v", err)
				return
			}

			b, _ := base64.StdEncoding.DecodeString(r)
			var allm map[string]struct{}
			json.Unmarshal(b, &allm)
			o.setMembers(allm)
		}

		for {
			select {
			case <-mbchkCtx.Done():
				mbchkDone <- nil
				return
			case <-first:
			case <-ticker.C:
			}

			if atomic.LoadInt32(&hbactive) == 0 {
				go heartbeat()
			}

			if hl, _ := o.HasLock(); !hl {
				continue
			}

			if atomic.LoadInt32(&active) == 0 {
				go ensureMembers()
			}
		}
	}()

	atomic.StoreInt32(&o.active, 1)
	defer atomic.StoreInt32(&o.active, 0)

	<-ctx.Done() // wait for termination
	return nil
}

// NewSemaphore returns a distributed semaphore object.
func (o *Op) NewSemaphore(name string, limit int64) (*Semaphore, error) {
	if atomic.LoadInt32(&o.active) != 1 {
		return nil, ErrNotRunning
	}

	return &Semaphore{name, limit, o}, nil
}

// Get reads a key (or keys) from Op.
// limit = 0 --> (default) latest only
// limit = -1 --> all (latest to oldest, [0]=latest)
// limit = -2 --> oldest version only
// limit > 0 --> items behind latest; 3 means latest + 2 versions behind, [0]=latest
func (o *Op) Get(ctx context.Context, key string, limit ...int64) ([]KeyValue, error) {
	defer func(begin time.Time) {
		o.logger.Printf("[Get] duration=%v", time.Since(begin))
	}(time.Now())

	ret := []KeyValue{}
	query := `select key, value, timestamp
from ` + o.logTable + `
where key = @key and timestamp is not null
order by timestamp desc limit 1`

	if len(limit) > 0 {
		switch {
		case limit[0] > 0:
			query = `"select key, value, timestamp
from ` + o.logTable + `
where key = @key and timestamp is not null
order by timestamp desc limit ` + fmt.Sprintf("%v", limit[0])
		case limit[0] == -1:
			query = `"select key, value, timestamp
from ` + o.logTable + `
where key = @key and timestamp is not null
order by timestamp desc`
		case limit[0] == -2:
			query = `"select key, value, timestamp
from ` + o.logTable + `
where key = @key and timestamp is not null
order by timestamp limit 1`
		}
	}

	stmt := spanner.Statement{SQL: query, Params: map[string]interface{}{"key": key}}
	iter := o.spannerClient.Single().Query(ctx, stmt)
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

// Put saves a key/value to Op. This call will try to block, at least roughly until spindle's
// timeout, to wait for the leader's availability to do actual writes before returning.
func (o *Op) Put(ctx context.Context, kv KeyValue, direct ...bool) error {
	if atomic.LoadInt32(&o.active) != 1 {
		return ErrNotRunning
	}

	defer func(begin time.Time) {
		o.logger.Printf("[Put] duration=%v", time.Since(begin))
	}(time.Now())

	var err error
	var tmpdirect, hl bool
	if len(direct) > 0 {
		tmpdirect = direct[0]
	} else {
		hl, _ = o.HasLock()
	}

	if tmpdirect || hl {
		b, _ := json.Marshal(kv)
		o.logger.Printf("[Put] leader: direct write: %v", string(b))
		_, err := o.spannerClient.Apply(ctx, []*spanner.Mutation{
			spanner.InsertOrUpdate(o.logTable,
				[]string{"id", "key", "value", "leader", "timestamp"},
				[]interface{}{uuid.NewString(), kv.Key, kv.Value, o.hostPort, spanner.CommitTimestamp},
			),
		})

		return err
	}

	// For non-leaders, we confirm the leader via spindle, and if so, ask leader to do the
	// actual write for us. Let's do a couple retries up to spindle's timeout.
	conn, err := o.getLeaderConn(ctx)
	if err != nil {
		return err
	}

	if conn != nil {
		defer conn.Close()
	}

	b, _ := json.Marshal(kv)
	enc := base64.StdEncoding.EncodeToString(b)
	reply, err := o.send(conn, fmt.Sprintf("%v %v\n", CmdWrite, enc))
	if err != nil {
		o.logger.Printf("[Put] send failed: %v", err)
		return err
	}

	o.logger.Printf("[Put] reply: %v", reply)

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

func (o *Op) Send(ctx context.Context, msg []byte) ([]byte, error) {
	if atomic.LoadInt32(&o.active) != 1 {
		return nil, ErrNotRunning
	}

	defer func(begin time.Time) {
		o.logger.Printf("[Send] duration=%v", time.Since(begin))
	}(time.Now())

	conn, err := o.getLeaderConn(ctx)
	if err != nil {
		return nil, err
	}

	if conn != nil {
		defer conn.Close()
	}

	enc := base64.StdEncoding.EncodeToString(msg)
	reply, err := o.send(conn, fmt.Sprintf("%v %v\n", CmdSend, enc))
	if err != nil {
		o.logger.Printf("[Send] send failed: %v", err)
		return nil, err
	}

	o.logger.Printf("[Send] reply: %v", reply)

	switch {
	case strings.HasPrefix(reply, CmdAck): // expect "ACK base64(reply)"
		ss := strings.Split(reply, " ")
		if len(ss) > 1 {
			return base64.StdEncoding.DecodeString(ss[1])
		}
	}

	// If not ACK, then the whole reply is an error string.
	return base64.StdEncoding.DecodeString(reply)
}

func (o *Op) send(conn net.Conn, msg string) (string, error) {
	_, err := conn.Write([]byte(msg))
	if err != nil {
		o.logger.Printf("Write failed: %v", err)
		return "", err
	}

	return o.recv(conn)
}

func (o *Op) recv(conn net.Conn) (string, error) {
	buffer, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		o.logger.Printf("ReadString failed: %v", err)
		return "", err
	}

	reply := buffer[:len(buffer)-1]
	return reply, nil
}

func (o *Op) buildAckReply(err error) string {
	if err != nil {
		ee := base64.StdEncoding.EncodeToString([]byte(err.Error()))
		return fmt.Sprintf("%v %v\n", CmdAck, ee)
	} else {
		return fmt.Sprintf("%v\n", CmdAck)
	}
}

func (o *Op) getLeaderConn(ctx context.Context) (net.Conn, error) {
	var conn net.Conn
	var err error
	subctx := context.WithValue(ctx, struct{}{}, nil)
	first := make(chan struct{}, 1)
	first <- struct{}{} // immediately the first time
	tcnt, tlimit := int64(0), (o.lockTimeout/2000)*2
	ticker := time.NewTicker(time.Second * 2) // processing can be more than this
	defer ticker.Stop()

	var active int32
	getConn := func() (net.Conn, error) {
		atomic.StoreInt32(&active, 1)
		defer atomic.StoreInt32(&active, 0)
		timeout := time.Second * 5
		leader, err := o.Leader()
		if err != nil {
			return nil, err
		}

		if leader == "" {
			return nil, ErrNoLeader
		}

		lconn, err := net.DialTimeout("tcp", leader, timeout)
		if err != nil {
			o.logger.Printf("[getLeaderConn] DialTimeout failed: %v", err)
			return nil, err
		}

		defer lconn.Close()
		reply, err := o.send(lconn, fmt.Sprintf("%v\n", CmdLeader))
		if err != nil {
			o.logger.Printf("[getLeaderConn] send failed: %v", err)
			return nil, err
		}

		if !strings.HasPrefix(reply, CmdAck) {
			return nil, ErrNoLeader
		}

		// Create a new connection to the confirmed leader.
		return net.DialTimeout("tcp", leader, timeout)
	}

	type conn_t struct {
		conn net.Conn
		err  error
	}

	for {
		select {
		case <-subctx.Done():
			return nil, context.Canceled
		case <-first:
		case <-ticker.C:
		}

		if atomic.LoadInt32(&active) == 1 {
			continue
		}

		ch := make(chan conn_t, 1)
		go func() {
			c, e := getConn()
			ch <- conn_t{c, e}
		}()

		res := <-ch
		conn = res.conn
		err = res.err

		tcnt++
		if err == nil || (tcnt > tlimit) {
			break
		}
	}

	return conn, nil
}

func (o *Op) getMembers() map[string]struct{} {
	o.mtx.Lock()
	defer o.mtx.Unlock()
	return o.members
}

func (o *Op) encodeMembers() string {
	o.mtx.Lock()
	defer o.mtx.Unlock()
	b, _ := json.Marshal(o.members)
	return base64.StdEncoding.EncodeToString(b)
}

func (o *Op) setMembers(m map[string]struct{}) {
	o.mtx.Lock()
	defer o.mtx.Unlock()
	o.members = m
}

func (o *Op) addMember(id string) {
	o.mtx.Lock()
	defer o.mtx.Unlock()
	o.members[id] = struct{}{}
}

func (o *Op) delMember(id string) {
	o.mtx.Lock()
	defer o.mtx.Unlock()
	delete(o.members, id)
}

// New creates an instance of Op. 'hostPort' should be in ip:port format. The internal spindle object
// lock table will be 'lockTable'; lock name will be 'lockName'. And 'logTable' will serve as our
// append-only, distributed key/value storage table.
func New(client *spanner.Client, hostPort, lockTable, lockName, logTable string, opts ...Option) *Op {
	op := &Op{
		hostPort:      hostPort,
		spannerClient: client,
		lockTable:     lockTable,
		lockName:      lockName,
		logTable:      logTable,
		members:       make(map[string]struct{}),
	}

	for _, opt := range opts {
		opt.Apply(op)
	}

	switch {
	case op.lockTimeout == 0:
		op.lockTimeout = 30000 // default 30s
	case op.lockTimeout < 2000:
		op.lockTimeout = 2000 // minimum 2s
	}

	if op.logger == nil {
		prefix := fmt.Sprintf("[hedge/%v] ", op.hostPort)
		op.logger = log.New(os.Stdout, prefix, log.LstdFlags)
	}

	return op
}
