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
	"github.com/hashicorp/memberlist"
	"google.golang.org/api/iterator"
)

const (
	CmdLeader     = "LDR" // for leader confirmation, reply="ACK"
	CmdWrite      = "PUT" // write key/value, fmt="PUT <base64(payload)> [noappend]"
	CmdSend       = "SND" // member to leader, fmt="SND <base64(payload)>"
	CmdPing       = "HEY" // heartbeat to indicate availability, fmt="HEY [id]"
	CmdMembers    = "MEM" // members info from leader to all, fmt="MEM base64(JSON(members))"
	CmdBroadcast  = "ALL" // broadcast to all, fmt="ALL base64(payload)"
	CmdAck        = "ACK" // generic reply, fmt="ACK"|"ACK base64(err)"|"ACK base64(JSON(members))"
	CmdSemaphore  = "SEM" // create semaphore, fmt="SEM {name} {limit} {caller}, reply="ACK"
	CmdSemAcquire = "SEA" // acquire semaphore, fmt="SEA {name} {caller}", reply="ACK[ base64([0:|1:]err)]" (0=final,1=retry)
	CmdSemRelease = "SER" // release semaphore, fmt="SER {name} {caller}"

	FlagNoAppend = "noappend"
)

var (
	ErrNotRunning   = fmt.Errorf("hedge: not running")
	ErrNoLeader     = fmt.Errorf("hedge: no leader available")
	ErrNoHandler    = fmt.Errorf("hedge: no message handler")
	ErrNotSupported = fmt.Errorf("hedge: not supported")
	ErrInvalidConn  = fmt.Errorf("hedge: invalid connection")

	cctx = func(ctx context.Context) context.Context {
		return context.WithValue(ctx, struct{}{}, nil)
	}
)

type FnMsgHandler func(data interface{}, msg []byte) ([]byte, error)

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

func (w withDuration) Apply(op *Op) { op.lockTimeout = int64(w) }

// WithDuration sets Op's internal spindle object's lease duration.
// Defaults to 30s when not set. Minimum value is 2s.
func WithDuration(v int64) Option { return withDuration(v) }

type withGroupSyncInterval time.Duration

func (w withGroupSyncInterval) Apply(op *Op) { op.syncInterval = time.Duration(w) }

// WithGroupSyncInterval sets the internal interval timeout to sync membership
// within the group. If not set, defaults to 30s. Minimum value is 2s.
func WithGroupSyncInterval(v time.Duration) Option { return withGroupSyncInterval(v) }

type withLeaderHandler struct {
	d interface{}
	h FnMsgHandler
}

func (w withLeaderHandler) Apply(op *Op) {
	op.fnLdrData = w.d
	op.fnLeader = w.h
}

// WithLeaderHandler sets the node's callback function when it is the current
// leader and when members send messages to it using the Send(...) API. Any
// arbitrary data represented by d will be passed to the callback h every
// time it is called. If d is nil, the default callback data will be the *Op
// object itself. The handler's returning []byte will serve as reply.
//
// Typical flow would be:
//  1. Any node (including the leader) calls the Send(...) API.
//  2. The current leader handles the call by reading the input.
//  3. Leader will then call FnLeaderHandler, passing the arbitrary data
//     along with the message.
//  4. FnLeaderHandler will process the data as leader, then returns the
//     reply to the calling member.
func WithLeaderHandler(d interface{}, h FnMsgHandler) Option {
	return withLeaderHandler{d, h}
}

type withBroadcastHandler struct {
	d interface{}
	h FnMsgHandler
}

func (w withBroadcastHandler) Apply(op *Op) {
	op.fnBcData = w.d
	op.fnBroadcast = w.h
}

// WithBroadcastHandler sets the node's callback function for broadcast messages
// from anyone in the group using the Broadcast(...) API. Any arbitrary data
// represented by d will be passed to the callback h every time it is called.
// If d is nil, the default callback data will be the *Op object itself. The
// handler's returning []byte will serve as reply.
//
// A nil broadcast handler disables the internal heartbeat function.
func WithBroadcastHandler(d interface{}, h FnMsgHandler) Option {
	return withBroadcastHandler{d, h}
}

type withLogger struct{ l *log.Logger }

func (w withLogger) Apply(op *Op) { op.logger = w.l }

// WithLogger sets Op's logger object. Can be silenced by setting v to:
//
//	log.New(ioutil.Discard, "", 0)
func WithLogger(v *log.Logger) Option { return withLogger{v} }

// Op is our main instance for hedge operations.
type Op struct {
	hostPort      string          // this instance's id; address:port
	spannerClient *spanner.Client // both for spindle and hedge
	lockTable     string          // spindle lock table
	lockName      string          // spindle lock name
	lockTimeout   int64           // spindle's lock lease duration in ms
	logTable      string          // append-only log table

	fnLeader    FnMsgHandler // leader message handler
	fnLdrData   interface{}  // arbitrary data passed to fnLeader
	fnBroadcast FnMsgHandler // broadcast message handler
	fnBcData    interface{}  // arbitrary data passed to fnBroadcast

	*spindle.Lock                     // handles our distributed lock
	members       map[string]struct{} // key=id
	syncInterval  time.Duration       // ensure membership
	mtx           sync.Mutex          // local mutex
	mtxSem        sync.Mutex          // semaphore mutex
	ensureOn      int32               // 1=semaphore checker running
	ensureCh      chan string         // please check this id
	ensureCtx     context.Context
	ensureCancel  context.CancelFunc
	ensureDone    chan struct{}
	active        int32       // 1=running, 0=off
	logger        *log.Logger // internal logger
}

// String implements the Stringer interface.
func (op *Op) String() string {
	return fmt.Sprintf("hostport:%s;spindle:%v;%v;%v",
		op.hostPort,
		op.spannerClient.DatabaseName(),
		op.lockTable,
		op.logTable,
	)
}

// HostPort returns the host:port (or name) of this instance.
func (op *Op) HostPort() string { return op.hostPort }

// Name is the same as HostPort.
func (op *Op) Name() string { return op.hostPort }

// IsRunning returns true if Op is already running.
func (op *Op) IsRunning() bool { return atomic.LoadInt32(&op.active) == 1 }

// Run starts the main handler. It blocks until ctx is cancelled,
// optionally sending an error message to done when finished.
func (op *Op) Run(ctx context.Context, done ...chan error) error {
	var err error
	defer func(e *error) {
		if len(done) > 0 {
			done[0] <- *e
		}
	}(&err)

	// Some housekeeping.
	if op.spannerClient == nil {
		err = fmt.Errorf("hedge: Spanner client cannot be nil")
		return err
	}

	for _, v := range []struct {
		name string
		val  string
	}{
		{"SpindleTable", op.lockTable},
		{"SpindleLockName", op.lockName},
		{"LogTable", op.logTable},
	} {
		if v.val == "" {
			err = fmt.Errorf("hedge: %v cannot be empty", v.name)
			return err
		}
	}

	// Setup our server for our internal protocol.
	addr, err := net.ResolveTCPAddr("tcp4", op.hostPort)
	if err != nil {
		return err
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}

	defer listener.Close()
	op.logger.Printf("tcp: listen on %v", op.hostPort)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				op.logger.Printf("listener.Accept failed: %v", err)
				return
			}

			if ctx.Err() != nil {
				op.logger.Printf("cancelled: %v", ctx.Err())
				return
			}

			go handleMsg(ctx, op, conn)
		}
	}()

	// Setup and start our internal spindle object.
	op.Lock = spindle.New(
		op.spannerClient,
		op.lockTable,
		fmt.Sprintf("hedge/spindle/lockname/%v", op.lockName),
		spindle.WithDuration(op.lockTimeout),
		spindle.WithId(op.hostPort),
		spindle.WithLogger(op.logger),
	)

	spindleDone := make(chan error, 1)
	ctxSpindle, cancel := context.WithCancel(context.Background())
	op.Lock.Run(ctxSpindle, spindleDone)
	defer func() {
		cancel()      // stop spindle;
		<-spindleDone // and wait
	}()

	// Start tracking online members.
	op.members[op.hostPort] = struct{}{}
	membersDone := make(chan error, 1)
	ctxMembers := cctx(ctx)
	first := make(chan struct{}, 1)
	first <- struct{}{} // immediately the first time
	ticker := time.NewTicker(op.syncInterval)
	defer func() {
		ticker.Stop()
		<-membersDone
	}()

	go func() {
		var active int32
		fnEnsureMembers := func() {
			atomic.StoreInt32(&active, 1)
			defer atomic.StoreInt32(&active, 0)
			ch := make(chan *string)
			emdone := make(chan struct{}, 1)
			todel := []string{}
			go func() {
				for {
					m := <-ch
					switch {
					case m == nil:
						emdone <- struct{}{}
						return
					default:
						todel = append(todel, *m)
					}
				}
			}()

			var w sync.WaitGroup
			allm := op.getMembers()
			for k := range allm {
				w.Add(1)
				go func(id string) {
					defer func() { w.Done() }()
					timeout := time.Second * 5
					conn, err := net.DialTimeout("tcp", id, timeout)
					if err != nil {
						ch <- &id // delete this
						return
					}

					var sb strings.Builder
					fmt.Fprintf(&sb, "%s\n", CmdPing)
					r, err := op.send(conn, sb.String())
					if err != nil {
						ch <- &id // delete this
						return
					}

					if r != CmdAck {
						ch <- &id // delete this
					}
				}(k)
			}

			w.Wait()
			ch <- nil // close;
			<-emdone  // and wait
			for _, rm := range todel {
				if rm != "" {
					op.logger.Printf("[leader] delete %v", rm)
					op.delMember(rm)
				}
			}

			// Broadcast active members to all.
			for k := range op.getMembers() {
				w.Add(1)
				go func(id string) {
					defer w.Done()
					timeout := time.Second * 5
					conn, err := net.DialTimeout("tcp", id, timeout)
					if err != nil {
						return
					}

					defer conn.Close()
					var sb strings.Builder
					fmt.Fprintf(&sb, "%s %s\n", CmdMembers, op.encodeMembers())
					op.send(conn, sb.String())
				}(k)
			}

			w.Wait()
		}

		var hbactive int32
		fnHeartbeat := func() {
			atomic.StoreInt32(&hbactive, 1)
			defer atomic.StoreInt32(&hbactive, 0)
			lconn, err := op.getLeaderConn(ctx)
			if err != nil {
				return
			}

			if lconn != nil {
				defer lconn.Close()
			}

			var sb strings.Builder
			fmt.Fprintf(&sb, "%s %s\n", CmdPing, op.hostPort)
			r, err := op.send(lconn, sb.String())
			if err != nil {
				return
			}

			b, _ := base64.StdEncoding.DecodeString(r)
			var allm map[string]struct{}
			json.Unmarshal(b, &allm)
			op.setMembers(allm)
		}

		for {
			select {
			case <-ctxMembers.Done():
				membersDone <- nil
				return
			case <-first:
			case <-ticker.C:
			}

			if op.fnBroadcast == nil {
				op.logger.Println("no broadcast support")
				membersDone <- nil
				return
			}

			if atomic.LoadInt32(&hbactive) == 0 {
				go fnHeartbeat() // tell leader we're online
			}

			if hl, _ := op.HasLock(); !hl {
				continue
			}

			if atomic.LoadInt32(&active) == 0 {
				go fnEnsureMembers() // leader only
			}
		}
	}()

	atomic.StoreInt32(&op.active, 1)
	defer atomic.StoreInt32(&op.active, 0)

	<-ctx.Done() // wait for termination

	if atomic.LoadInt32(&op.ensureOn) == 1 {
		op.ensureCancel() // stop semaphore checker;
		<-op.ensureDone   // and wait
	}

	return nil
}

// NewSemaphore returns a distributed semaphore object.
func (op *Op) NewSemaphore(ctx context.Context, name string, limit int) (*Semaphore, error) {
	if atomic.LoadInt32(&op.active) != 1 {
		return nil, ErrNotRunning
	}

	if strings.Contains(name, " ") {
		return nil, fmt.Errorf("name cannot have whitespace(s)")
	}

	conn, err := op.getLeaderConn(ctx)
	if err != nil {
		return nil, err
	}

	if conn != nil {
		defer conn.Close()
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "%s %s %d %s\n", CmdSemaphore, name, limit, op.hostPort)
	reply, err := op.send(conn, sb.String())
	if err != nil {
		return nil, err
	}

	switch {
	case strings.HasPrefix(reply, CmdAck):
		ss := strings.Split(reply, " ")
		if len(ss) > 1 { // failed
			dec, _ := base64.StdEncoding.DecodeString(ss[1])
			return nil, fmt.Errorf(string(dec))
		}
	default:
		return nil, ErrNotSupported
	}

	return &Semaphore{name, limit, op}, nil
}

// Get reads a key (or keys) from Op.
// The values of limit are:
//
//	limit = 0  --> (default) latest only
//	limit = -1 --> all (latest to oldest, [0]=latest)
//	limit = -2 --> oldest version only
//	limit > 0  --> items behind latest; 3 means latest + 2 versions behind, [0]=latest
func (op *Op) Get(ctx context.Context, key string, limit ...int64) ([]KeyValue, error) {
	ret := []KeyValue{}
	var q strings.Builder
	fmt.Fprintf(&q, "select key, value, timestamp ")
	fmt.Fprintf(&q, "from %s ", op.logTable)
	fmt.Fprintf(&q, "where key = @key and timestamp is not null ")
	fmt.Fprintf(&q, "order by timestamp desc limit 1")

	if len(limit) > 0 {
		switch {
		case limit[0] > 0:
			q.Reset()
			fmt.Fprintf(&q, "select key, value, timestamp ")
			fmt.Fprintf(&q, "from %s ", op.logTable)
			fmt.Fprintf(&q, "where key = @key and timestamp is not null ")
			fmt.Fprintf(&q, "order by timestamp desc limit %v", limit[0])
		case limit[0] == -1:
			q.Reset()
			fmt.Fprintf(&q, "select key, value, timestamp ")
			fmt.Fprintf(&q, "from %s ", op.logTable)
			fmt.Fprintf(&q, "where key = @key and timestamp is not null ")
			fmt.Fprintf(&q, "order by timestamp desc")
		case limit[0] == -2:
			q.Reset()
			fmt.Fprintf(&q, "select key, value, timestamp ")
			fmt.Fprintf(&q, "from %s ", op.logTable)
			fmt.Fprintf(&q, "where key = @key and timestamp is not null ")
			fmt.Fprintf(&q, "order by timestamp limit 1")
		}
	}

	stmt := spanner.Statement{SQL: q.String(), Params: map[string]interface{}{"key": key}}
	iter := op.spannerClient.Single().Query(ctx, stmt)
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

type PutOptions struct {
	// If true, do a direct write, no need to fwd to leader.
	DirectWrite bool

	// If true, don't do an append-write; overwrite the latest. Note that even if you set this
	// to true, if you do another Put the next time with this field set as false (default),
	// the previous write will now be gone, or will now be part of the history.
	NoAppend bool
}

// Put saves a key/value to Op. This call will try to block, at least roughly until spindle's
// timeout, to wait for the leader's availability to do actual writes before returning.
func (op *Op) Put(ctx context.Context, kv KeyValue, po ...PutOptions) error {
	var err error
	var direct, noappend, hl bool
	if len(po) > 0 {
		direct = po[0].DirectWrite
		noappend = po[0].NoAppend
	} else {
		hl, _ = op.HasLock()
	}

	id := uuid.NewString()
	if noappend {
		id = "-"
	}

	if direct || hl {
		b, _ := json.Marshal(kv)
		op.logger.Printf("[Put] leader: direct write: %v", string(b))
		_, err := op.spannerClient.Apply(ctx, []*spanner.Mutation{
			spanner.InsertOrUpdate(op.logTable,
				[]string{"id", "key", "value", "leader", "timestamp"},
				[]interface{}{id, kv.Key, kv.Value, op.hostPort, spanner.CommitTimestamp},
			),
		})

		return err
	}

	// For non-leaders, we confirm the leader via spindle, and if so, ask leader to do the
	// actual write for us. Let's do a couple retries up to spindle's timeout.
	conn, err := op.getLeaderConn(ctx)
	if err != nil {
		return err
	}

	if conn != nil {
		defer conn.Close()
	}

	b, _ := json.Marshal(kv)
	enc := base64.StdEncoding.EncodeToString(b)
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s %s\n", CmdWrite, enc)
	if noappend {
		sb.Reset()
		fmt.Fprintf(&sb, "%s %s %s\n", CmdWrite, enc, FlagNoAppend)
	}

	reply, err := op.send(conn, sb.String())
	if err != nil {
		return err
	}

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

// Send sends msg to the current leader. Any node can send messages,
// including the leader itself (send to self). It also blocks until
// it receives the reply from the leader's message handler.
func (op *Op) Send(ctx context.Context, msg []byte) ([]byte, error) {
	conn, err := op.getLeaderConn(ctx)
	if err != nil {
		return nil, err
	}

	if conn != nil {
		defer conn.Close()
	}

	enc := base64.StdEncoding.EncodeToString(msg)
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s %s\n", CmdSend, enc)
	reply, err := op.send(conn, sb.String())
	if err != nil {
		return nil, err
	}

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

type BroadcastOutput struct {
	Id    string
	Reply []byte
	Error error
}

// Broadcast sends msg to all nodes (send to all). Any node can broadcast
// messages, including the leader itself. Note that this is best-effort
// basis only; by the time you call this API, the handler might not have
// all the active members in record yet, as is the usual situation with
// k8s deployments, where pods come and go, and our internal heartbeat
// protocol hasn't been completed yet. This call will also block until it
// receives all the reply from all nodes' broadcast handlers.
func (op *Op) Broadcast(ctx context.Context, msg []byte) []BroadcastOutput {
	if atomic.LoadInt32(&op.active) != 1 || op.fnBroadcast == nil {
		return nil // not running or no broadcast support
	}

	outs := []BroadcastOutput{}
	var w sync.WaitGroup
	members := op.getMembers()
	outch := make(chan BroadcastOutput, len(members))
	for k := range members {
		w.Add(1)
		go func(id string) {
			defer w.Done()
			timeout := time.Second * 5
			conn, err := net.DialTimeout("tcp", id, timeout)
			if err != nil {
				outch <- BroadcastOutput{Id: id, Error: err}
				return
			}

			defer conn.Close()
			enc := base64.StdEncoding.EncodeToString(msg)
			var sb strings.Builder
			fmt.Fprintf(&sb, "%s %s\n", CmdBroadcast, enc)
			reply, err := op.send(conn, sb.String())
			if err != nil {
				outch <- BroadcastOutput{Id: id, Error: err}
				return
			}

			switch {
			case strings.HasPrefix(reply, CmdAck): // expect "ACK base64(reply)"
				ss := strings.Split(reply, " ")
				if len(ss) > 1 {
					r, e := base64.StdEncoding.DecodeString(ss[1])
					outch <- BroadcastOutput{Id: id, Reply: r, Error: e}
					return
				}
			}

			// If not ACK, then the whole reply is an error string.
			r, _ := base64.StdEncoding.DecodeString(reply)
			outch <- BroadcastOutput{Id: id, Error: fmt.Errorf(string(r))}
		}(k)
	}

	w.Wait()
	for range members {
		outs = append(outs, <-outch)
	}

	return outs
}

// Members returns a list of members in the cluster/group.
func (op *Op) Members() []string {
	members := []string{}
	m := op.getMembers()
	for k := range m {
		members = append(members, k)
	}

	return members
}

func (op *Op) send(conn net.Conn, msg string) (string, error) {
	if conn == nil {
		return "", ErrInvalidConn
	}

	_, err := conn.Write([]byte(msg))
	if err != nil {
		return "", err
	}

	return op.recv(conn)
}

func (op *Op) recv(conn net.Conn) (string, error) {
	if conn == nil {
		return "", ErrInvalidConn
	}

	buffer, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return "", err
	}

	var reply string
	if buffer != "" {
		reply = buffer[:len(buffer)-1]
	}

	return reply, nil
}

func (op *Op) buildAckReply(err error) string {
	var sb strings.Builder
	if err != nil {
		ee := base64.StdEncoding.EncodeToString([]byte(err.Error()))
		fmt.Fprintf(&sb, "%s %s\n", CmdAck, ee)
		return sb.String()
	} else {
		fmt.Fprintf(&sb, "%s\n", CmdAck)
		return sb.String()
	}
}

func (op *Op) getLeaderConn(ctx context.Context) (net.Conn, error) {
	var conn net.Conn
	var err error
	subctx := context.WithValue(ctx, struct{}{}, nil)
	first := make(chan struct{}, 1)
	first <- struct{}{} // immediately the first time
	tcnt, tlimit := int64(0), (op.lockTimeout/2000)*2
	ticker := time.NewTicker(time.Second * 2) // processing can be more than this
	defer ticker.Stop()

	var active int32
	getConn := func() (net.Conn, error) {
		atomic.StoreInt32(&active, 1)
		defer atomic.StoreInt32(&active, 0)
		timeout := time.Second * 5
		leader, err := op.Leader()
		if err != nil {
			return nil, err
		}

		if leader == "" {
			return nil, ErrNoLeader
		}

		lconn, err := net.DialTimeout("tcp", leader, timeout)
		if err != nil {
			return nil, err
		}

		defer lconn.Close()
		var sb strings.Builder
		fmt.Fprintf(&sb, "%s\n", CmdLeader)
		reply, err := op.send(lconn, sb.String())
		if err != nil {
			return nil, err
		}

		if !strings.HasPrefix(reply, CmdAck) {
			return nil, ErrNoLeader
		}

		// Create a new connection to the confirmed leader.
		return net.DialTimeout("tcp", leader, timeout)
	}

	type connT struct {
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

		ch := make(chan connT, 1)
		go func() {
			c, e := getConn()
			ch <- connT{c, e}
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

func (op *Op) getMembers() map[string]struct{} {
	op.mtx.Lock()
	copy := make(map[string]struct{})
	for k, v := range op.members {
		copy[k] = v
	}

	op.mtx.Unlock()
	return copy
}

func (op *Op) encodeMembers() string {
	op.mtx.Lock()
	defer op.mtx.Unlock()
	b, _ := json.Marshal(op.members)
	return base64.StdEncoding.EncodeToString(b)
}

func (op *Op) setMembers(m map[string]struct{}) {
	op.mtx.Lock()
	defer op.mtx.Unlock()
	op.members = m
}

func (op *Op) addMember(id string) {
	op.mtx.Lock()
	defer op.mtx.Unlock()
	op.members[id] = struct{}{}
}

func (op *Op) delMember(id string) {
	op.mtx.Lock()
	defer op.mtx.Unlock()
	delete(op.members, id)
}

// New creates an instance of Op. hostPort can be in "ip:port" format, or ":port" format, in which case
// the IP part will be resolved internally, or empty, in which case port 8080 will be used. The internal
// spindle object's lock table name will be lockTable, and lockName is the lock name. logTable will
// serve as our append-only, distributed key/value storage table.
func New(client *spanner.Client, hostPort, lockTable, lockName, logTable string, opts ...Option) *Op {
	op := &Op{
		hostPort:      hostPort,
		spannerClient: client,
		lockTable:     lockTable,
		lockName:      lockName,
		logTable:      logTable,
		members:       make(map[string]struct{}),
		ensureCh:      make(chan string),
		ensureDone:    make(chan struct{}, 1),
		Lock:          &spindle.Lock{}, // init later
	}

	for _, opt := range opts {
		opt.Apply(op)
	}

	host, port, _ := net.SplitHostPort(op.hostPort)
	switch {
	case host == "" && port != "":
		// We will use memberlist for IP resolution.
		list, _ := memberlist.Create(memberlist.DefaultLANConfig())
		localNode := list.LocalNode()
		lh, _, _ := net.SplitHostPort(localNode.Address())
		op.hostPort = net.JoinHostPort(lh, port)
		list.Shutdown()
	case host == "" && port == "":
		// We will use memberlist for IP resolution.
		list, _ := memberlist.Create(memberlist.DefaultLANConfig())
		localNode := list.LocalNode()
		lh, _, _ := net.SplitHostPort(localNode.Address())
		op.hostPort = net.JoinHostPort(lh, "8080")
		list.Shutdown()
	}

	switch {
	case op.lockTimeout == 0:
		op.lockTimeout = 30000 // default 30s
	case op.lockTimeout < 2000:
		op.lockTimeout = 2000 // minimum 2s
	}

	switch {
	case op.syncInterval == 0:
		op.syncInterval = time.Second * 30 // default
	case op.syncInterval < (time.Second * 2):
		op.syncInterval = time.Second * 2 // minimum
	}

	if op.logger == nil {
		prefix := fmt.Sprintf("[hedge/%v] ", op.hostPort)
		op.logger = log.New(os.Stdout, prefix, log.LstdFlags)
	}

	return op
}
