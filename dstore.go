package dstore

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/flowerinthenight/spindle"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

const (
	spindleLockName = "dstorespindlelock"
)

var (
	ErrNotRunning = fmt.Errorf("dstore: not running")
)

// KeyValue represents an item in our log.
type KeyValue struct {
	Key       string
	Value     string
	Timestamp time.Time // ignored in Put()
}

// Store is our main distributed, append-only log storage object.
type Store struct {
	id      string          // this instance's unique id
	client  *spanner.Client // Spanner client
	natsCon *nats.Conn      // our NATS connection
	logger  *log.Logger     // can be silenced by `log.New(ioutil.Discard, "", 0)`

	*spindle.Lock                     // handles our distributed lock
	lockTable     string              // spindle lock table
	lockName      string              // spindle lock name
	logTable      string              // append-only log table
	queue         map[string]struct{} // for tracking outgoing/incoming NATS messages
	writeTimeout  int64               // Put() timeout
	active        int32               // 1=running, 0=off
	sync.Mutex
}

// Run starts the main goroutine handler. It terminates upon 'ctx' cancellation,
// optionally sending a message to 'done' when finished.
func (s *Store) Run(ctx context.Context, done ...chan error) error {
	if s.client == nil {
		return fmt.Errorf("dstore: Spanner client cannot be nil")
	}

	if s.natsCon == nil {
		return fmt.Errorf("dstore: NATS connection cannot be nil")
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
			return fmt.Errorf("dstore: %v cannot be empty", v.name)
		}
	}

	s.Lock = spindle.New(
		s.client,
		s.lockTable,
		s.lockTable,
		spindle.WithDuration(30000),
		spindle.WithId(fmt.Sprintf("dstore/spindle/%v", s.id)),
	)

	spindleDone := make(chan error, 1)
	s.Lock.Run(ctx, spindleDone)

	return nil
}

// Put saves a key/value to Store.
func (s *Store) Put(ctx context.Context, kv KeyValue) error {
	if atomic.LoadInt32(&s.active) != 1 {
		return ErrNotRunning
	}

	return nil
}

// Get reads a key (or keys) from Store.
// 0 (default) = latest only
// -1 = all (latest to oldest, [0]=latest)
// -2 = oldest version only
// >0 = items behind latest; 3 means latest + 2 versions behind, [0]=latest
func (s *Store) Get(ctx context.Context, key string, limit ...int64) ([]KeyValue, error) {
	return nil, nil
}

func (s *Store) onRecv(m *nats.Msg) {
	var e cloudevents.Event
	err := json.Unmarshal(m.Data, &e)
	if err != nil {
		return
	}
}

// newMsg returns a JSON standard cloudevent message.
// See https://github.com/cloudevents/sdk-go for more details.
func (s *Store) newMsg(data interface{}) cloudevents.Event {
	m := cloudevents.NewEvent()
	m.SetID(uuid.New().String())
	m.SetSource(fmt.Sprintf("dstore/%s", s.id))
	m.SetType("dstore.event.internal")
	m.SetData(cloudevents.ApplicationJSON, data)
	return m
}

// Config is our configuration to New().
type Config struct {
	Id              string          // optional, will generate uuid if empty
	Client          *spanner.Client // required, Spanner client
	NatsCon         *nats.Conn      // required, NATS connection for communication
	SpindleTable    string          // required, table name for *spindle.Lock
	SpindleLockName string          // optional, "dstorespindlelock" by default
	LogTable        string          // required, table name for the append-only storage
	WriteTimeout    int64           // optional, wait time (in ms) for Put(), default is 5000ms
	Logger          *log.Logger     // optional, can be silenced by `log.New(ioutil.Discard, "", 0)`
}

// New creates an instance of Store.
func New(cfg Config) *Store {
	s := &Store{
		id:           cfg.Id,
		client:       cfg.Client,
		natsCon:      cfg.NatsCon,
		logger:       cfg.Logger,
		lockTable:    cfg.SpindleTable,
		lockName:     cfg.SpindleLockName,
		logTable:     cfg.LogTable,
		queue:        make(map[string]struct{}),
		writeTimeout: cfg.WriteTimeout,
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
		prefix := fmt.Sprintf("[dstore][%v] ", s.id)
		s.logger = log.New(os.Stdout, prefix, log.LstdFlags)
	}

	return s
}
