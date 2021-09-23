package dstore

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/spanner"
	lspubsub "github.com/flowerinthenight/longsub/gcppubsub"
	"github.com/flowerinthenight/spindle"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	spindleLockName = "dstorespindlelock"

	CmdWrite = "WRITE"
	CmdAck   = "ACK"
)

var (
	ErrNotRunning = fmt.Errorf("dstore: not running")
)

type cmd_t struct {
	Ctrl string // WRITE|ACK
	Data []byte // LogItem if WRITE, id if ACK
}

type KeyValue struct {
	Key       string
	Value     string
	Timestamp time.Time // ignored in Put()
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

	pubsubProject    string
	pubsubClientOpts []option.ClientOption
	pub              *pubsub.Topic // internal publisher

	logger *log.Logger // can be silenced by `log.New(ioutil.Discard, "", 0)`

	*spindle.Lock                        // handles our distributed lock
	lockTable     string                 // spindle lock table
	lockName      string                 // spindle lock name
	logTable      string                 // append-only log table
	queue         map[string]chan []byte // for tracking outgoing/incoming messages
	writeTimeout  int64                  // Put() timeout
	active        int32                  // 1=running, 0=off
	mtx           sync.Mutex             // local lock
}

// String returns some friendly information.
func (s *Store) String() string {
	return fmt.Sprintf("name:%v/%s spindle:%v;%v;%v pubsub:%v",
		s.group,
		s.id,
		s.spannerClient.DatabaseName(),
		s.lockTable,
		s.logTable,
		s.pubsubProject,
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

	s.Lock = spindle.New(
		s.spannerClient,
		s.lockTable,
		fmt.Sprintf("dstore/spindle/lockname/%v", s.group),
		spindle.WithDuration(30000), // 30s duration
		spindle.WithId(fmt.Sprintf("dstore/spindle/%v", s.id)),
	)

	spindledone := make(chan error, 1)
	spindlectx, cancel := context.WithCancel(context.Background())
	s.Lock.Run(spindlectx, spindledone)
	defer func() {
		cancel()      // stop spindle;
		<-spindledone // and wait
	}()

	// Setup our broadcast topic.
	client, err := pubsub.NewClient(ctx, s.pubsubProject, s.pubsubClientOpts...)
	if err != nil {
		return err
	}

	defer client.Close()

	// Most likely, multiple instances will be calling this roughly at the same time.
	for i := 0; i < 30; i++ { // 1min?
		pub, err := s.getTopic(ctx, client)
		if err != nil {
			s.logger.Printf("dstore: attemtp #%v: getTopic failed, retry: %v", i, err)
			time.Sleep(time.Second * 2)
			continue
		}

		s.pub = pub
		break
	}

	if s.pub == nil {
		err = fmt.Errorf("dstore: publisher setup failed")
		return err
	}

	sub, err := s.getSubscription(ctx, client, s.pub)
	if err != nil {
		return err
	}

	defer func() {
		err := sub.Delete(ctx) // best-effort cleanup
		if err != nil {
			s.logger.Printf("sub.Delete failed: %v", err)
		}
	}()

	subdone := make(chan error, 1)
	go func() {
		lssub := lspubsub.NewLengthySubscriber(nil, s.pubsubProject, s.subName(), s.onRecv)
		err := lssub.Start(context.WithValue(ctx, struct{}{}, nil), subdone)
		if err != nil {
			s.logger.Printf("lssub.Start failed: %v", err)
		}
	}()

	defer func() { <-subdone }()
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
func (s *Store) Put(ctx context.Context, kv KeyValue) error {
	if atomic.LoadInt32(&s.active) != 1 {
		return ErrNotRunning
	}

	defer func(begin time.Time) {
		s.logger.Printf("[Put] duration=%v", time.Since(begin))
	}(time.Now())

	id := uuid.NewString()
	leader, _ := s.HasLock()
	// NOTE: test only, rm !
	if !leader { // we're in luck, quite straightforward
		_, err := s.spannerClient.Apply(ctx, []*spanner.Mutation{
			spanner.InsertOrUpdate(s.logTable,
				[]string{"id", "key", "value", "leader", "timestamp"},
				[]interface{}{id, kv.Key, kv.Value, s.id, spanner.CommitTimestamp},
			),
		})

		return err
	}

	// For non-leaders, do a 2-stage write.
	// 1) Broadcast the write to all group members. All non-leaders receiving the message
	// will just ignore it, including us.
	// 2) The leader who receives the write message will write kv without the timestamp.
	// Leader will then broadcast the 'ack' message to all members. Non-senders will just
	// ignore the ack, but we will complete the write by updating the timestamp. Then the
	// write is complete.

	ch := make(chan []byte, 1)
	func() {
		s.mtx.Lock()
		defer s.mtx.Unlock()
		s.queue[id] = ch
	}()

	li := LogItem{
		Id:    id,
		Key:   kv.Key,
		Value: kv.Value,
	}

	// Broadcast write to leader.
	b, _ := json.Marshal(li)
	c := cmd_t{Ctrl: CmdWrite, Data: b}
	b, _ = json.Marshal(c)
	res := s.pub.Publish(ctx, &pubsub.Message{Data: b})
	_, err := res.Get(ctx)
	if err != nil {
		return err
	}

	// Read reply, if any.
	var timeout = time.Second * time.Duration(s.writeTimeout)
	subctx := context.WithValue(ctx, struct{}{}, nil)

	select {
	case <-subctx.Done():
		break
	case <-time.After(timeout):
		break
	case b := <-ch:
		_ = b
	}

	return nil
}

func (s *Store) onRecv(app interface{}, data []byte) error {
	ctx := context.Background()
	s.logger.Println(string(data))
	var cmd cmd_t
	err := json.Unmarshal(data, &cmd)
	if err != nil {
		s.logger.Printf("Unmarshal failed: %v", err)
		return err
	}

	leader, _ := s.HasLock()
	if leader {
		switch {
		case cmd.Ctrl == CmdWrite:
			var li LogItem
			json.Unmarshal(cmd.Data, &li)
			_, err := s.spannerClient.Apply(ctx, []*spanner.Mutation{
				spanner.InsertOrUpdate(s.logTable,
					[]string{"id", "key", "value", "leader"},
					[]interface{}{li.Id, li.Key, li.Value, s.id},
				),
			})

			if err != nil {
				s.logger.Printf("InsertOrUpdate failed: %v", err)
				return err
			}

			// Broadcast our ACK reply.
			rep := cmd_t{Ctrl: CmdAck, Data: []byte(li.Id)}
			b, _ := json.Marshal(rep)
			res := s.pub.Publish(ctx, &pubsub.Message{Data: b})
			_, err = res.Get(ctx)
			if err != nil {
				s.logger.Printf("Publish (ACK/%v) failed: %v", li.Id, err)
				return err
			}
		case cmd.Ctrl == CmdAck:
			s.logger.Printf("do nothing")
		}
	}

	return nil
}

func (s *Store) getTopic(ctx context.Context, client *pubsub.Client) (*pubsub.Topic, error) {
	topic := client.Topic(s.pubName())
	exists, err := topic.Exists(ctx)
	if err != nil {
		return nil, err
	}

	if !exists {
		return client.CreateTopic(ctx, s.pubName())
	}

	return topic, nil
}

func (s *Store) getSubscription(ctx context.Context, client *pubsub.Client, topic *pubsub.Topic) (*pubsub.Subscription, error) {
	sub := client.Subscription(s.subName())
	exists, err := sub.Exists(ctx)
	if err != nil {
		return nil, err
	}

	if !exists {
		return client.CreateSubscription(ctx, s.subName(), pubsub.SubscriptionConfig{
			Topic:       topic,
			AckDeadline: time.Second * 60,
		})
	}

	return sub, nil
}

func (s *Store) pubName() string { return "dstore-pub-" + s.group }
func (s *Store) subName() string { return "dstore-sub-" + s.id }

// Config is our configuration to New().
type Config struct {
	// Required: the group name this instance belongs to.
	// NOTE: Will also be used as PubSub topic name, so naming conventions apply.
	// See https://cloud.google.com/pubsub/docs/admin#resource_names
	GroupName        string
	Id               string                // optional: this instance's unique id, will generate uuid if empty
	SpannerClient    *spanner.Client       // required: Spanner client, project is implicit, will not use 'PubSubProject'
	SpindleTable     string                // required: table name for *spindle.Lock
	SpindleLockName  string                // optional: "dstorespindlelock" by default
	LogTable         string                // required: table name for the append-only storage
	PubSubProject    string                // optional: project for PubSub operations, get from 'SpannerClient' if empty
	PubSubClientOpts []option.ClientOption // optional: additional opts when creating the PubSub client
	WriteTimeout     int64                 // optional: wait time (in ms) for Put(), default is 5000ms
	Logger           *log.Logger           // optional: can be silenced by `log.New(ioutil.Discard, "", 0)`
}

// New creates an instance of Store.
func New(cfg Config) *Store {
	s := &Store{
		group:            cfg.GroupName,
		id:               cfg.Id,
		spannerClient:    cfg.SpannerClient,
		lockTable:        cfg.SpindleTable,
		lockName:         cfg.SpindleLockName,
		logTable:         cfg.LogTable,
		pubsubProject:    cfg.PubSubProject,
		pubsubClientOpts: cfg.PubSubClientOpts,
		queue:            make(map[string]chan []byte),
		writeTimeout:     cfg.WriteTimeout,
		logger:           cfg.Logger,
	}

	if s.id == "" {
		s.id = uuid.NewString()
	}

	if s.lockName == "" {
		s.lockName = spindleLockName
	}

	if s.pubsubProject == "" {
		db := s.spannerClient.DatabaseName()
		s.pubsubProject = strings.Split(db, "/")[1]
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
