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
	cloudevents "github.com/cloudevents/sdk-go/v2"
	lspubsub "github.com/flowerinthenight/longsub/gcppubsub"
	"github.com/flowerinthenight/spindle"
	"github.com/google/uuid"
	"google.golang.org/api/option"
)

const (
	spindleLockName = "dstorespindlelock"
)

var (
	ErrNotRunning = fmt.Errorf("dstore: not running")
)

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

	*spindle.Lock                     // handles our distributed lock
	lockTable     string              // spindle lock table
	lockName      string              // spindle lock name
	logTable      string              // append-only log table
	queue         map[string]struct{} // for tracking outgoing/incoming NATS messages
	writeTimeout  int64               // Put() timeout
	active        int32               // 1=running, 0=off
	sync.Mutex
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

	defer sub.Delete(ctx)
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

// Put saves a key/value to Store.
func (s *Store) Put(ctx context.Context, kv LogItem) error {
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
func (s *Store) Get(ctx context.Context, key string, limit ...int64) ([]LogItem, error) {
	return nil, nil
}

func (s *Store) onRecv(ctx interface{}, data []byte) error {
	s.logger.Println(string(data))
	var e cloudevents.Event
	err := json.Unmarshal(data, &e)
	if err != nil {
		return err
	}

	return nil
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
	GroupName string

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
		queue:            make(map[string]struct{}),
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
