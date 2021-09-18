package dstore

import (
	"context"
	"fmt"
	"time"

	"github.com/flowerinthenight/spindle"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// KeyValue represents an item in our log.
type KeyValue struct {
	Key       string
	Value     []byte
	Timestamp time.Time // ignored in Put()
}

// Storer defines the interface for storage backend for Store.
type Storer interface {
	// Put saves a key/value to Storer.
	Put(kv KeyValue) error

	// Get a key (or keys) from Storer.
	// 0 (default) = latest only
	// -1 = all (latest to oldest, [0]=latest)
	// -2 = oldest version only
	// >0 = items behind latest; 3 means latest + 2 versions behind, [0]=latest
	Get(key string, limit ...int64) ([]KeyValue, error)
}

// Store is our main distributed, append-only log storage object.
type Store struct {
	id            string     // this instance's unique id
	natsCon       *nats.Conn // our NATS connection
	*spindle.Lock            // handles our distributed lock
	Storer                   // our storage backend
}

// Start starts the main goroutine for *spindle.Lock. It terminates upon
// 'ctx' cancellation, optionally sending a message to 'done' when finished.
func (s *Store) Start(ctx context.Context, done ...chan error) error {
	if s.natsCon == nil {
		return fmt.Errorf("NatsCon cannot be nil")
	}

	if s.Lock == nil {
		return fmt.Errorf("Lock cannot be nil")
	}

	if s.Storer == nil {
		return fmt.Errorf("Storer cannot be nil")
	}

	return nil
}

// Config is our configuration to New().
type Config struct {
	Id      string        // will generate uuid if empty
	NatsCon *nats.Conn    // NATS connection for communication
	Lock    *spindle.Lock // distributed locker
	Log     Storer        // append-only log
}

// New creates an instance of Store.
func New(cfg Config) *Store {
	s := &Store{cfg.Id, cfg.NatsCon, cfg.Lock, cfg.Log}
	if s.id == "" {
		s.id = uuid.NewString()
	}

	return s
}
