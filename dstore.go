package dstore

import (
	"context"
	"fmt"

	"github.com/flowerinthenight/spindle"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// Storer defines the interface for storage backend for Store.
type Storer interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
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

type Config struct {
	Id      string // will generate uuid if empty
	NatsCon *nats.Conn
	Lock    *spindle.Lock
	Log     Storer
}

func New(cfg Config) *Store {
	s := &Store{cfg.Id, cfg.NatsCon, cfg.Lock, cfg.Log}
	if s.id == "" {
		s.id = uuid.NewString()
	}

	return s
}
