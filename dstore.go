package dstore

import (
	"context"
	"fmt"

	"github.com/flowerinthenight/spindle"
	"github.com/nats-io/nats.go"
)

// Storer defines the interface for storage backend for Store.
type Storer interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
}

// Store is our main distributed, append-only log storage object.
type Store struct {
	Id      string        // this instance's unique id
	Locker  *spindle.Lock // handles our distributed lock
	NatsCon *nats.Conn    // our NATS connection
	Storer                // our storage backend
}

// Start starts the main goroutine for *spindle.Lock. It terminates upon
// 'ctx' cancellation, optionally sending a message to 'done' when finished.
func (s *Store) Start(ctx context.Context, done ...chan error) error {
	if s.Locker == nil {
		return fmt.Errorf("Locker cannot be nil")
	}

	if s.Storer == nil {
		return fmt.Errorf("Storer cannot be nil")
	}

	if s.NatsCon == nil {
		return fmt.Errorf("NatsCon cannot be nil")
	}

	return nil
}
