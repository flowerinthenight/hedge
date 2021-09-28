package hedge

import (
	"context"
	"fmt"
)

var (
	ErrNotImplemented = fmt.Errorf("hedge: not implemented")
)

type Semaphore struct {
	name  string
	limit int64
	op    *Op
}

func (s *Semaphore) Acquire(ctx context.Context, name string) error {
	return ErrNotImplemented
}

func (s *Semaphore) TryAcquire(ctx context.Context, name string) error {
	return ErrNotImplemented
}
