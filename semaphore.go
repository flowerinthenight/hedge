package hedge

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

const (
	semNamef   = "__hedge/semaphore/%v"
	semCallerf = "__caller=%v"
	semLimitf  = "limit=%v"
	markDel    = "delete-on-empty"
)

var (
	ErrSemFull = fmt.Errorf("hedge/semaphore: semaphore full")
)

// Semaphore represents a distributed semaphore object.
type Semaphore struct {
	name  string
	limit int
	op    *Op
}

// Acquire acquires a semaphore. This call will block until the semaphore is acquired.
// By default, this call will basically block forever until the semaphore is acquired
// or until ctx expires or is cancelled.
func (s *Semaphore) Acquire(ctx context.Context) error { return s.acquire(ctx, false) }

// TryAcquire is like Acquire() but will not block until the semaphore is acquired.
// It will only attempt to acquire the semaphore and will return immediately on either
// success or failure, or until ctx expires or is cancelled.
func (s *Semaphore) TryAcquire(ctx context.Context) error { return s.acquire(ctx, true) }

func (s *Semaphore) acquire(ctx context.Context, noretry bool) error {
	subctx := context.WithValue(ctx, struct{}{}, nil)
	first := make(chan struct{}, 1)
	first <- struct{}{} // immediately the first time
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()

	var active atomic.Int32
	acquire := func() (bool, error) { // true means okay to retry
		active.Store(1)
		defer active.Store(0)
		conn, err := s.op.getLeaderConn(ctx)
		if err != nil {
			return true, err
		}

		defer conn.Close()
		var sb strings.Builder
		fmt.Fprintf(&sb, "%s %s %s\n", CmdSemAcquire, s.name, s.op.hostPort)
		reply, err := s.op.send(conn, sb.String())
		if err != nil {
			return false, err
		}

		switch {
		case strings.HasPrefix(reply, CmdAck):
			ss := strings.Split(reply, " ")
			if len(ss) > 1 { // failed
				dec, _ := base64.StdEncoding.DecodeString(ss[1])
				switch {
				case strings.HasPrefix(string(dec), "0:"):
					serr := strings.Replace(string(dec), "0:", "", 1)
					return false, fmt.Errorf(serr)
				case strings.HasPrefix(string(dec), "1:"):
					serr := strings.Replace(string(dec), "1:", "", 1)
					return true, fmt.Errorf(serr)
				default: // shouldn't be the case, hopefully
					return false, fmt.Errorf(string(dec))
				}
			}
		default:
			return false, ErrNotSupported
		}

		return false, nil
	}

	for {
		select {
		case <-subctx.Done():
			return context.Canceled
		case <-first:
		case <-ticker.C:
		}

		if active.Load() == 1 {
			continue
		}

		type acq_t struct {
			retry bool
			err   error
		}

		ch := make(chan acq_t, 1)
		go func() {
			r, e := acquire()
			ch <- acq_t{r, e}
		}()

		ret := <-ch
		switch {
		case ret.err == nil:
			return nil
		default:
			if noretry {
				return ret.err
			} else {
				if ret.retry {
					continue
				} else {
					return ret.err
				}
			}
		}
	}
}

// Release releases a semaphore. Although recommended to release all acquired semaphores, this is still
// a best-effort release as any caller could disappear/crash while holding a semaphore. To remedy this,
// the current leader will attempt to track all semaphore owners and remove the non-responsive ones after
// some delay. A downside of not calling release properly will cause other semaphore acquirers to block
// just a bit longer while leader does the cleanup, whereas calling release will free up space immediately
// allowing other semaphore acquirers to not wait that long.
func (s *Semaphore) Release(ctx context.Context) error {
	conn, err := s.op.getLeaderConn(ctx)
	if err != nil {
		return err
	}

	defer conn.Close()
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s %s %s\n", CmdSemRelease, s.name, s.op.hostPort)
	reply, err := s.op.send(conn, sb.String())
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
	}

	return nil
}

// We will use the current logTable as our semaphore storage.
// Naming convention(s):
//
//	key="hedge/semaphore/{name}", id="limit={v}", value={caller}
func createSemaphoreEntry(ctx context.Context, op *Op, name, caller string, limit int) error {
	_, err := op.spannerClient.ReadWriteTransaction(ctx,
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			var q strings.Builder
			fmt.Fprintf(&q, "insert %s ", op.logTable)
			fmt.Fprintf(&q, "(key, id, value, leader, timestamp) values (")
			fmt.Fprintf(&q, "'%s', ", fmt.Sprintf(semNamef, name))
			fmt.Fprintf(&q, "'%s', ", fmt.Sprintf(semLimitf, limit))
			fmt.Fprintf(&q, "'%s', ", caller)
			fmt.Fprintf(&q, "'%s', ", op.hostPort)
			fmt.Fprintf(&q, "PENDING_COMMIT_TIMESTAMP())")

			_, err := txn.Update(ctx, spanner.Statement{SQL: q.String()})
			return err
		},
	)

	return err
}

func readSemaphoreEntry(ctx context.Context, op *Op, name string) (*LogItem, error) {
	var q strings.Builder
	fmt.Fprintf(&q, "select key, id, value, leader, timestamp ")
	fmt.Fprintf(&q, "from %s ", op.logTable)
	fmt.Fprintf(&q, "where key = @name")

	stmt := spanner.Statement{
		SQL: q.String(),
		Params: map[string]any{
			"name": fmt.Sprintf(semNamef, name),
		},
	}

	iter := op.spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		var v LogItem
		err = row.ToStruct(&v)
		if err != nil {
			return nil, err
		}

		// Should only be one item.
		return &v, nil
	}

	return nil, fmt.Errorf("%v not found", name)
}

func createAcquireSemaphoreEntry(ctx context.Context, op *Op, name, caller string, limit int) (bool, error) {
	// First, see if caller already acquired this semaphore.
	var q strings.Builder
	fmt.Fprintf(&q, "select key, id ")
	fmt.Fprintf(&q, "from %s ", op.logTable)
	fmt.Fprintf(&q, "where key = @key and id = @id")

	stmt := spanner.Statement{
		SQL: q.String(),
		Params: map[string]any{
			"key": fmt.Sprintf(semCallerf, caller),
			"id":  fmt.Sprintf(semNamef, name),
		},
	}

	var cnt int
	iter := op.spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done || err != nil {
			break
		}

		var v LogItem
		err = row.ToStruct(&v)
		if err != nil {
			break
		}

		if v.Key != "" && v.Id != "" {
			cnt++
		}
	}

	if cnt > 0 {
		return false, fmt.Errorf("already acquired")
	}

	var free bool
	_, err := op.spannerClient.ReadWriteTransaction(ctx,
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			getEntriesCount := func() int64 {
				var q strings.Builder
				fmt.Fprintf(&q, "select count(id) id ")
				fmt.Fprintf(&q, "from %s ", op.logTable)
				fmt.Fprintf(&q, "where id = @id")

				stmt := spanner.Statement{
					SQL: q.String(),
					Params: map[string]any{
						"id": fmt.Sprintf(semNamef, name),
					},
				}

				var cnt int64
				iter := txn.Query(ctx, stmt)
				defer iter.Stop()
				for {
					row, err := iter.Next()
					if err == iterator.Done || err != nil {
						break
					}

					if err := row.Columns(&cnt); err != nil {
						break
					}
				}

				return cnt
			}

			// Next, see if there is still semaphore space.
			free = getEntriesCount() < int64(limit)
			if !free {
				return ErrSemFull
			}

			// Finally, create the acquire semaphore entry.
			var q strings.Builder
			fmt.Fprintf(&q, "insert %s", op.logTable)
			fmt.Fprintf(&q, "(key, id, value, leader, timestamp) values (")
			fmt.Fprintf(&q, "'%s', ", fmt.Sprintf(semCallerf, caller))
			fmt.Fprintf(&q, "'%s', ", fmt.Sprintf(semNamef, name))
			fmt.Fprintf(&q, "'%s', ", caller)
			fmt.Fprintf(&q, "'%s', ", op.hostPort)
			fmt.Fprintf(&q, "PENDING_COMMIT_TIMESTAMP())")

			_, err := txn.Update(ctx, spanner.Statement{SQL: q.String()})
			if err != nil {
				return err
			}

			// Finally, we mark this semaphore as full (once). Will be used in release later.
			if getEntriesCount() >= int64(limit) {
				var q strings.Builder
				fmt.Fprintf(&q, "update %s ", op.logTable)
				fmt.Fprintf(&q, "set value = @val where key = @name")

				txn.Update(ctx, spanner.Statement{
					SQL: q.String(),
					Params: map[string]any{
						"val":  markDel,
						"name": fmt.Sprintf(semNamef, name),
					},
				})
			}

			return nil
		},
	)

	switch {
	case err != nil && !free:
		return true, err
	default:
		return false, err
	}
}

func releaseSemaphore(ctx context.Context, op *Op, name, caller, value string, limit int) error {
	_ = limit
	_, err := op.spannerClient.ReadWriteTransaction(ctx,
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			// First, attempt to remove the calling entry.
			var q strings.Builder
			fmt.Fprintf(&q, "delete from %s ", op.logTable)
			fmt.Fprintf(&q, "where key = @key and id = @id")

			txn.Update(ctx, spanner.Statement{ // best-effort, could fail
				SQL: q.String(),
				Params: map[string]any{
					"key": fmt.Sprintf(semCallerf, caller),
					"id":  fmt.Sprintf(semNamef, name),
				},
			})

			// Next, see if there are no more entries.
			q.Reset()
			fmt.Fprintf(&q, "select count(id) id ")
			fmt.Fprintf(&q, "from %s ", op.logTable)
			fmt.Fprintf(&q, "where id = @id")

			stmt := spanner.Statement{
				SQL:    q.String(),
				Params: map[string]any{"id": fmt.Sprintf(semNamef, name)},
			}

			var cnt int64
			iter := txn.Query(ctx, stmt)
			defer iter.Stop()
			for {
				row, err := iter.Next()
				if err == iterator.Done || err != nil {
					break
				}

				if err := row.Columns(&cnt); err != nil {
					break
				}
			}

			if cnt != 0 {
				return nil
			}

			if value == markDel {
				// Finally, if no more entries, let's remove the actual semaphore entry
				// so we can reuse this name, perhaps with a different limit.
				q.Reset()
				fmt.Fprintf(&q, "delete from %s ", op.logTable)
				fmt.Fprintf(&q, "where key = @key")

				txn.Update(ctx, spanner.Statement{
					SQL:    q.String(),
					Params: map[string]any{"key": fmt.Sprintf(semNamef, name)},
				})
			}

			return nil
		},
	)

	return err
}

type ensureT struct {
	sync.Mutex
	m map[string]struct{}
}

func ensureLock() *ensureT { return &ensureT{m: make(map[string]struct{})} }

func (e *ensureT) add(name string) {
	e.Lock()
	defer e.Unlock()
	e.m[name] = struct{}{}
}

func (e *ensureT) del(name string) {
	e.Lock()
	defer e.Unlock()
	delete(e.m, name)
}

func (e *ensureT) exists(name string) bool {
	e.Lock()
	defer e.Unlock()
	_, ok := e.m[name]
	return ok
}

// Triggered during semaphore acquisition; meaning, this is only called when we are leader.
func ensureLiveness(ctx context.Context, op *Op) {
	if op.ensureOn.Load() == 1 {
		return // one checker per leader
	}

	op.ensureOn.Store(1)
	defer op.ensureOn.Store(0)
	op.ensureCtx, op.ensureCancel = context.WithCancel(ctx)

	enlock := ensureLock()
	ensure := func(name string) {
		enlock.add(name)
		defer enlock.del(name)

		var q strings.Builder
		fmt.Fprintf(&q, "select key from %s ", op.logTable)
		fmt.Fprintf(&q, "where id = @id")

		stmt := spanner.Statement{
			SQL: q.String(),
			Params: map[string]any{
				"id": fmt.Sprintf(semNamef, name),
			},
		}

		ids := []string{}
		iter := op.spannerClient.Single().Query(ctx, stmt)
		defer iter.Stop()
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}

			if err != nil {
				break
			}

			var v LogItem
			err = row.ToStruct(&v)
			if err != nil {
				continue
			}

			ids = append(ids, v.Key)
		}

		if len(ids) > 0 {
			todel := make(chan string, len(ids))
			var w sync.WaitGroup
			for _, id := range ids {
				w.Add(1)
				go func(t string) {
					var rmId string
					defer func(rm *string) {
						todel <- *rm
						w.Done()
					}(&rmId)

					timeout := time.Second * 5
					caller := strings.Split(t, "=")[1]
					conn, err := net.DialTimeout("tcp", caller, timeout)
					if err != nil {
						rmId = t // delete this
						return
					}

					var sb strings.Builder
					fmt.Fprintf(&sb, "%s\n", CmdPing)
					r, err := op.send(conn, sb.String())
					if err != nil {
						rmId = t // delete this
						return
					}

					if r != CmdAck {
						rmId = t // delete this
					}
				}(id)
			}

			w.Wait()
			rms := []string{}
			for range ids {
				rm := <-todel
				if rm != "" {
					rms = append(rms, rm)
				}
			}

			if len(rms) > 0 {
				op.logger.Printf("[ensure/sem] delete: %v", rms)
				op.spannerClient.ReadWriteTransaction(ctx,
					func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
						q.Reset()
						fmt.Fprintf(&q, "delete from %s ", op.logTable)
						fmt.Fprintf(&q, "where key in ('%s')", strings.Join(rms, "','"))
						_, err := txn.Update(ctx, spanner.Statement{SQL: q.String()})
						return err
					},
				)
			}

			time.Sleep(time.Second * 5)
		}
	}

	for {
		var name string
		select {
		case <-op.ensureCtx.Done():
			op.ensureDone <- struct{}{}
			return
		case name = <-op.ensureCh:
		}

		if enlock.exists(name) {
			continue
		}

		go ensure(name)
	}
}
