package hedge

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

const (
	semNamef  = "hedge/semaphore/%v"
	semLimitf = "limit=%v"
)

// Semaphore represents a distributed semaphore object.
type Semaphore struct {
	name  string
	limit int
	op    *Op
}

// Acquire acquires a semaphore. This call will block until the semaphore is acquired.
// By default, this call will basically block forever until the semaphore is acquired.
// To simulate a 'TryAcquire'-like call, you can put a timeout to (or cancel) 'ctx'.
// At least with this, you only block the call until either you get the semaphore or
// 'ctx' expires or is cancelled.
func (s *Semaphore) Acquire(ctx context.Context) error {
	subctx := context.WithValue(ctx, struct{}{}, nil)
	first := make(chan struct{}, 1)
	first <- struct{}{} // immediately the first time
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()

	var active int32
	acquire := func() (bool, error) { // true means okay to retry
		atomic.StoreInt32(&active, 1)
		defer atomic.StoreInt32(&active, 0)
		conn, err := s.op.getLeaderConn(ctx)
		if err != nil {
			return true, err
		}

		defer conn.Close()
		msg := fmt.Sprintf("%v %v %v\n",
			CmdSemAcquire, s.name, s.op.hostPort)

		reply, err := s.op.send(conn, msg)
		if err != nil {
			s.op.logger.Printf("send failed: %v", err)
			return false, err
		}

		s.op.logger.Printf("reply: %v", reply)

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
			return false, fmt.Errorf("unknown reply")
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

		if atomic.LoadInt32(&active) == 1 {
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
			if ret.retry {
				s.op.logger.Printf("retry: %v", ret.err)
				continue
			} else {
				return ret.err
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
	msg := fmt.Sprintf("%v %v %v\n",
		CmdSemRelease, s.name, s.op.hostPort)

	reply, err := s.op.send(conn, msg)
	if err != nil {
		s.op.logger.Printf("send failed: %v", err)
		return err
	}

	s.op.logger.Printf("reply: %v", reply)

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

// We will use the current 'logTable' as our semaphore storage.
// Naming: key="hedge/semaphore/{name}", id="limit={v}", value={caller}
func createSemaphoreEntry(ctx context.Context, op *Op, name, caller string, limit int) error {
	_, err := op.spannerClient.ReadWriteTransaction(
		ctx,
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			sql := `
insert ` + op.logTable + `
  (key, id, value, leader, timestamp)
values (
  '` + fmt.Sprintf(semNamef, name) + `',
  '` + fmt.Sprintf(semLimitf, limit) + `',
  '` + caller + `',
  '` + op.hostPort + `',
  PENDING_COMMIT_TIMESTAMP()
)`

			_, err := txn.Update(ctx, spanner.Statement{SQL: sql})
			return err
		},
	)

	return err
}

func readSemaphoreEntry(ctx context.Context, op *Op, name string) (*LogItem, error) {
	sql := `
select key, id, value, leader, timestamp
from ` + op.logTable + `
where key = @name`

	stmt := spanner.Statement{
		SQL: sql,
		Params: map[string]interface{}{
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
	sql := `select key, id from ` + op.logTable + ` where key = @key and id = @id`
	stmt := spanner.Statement{
		SQL: sql,
		Params: map[string]interface{}{
			"key": fmt.Sprintf("__caller=%v", caller),
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
	_, err := op.spannerClient.ReadWriteTransaction(
		ctx,
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			// Next, see if there is still semaphore space.
			sql := `select count(id) id from ` + op.logTable + ` where id = @id`
			stmt := spanner.Statement{
				SQL: sql,
				Params: map[string]interface{}{
					"id": fmt.Sprintf(semNamef, name),
				},
			}

			iter := txn.Query(ctx, stmt)
			defer iter.Stop()
			for {
				row, err := iter.Next()
				if err == iterator.Done || err != nil {
					break
				}

				var cnt int64
				if err := row.Columns(&cnt); err != nil {
					break
				}

				op.logger.Printf("[acquire] cnt=%v, limit=%v", cnt, limit)
				free = cnt < int64(limit)
			}

			if !free {
				return fmt.Errorf("currently full, try again")
			}

			// Finally, create the acquire semaphore entry.
			sql = `
insert ` + op.logTable + `
  (key, id, value, leader, timestamp)
values (
  '` + fmt.Sprintf("__caller=%v", caller) + `',
  '` + fmt.Sprintf(semNamef, name) + `',
  '` + caller + `',
  '` + op.hostPort + `',
  PENDING_COMMIT_TIMESTAMP())`

			_, err := txn.Update(ctx, spanner.Statement{SQL: sql})
			return err
		},
	)

	switch {
	case err != nil && !free:
		return true, err
	default:
		return false, err
	}
}

func releaseSemaphore(ctx context.Context, op *Op, name, caller string, limit int) error {
	_, err := op.spannerClient.ReadWriteTransaction(
		ctx,
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			// First, attempt to remove the calling entry.
			sql := `delete from ` + op.logTable + ` where key = @key and id = @id`
			stmt := spanner.Statement{
				SQL: sql,
				Params: map[string]interface{}{
					"key": fmt.Sprintf("__caller=%v", caller),
					"id":  fmt.Sprintf(semNamef, name),
				},
			}

			n, err := txn.Update(ctx, stmt) // best-effort, could fail
			op.logger.Printf("[delete/acquire] n=%v, err=%v, sql=%v", n, err, sql)

			// Next, see if there are no more entries.
			sql = `select count(id) id from ` + op.logTable + ` where id = @id`
			stmt = spanner.Statement{
				SQL: sql,
				Params: map[string]interface{}{
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

				op.logger.Printf("[release] cnt=%v, limit=%v", cnt, limit)
			}

			if cnt != 0 {
				return nil
			}

			// Finally, if no more entries, let's remove the actual semaphore entry
			// so we can reuse this name, perhaps with a different limit.
			sql = `delete from ` + op.logTable + ` where key = @key`
			stmt = spanner.Statement{
				SQL: sql,
				Params: map[string]interface{}{
					"key": fmt.Sprintf(semNamef, name),
				},
			}

			n, err = txn.Update(ctx, stmt) // best-effort, could fail
			op.logger.Printf("[delete/semaphore] n=%v, err=%v, sql=%v", n, err, sql)
			return err
		},
	)

	return err
}
