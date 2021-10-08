package hedge

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
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

type Semaphore struct {
	name  string
	limit int
	op    *Op
}

// Acquire a semaphore. This call will block until the semaphore is acquired.
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

func (s *Semaphore) Release(ctx context.Context) error {
	// CmdSemRelease = "SER" // release semaphore, fmt="SER {name} {caller}"
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

type retryer interface {
	CanRetry() bool
}

type retryError struct {
	error
	retry bool
}

func (re retryError) CanRetry() bool { return re.retry }

func createAcquireSemaphoreEntry(ctx context.Context, op *Op, name, caller string, limit int) (bool, error) {
	_, err := op.spannerClient.ReadWriteTransaction(
		ctx,
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
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
			iter1 := txn.Query(ctx, stmt)
			defer iter1.Stop()
			for {
				_, err := iter1.Next()
				if err == iterator.Done || err != nil {
					break
				}

				cnt++
			}

			if cnt > 0 {
				return retryError{fmt.Errorf("already acquired"), false}
			}

			// Next, see if there is still semaphore space.
			sql = `select count(id) id from ` + op.logTable + ` where id = @id`
			stmt = spanner.Statement{
				SQL: sql,
				Params: map[string]interface{}{
					"id": fmt.Sprintf(semNamef, name),
				},
			}

			var free bool
			iter2 := txn.Query(ctx, stmt)
			defer iter2.Stop()
			for {
				row, err := iter2.Next()
				if err == iterator.Done || err != nil {
					break
				}

				var n spanner.NullInt64
				if err := row.Columns(&n); err != nil {
					break
				}

				ncur, err := strconv.Atoi(n.String())
				if err != nil {
					op.logger.Printf("Atoi failed (n=%v): %v", n.String(), err)
				} else {
					free = ncur < limit
				}
			}

			if !free {
				return retryError{fmt.Errorf("currently full, try again"), true}
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

			// _, err := txn.Update(ctx, spanner.Statement{SQL: sql})
			// if err != nil {
			// 	return retryError{err, false}
			// }

			return retryError{nil, false}
		},
	)

	return false, err
}
