package hedge

import (
	"context"
	"fmt"

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

func (s *Semaphore) Acquire(ctx context.Context, name string) error {
	return nil
}

// Attempt to write the semaphore record. This could fail.
// We will use the current 'logTable' as our semaphore storage.
// Naming: key="hedge/semaphore/{name}", id="limit={v}", value="{caller}"
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
