package hedge

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	gaxv2 "github.com/googleapis/gax-go/v2"
)

const (
	db = "projects/test-project/instances/test-instance/databases/testdb"
)

func TestBasic(t *testing.T) {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		t.Error(err)
		return
	}

	defer client.Close()
	op := New(client, ":8080", "locktable", "mylock", "logtable",
		WithLeaderHandler(
			nil,
			func(data interface{}, msg []byte) ([]byte, error) {
				t.Log("[send] received:", string(msg))
				return []byte("send " + string(msg)), nil
			},
		),
		WithBroadcastHandler(
			nil,
			func(data interface{}, msg []byte) ([]byte, error) {
				t.Log("[broadcast/semaphore] received:", string(msg))
				return nil, nil
			},
		),
	)

	done := make(chan error, 1)
	quit, cancel := context.WithCancel(ctx)
	go func() {
		err := op.Run(quit, done)
		if err != nil {
			t.Error(err)
		}
	}()

	var cnt int
	bo := gaxv2.Backoff{
		Initial:    time.Second,
		Max:        time.Second * 30,
		Multiplier: 2,
	}

	for {
		cnt++
		locked, token := op.HasLock()
		switch {
		case locked:
			t.Logf("lock obtained, token=%v", token)
			break
		default:
			t.Log("lock not obtained, retry")
			time.Sleep(bo.Pause())
			continue
		}

		if cnt >= 10 {
			t.Fatalf("can't get lock")
		}

		break
	}

	cancel()
	<-done
}
