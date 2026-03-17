package hedge

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/google/uuid"
)

const (
	db = "projects/test-project/instances/test-instance/databases/testdb"
)

func initSpanner(ctx context.Context, db string) (*spanner.Client, error) {
	parts := strings.Split(db, "/")
	projectID := parts[1]
	instanceID := parts[3]
	databaseID := parts[5]

	instanceAdmin, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return nil, err
	}
	defer instanceAdmin.Close()

	op1, err := instanceAdmin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", projectID),
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", projectID),
			DisplayName: "Test Instance",
			NodeCount:   1,
		},
	})
	if err != nil {
		if !strings.Contains(err.Error(), "AlreadyExists") {
			return nil, err
		}
	} else {
		if _, err := op1.Wait(ctx); err != nil {
			return nil, err
		}
	}

	databaseAdmin, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return nil, err
	}
	defer databaseAdmin.Close()

	b, err := os.ReadFile("testdata/emuddl.sql")
	if err != nil {
		return nil, err
	}

	stmts := []string{}
	for _, stmt := range strings.Split(string(b), ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt != "" {
			stmts = append(stmts, stmt)
		}
	}

	op2, err := databaseAdmin.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseID),
		ExtraStatements: stmts,
	})
	if err != nil {
		if !strings.Contains(err.Error(), "AlreadyExists") {
			return nil, err
		}
	} else {
		if _, err := op2.Wait(ctx); err != nil {
			return nil, err
		}
	}

	return spanner.NewClient(ctx, db)
}

func TestEmulator(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("SPANNER_EMULATOR_HOST not set, skipping emulator tests")
	}

	ctx := context.Background()
	client, err := initSpanner(ctx, db)
	if err != nil {
		t.Fatalf("failed to init spanner: %v", err)
	}

	// Clean up database for idempotency in local testing
	_, _ = client.Apply(ctx, []*spanner.Mutation{
		spanner.Delete("locktable", spanner.AllKeys()),
		spanner.Delete("logtable", spanner.AllKeys()),
	})

	// Create a new hedge instance
	op := New(client, "localhost:12345", "locktable", uuid.NewString(), "logtable")

	// Test basic getters
	if op.HostPort() != "localhost:12345" {
		t.Errorf("expected localhost:12345, got %v", op.HostPort())
	}

	// Start it
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go op.Run(runCtx)

	// Wait for it to become leader, since it's the only one
	// Spindle can take ~10-15s to become leader. Let's wait a bit longer and retry.
	var isLeader bool
	for i := 0; i < 20; i++ {
		isLeader, _ = op.HasLock()
		if isLeader {
			break
		}
		time.Sleep(1 * time.Second)
	}

	if !isLeader {
		t.Fatalf("expected to be leader, but isn't")
	}

	leader, err := op.Leader()
	if err != nil {
		t.Fatalf("Leader err: %v", err)
	}
	if leader != "localhost:12345" {
		t.Fatalf("expected leader localhost:12345, got %v", leader)
	}

	// Test Put and Get
	key := uuid.NewString()
	val := "my-value"
	err = op.Put(ctx, KeyValue{Key: key, Value: val})
	if err != nil {
		t.Fatalf("Put err: %v", err)
	}

	// Since Get reads from Spanner, we can do it immediately
	kvs, err := op.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get err: %v", err)
	}

	if len(kvs) != 1 {
		t.Fatalf("expected 1 kv, got %v", len(kvs))
	}

	if kvs[0].Value != val {
		t.Fatalf("expected value %v, got %v", val, kvs[0].Value)
	}

	// Test Semantic Version (NoAppend flag test)
	val2 := "my-value-2"
	err = op.Put(ctx, KeyValue{Key: key, Value: val2}, PutOptions{NoAppend: true})
	if err != nil {
		t.Fatalf("Put NoAppend err: %v", err)
	}

	kvs, err = op.Get(ctx, key, -1)
	if err != nil {
		t.Fatalf("Get err: %v", err)
	}

	if len(kvs) != 2 {
		t.Fatalf("expected 2 kvs, got %v", len(kvs))
	}

	// Test Semaphore
	sem, err := op.NewSemaphore(ctx, "test-sem", 1)
	if err != nil {
		t.Fatalf("NewSemaphore err: %v", err)
	}

	// Acquire
	err = sem.Acquire(ctx)
	if err != nil {
		t.Fatalf("Semaphore Acquire err: %v", err)
	}

	// Try acquiring again with context timeout should fail
	ctxTimeout, cancelTimeout := context.WithTimeout(ctx, 1*time.Second)
	defer cancelTimeout()
	err = sem.Acquire(ctxTimeout)
	if err == nil {
		t.Fatalf("expected semaphore acquire to fail, but it succeeded")
	}

	// Release
	err = sem.Release(ctx)
	if err != nil {
		t.Fatalf("Semaphore Release err: %v", err)
	}

	// Try acquiring again, should succeed
	// Note: since limit was 1, releasing it caused it to be fully deleted.
	// So we need to re-create it first.
	sem, err = op.NewSemaphore(ctx, "test-sem", 1)
	if err != nil {
		t.Fatalf("NewSemaphore again err: %v", err)
	}
	err = sem.Acquire(ctx)
	if err != nil {
		t.Fatalf("Semaphore Acquire again err: %v", err)
	}
}
