package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/dstore"
)

var (
	dbstr        = flag.String("db", "", "fmt: projects/{v}/instances/{v}/databases/{v}")
	group        = flag.String("group", "dstore-demo-group", "group name, common to all instances")
	spindleTable = flag.String("spindletable", "testlease", "see https://github.com/flowerinthenight/spindle for more info")
	logTable     = flag.String("logtable", "testdstore_log", "the table for our log data")
)

func main() {
	flag.Parse()
	client, err := spanner.NewClient(context.Background(), *dbstr)
	if err != nil {
		log.Println(err)
		return
	}

	s := dstore.New(dstore.Config{
		GroupName:     *group,
		Id:            "dstore-demo-1",
		SpannerClient: client,
		SpindleTable:  *spindleTable,
		LogTable:      *logTable,
	})

	log.Println(s)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		s.Run(ctx, done)
	}()

	// Interrupt handler.
	go func() {
		sigch := make(chan os.Signal)
		signal.Notify(sigch, syscall.SIGINT, syscall.SIGTERM)
		log.Printf("signal: %v", <-sigch)
		cancel()
	}()

	<-done
}
