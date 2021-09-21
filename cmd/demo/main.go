package main

import (
	"context"
	"flag"
	"log"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/dstore"
)

var (
	dbstr = flag.String("db", "", "fmt: projects/{v}/instances/{v}/databases/{v}")
)

func main() {
	flag.Parse()
	db, err := spanner.NewClient(context.Background(), *dbstr)
	if err != nil {
		log.Println(err)
		return
	}

	s := dstore.New(dstore.Config{
		GroupName:     "dstore-demo-group",
		Id:            "dstore-demo-1",
		SpannerClient: db,
		SpindleTable:  "testlease",
		LogTable:      "testdstore_log",
	})

	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx, done)

	time.Sleep(time.Minute * 2)
	cancel()
	<-done
}
