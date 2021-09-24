package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/dstore"
	lspubsub "github.com/flowerinthenight/longsub/gcppubsub"
)

var (
	dbstr        = flag.String("db", "", "fmt: projects/{v}/instances/{v}/databases/{v}")
	group        = flag.String("group", "dstore-demo-group", "group name, common to all instances")
	id           = flag.String("id", os.Getenv("K8S_POD_IP"), "this instance's unique id within the group") // see deployment_template.yaml
	spindleTable = flag.String("spindletable", "testlease", "see https://github.com/flowerinthenight/spindle for more info")
	logTable     = flag.String("logtable", "testdstore_log", "the table for our log data")
)

func ctrl(app interface{}, data []byte) error {
	s := app.(*dstore.Store)
	ctx := context.Background()

	log.Println("recv:", string(data))
	ss := strings.Split(string(data), " ")

	switch strings.ToLower(ss[0]) {
	case "put": // "put <key> <value>"
		if len(ss) < 3 {
			log.Println("invalid msg fmt, should be `put <key> <value>`")
			break
		}

		err := s.Put(ctx, dstore.KeyValue{
			Key:   ss[1],
			Value: ss[2],
		})

		if err != nil {
			log.Println(err)
			break
		}
	case "get": // "get <key>"
		v, err := s.Get(ctx, ss[1])
		if err != nil {
			log.Println(err)
			break
		}

		log.Printf("%+v", v)
	}

	return nil
}

func main() {
	flag.Parse()
	client, err := spanner.NewClient(context.Background(), *dbstr)
	if err != nil {
		log.Println(err)
		return
	}

	s := dstore.New(dstore.Config{
		GroupName:     *group,
		Id:            *id,
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

	project := strings.Split(client.DatabaseName(), "/")[1]
	t, err := lspubsub.GetTopic(project, "dstore-demo-pubctrl")
	if err != nil {
		log.Fatal(err)
	}

	subname := "dstore-demo-subctrl"
	_, err = lspubsub.GetSubscription(project, subname, t)
	if err != nil {
		log.Fatal(err)
	}

	donectl := make(chan error, 1)
	go func() {
		lscmd := lspubsub.NewLengthySubscriber(s, project, subname, ctrl, lspubsub.WithNoExtend(true))
		err := lscmd.Start(context.WithValue(ctx, struct{}{}, nil), donectl)
		if err != nil {
			log.Fatal(err)
		}
	}()

	// Interrupt handler.
	go func() {
		sigch := make(chan os.Signal)
		signal.Notify(sigch, syscall.SIGINT, syscall.SIGTERM)
		log.Printf("signal: %v", <-sigch)
		cancel()
	}()

	<-donectl
	<-done
}
