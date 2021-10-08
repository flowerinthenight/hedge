package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/hedge"
	lspubsub "github.com/flowerinthenight/longsub/gcppubsub"
)

var (
	dbstr        = flag.String("db", "", "fmt: projects/{v}/instances/{v}/databases/{v}")
	lockName     = flag.String("lockname", "hedge-demo-group", "lock name, common to all instances")
	id           = flag.String("id", os.Getenv("K8S_POD_IP"), "this instance's unique id within the group") // see deployment_template.yaml
	spindleTable = flag.String("spindletable", "testlease", "see https://github.com/flowerinthenight/spindle for more info")
	logTable     = flag.String("logtable", "testhedge_log", "the table for our log data")
)

func onMessage(app interface{}, data []byte) error {
	op := app.(*hedge.Op)
	ctx := context.Background()

	log.Println("recv:", string(data))
	ss := strings.Split(string(data), " ")

	switch strings.ToLower(ss[0]) {
	case "put": // "put <key> <value>"
		if len(ss) < 3 {
			log.Println("invalid msg fmt, should be `put <key> <value>`")
			break
		}

		err := op.Put(ctx, hedge.KeyValue{
			Key:   ss[1],
			Value: ss[2],
		})

		if err != nil {
			log.Println(err)
			break
		}
	case "get": // "get <key>"
		v, err := op.Get(ctx, ss[1])
		if err != nil {
			log.Println(err)
			break
		}

		b, _ := json.Marshal(v)
		log.Printf("%v", string(b))
	case "send": // send <payload>
		if len(ss) < 2 {
			log.Println("invalid msg fmt, should be `send <msg>`")
			break
		}

		v, err := op.Send(context.Background(), []byte(ss[1]))
		if err != nil {
			log.Println(err)
			break
		}

		log.Printf("reply(send): %v", string(v))
	case "broadcast": // broadcast <payload>
		if len(ss) < 2 {
			log.Println("invalid msg fmt, should be `broadcast <msg>`")
			break
		}

		vv := op.Broadcast(context.Background(), []byte(ss[1]))
		for _, v := range vv {
			log.Printf("reply(broadcast): id=%v, reply=%v, err=%v",
				v.Id, string(v.Reply), v.Error)
		}
	case "create-sem": // create-sem <name> <limit>
		ctx := context.Background()
		if len(ss) != 3 {
			log.Println("invalid msg fmt, should be `create-sem <name> <limit>`")
			break
		}

		lmt, err := strconv.Atoi(ss[2])
		if err != nil {
			log.Println("invalid msg fmt, should be `create-sem <name> <limit>`")
			break
		}

		s, err := op.NewSemaphore(ctx, ss[1], lmt)
		if err != nil {
			log.Printf("NewSemaphore failed: %v", err)
			break
		}

		err = s.Acquire(ctx)
		if err != nil {
			log.Printf("Acquire failed: %v", err)
			break
		}

		log.Printf("semaphore acquired: %v", ss[1])
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

	defer client.Close()
	xdata := "some arbitrary data"
	op := hedge.New(client, *id+":8080", *spindleTable, *lockName, *logTable,
		hedge.WithLeaderHandler(
			xdata,
			func(data interface{}, msg []byte) ([]byte, error) {
				log.Println("[send] xdata:", data.(string))
				log.Println("[send] received:", string(msg))
				return []byte("send " + string(msg)), nil
			},
		),
		hedge.WithBroadcastHandler(
			xdata,
			func(data interface{}, msg []byte) ([]byte, error) {
				log.Println("[broadcast] xdata:", data.(string))
				log.Println("[broadcast] received:", string(msg))
				return []byte("broadcast " + string(msg)), nil
			},
		),
	)

	log.Println(op)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go op.Run(ctx, done)

	project := strings.Split(client.DatabaseName(), "/")[1]
	t, err := lspubsub.GetTopic(project, "hedge-demo-pubctrl")
	if err != nil {
		log.Fatal(err)
	}

	subname := "hedge-demo-subctrl"
	_, err = lspubsub.GetSubscription(project, subname, t)
	if err != nil {
		log.Fatal(err)
	}

	donectl := make(chan error, 1)
	go func() {
		lscmd := lspubsub.NewLengthySubscriber(op, project, subname, onMessage, lspubsub.WithNoExtend(true))
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
