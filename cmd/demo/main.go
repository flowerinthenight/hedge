package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

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
	case "semaphore": // semaphore <name> <limit>
		// Whoever receives this msg will do a broadcast to all nodes, which in turn
		// will attempt to acquire the semaphore <name>.
		if len(ss) != 3 {
			log.Println("invalid msg fmt, should be `semaphore <name> <limit>`")
			break
		}

		msg := fmt.Sprintf("%v %v", ss[1], ss[2])
		op.Broadcast(context.Background(), []byte(msg))
	}

	return nil
}

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
	client, err := spanner.NewClient(context.Background(), *dbstr)
	if err != nil {
		log.Println(err)
		return
	}

	defer client.Close()
	xdata := "some arbitrary data"
	op := hedge.New(client, *id+":8080", *spindleTable, *lockName, *logTable,
		hedge.WithLeaderHandler(
			xdata, // if you don't need *Op object
			func(data interface{}, msg []byte) ([]byte, error) {
				log.Println("[send] xdata:", data.(string))
				log.Println("[send] received:", string(msg))
				return []byte("send " + string(msg)), nil
			},
		),
		hedge.WithBroadcastHandler(
			nil, // since this is nil, 'data' should be 'op'
			func(data interface{}, msg []byte) ([]byte, error) {
				log.Println("[broadcast/semaphore] received:", string(msg))
				ss := strings.Split(string(msg), " ")
				name, slimit := ss[0], ss[1]
				limit, err := strconv.Atoi(slimit)
				if err != nil {
					log.Println("invalid limit:", err)
					return nil, err
				}

				go func() {
					op := data.(*hedge.Op)
					min, max := 10, 30
					tm := rand.Intn(max-min+1) + min
					s, err := op.NewSemaphore(context.Background(), name, limit)
					if err != nil {
						log.Println("NewSemaphore failed:", err)
						return
					}

					err = s.Acquire(context.Background())
					if err != nil {
						log.Println("Acquire failed:", err)
						return
					}

					log.Printf("semaphore acquired! simulate work for %vs, id=%v", tm, op.HostPost())
					time.Sleep(time.Second * time.Duration(tm))

					log.Printf("release semaphore, id=%v", op.HostPost())
					s.Release(context.Background())
				}()

				return nil, nil
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
