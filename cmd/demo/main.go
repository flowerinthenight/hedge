package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/hedge"
)

var (
	dbstr        = flag.String("db", "", "fmt: projects/{v}/instances/{v}/databases/{v}")
	lockName     = flag.String("lockname", "hedge-demo-group", "lock name, common to all instances")
	spindleTable = flag.String("spindletable", "testlease", "see https://github.com/flowerinthenight/spindle for more info")
	logTable     = flag.String("logtable", "testhedge_log", "the table for our log data")
)

func onMessage(app interface{}, data []byte) error {
	op := app.(*hedge.Op)
	ctx := context.Background()

	log.Println("recv:", string(data))
	ss := strings.Split(string(data), " ")

	switch strings.ToLower(ss[0]) {
	case "broadcast": // broadcast <payload>
		if len(ss) < 2 {
			log.Println("invalid msg fmt, should be `broadcast <msg>`")
			break
		}

		vv := op.Broadcast(ctx, []byte(ss[1]))
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
	op := hedge.New(client, ":8080", *spindleTable, *lockName, *logTable,
		hedge.WithGroupSyncInterval(time.Second*5),
		hedge.WithLeaderHandler(
			nil, // since this is nil, 'data' should be 'op'
			func(data interface{}, msg []byte) ([]byte, error) {
				op := data.(*hedge.Op)
				hostname, _ := os.Hostname()
				name := fmt.Sprintf("%v/%v", hostname, op.Name())
				log.Println("[send] received:", string(msg))
				reply := fmt.Sprintf("leader [%v] received the message [%v] on %v",
					name, string(msg), time.Now().Format(time.RFC3339))
				return []byte(reply), nil
			},
		),
		hedge.WithBroadcastHandler(
			nil, // since this is nil, 'data' should be 'op'
			func(data interface{}, msg []byte) ([]byte, error) {
				op := data.(*hedge.Op)
				hostname, _ := os.Hostname()
				name := fmt.Sprintf("%v/%v", hostname, op.Name())
				log.Println("[broadcast] received:", string(msg))
				reply := fmt.Sprintf("node [%v] received the broadcast message [%v] on %v",
					name, string(msg), time.Now().Format(time.RFC3339))
				return []byte(reply), nil

				// log.Println("[broadcast/semaphore] received:", string(msg))
				// ss := strings.Split(string(msg), " ")
				// name, slimit := ss[0], ss[1]
				// limit, err := strconv.Atoi(slimit)
				// if err != nil {
				// 	log.Println("invalid limit:", err)
				// 	return nil, err
				// }

				// go func() {
				// 	op := data.(*hedge.Op)
				// 	min, max := 10, 30
				// 	tm := rand.Intn(max-min+1) + min
				// 	s, err := op.NewSemaphore(context.Background(), name, limit)
				// 	if err != nil {
				// 		log.Println("NewSemaphore failed:", err)
				// 		return
				// 	}

				// 	err = s.Acquire(context.Background())
				// 	if err != nil {
				// 		log.Println("Acquire failed:", err)
				// 		return
				// 	}

				// 	log.Printf("semaphore acquired! simulate work for %vs, id=%v", tm, op.HostPort())
				// 	time.Sleep(time.Second * time.Duration(tm))

				// 	log.Printf("release semaphore, id=%v", op.HostPort())
				// 	s.Release(context.Background())
				// }()

				// return nil, nil
			},
		),
	)

	log.Println(op)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go op.Run(ctx, done)

	mux := http.NewServeMux()
	mux.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		hostname, _ := os.Hostname()
		var key, value string

		// For /put, we expect a fmt: "key value"
		b, _ := ioutil.ReadAll(r.Body)
		defer r.Body.Close()
		if len(string(b)) > 0 {
			ss := strings.Split(string(b), " ")
			if len(ss) < 2 {
				w.Write([]byte("invalid msg format"))
				return
			}

			key = ss[0]
			value = strings.Join(ss[1:], " ")
		}

		if key == "" || value == "" {
			w.Write([]byte("invalid msg format"))
			return
		}

		err := op.Put(ctx, hedge.KeyValue{Key: key, Value: value})
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}

		out := fmt.Sprintf("put: sender=%v, key=%v, value=%v", hostname, key, value)
		w.Write([]byte(out))
	})

	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		hostname, _ := os.Hostname()
		b, _ := ioutil.ReadAll(r.Body)
		defer r.Body.Close()
		v, err := op.Get(ctx, string(b))
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}

		out := fmt.Sprintf("get: sender=%v, key=%v, value=%+v", hostname, string(b), v)
		w.Write([]byte(out))
	})

	mux.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {
		hostname, _ := os.Hostname()
		msg := "hello" // default
		b, _ := ioutil.ReadAll(r.Body)
		defer r.Body.Close()
		if len(string(b)) > 0 {
			msg = string(b)
		}

		log.Printf("sending %q msg to leader...", msg)
		v, err := op.Send(context.Background(), []byte(msg))
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}

		log.Printf("reply: %v", string(v))
		out := fmt.Sprintf("sender=%v, reply=%v", hostname, string(v))
		w.Write([]byte(out))
	})

	mux.HandleFunc("/broadcast", func(w http.ResponseWriter, r *http.Request) {
		hostname, _ := os.Hostname()
		msg := "hello" // default
		b, _ := ioutil.ReadAll(r.Body)
		defer r.Body.Close()
		if len(string(b)) > 0 {
			msg = string(b)
		}

		outs := []string{}
		log.Printf("broadcast %q msg to all...", msg)
		vv := op.Broadcast(context.Background(), []byte(msg))
		for _, v := range vv {
			if v.Error != nil {
				out := fmt.Sprintf("broadcast: sender=%v, reply=%v", hostname, v.Error.Error())
				outs = append(outs, out)
			} else {
				out := fmt.Sprintf("broadcast: sender=%v, reply=%v", hostname, string(v.Reply))
				outs = append(outs, out)
			}
		}

		w.Write([]byte(strings.Join(outs, "\n")))
	})

	s := &http.Server{Addr: ":8081", Handler: mux}
	go s.ListenAndServe()

	// Interrupt handler.
	go func() {
		sigch := make(chan os.Signal)
		signal.Notify(sigch, syscall.SIGINT, syscall.SIGTERM)
		log.Printf("signal: %v", <-sigch)
		cancel()
	}()

	<-done // wait ctrl+c
	s.Shutdown(ctx)
}
