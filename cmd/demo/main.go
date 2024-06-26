package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/hedge"
	protov1 "github.com/flowerinthenight/hedge/proto/v1"
	"github.com/google/uuid"
)

var (
	dbstr        = flag.String("db", "", "fmt: projects/{v}/instances/{v}/databases/{v}")
	lockName     = flag.String("lockname", "hedge-demo-group", "lock name, common to all instances")
	spindleTable = flag.String("spindletable", "testlease", "see https://github.com/flowerinthenight/spindle for more info")
	logTable     = flag.String("logtable", "", "the table for our log data (optional)")
)

func main() {
	flag.Parse()
	ctx, cancel := context.WithCancel(context.Background())
	client, err := spanner.NewClient(ctx, *dbstr)
	if err != nil {
		slog.Error("NewClient failed:", "err", err)
		return
	}

	defer client.Close()
	ldrIn := make(chan *hedge.StreamMessage)
	ldrOut := make(chan *hedge.StreamMessage)
	go func(_ctx context.Context) {
		for {
			select {
			case <-_ctx.Done():
				return
			case m := <-ldrIn:
				b, _ := json.Marshal(m)
				slog.Info("input stream:", "val", string(b))
				ldrOut <- &hedge.StreamMessage{Payload: &protov1.Payload{Data: []byte("one")}}
				ldrOut <- &hedge.StreamMessage{Payload: &protov1.Payload{Data: []byte("two")}}
				ldrOut <- nil // end
			}
		}
	}(context.WithValue(ctx, struct{}{}, nil))

	bcastIn := make(chan *hedge.StreamMessage)
	bcastOut := make(chan *hedge.StreamMessage)
	host, _ := os.Hostname()
	go func(_ctx context.Context) {
		for {
			select {
			case <-_ctx.Done():
				return
			case m := <-bcastIn:
				slog.Info("input stream:", "val", string(m.Payload.Data))
				bcastOut <- &hedge.StreamMessage{Payload: &protov1.Payload{Data: []byte("1_" + host)}}
				bcastOut <- &hedge.StreamMessage{Payload: &protov1.Payload{Data: []byte("2_" + host)}}
				bcastOut <- nil // end
			}
		}
	}(context.WithValue(ctx, struct{}{}, nil))

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
		hedge.WithLeaderStreamChannels(ldrIn, ldrOut),
		hedge.WithBroadcastStreamChannels(bcastIn, bcastOut),
	)

	log.Println(op)
	done := make(chan error, 1)
	go op.Run(ctx, done)

	mux := http.NewServeMux()
	mux.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		hostname, _ := os.Hostname()
		var key, value string

		// For /put, we expect a fmt: "key value"
		b, _ := io.ReadAll(r.Body)
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
		b, _ := io.ReadAll(r.Body)
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
		b, _ := io.ReadAll(r.Body)
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

	mux.HandleFunc("/streamsend", func(w http.ResponseWriter, r *http.Request) {
		ret, err := op.StreamToLeader(context.Background())
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}

		ret.In <- &hedge.StreamMessage{Payload: &protov1.Payload{Data: []byte("test")}}
		close(ret.In) // we're done with input
		for m := range ret.Out {
			slog.Info("reply:", "out", string(m.Payload.Data))
		}

		w.Write([]byte("OK"))
	})

	mux.HandleFunc("/broadcast", func(w http.ResponseWriter, r *http.Request) {
		hostname, _ := os.Hostname()
		msg := "hello" // default
		b, _ := io.ReadAll(r.Body)
		defer r.Body.Close()
		if len(string(b)) > 0 {
			msg = string(b)
		}

		outs := []string{}
		log.Printf("broadcast %q msg to all...", msg)
		stream := false
		if stream {
			ch := make(chan hedge.BroadcastOutput)
			go op.Broadcast(context.Background(), []byte(msg), hedge.BroadcastArgs{Out: ch})
			for v := range ch {
				if v.Error != nil {
					out := fmt.Sprintf("broadcast: sender=%v, reply=%v", hostname, v.Error.Error())
					outs = append(outs, out)
				} else {
					out := fmt.Sprintf("broadcast: sender=%v, reply=%v", hostname, string(v.Reply))
					outs = append(outs, out)
				}
			}
		} else {
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
		}

		w.Write([]byte(strings.Join(outs, "\n")))
	})

	mux.HandleFunc("/streambroadcast", func(w http.ResponseWriter, r *http.Request) {
		ret, err := op.StreamBroadcast(context.Background())
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}

		ret.In <- &hedge.StreamMessage{Payload: &protov1.Payload{Data: []byte("test")}}
		close(ret.In) // we're done with input

		var wg sync.WaitGroup
		for k, v := range ret.Outs {
			wg.Add(1)
			go func(node string, ch chan *hedge.StreamMessage) {
				defer wg.Done()
				for m := range ch {
					slog.Info("reply:", "node", node, "data", string(m.Payload.Data))
				}
			}(k, v)
		}

		wg.Wait()
		w.Write([]byte("OK"))
	})

	mux.HandleFunc("/distmem", func(w http.ResponseWriter, r *http.Request) {
		defer func(start time.Time) {
			slog.Info("distmem:", "duration", time.Since(start))
		}(time.Now())

		dm := op.NewDistMem("sampledistmem", 50_000_000) // 50MB
		writer, err := dm.Writer()
		if err != nil {
			slog.Error("Writer failed:", "err", err)
			return
		}

		defer writer.Close()
		var n int
		var t time.Duration
		for i := 0; i < 1_000_000; i++ {
			data := fmt.Sprintf("%v_%v_%v", i, uuid.NewString(), uuid.NewString())
			n += len([]byte(data))
			s := time.Now()
			writer.Write([]byte(data))
			t = t + time.Since(s)
		}

		slog.Info("write_lastIndex:", "val", writer.LastIndex(), "n", n, "t", t)

		t = 0
		s := time.Now()

		n = 0
		reader, _ := dm.Reader()
		var oldLastIndex int64
		for {
			if !reader.Next() {
				slog.Info("next done")
				break
			}

			li := reader.LastIndex()
			if (li - oldLastIndex) != 1 {
				slog.Info("wrong index:", "index", li, "old", oldLastIndex)
			}

			data := reader.Read()
			n += len(data)
			oldLastIndex = li
		}

		t = t + time.Since(s)
		slog.Info("read_lastIndex:", "val", reader.LastIndex(), "n", n, "t", t)
		w.Write([]byte("OK"))
	})

	s := &http.Server{Addr: ":9090", Handler: mux}
	go s.ListenAndServe()

	// Interrupt handler.
	go func() {
		sigch := make(chan os.Signal, 1)
		signal.Notify(sigch, syscall.SIGINT, syscall.SIGTERM)
		log.Printf("signal: %v", <-sigch)
		cancel()
	}()

	<-done // wait ctrl+c
	s.Shutdown(ctx)
}
