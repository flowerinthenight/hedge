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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/hedge"
	pb "github.com/flowerinthenight/hedge/proto/v1"
	"github.com/google/uuid"
	"golang.org/x/exp/mmap"
	"golang.org/x/sync/errgroup"
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
				ldrOut <- &hedge.StreamMessage{Payload: &pb.Payload{Data: []byte("one")}}
				ldrOut <- &hedge.StreamMessage{Payload: &pb.Payload{Data: []byte("two")}}
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
				bcastOut <- &hedge.StreamMessage{Payload: &pb.Payload{Data: []byte("1_" + host)}}
				bcastOut <- &hedge.StreamMessage{Payload: &pb.Payload{Data: []byte("2_" + host)}}
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

		ret.In <- &hedge.StreamMessage{Payload: &pb.Payload{Data: []byte("test")}}
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

		ret.In <- &hedge.StreamMessage{Payload: &pb.Payload{Data: []byte("test")}}
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

	mux.HandleFunc("/sos", func(w http.ResponseWriter, r *http.Request) {
		defer func(start time.Time) {
			slog.Info("distmem:", "duration", time.Since(start))
		}(time.Now())

		name := "distmem_" + time.Now().Format(time.RFC3339)
		rname := r.URL.Query().Get("name")
		if rname != "" {
			name = rname
		}

		slog.Info("start distmem:", "name", name)
		limit := 14_000 // 4 pods, all
		// limit := 2_500

		sos := func() *hedge.SoS {
			sos := op.NewSoS(name, &hedge.SoSOptions{
				MemLimit:   150_000,
				DiskLimit:  120_000,
				Expiration: 30,
			})

			writer, err := sos.Writer()
			if err != nil {
				slog.Error("Writer failed:", "err", err)
				return nil
			}

			defer writer.Close()
			var n int
			for i := 0; i < limit; i++ {
				data := fmt.Sprintf("2_%v_%v", uuid.NewString(), time.Now().Format(time.RFC3339))
				n += len([]byte(data))
				writer.Write([]byte(data))
			}

			slog.Info("write_dm:", "i", limit, "n", n, "write_err", writer.Err())
			return sos
		}()

		if sos == nil {
			slog.Error("failed in creating SoS object")
			return
		}

		func() {
			reader, err := sos.Reader()
			if err != nil {
				slog.Error(err.Error())
				return
			}

			out := make(chan []byte)
			eg := new(errgroup.Group)
			eg.Go(func() error {
				var i, n, total int
				for d := range out {
					ss := strings.Split(string(d), "_")
					if len(ss) != 3 {
						slog.Error("bad fmt:", "len", len(ss))
						continue
					}

					t, err := strconv.Atoi(ss[0])
					if err != nil {
						slog.Error("Atoi failed:", "err", err)
						continue
					}

					total += t
					_, err = time.Parse(time.RFC3339, ss[2])
					if err != nil {
						slog.Error("Parse failed:", "err", err)
						continue
					}

					n += len(d)
					i++
				}

				slog.Info("read_dm:", "i", i, "n", n, "total", total)
				return nil
			})

			reader.Read(out)
			eg.Wait()
			reader.Close()
			slog.Info("read_dm:", "read_err", reader.Err())
		}()

		sos.Close()
		w.Write([]byte("OK"))
	})

	mux.HandleFunc("/soslocal", func(w http.ResponseWriter, r *http.Request) {
		defer func(start time.Time) {
			slog.Info("distmem:", "duration", time.Since(start))
		}(time.Now())

		type kcT struct {
			Key           string  `json:"key"`
			TrueUnblended float64 `json:"trueUnblended"`
			Unblended     float64 `json:"unblended"`
			Usage         float64 `json:"usage"`
		}

		locs, _ := os.ReadFile("readlocs")
		ss := strings.Split(string(locs), " ")

		ra, err := mmap.Open("readdata")
		if err != nil {
			slog.Error(err.Error())
			return
		}

		defer ra.Close()

		name := "distmem_" + time.Now().Format(time.RFC3339)
		rname := r.URL.Query().Get("name")
		if rname != "" {
			name = rname
		}

		slog.Info("start distmem:", "name", name)

		sos := func() *hedge.SoS {
			sos := op.NewSoS(name, &hedge.SoSOptions{
				MemLimit:   100_000,
				Expiration: 30,
			})

			writer, err := sos.Writer()
			if err != nil {
				slog.Error("Writer failed:", "err", err)
				return nil
			}

			var i, wt int
			var off int64
			locs := []int{}
			for _, sloc := range ss {
				i++
				loc, _ := strconv.ParseInt(sloc, 10, 64)
				locs = append(locs, int(loc))
				b := make([]byte, loc)
				n, err := ra.ReadAt(b, off)
				if err != nil {
					slog.Error(err.Error())
					break
				}

				var kc kcT
				err = json.Unmarshal(b, &kc)
				if err != nil {
					slog.Error(err.Error())
					break
				}

				if int64(n) != loc {
					slog.Error("not equal:", "n", n, "loc", loc)
				}

				off = off + int64(n)
				wt += n
				writer.Write(b)
			}

			writer.Close()
			slog.Info("total_write:",
				"val", wt,
				"err", writer.Err(),
			)

			return sos
		}()

		func() {
			reader, _ := sos.Reader()
			out := make(chan []byte)
			eg := new(errgroup.Group)
			eg.Go(func() error {
				var print int
				var i, rt int
				for d := range out {
					i++
					var kc kcT
					err = json.Unmarshal(d, &kc)
					if err != nil {
						if print < 2 {
							slog.Error(err.Error(), "i", i, "raw", string(d))
							print++
						}

						continue
					}

					rt += len(d)
				}

				slog.Info("total_read:", "val", rt)
				return nil
			})

			reader.Read(out)
			eg.Wait()
			reader.Close()
			slog.Info("read_dm:", "read_err", reader.Err())
		}()

		sos.Close()
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
