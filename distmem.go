package hedge

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"

	pb "github.com/flowerinthenight/hedge/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	metaName  = "name"
	metaLimit = "limit"
)

type metaT struct {
	size   uint64
	grpc   int32
	conn   *grpc.ClientConn
	client pb.HedgeClient
	writer pb.Hedge_DMemWriteClient
	reader pb.Hedge_DMemReadClient
}

type DistMemOptions struct {
	Limit uint64 // size limit (bytes) before spill-over
}

type DistMem struct {
	sync.Mutex

	Name string

	op     *Op
	nodes  []uint64          // 0=local, ...=spillover
	meta   map[uint64]*metaT // per-node metadata
	limit  uint64
	hasher hashT
	data   map[uint64][][]byte
	wmtx   *sync.Mutex
	writer *writerT
}

type writerT struct {
	lo bool // local write only
	dm *DistMem
	ch chan []byte
	on int32
}

func (w *writerT) Write(data []byte) { w.ch <- data }

func (w *writerT) Close() {
	if atomic.LoadInt32(&w.on) == 0 {
		return
	}

	close(w.ch)
	atomic.StoreInt32(&w.on, 0)
	w.dm.wmtx.Unlock()
}

func (w *writerT) start() {
	go func() {
		atomic.StoreInt32(&w.on, 1)
		ctx := context.Background()
		var all int
		var normCount int
		var spillCount int
		var noSpaceCount int
		node := w.dm.nodes[0]
		for data := range w.ch {
			all++
			var err error
			var nextName string
			size := atomic.LoadUint64(&w.dm.meta[node].size)
			limit := atomic.LoadUint64(&w.dm.limit)
			if !w.lo && size >= limit {
				nextName, node = w.dm.nextNode()
				if nextName == "" {
					noSpaceCount++
					continue
				}

				if atomic.LoadInt32(&w.dm.meta[node].grpc) == 0 {
					func() error {
						host, port, _ := net.SplitHostPort(nextName)
						pi, _ := strconv.Atoi(port)
						nextName = net.JoinHostPort(host, fmt.Sprintf("%v", pi+1))

						var opts []grpc.DialOption
						opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
						w.dm.meta[node].conn, err = grpc.NewClient(nextName, opts...)
						if err != nil {
							w.dm.op.logger.Println("NewClient failed:", nextName, err)
							return err
						}

						w.dm.meta[node].client = pb.NewHedgeClient(w.dm.meta[node].conn)
						w.dm.meta[node].writer, err = w.dm.meta[node].client.DMemWrite(ctx)
						if err != nil {
							w.dm.op.logger.Println("DMemWrite failed:", nextName, err)
							return err
						}

						atomic.AddInt32(&w.dm.meta[node].grpc, 1)
						return nil
					}()
				}
			}

			switch {
			case !w.lo && node != w.dm.me():
				spillCount++
				err := w.dm.meta[node].writer.Send(&pb.Payload{
					Meta: map[string]string{
						metaName:  w.dm.Name,
						metaLimit: fmt.Sprintf("%v", w.dm.limit),
					},
					Data: data,
				})

				if err != nil {
					w.dm.op.logger.Println("spillover: Send failed:", err)
				}

				atomic.AddUint64(&w.dm.meta[node].size, uint64(len(data)))
			default:
				if size < limit {
					normCount++
					if _, ok := w.dm.data[node]; !ok {
						w.dm.data[node] = [][]byte{}
					}

					w.dm.data[node] = append(w.dm.data[node], data)
					atomic.AddUint64(&w.dm.meta[node].size, uint64(len(data)))
				}
			}
		}

		for k, v := range w.dm.data {
			w.dm.op.logger.Println(k, "sliceLen", len(v))
		}

		names := []string{}
		for k := range w.dm.op.dms {
			names = append(names, k)
		}

		w.dm.op.logger.Println(
			"all:", all,
			"add:", normCount+spillCount+noSpaceCount,
			"norm:", normCount,
			"spill:", spillCount,
			"noSpace:", noSpaceCount,
			"nodes:", w.dm.nodes,
			"dms:", names,
		)

		// nodes := []uint64{}
		// for k := range w.dm.meta {
		// 	nodes = append(nodes, k)
		// }

		// for _, n := range nodes {
		// 	atomic.StoreInt32(&w.dm.meta[n].grpc, 0)
		// 	if w.dm.meta[n].writer != nil {
		// 		w.dm.meta[n].writer.CloseSend()
		// 	}

		// 	if w.dm.meta[n].conn != nil {
		// 		w.dm.meta[n].conn.Close()
		// 	}
		// }
	}()
}

type writerOptionsT struct {
	LocalOnly bool
}

func (dm *DistMem) Writer(opts ...*writerOptionsT) (*writerT, error) {
	dm.wmtx.Lock()
	var localOnly bool
	if len(opts) > 0 {
		localOnly = opts[0].LocalOnly
	}

	dm.Lock()
	dm.writer = &writerT{
		lo: localOnly,
		dm: dm,
		ch: make(chan []byte),
	}

	dm.Unlock()
	dm.writer.start()
	return dm.writer, nil
}

type readerT struct{ dm *DistMem }

func (r *readerT) Read(out chan []byte) {
	go func() {
		defer close(out)
		ctx := context.Background()
		for _, node := range r.dm.nodes {
			var err error
			switch {
			case node != r.dm.me():
				func() {
					slog.Info("read: create reader for:", "node", node)
					r.dm.meta[node].reader, err = r.dm.meta[node].client.DMemRead(ctx)
					if err != nil {
						r.dm.op.logger.Println("DMemRead failed:", err)
						return
					}
				}()

				err = r.dm.meta[node].reader.Send(&pb.Payload{
					Meta: map[string]string{
						metaName:  r.dm.Name,
						metaLimit: fmt.Sprintf("%v", r.dm.limit),
					},
				})

				if err != nil {
					r.dm.op.logger.Println("Send failed:", err)
				}

				var n int
				for {
					in, err := r.dm.meta[node].reader.Recv()
					if err == io.EOF {
						break
					}

					if err != nil {
						break
					}

					out <- in.Data
					n++
				}

				slog.Info("from_svc:", "n", n, "node", node)
			default: // local
				for _, d := range r.dm.data[node] {
					out <- d
				}
			}
		}
	}()
}

func (dm *DistMem) Reader() (*readerT, error) { return &readerT{dm: dm}, nil }

func (dm *DistMem) nextNode() (string, uint64) {
	var mb string
	members := dm.op.Members()
	for _, member := range members {
		nn := dm.hasher.Sum64([]byte(member))
		if nn == dm.me() {
			continue
		}

		if _, ok := dm.data[nn]; ok {
			continue
		}

		mb = member
		dm.nodes = append(dm.nodes, nn)
		dm.meta[nn] = &metaT{}
		dm.data[nn] = [][]byte{}
		break
	}

	return mb, dm.nodes[len(dm.nodes)-1]
}

func (dm *DistMem) me() uint64 { return dm.hasher.Sum64([]byte(dm.op.Name())) }

func newDistMem(name string, op *Op, opts ...*DistMemOptions) *DistMem {
	dm := &DistMem{
		Name: name,
		op:   op,
		meta: make(map[uint64]*metaT),
		data: map[uint64][][]byte{},
		wmtx: &sync.Mutex{},
	}

	dm.nodes = []uint64{dm.me()} // 0 = local
	dm.meta[dm.me()] = &metaT{}  // init local

	if len(opts) > 0 {
		dm.limit = opts[0].Limit
	}

	if dm.limit == 0 {
		si := syscall.Sysinfo_t{}
		syscall.Sysinfo(&si)
		dm.limit = si.Freeram / 2 // half of free mem
	}

	return dm
}
