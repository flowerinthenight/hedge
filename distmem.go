package hedge

import (
	"context"
	"fmt"
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
	conn   *grpc.ClientConn
	client pb.HedgeClient
	stream pb.Hedge_DMemWriteClient
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
	writer *writerT
	hasher hashT
	data   map[uint64][][]byte
}

type writerT struct {
	lo  bool // local write only
	dm  *DistMem
	ch  chan []byte
	off int32
}

func (w *writerT) Write(data []byte) { w.ch <- data }

func (w *writerT) Close() {
	if atomic.LoadInt32(&w.off) > 0 {
		return
	}

	close(w.ch)
	atomic.AddInt32(&w.off, 1)
}

func (w *writerT) start() {
	go func() {
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

				if w.dm.meta[node].conn == nil {
					host, port, _ := net.SplitHostPort(nextName)
					pi, _ := strconv.Atoi(port)
					nextName = net.JoinHostPort(host, fmt.Sprintf("%v", pi+1))

					var opts []grpc.DialOption
					opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
					w.dm.meta[node].conn, err = grpc.NewClient(nextName, opts...)
					if err != nil {
						w.dm.op.logger.Println("NewClient failed:", nextName, err)
					}

					w.dm.meta[node].client = pb.NewHedgeClient(w.dm.meta[node].conn)
					w.dm.meta[node].stream, err = w.dm.meta[node].client.DMemWrite(ctx)
					if err != nil {
						w.dm.op.logger.Println("DMemWrite failed:", nextName, err)
					}
				}
			}

			switch {
			case !w.lo && node != w.dm.me():
				spillCount++
				err := w.dm.meta[node].stream.Send(&pb.Payload{
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
			var size int
			for _, j := range v {
				size += len(j)
			}

			w.dm.op.logger.Println(k, "sliceLen", len(v), "accumLen:", size)
		}

		w.dm.op.logger.Println(
			"all:", all,
			"add:", normCount+spillCount+noSpaceCount,
			"norm:", normCount,
			"spill:", spillCount,
			"noSpace:", noSpaceCount,
			"nodes:", w.dm.nodes,
			"meta:", w.dm.meta,
		)

		for _, v := range w.dm.meta {
			if v.conn != nil {
				v.stream.CloseSend()
				v.conn.Close()
			}
		}
	}()
}

type writerOptionsT struct {
	LocalOnly bool
}

func (dm *DistMem) Writer(opts ...*writerOptionsT) (*writerT, error) {
	ref := func() bool {
		dm.Lock()
		ok := dm.writer != nil
		dm.Unlock()
		return ok
	}()

	if ref {
		return nil, fmt.Errorf("can only have one writer")
	}

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

type readerT struct {
	li int64
	dm *DistMem
	w  *writerT
}

// func (r *readerT) Next() bool {
// 	ni := atomic.AddInt64(&r.li, 1)
// 	return ni <= atomic.LoadInt64(&r.w.li)
// }

// func (r *readerT) Read() []byte {
// 	node := r.dm.nodes[0]
// 	i := atomic.LoadInt64(&r.li)
// 	if _, ok1 := r.dm.data[node]; ok1 {
// 		if _, ok2 := r.dm.data[node][i]; ok2 {
// 			return r.dm.data[node][i]
// 		}
// 	}

// 	return nil
// }

// func (r *readerT) LastIndex() int64 { return atomic.LoadInt64(&r.li) }

// func (dm *DistMem) Reader() (*readerT, error) {
// 	r := &readerT{
// 		li: -1,
// 		dm: dm,
// 		w:  dm.writer,
// 	}

// 	return r, nil
// }

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
		Name: fmt.Sprintf("%v/%v", op.Name(), name),
		op:   op,
		meta: make(map[uint64]*metaT),
		data: map[uint64][][]byte{},
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
