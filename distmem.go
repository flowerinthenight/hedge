package hedge

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	pb "github.com/flowerinthenight/hedge/proto/v1"
	"golang.org/x/exp/mmap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	metaName  = "name"
	metaLimit = "limit"
)

type metaT struct {
	msize  uint64
	dsize  uint64
	grpc   int32
	conn   *grpc.ClientConn
	client pb.HedgeClient
	writer pb.Hedge_DMemWriteClient
	reader pb.Hedge_DMemReadClient
}

type DistMemOptions struct {
	MemLimit  uint64 // size limit (bytes) before spill-over
	DiskLimit uint64 // size limit (bytes) before spill-over
}

type DistMem struct {
	sync.Mutex

	Name string

	op     *Op
	nodes  []uint64            // 0=local, ...=spillover
	meta   map[uint64]*metaT   // per-node metadata
	mlimit uint64              // mem limit
	dlimit uint64              // disk limit
	hasher hashT               // for node id
	data   map[uint64][][]byte // mem data
	locs   []int               // disk lens, local only
	wmtx   *sync.Mutex         // one active writer only
	writer *writerT            // writer object
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
		var tmem, tdisk, tnet time.Duration
		var all int
		var normCount int
		var diskCount int
		var netCount int
		var noSpaceCount int
		node := w.dm.nodes[0]
		var file *os.File
		for data := range w.ch {
			all++
			var err error
			var nextName string
			msize := atomic.LoadUint64(&w.dm.meta[node].msize)
			mlimit := atomic.LoadUint64(&w.dm.mlimit)
			dsize := atomic.LoadUint64(&w.dm.meta[node].dsize)
			dlimit := atomic.LoadUint64(&w.dm.dlimit)

			// Let's go to the next node.
			if !w.lo && msize > 0 && msize >= mlimit && dsize > 0 && dsize >= dlimit {
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
				netCount++
				s := time.Now()
				err := w.dm.meta[node].writer.Send(&pb.Payload{
					Meta: map[string]string{
						metaName:  w.dm.Name,
						metaLimit: fmt.Sprintf("%v", w.dm.mlimit),
					},
					Data: data,
				})

				if err != nil {
					w.dm.op.logger.Println("spillover: Send failed:", err)
				}

				atomic.AddUint64(&w.dm.meta[node].msize, uint64(len(data)))
				tnet += time.Since(s)
			default:
				if msize < mlimit {
					// Use local memory.
					normCount++
					s := time.Now()
					if _, ok := w.dm.data[node]; !ok {
						w.dm.data[node] = [][]byte{}
					}

					w.dm.data[node] = append(w.dm.data[node], data)
					atomic.AddUint64(&w.dm.meta[node].msize, uint64(len(data)))
					tmem += time.Since(s)
				} else {
					// Use local disk.
					diskCount++
					s := time.Now()
					ok := true
					if file == nil {
						flag := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
						file, err = os.OpenFile(w.dm.localFile(), flag, 0644)
						if err != nil {
							ok = false
						}
					}

					if ok {
						n, _ := file.Write(data)
						w.dm.locs = append(w.dm.locs, n)
						atomic.AddUint64(&w.dm.meta[node].dsize, uint64(n))
					}

					tdisk += time.Since(s)
				}
			}
		}

		for k, v := range w.dm.data {
			slog.Info("mem:", "node", k, "sliceLen", len(v))
		}

		file.Close()
		slog.Info("file:", "locs", len(w.dm.locs))

		names := []string{}
		for k := range w.dm.op.dms {
			names = append(names, k)
		}

		slog.Info(
			"counts:",
			"all", all,
			"add", normCount+diskCount+netCount+noSpaceCount,
			"norm", normCount,
			"disk", diskCount,
			"net", netCount,
			"nospace", noSpaceCount,
			"nodes", w.dm.nodes,
			"dms", names,
			"tmem", tmem,
			"tdisk", tdisk,
			"tnet", tnet,
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
		var tmem, tdisk, tnet time.Duration
		for _, node := range r.dm.nodes {
			var err error
			switch {
			case node != r.dm.me():
				s := time.Now()
				func() {
					r.dm.meta[node].reader, err = r.dm.meta[node].client.DMemRead(ctx)
					if err != nil {
						r.dm.op.logger.Println("DMemRead failed:", err)
						return
					}
				}()

				err = r.dm.meta[node].reader.Send(&pb.Payload{
					Meta: map[string]string{
						metaName:  r.dm.Name,
						metaLimit: fmt.Sprintf("%v", r.dm.mlimit),
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

				tnet += time.Since(s)
				slog.Info("from_svc:", "node", node, "n", n)
			default:
				func() {
					s := time.Now()
					for _, d := range r.dm.data[node] {
						out <- d
					}

					tmem += time.Since(s)
				}()

				if len(r.dm.locs) > 0 {
					func() {
						s := time.Now()
						ra, err := mmap.Open(r.dm.localFile())
						if err != nil {
							slog.Error("Open failed:", "err", err)
							return
						}

						defer ra.Close()
						var off int64
						for _, n := range r.dm.locs {
							b := make([]byte, n)
							n, err := ra.ReadAt(b, off)
							if err != nil {
								slog.Error("ReadAt failed:", "err", err)
							}

							out <- b
							off = off + int64(n)
						}

						tdisk += time.Since(s)
					}()
				}
			}
		}

		slog.Info("perf:", "tmem", tmem, "tdisk", tdisk, "tnet", tnet)
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

func (dm *DistMem) localFile() string {
	name1 := fmt.Sprintf("%v", dm.me())
	name2 := dm.hasher.Sum64([]byte(dm.Name))
	return fmt.Sprintf("%v_%v.dat", name1, name2)
}

func newDistMem(name string, op *Op, opts ...*DistMemOptions) *DistMem {
	dm := &DistMem{
		Name: name,
		op:   op,
		meta: make(map[uint64]*metaT),
		data: map[uint64][][]byte{},
		locs: []int{},
		wmtx: &sync.Mutex{},
	}

	dm.nodes = []uint64{dm.me()} // 0 = local
	dm.meta[dm.me()] = &metaT{}  // init local

	if len(opts) > 0 {
		dm.mlimit = opts[0].MemLimit
		dm.dlimit = opts[0].DiskLimit
	}

	if dm.mlimit == 0 {
		si := syscall.Sysinfo_t{}
		syscall.Sysinfo(&si)
		dm.mlimit = si.Freeram / 2 // half of free mem
	}

	if dm.dlimit == 0 {
		dm.dlimit = 2_147_483_648 // 2GB by default
	}

	return dm
}
