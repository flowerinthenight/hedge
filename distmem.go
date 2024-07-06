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

	pb "github.com/flowerinthenight/hedge/proto/v1"
	"golang.org/x/exp/mmap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	metaName      = "name"
	metaMemLimit  = "mlimit"
	metaDiskLimit = "dlimit"
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
	MemLimit  uint64 // mem limit (bytes) before spill-over
	DiskLimit uint64 // disk limit (bytes) before spill-over
}

type memT struct {
	data  []byte
	mlocs []int
}

type DistMem struct {
	sync.Mutex

	Name string

	op     *Op
	nodes  []uint64          // 0=local, ...=spillover
	meta   map[uint64]*metaT // per-node metadata
	mlimit uint64            // mem limit
	dlimit uint64            // disk limit
	hasher hashT             // for node id
	data   map[uint64]*memT  // mem data
	mlocs  []int             // mem offsets
	dlocs  []int             // disk offsets
	wmtx   *sync.Mutex       // one active writer only
	writer *writerT          // writer object
}

type writerT struct {
	sync.Mutex
	lo   bool // local write only
	dm   *DistMem
	ch   chan []byte
	on   int32
	err  error
	done chan struct{}
}

func (w *writerT) Err() error {
	w.Lock()
	defer w.Unlock()
	return w.err
}

func (w *writerT) Write(data []byte) { w.ch <- data }

func (w *writerT) Close() {
	if atomic.LoadInt32(&w.on) == 0 {
		return
	}

	close(w.ch)
	<-w.done // wait for start()
	atomic.StoreInt32(&w.on, 0)
	w.dm.wmtx.Unlock()
}

func (w *writerT) start() {
	defer func() { w.done <- struct{}{} }()
	atomic.StoreInt32(&w.on, 1)
	ctx := context.Background()
	node := w.dm.nodes[0]
	var file *os.File

	var allCount int
	var memCount int
	var diskCount int
	var netCount int
	var failCount int

	for data := range w.ch {
		allCount++
		var err error
		var nextName string
		msize := atomic.LoadUint64(&w.dm.meta[node].msize)
		mlimit := atomic.LoadUint64(&w.dm.mlimit)
		dsize := atomic.LoadUint64(&w.dm.meta[node].dsize)
		dlimit := atomic.LoadUint64(&w.dm.dlimit)

		// Local (or next hop) is full. Go to the next node.
		if !w.lo && ((msize + dsize) >= (mlimit + dlimit)) {
			nextName, node = w.dm.nextNode()
			if nextName == "" {
				failCount++
				w.Lock()
				w.err = fmt.Errorf("cannot find next node")
				w.Unlock()
				continue
			}

			if atomic.LoadInt32(&w.dm.meta[node].grpc) == 0 {
				err = func() error {
					host, port, _ := net.SplitHostPort(nextName)
					pi, _ := strconv.Atoi(port)
					nextName = net.JoinHostPort(host, fmt.Sprintf("%v", pi+1))

					var opts []grpc.DialOption
					opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
					w.dm.meta[node].conn, err = grpc.NewClient(nextName, opts...)
					if err != nil {
						return fmt.Errorf("NewClient (%v) failed: %w", nextName, err)
					}

					w.dm.meta[node].client = pb.NewHedgeClient(w.dm.meta[node].conn)
					w.dm.meta[node].writer, err = w.dm.meta[node].client.DMemWrite(ctx)
					if err != nil {
						return fmt.Errorf("DMemWrite (%v) failed: %w", nextName, err)
					}

					atomic.AddInt32(&w.dm.meta[node].grpc, 1)
					return nil
				}()

				if err != nil {
					w.Lock()
					w.err = err
					w.Unlock()
				}
			}
		}

		switch {
		case !w.lo && node != w.dm.me():
			netCount++
			err := w.dm.meta[node].writer.Send(&pb.Payload{
				Meta: map[string]string{
					metaName:      w.dm.Name,
					metaMemLimit:  fmt.Sprintf("%v", w.dm.mlimit),
					metaDiskLimit: fmt.Sprintf("%v", w.dm.dlimit),
				},
				Data: data,
			})

			if err != nil {
				w.Lock()
				w.err = fmt.Errorf("Send failed: %w", err)
				w.Unlock()
			}

			atomic.AddUint64(&w.dm.meta[node].msize, uint64(len(data)))
		default:
			if msize < mlimit {
				// Use local memory.
				memCount++
				if _, ok := w.dm.data[node]; !ok {
					w.dm.data[node] = &memT{
						data:  []byte{},
						mlocs: []int{},
					}
				}

				w.dm.data[node].data = append(w.dm.data[node].data, data...)
				w.dm.data[node].mlocs = append(w.dm.data[node].mlocs, len(data))
				atomic.AddUint64(&w.dm.meta[node].msize, uint64(len(data)))
			} else {
				// Use local disk.
				diskCount++
				if file == nil {
					flag := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
					file, err = os.OpenFile(w.dm.localFile(), flag, 0644)
					if err != nil {
						slog.Error("OpenFile failed:", "err", err)
					}
				}

				n, err := file.Write(data)
				if err != nil {
					w.Lock()
					w.err = fmt.Errorf("Write failed: %w", err)
					w.Unlock()
				} else {
					w.dm.dlocs = append(w.dm.dlocs, n)
					atomic.AddUint64(&w.dm.meta[node].dsize, uint64(n))

					if n != len(data) {
						slog.Info("disk:", "me", w.dm.me(), "written", n, "origlen", len(data))
					}
				}
			}
		}
	}

	slog.Info(
		"write:",
		"all", allCount,
		"add", memCount+diskCount+netCount+failCount,
		"mem", memCount,
		"disk", diskCount,
		"net", netCount,
		"fail", failCount,
		"nodes", w.dm.nodes,
	)

	file.Sync()
	file.Close()
	nodes := []uint64{}
	for k := range w.dm.meta {
		nodes = append(nodes, k)
	}

	for _, n := range nodes {
		if w.dm.meta[n].writer != nil {
			w.dm.meta[n].writer.CloseSend()
		}
	}
}

type writerOptions struct {
	LocalOnly bool
}

func (dm *DistMem) Writer(opts ...*writerOptions) (*writerT, error) {
	dm.wmtx.Lock()
	var localOnly bool
	if len(opts) > 0 {
		localOnly = opts[0].LocalOnly
	}

	dm.writer = &writerT{
		lo:   localOnly,
		dm:   dm,
		ch:   make(chan []byte),
		done: make(chan struct{}, 1),
	}

	go dm.writer.start()
	return dm.writer, nil
}

type readerT struct {
	sync.Mutex
	lo   bool // local read only
	dm   *DistMem
	on   int32
	err  error
	done chan struct{}
}

func (r *readerT) Read(out chan []byte) {
	eg := new(errgroup.Group)
	eg.Go(func() error {
		atomic.StoreInt32(&r.on, 1)
		ctx := context.Background()
		for _, node := range r.dm.nodes {
			var err error
			switch {
			case !r.lo && node != r.dm.me():
				func() {
					r.dm.meta[node].reader, err = r.dm.meta[node].client.DMemRead(ctx)
					if err != nil {
						r.Lock()
						r.err = fmt.Errorf("DMemRead failed: %v", err)
						r.Unlock()
						return
					}
				}()

				err = r.dm.meta[node].reader.Send(&pb.Payload{
					Meta: map[string]string{
						metaName:      r.dm.Name,
						metaMemLimit:  fmt.Sprintf("%v", r.dm.mlimit),
						metaDiskLimit: fmt.Sprintf("%v", r.dm.dlimit),
					},
				})

				if err != nil {
					r.Lock()
					r.err = fmt.Errorf("Send failed: %v", err)
					r.Unlock()
					continue
				}

				var n int
				for {
					in, err := r.dm.meta[node].reader.Recv()
					if err == io.EOF {
						break
					}

					if err != nil {
						r.Lock()
						r.err = fmt.Errorf("Recv failed: %v", err)
						r.Unlock()
						break
					}

					out <- in.Data
					n++
				}
			default:
				func() {
					var n, count int
					for _, off := range r.dm.data[node].mlocs {
						out <- r.dm.data[node].data[n : n+off]
						n += off
						count++
					}

					if r.lo {
						slog.Info("via_svc:", "me", r.dm.me(), "memCount", count)
					}
				}()

				if len(r.dm.dlocs) > 0 {
					func() {
						ra, err := mmap.Open(r.dm.localFile())
						if err != nil {
							r.Lock()
							r.err = fmt.Errorf("Open failed: %v", err)
							r.Unlock()
							return
						}

						defer ra.Close()
						var off int64
						var count int
						for _, loc := range r.dm.dlocs {
							b := make([]byte, loc)
							n, err := ra.ReadAt(b, off)
							if err != nil {
								slog.Error("ReadAt failed:", "off", off, "n", n, "err", err)
								r.Lock()
								r.err = fmt.Errorf("ReadAt %v failed: %v", off, err)
								r.Unlock()
							}

							if n != loc {
								slog.Info("read_disk_fail:", "me", r.dm.me(), "n", n, "loc", loc)
							}

							out <- b
							off = off + int64(off)
							count++
						}

						if r.lo {
							slog.Info("via_svc:", "me", r.dm.me(), "diskCount", count)
						}
					}()
				}
			}
		}

		return nil
	})

	eg.Wait()
	close(out)
	r.done <- struct{}{}
}

func (r *readerT) Err() error {
	r.Lock()
	defer r.Unlock()
	return r.err
}

func (r *readerT) Close() {
	if atomic.LoadInt32(&r.on) == 0 {
		return
	}

	<-r.done // wait for loop()
	atomic.StoreInt32(&r.on, 0)
}

type readerOptions struct {
	LocalOnly bool
}

func (dm *DistMem) Reader(opts ...*readerOptions) (*readerT, error) {
	var localOnly bool
	if len(opts) > 0 {
		localOnly = opts[0].LocalOnly
	}

	return &readerT{lo: localOnly, dm: dm, done: make(chan struct{}, 1)}, nil
}

func (dm *DistMem) Clear() {
	dm.Lock()
	defer dm.Unlock()
	nodes := []uint64{}
	for k := range dm.meta {
		nodes = append(nodes, k)
	}

	ctx := context.Background()
	for _, n := range nodes {
		if dm.meta[n].conn != nil {
			dm.meta[n].client.DMemClear(ctx, &pb.Payload{
				Meta: map[string]string{metaName: dm.Name},
			})
		}
	}

	// dm.nodes = []uint64{}
	// dm.meta = make(map[uint64]*metaT)
	// dm.data = map[uint64][][]byte{}
	// dm.locs = []int{}
	// dm.nodes = []uint64{dm.me()} // 0 = local
	// dm.meta[dm.me()] = &metaT{}  // init local
	// os.Remove(dm.localFile())
}

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
		dm.data[nn] = &memT{data: []byte{}, mlocs: []int{}}
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
		Name:  name,
		op:    op,
		meta:  make(map[uint64]*metaT),
		data:  map[uint64]*memT{},
		dlocs: []int{},
		wmtx:  &sync.Mutex{},
	}

	dm.nodes = []uint64{dm.me()}
	dm.meta[dm.me()] = &metaT{}
	dm.data[dm.me()] = &memT{
		data:  []byte{},
		mlocs: []int{},
	}

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
		dm.dlimit = 1 << 30 // 1GB by default
	}

	return dm
}
