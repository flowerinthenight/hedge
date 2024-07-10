package hedge

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

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
	metaExpire    = "expire"
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
	MemLimit   uint64 // mem limit (bytes) before spill-over
	DiskLimit  uint64 // disk limit (bytes) before spill-over
	Expiration int64  // expiration in seconds, default 1hr
}

type memT struct {
	data  []byte
	mlocs []int
}

// DistMem represents an object for distributed memory read/writes.
type DistMem struct {
	sync.Mutex
	Name string

	op     *Op
	nodes  []uint64          // 0=local, 1..n=network
	meta   map[uint64]*metaT // per-node metadata, key=node
	mlimit uint64            // mem limit
	dlimit uint64            // disk limit
	hasher hashT             // for node id
	data   map[uint64]*memT  // mem data , key=node
	dlocs  []int             // disk offsets
	mlock  *sync.Mutex       // local mem lock
	dlock  *sync.Mutex       // local file lock
	wmtx   *sync.Mutex       // one active writer only
	writer *Writer           // writer object
	wrefs  int64             // writer reference count
	rrefs  int64             // reader reference count
	on     int32

	age   time.Duration
	start time.Time
}

type Writer struct {
	sync.Mutex
	lo   bool // local write only
	dm   *DistMem
	ch   chan []byte
	on   int32
	err  error
	done chan struct{}
}

// Err returns the last recorded error during the write operation.
func (w *Writer) Err() error {
	w.Lock()
	defer w.Unlock()
	return w.err
}

// Write writes data to the underlying storage.
func (w *Writer) Write(data []byte) { w.ch <- data }

// Close closes the writer object.
func (w *Writer) Close() {
	if atomic.LoadInt32(&w.on) == 0 {
		return
	}

	close(w.ch)
	<-w.done // wait for start()
	atomic.StoreInt32(&w.on, 0)
	atomic.AddInt64(&w.dm.wrefs, -1)
	w.dm.wmtx.Unlock()
}

func (w *Writer) start() {
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

	var mlock bool
	var dlock bool

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
					metaExpire:    fmt.Sprintf("%v", int64(w.dm.age.Seconds())),
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
				memCount++
				if !mlock {
					w.dm.mlock.Lock()
					mlock = true
				}

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
				diskCount++
				if !dlock {
					w.dm.dlock.Lock()
					dlock = true
				}

				if file == nil {
					flag := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
					file, err = os.OpenFile(w.dm.localFile(), flag, 0644)
					if err != nil {
						w.dm.op.logger.Println("OpenFile failed:", err)
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
				}
			}
		}
	}

	// slog.Info(
	// 	"write:",
	// 	"all", allCount,
	// 	"add", memCount+diskCount+netCount+failCount,
	// 	"mem", memCount,
	// 	"disk", diskCount,
	// 	"net", netCount,
	// 	"fail", failCount,
	// 	"nodes", w.dm.nodes,
	// )

	if mlock {
		w.dm.mlock.Unlock()
	}

	file.Sync()
	file.Close()
	if dlock {
		w.dm.dlock.Unlock()
	}

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

// Writer returns a writer object for writing data to DistMem. The
// caller needs to call writer.Close() after use. Options is only
// used internally, not exposed to callers.
func (dm *DistMem) Writer(opts ...*writerOptions) (*Writer, error) {
	dm.wmtx.Lock()
	var localOnly bool
	if len(opts) > 0 {
		localOnly = opts[0].LocalOnly
	}

	dm.writer = &Writer{
		lo:   localOnly,
		dm:   dm,
		ch:   make(chan []byte),
		done: make(chan struct{}, 1),
	}

	go dm.writer.start()
	atomic.AddInt64(&dm.wrefs, 1)
	return dm.writer, nil
}

type Reader struct {
	sync.Mutex
	lo   bool // local read only
	dm   *DistMem
	on   int32
	err  error
	done chan struct{}
}

// Read reads the underlying data and streams them to the `out` channel.
func (r *Reader) Read(out chan []byte) {
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
						metaExpire:    fmt.Sprintf("%v", int64(r.dm.age.Seconds())),
					},
				})

				if err != nil {
					r.Lock()
					r.err = fmt.Errorf("Send failed: %v", err)
					r.Unlock()
					continue
				}

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
				}
			default:
				func() {
					r.dm.mlock.Lock()
					defer r.dm.mlock.Unlock()
					var n int
					for _, off := range r.dm.data[node].mlocs {
						out <- r.dm.data[node].data[n : n+off]
						n += off
					}
				}()

				func() {
					r.dm.dlock.Lock()
					defer r.dm.dlock.Unlock()
					if len(r.dm.dlocs) == 0 {
						return
					}

					ra, err := mmap.Open(r.dm.localFile())
					if err != nil {
						r.Lock()
						r.err = fmt.Errorf("Open failed: %v", err)
						r.Unlock()
						return
					}

					defer ra.Close()
					var off int64
					for _, loc := range r.dm.dlocs {
						buf := make([]byte, loc)
						n, err := ra.ReadAt(buf, off)
						if err != nil {
							r.Lock()
							r.err = fmt.Errorf("ReadAt failed: %v", err)
							r.Unlock()
						}

						out <- buf
						off = off + int64(n)
					}
				}()
			}
		}

		return nil
	})

	eg.Wait()
	close(out)
	r.done <- struct{}{}
}

// Err returns the last recorded error, if any, during the read operation.
func (r *Reader) Err() error {
	r.Lock()
	defer r.Unlock()
	return r.err
}

// Close closes the reader object.
func (r *Reader) Close() {
	if atomic.LoadInt32(&r.on) == 0 {
		return
	}

	<-r.done // wait for loop()
	atomic.AddInt64(&r.dm.rrefs, -1)
	atomic.StoreInt32(&r.on, 0)
}

type readerOptions struct {
	LocalOnly bool
}

// Reader returns a reader object for reading data from DistMem.
// The caller needs to call reader.Close() after use. Options is
// only used internally, not exposed to callers.
func (dm *DistMem) Reader(opts ...*readerOptions) (*Reader, error) {
	var localOnly bool
	if len(opts) > 0 {
		localOnly = opts[0].LocalOnly
	}

	reader := &Reader{
		lo:   localOnly,
		dm:   dm,
		done: make(chan struct{}, 1),
	}

	atomic.AddInt64(&dm.rrefs, 1)
	return reader, nil
}

// Close closes the DistMem object.
func (dm *DistMem) Close() {
	if atomic.LoadInt32(&dm.on) == 0 {
		return
	}

	dm.Lock()
	defer dm.Unlock()
	nodes := []uint64{}
	for k := range dm.meta {
		nodes = append(nodes, k)
	}

	ctx := context.Background()
	for _, n := range nodes {
		if dm.meta[n].conn != nil {
			dm.meta[n].client.DMemClose(ctx, &pb.Payload{
				Meta: map[string]string{metaName: dm.Name},
			})
		}
	}

	atomic.StoreInt32(&dm.on, 0)
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

func (dm *DistMem) cleaner() {
	eg := new(errgroup.Group)
	eg.Go(func() error {
		started := dm.start
		for {
			time.Sleep(time.Second * 5)
			wrefs := atomic.LoadInt64(&dm.wrefs)
			rrefs := atomic.LoadInt64(&dm.rrefs)
			if (wrefs + rrefs) > 0 {
				started = time.Now()
				continue
			}

			if time.Since(started) > dm.age {
				func() {
					// Cleanup memory area:
					dm.op.dms[dm.Name].mlock.Lock()
					dm.op.dms[dm.Name].mlock.Unlock()
					for _, node := range dm.op.dms[dm.Name].nodes {
						dm.op.dms[dm.Name].data[node].data = []byte{}
					}
				}()

				// Cleanup disk area:
				dm.op.dms[dm.Name].dlock.Lock()
				os.Remove(dm.localFile())
				dm.op.dms[dm.Name].dlock.Unlock()

				// Remove the main map entry:
				dm.op.dmsLock.Lock()
				delete(dm.op.dms, dm.Name)
				dm.op.dmsLock.Unlock()
				break
			}
		}

		return nil
	})

	eg.Wait()
}

func newDistMem(name string, op *Op, opts ...*DistMemOptions) *DistMem {
	dm := &DistMem{
		Name:  name,
		op:    op,
		meta:  make(map[uint64]*metaT),
		data:  map[uint64]*memT{},
		dlocs: []int{},
		mlock: &sync.Mutex{},
		dlock: &sync.Mutex{},
		wmtx:  &sync.Mutex{},
	}

	atomic.StoreInt32(&dm.on, 1)
	dm.nodes = []uint64{dm.me()}
	dm.meta[dm.me()] = &metaT{}
	dm.data[dm.me()] = &memT{
		data:  []byte{},
		mlocs: []int{},
	}

	if len(opts) > 0 {
		dm.mlimit = opts[0].MemLimit
		dm.dlimit = opts[0].DiskLimit
		if opts[0].Expiration > 0 {
			dm.age = time.Second * time.Duration(opts[0].Expiration)
		}
	}

	if dm.mlimit == 0 {
		si := syscall.Sysinfo_t{}
		syscall.Sysinfo(&si)
		dm.mlimit = si.Freeram / 2 // half of free mem
	}

	if dm.dlimit == 0 {
		dm.dlimit = 1 << 30 // 1GB by default
	}

	if dm.age == 0 {
		dm.age = time.Hour * 2
	}

	dm.start = time.Now()
	go dm.cleaner()
	return dm
}
