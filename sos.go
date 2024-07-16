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

var (
	errNoInit = fmt.Errorf("sos: not properly initialized")
)

type metaT struct {
	msize  atomic.Uint64
	dsize  atomic.Uint64
	grpc   atomic.Int32
	conn   *grpc.ClientConn
	client pb.HedgeClient
	writer pb.Hedge_SoSWriteClient
	reader pb.Hedge_SoSReadClient
}

type SoSOptions struct {
	// MemLimit sets the memory limit in bytes to be used per node.
	MemLimit uint64

	//  DiskLimit sets the disk limit in bytes to be used per node.
	DiskLimit uint64

	// Expiration sets the TTL (time-to-live) of the backing storage.
	// If not set, the default is 1hr.
	Expiration int64
}

type memT struct {
	data  []byte
	mlocs []int
}

// SoS (Spillover-Store) represents an object for spill-over (or stitched)
// storage. Useful for load-process-discard types of data processing. The
// order of storage priority is local memory, local disk, other pod's
// memory, other pod's disk, and so on.
//
// Limitation: At the moment, it's not allowed to reuse a name for SOS
// once it's used and closed within hedge's lifetime.
type SoS struct {
	sync.Mutex

	Name string // the name of this instance

	op     *Op               // cluster coordinator
	nodes  []uint64          // 0=local, 1..n=network
	meta   map[uint64]*metaT // per-node metadata, key=node
	mlimit atomic.Uint64     // mem limit
	dlimit atomic.Uint64     // disk limit
	hasher hashT             // for node id
	data   map[uint64]*memT  // mem data , key=node
	dlocs  []int             // disk offsets
	mlock  *sync.Mutex       // local mem lock
	dlock  *sync.Mutex       // local file lock
	wmtx   *sync.Mutex       // one active writer only
	writer *Writer           // writer object
	wrefs  atomic.Int64      // writer reference count
	rrefs  atomic.Int64      // reader reference count
	on     atomic.Int32

	age   time.Duration
	start time.Time
}

type Writer struct {
	sync.Mutex
	lo   bool // local write only
	sos  *SoS
	ch   chan []byte
	on   atomic.Int32
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
	if w.on.Load() == 0 {
		return
	}

	close(w.ch)
	<-w.done // wait for start()
	w.on.Store(0)
	w.sos.wrefs.Add(-1)
	w.sos.wmtx.Unlock()
}

func (w *Writer) start() {
	defer func() { w.done <- struct{}{} }()
	w.on.Store(1)
	ctx := context.Background()
	node := w.sos.nodes[0]
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
		msize := w.sos.meta[node].msize.Load()
		mlimit := w.sos.mlimit.Load()
		dsize := w.sos.meta[node].dsize.Load()
		dlimit := w.sos.dlimit.Load()

		// Local (or next hop) is full. Go to the next node.
		if !w.lo && ((msize + dsize) >= (mlimit + dlimit)) {
			nextName, node = w.sos.nextNode()
			if nextName == "" {
				failCount++
				w.Lock()
				w.err = fmt.Errorf("cannot find next node")
				w.Unlock()
				continue
			}

			if w.sos.meta[node].grpc.Load() == 0 {
				err = func() error {
					host, port, _ := net.SplitHostPort(nextName)
					pi, _ := strconv.Atoi(port)
					nextName = net.JoinHostPort(host, fmt.Sprintf("%v", pi+1))

					var opts []grpc.DialOption
					opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
					w.sos.meta[node].conn, err = grpc.NewClient(nextName, opts...)
					if err != nil {
						return fmt.Errorf("NewClient (%v) failed: %w", nextName, err)
					}

					w.sos.meta[node].client = pb.NewHedgeClient(w.sos.meta[node].conn)
					w.sos.meta[node].writer, err = w.sos.meta[node].client.SoSWrite(ctx)
					if err != nil {
						return fmt.Errorf("DMemWrite (%v) failed: %w", nextName, err)
					}

					w.sos.meta[node].grpc.Add(1)
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
		case !w.lo && node != w.sos.me():
			netCount++
			err := w.sos.meta[node].writer.Send(&pb.Payload{
				Meta: map[string]string{
					metaName:      w.sos.Name,
					metaMemLimit:  fmt.Sprintf("%v", w.sos.mlimit.Load()),
					metaDiskLimit: fmt.Sprintf("%v", w.sos.dlimit.Load()),
					metaExpire:    fmt.Sprintf("%v", int64(w.sos.age.Seconds())),
				},
				Data: data,
			})

			if err != nil {
				w.Lock()
				w.err = fmt.Errorf("Send failed: %w", err)
				w.Unlock()
			}

			w.sos.meta[node].msize.Add(uint64(len(data)))
		default:
			if msize < mlimit {
				memCount++
				if !mlock {
					w.sos.mlock.Lock()
					mlock = true
				}

				if _, ok := w.sos.data[node]; !ok {
					w.sos.data[node] = &memT{
						data:  []byte{},
						mlocs: []int{},
					}
				}

				w.sos.data[node].data = append(w.sos.data[node].data, data...)
				w.sos.data[node].mlocs = append(w.sos.data[node].mlocs, len(data))
				w.sos.meta[node].msize.Add(uint64(len(data)))
			} else {
				diskCount++
				if !dlock {
					w.sos.dlock.Lock()
					dlock = true
				}

				if file == nil {
					flag := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
					file, err = os.OpenFile(w.sos.localFile(), flag, 0644)
					if err != nil {
						w.sos.op.logger.Println("OpenFile failed:", err)
					}
				}

				n, err := file.Write(data)
				if err != nil {
					w.Lock()
					w.err = fmt.Errorf("Write failed: %w", err)
					w.Unlock()
				} else {
					w.sos.dlocs = append(w.sos.dlocs, n)
					w.sos.meta[node].dsize.Add(uint64(n))
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
	// 	"nodes", w.sos.nodes,
	// )

	if mlock {
		w.sos.mlock.Unlock()
	}

	file.Sync()
	file.Close()
	if dlock {
		w.sos.dlock.Unlock()
	}

	nodes := []uint64{}
	for k := range w.sos.meta {
		nodes = append(nodes, k)
	}

	for _, n := range nodes {
		if w.sos.meta[n].writer != nil {
			w.sos.meta[n].writer.CloseSend()
		}
	}
}

type writerOptions struct {
	LocalOnly bool
}

// Writer returns a writer object for writing data to SoS. The
// caller needs to call writer.Close() after use. Options is
// only used internally, not exposed to callers.
func (sos *SoS) Writer(opts ...*writerOptions) (*Writer, error) {
	if sos.on.Load() == 0 {
		return nil, errNoInit
	}

	sos.wmtx.Lock()
	var localOnly bool
	if len(opts) > 0 {
		localOnly = opts[0].LocalOnly
	}

	sos.writer = &Writer{
		lo:   localOnly,
		sos:  sos,
		ch:   make(chan []byte),
		done: make(chan struct{}, 1),
	}

	go sos.writer.start()
	sos.wrefs.Add(1)
	return sos.writer, nil
}

type Reader struct {
	sync.Mutex
	lo   bool // local read only
	sos  *SoS
	on   atomic.Int32
	err  error
	done chan struct{}
}

// Read reads the underlying data and streams them to the `out` channel.
func (r *Reader) Read(out chan []byte) {
	eg := new(errgroup.Group)
	eg.Go(func() error {
		r.on.Store(1)
		ctx := context.Background()
		for _, node := range r.sos.nodes {
			var err error
			switch {
			case !r.lo && node != r.sos.me():
				func() {
					r.sos.meta[node].reader, err = r.sos.meta[node].client.SoSRead(ctx)
					if err != nil {
						r.Lock()
						r.err = fmt.Errorf("DMemRead failed: %v", err)
						r.Unlock()
						return
					}
				}()

				err = r.sos.meta[node].reader.Send(&pb.Payload{
					Meta: map[string]string{
						metaName:      r.sos.Name,
						metaMemLimit:  fmt.Sprintf("%v", r.sos.mlimit.Load()),
						metaDiskLimit: fmt.Sprintf("%v", r.sos.dlimit.Load()),
						metaExpire:    fmt.Sprintf("%v", int64(r.sos.age.Seconds())),
					},
				})

				if err != nil {
					r.Lock()
					r.err = fmt.Errorf("Send failed: %v", err)
					r.Unlock()
					continue
				}

				for {
					in, err := r.sos.meta[node].reader.Recv()
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
					r.sos.mlock.Lock()
					defer r.sos.mlock.Unlock()
					var n int
					for _, off := range r.sos.data[node].mlocs {
						out <- r.sos.data[node].data[n : n+off]
						n += off
					}
				}()

				func() {
					r.sos.dlock.Lock()
					defer r.sos.dlock.Unlock()
					if len(r.sos.dlocs) == 0 {
						return
					}

					ra, err := mmap.Open(r.sos.localFile())
					if err != nil {
						r.Lock()
						r.err = fmt.Errorf("Open failed: %v", err)
						r.Unlock()
						return
					}

					defer ra.Close()
					var off int64
					for _, loc := range r.sos.dlocs {
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
	if r.on.Load() == 0 {
		return
	}

	<-r.done // wait for loop()
	r.sos.rrefs.Add(-1)
	r.on.Store(0)
}

type readerOptions struct {
	LocalOnly bool
}

// Reader returns a reader object for reading data from SoS. The
// caller needs to call reader.Close() after use. Options is only
// used internally, not exposed to callers.
func (sos *SoS) Reader(opts ...*readerOptions) (*Reader, error) {
	if sos.on.Load() == 0 {
		return nil, errNoInit
	}

	var localOnly bool
	if len(opts) > 0 {
		localOnly = opts[0].LocalOnly
	}

	reader := &Reader{
		lo:   localOnly,
		sos:  sos,
		done: make(chan struct{}, 1),
	}

	sos.rrefs.Add(1)
	return reader, nil
}

// Close closes the SoS object.
func (sos *SoS) Close() {
	if sos.on.Load() == 0 {
		return
	}

	sos.Lock()
	defer sos.Unlock()
	nodes := []uint64{}
	for k := range sos.meta {
		nodes = append(nodes, k)
	}

	ctx := context.Background()
	for _, n := range nodes {
		if sos.meta[n].conn != nil {
			sos.meta[n].client.SoSClose(ctx, &pb.Payload{
				Meta: map[string]string{metaName: sos.Name},
			})
		}
	}

	sos.on.Store(0)
}

func (sos *SoS) nextNode() (string, uint64) {
	var mb string
	members := sos.op.Members()
	for _, member := range members {
		nn := sos.hasher.Sum64([]byte(member))
		if nn == sos.me() {
			continue
		}

		if _, ok := sos.data[nn]; ok {
			continue
		}

		mb = member
		sos.nodes = append(sos.nodes, nn)
		sos.meta[nn] = &metaT{}
		sos.data[nn] = &memT{data: []byte{}, mlocs: []int{}}
		break
	}

	return mb, sos.nodes[len(sos.nodes)-1]
}

func (sos *SoS) me() uint64 { return sos.hasher.Sum64([]byte(sos.op.Name())) }

func (sos *SoS) localFile() string {
	name1 := fmt.Sprintf("%v", sos.me())
	name2 := sos.hasher.Sum64([]byte(sos.Name))
	return fmt.Sprintf("%v_%v.dat", name1, name2)
}

func (sos *SoS) cleaner() {
	eg := new(errgroup.Group)
	eg.Go(func() error {
		started := sos.start
		for {
			time.Sleep(time.Second * 5)
			wrefs := sos.wrefs.Load()
			rrefs := sos.rrefs.Load()
			if (wrefs + rrefs) > 0 {
				started = time.Now()
				continue
			}

			if time.Since(started) > sos.age {
				func() {
					// Cleanup memory area:
					sos.op.soss[sos.Name].mlock.Lock()
					sos.op.soss[sos.Name].mlock.Unlock()
					for _, node := range sos.op.soss[sos.Name].nodes {
						sos.op.soss[sos.Name].data[node].data = []byte{}
					}
				}()

				// Cleanup disk area:
				sos.op.soss[sos.Name].dlock.Lock()
				os.Remove(sos.localFile())
				sos.op.soss[sos.Name].dlock.Unlock()

				// Remove the main map entry:
				sos.op.sosLock.Lock()
				delete(sos.op.soss, sos.Name)
				sos.op.sosLock.Unlock()
				break
			}
		}

		return nil
	})

	eg.Wait()
}

func newSoS(name string, op *Op, opts ...*SoSOptions) *SoS {
	sos := &SoS{
		Name:  name,
		op:    op,
		meta:  make(map[uint64]*metaT),
		data:  map[uint64]*memT{},
		dlocs: []int{},
		mlock: &sync.Mutex{},
		dlock: &sync.Mutex{},
		wmtx:  &sync.Mutex{},
	}

	sos.on.Store(1)
	sos.nodes = []uint64{sos.me()}
	sos.meta[sos.me()] = &metaT{}
	sos.data[sos.me()] = &memT{
		data:  []byte{},
		mlocs: []int{},
	}

	if len(opts) > 0 {
		sos.mlimit.Store(opts[0].MemLimit)
		sos.dlimit.Store(opts[0].DiskLimit)
		if opts[0].Expiration > 0 {
			sos.age = time.Second * time.Duration(opts[0].Expiration)
		}
	}

	if sos.mlimit.Load() == 0 {
		si := syscall.Sysinfo_t{}
		syscall.Sysinfo(&si)
		sos.mlimit.Store(si.Freeram / 2) // half of free mem
	}

	if sos.dlimit.Load() == 0 {
		sos.dlimit.Store(1 << 30) // 1GB by default
	}

	if sos.age == 0 {
		sos.age = time.Hour * 1
	}

	sos.start = time.Now()
	go sos.cleaner()
	return sos
}
