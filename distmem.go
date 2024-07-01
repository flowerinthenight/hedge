package hedge

import (
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"

	"google.golang.org/grpc"
)

type metaT struct {
	size uint64
	conn *grpc.ClientConn
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
	data   map[uint64]map[int64][]byte // key=op.Name
}

type writerT struct {
	li int64
	dm *DistMem
	ch chan []byte
}

func (w *writerT) Write(data []byte) int64 {
	w.ch <- data
	return w.LastIndex()
}

func (w *writerT) Close() { close(w.ch) }

func (w *writerT) LastIndex() int64 { return atomic.LoadInt64(&w.li) }

func (w *writerT) start() {
	go func() {
		var spillCount int
		node := w.dm.nodes[0]
		for d := range w.ch {
			size := atomic.LoadUint64(&w.dm.meta[node].size)
			limit := atomic.LoadUint64(&w.dm.limit)
			if size >= limit {
				node = w.dm.nextNode()
				w.dm.op.logger.Println("spillover to:", node)
			}

			switch {
			case node != w.dm.me():
				spillCount++
			default:
				if _, ok := w.dm.data[node]; !ok {
					w.dm.data[node] = make(map[int64][]byte)
				}

				ni := atomic.LoadInt64(&w.li) + 1
				w.dm.data[node][ni] = d
				atomic.AddUint64(&w.dm.meta[node].size, uint64(len(d)))
				atomic.StoreInt64(&w.li, ni)
			}
		}

		w.dm.op.logger.Println("spillCount:", spillCount)
	}()
}

func (dm *DistMem) Writer() (*writerT, error) {
	ref := func() bool {
		dm.Lock()
		ok := dm.writer != nil
		dm.Unlock()
		return ok
	}()

	if ref {
		return nil, fmt.Errorf("can only have one writer")
	}

	dm.Lock()
	dm.writer = &writerT{
		li: -1,
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

func (r *readerT) Next() bool {
	ni := atomic.AddInt64(&r.li, 1)
	return ni <= atomic.LoadInt64(&r.w.li)
}

func (r *readerT) Read() []byte {
	node := r.dm.nodes[0]
	i := atomic.LoadInt64(&r.li)
	if _, ok1 := r.dm.data[node]; ok1 {
		if _, ok2 := r.dm.data[node][i]; ok2 {
			return r.dm.data[node][i]
		}
	}

	return nil
}

func (r *readerT) LastIndex() int64 { return atomic.LoadInt64(&r.li) }

func (dm *DistMem) Reader() (*readerT, error) {
	r := &readerT{
		li: -1,
		dm: dm,
		w:  dm.writer,
	}

	return r, nil
}

func (dm *DistMem) nextNode() uint64 {
	members := dm.op.Members()
	for _, member := range members {
		nn := dm.hasher.Sum64([]byte(member))
		if nn == dm.me() {
			continue
		}

		if _, ok := dm.data[nn]; ok {
			continue
		}

		dm.op.logger.Println("nextNode:", member)
		dm.nodes = append(dm.nodes, nn)
		dm.meta[nn] = &metaT{}
		dm.data[nn] = make(map[int64][]byte)
		break
	}

	return dm.nodes[len(dm.nodes)-1]
}

func (dm *DistMem) me() uint64 { return dm.hasher.Sum64([]byte(dm.op.Name())) }

func newDistMem(name string, op *Op, opts ...*DistMemOptions) *DistMem {
	dm := &DistMem{
		Name: fmt.Sprintf("%v/%v", op.Name(), name),
		op:   op,
		meta: make(map[uint64]*metaT),
		data: map[uint64]map[int64][]byte{},
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

	dm.op.logger.Println("me:", dm.me(), "limit:", dm.limit)
	return dm
}
