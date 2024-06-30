package hedge

import (
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
)

type DistMemOptions struct {
	Limit uint64 // size limit (bytes) before spill-over
}

type DistMem struct {
	sync.Mutex

	Name string

	op     *Op
	data   map[string]map[int64][]byte // key=op.Name
	size   uint64
	limit  uint64
	writer *writerT
}

type writerT struct {
	so bool
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
		printed := false
		for d := range w.ch {
			size := atomic.LoadUint64(&w.dm.size)
			limit := atomic.LoadUint64(&w.dm.limit)
			if size >= limit {
				w.so = true
			}

			switch {
			case w.so:
				if !printed {
					w.dm.op.logger.Println(w.dm.Name, "spillover")
					printed = true
				}
			default: // local mem
				name := w.dm.Name
				if _, ok := w.dm.data[name]; !ok {
					w.dm.data[name] = make(map[int64][]byte)
				}

				ni := atomic.LoadInt64(&w.li) + 1
				w.dm.data[name][ni] = d
				atomic.AddUint64(&w.dm.size, uint64(len(d)))
				atomic.StoreInt64(&w.li, ni)
			}
		}
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
	name := r.dm.Name
	i := atomic.LoadInt64(&r.li)
	if _, ok1 := r.dm.data[name]; ok1 {
		if _, ok2 := r.dm.data[name][i]; ok2 {
			return r.dm.data[name][i]
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

func NewDistMem(name string, op *Op, opts ...*DistMemOptions) *DistMem {
	dm := &DistMem{
		Name: fmt.Sprintf("%v/%v", op.Name(), name),
		op:   op,
		data: map[string]map[int64][]byte{},
	}

	if len(opts) > 0 {
		dm.limit = opts[0].Limit
	}

	if dm.limit == 0 {
		si := syscall.Sysinfo_t{}
		syscall.Sysinfo(&si)
		dm.limit = si.Freeram / 2 // half of free mem
	}

	dm.op.logger.Println("limit:", dm.limit)
	return dm
}
