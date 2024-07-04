package hedge

import (
	"io"
	"strconv"

	protov1 "github.com/flowerinthenight/hedge/proto/v1"
	"golang.org/x/sync/errgroup"
)

type service struct {
	op *Op

	protov1.UnimplementedHedgeServer
}

func (s *service) Send(hs protov1.Hedge_SendServer) error {
	ctx := hs.Context()
	g := new(errgroup.Group)
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			in, err := hs.Recv()
			if err == io.EOF {
				return nil
			}

			if err != nil {
				return err
			}

			s.op.leaderStreamIn <- &StreamMessage{
				Payload: in,
			}
		}
	})

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			out := <-s.op.leaderStreamOut
			if out == nil {
				return nil
			}

			hs.Send(out.Payload)
		}
	})

	return g.Wait()
}

func (s *service) Broadcast(hs protov1.Hedge_BroadcastServer) error {
	ctx := hs.Context()
	g := new(errgroup.Group)
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			in, err := hs.Recv()
			if err == io.EOF {
				return nil
			}

			if err != nil {
				return err
			}

			s.op.broadcastStreamIn <- &StreamMessage{
				Payload: in,
			}
		}
	})

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			out := <-s.op.broadcastStreamOut
			if out == nil {
				return nil
			}

			hs.Send(out.Payload)
		}
	})

	return g.Wait()
}

func (s *service) DMemWrite(hs protov1.Hedge_DMemWriteServer) error {
	var err error
	ctx := hs.Context()
	var writer *writerT

loop:
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			break loop
		default:
		}

		in, err := hs.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			s.op.logger.Println("service: Recv failed:", err)
			break
		}

		name := in.Meta[metaName]
		limit, _ := strconv.ParseUint(in.Meta[metaLimit], 10, 64)
		s.op.dms[name] = s.op.NewDistMem(name, Limit{MemLimit: limit})
		if writer == nil {
			writer, _ = s.op.dms[name].Writer(&writerOptionsT{
				LocalOnly: true,
			})
		}

		writer.Write(in.Data)
	}

	if writer != nil {
		writer.Close()
	}

	return err
}

func (s *service) DMemRead(hs protov1.Hedge_DMemReadServer) error {
	var err error
	in, err := hs.Recv()
	if err == io.EOF {
		return nil
	}

	if err != nil {
		s.op.logger.Println("service: Recv failed:", err)
		return nil
	}

	name := in.Meta[metaName]
	limit, _ := strconv.ParseUint(in.Meta[metaLimit], 10, 64)
	s.op.dms[name] = s.op.NewDistMem(name, Limit{MemLimit: limit})
	reader, _ := s.op.dms[name].Reader()
	out := make(chan []byte)
	eg := new(errgroup.Group)
	eg.Go(func() error {
		for d := range out {
			hs.Send(&protov1.Payload{Data: d})
		}

		return nil
	})

	reader.Read(out)
	eg.Wait()

	return nil
}
