package hedge

import (
	"context"
	"io"
	"strconv"

	pb "github.com/flowerinthenight/hedge/proto/v1"
	"golang.org/x/sync/errgroup"
)

type service struct {
	op *Op

	pb.UnimplementedHedgeServer
}

func (s *service) Send(hs pb.Hedge_SendServer) error {
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

func (s *service) Broadcast(hs pb.Hedge_BroadcastServer) error {
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

func (s *service) DMemWrite(hs pb.Hedge_DMemWriteServer) error {
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
		mlimit, _ := strconv.ParseUint(in.Meta[metaMemLimit], 10, 64)
		dlimit, _ := strconv.ParseUint(in.Meta[metaDiskLimit], 10, 64)
		s.op.dms[name] = s.op.NewDistMem(name, Limit{
			MemLimit:  mlimit,
			DiskLimit: dlimit,
		})

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

func (s *service) DMemRead(hs pb.Hedge_DMemReadServer) error {
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
	mlimit, _ := strconv.ParseUint(in.Meta[metaMemLimit], 10, 64)
	dlimit, _ := strconv.ParseUint(in.Meta[metaDiskLimit], 10, 64)
	s.op.dms[name] = s.op.NewDistMem(name, Limit{
		MemLimit:  mlimit,
		DiskLimit: dlimit,
	})

	reader, _ := s.op.dms[name].Reader()
	out := make(chan []byte)
	eg := new(errgroup.Group)
	eg.Go(func() error {
		for d := range out {
			hs.Send(&pb.Payload{Data: d})
		}

		return nil
	})

	reader.Read(out)
	eg.Wait()

	return nil
}

func (s *service) DMemClear(ctx context.Context, in *pb.Payload) (*pb.Payload, error) {
	name := in.Meta[metaName]
	s.op.dms[name].Clear()
	return &pb.Payload{}, nil
}
