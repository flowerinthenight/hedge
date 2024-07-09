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
				s.op.logger.Println("Recv failed:", err)
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
				s.op.logger.Println("Recv failed:", err)
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
	var writer *Writer

	var count int

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
			s.op.logger.Println("Recv failed:", err)
			break
		}

		name := in.Meta[metaName]
		if _, ok := s.op.dms[name]; !ok {
			mlimit, _ := strconv.ParseUint(in.Meta[metaMemLimit], 10, 64)
			dlimit, _ := strconv.ParseUint(in.Meta[metaDiskLimit], 10, 64)
			age, _ := strconv.ParseInt(in.Meta[metaExpire], 10, 64)
			s.op.dms[name] = s.op.NewDistMem(name, &DistMemOptions{
				MemLimit:   mlimit,
				DiskLimit:  dlimit,
				Expiration: age,
			})
		}

		if writer == nil {
			writer, _ = s.op.dms[name].Writer(&writerOptions{
				LocalOnly: true,
			})
		}

		writer.Write(in.Data)
		count++
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
		s.op.logger.Println("Recv failed:", err)
		return nil
	}

	name := in.Meta[metaName]
	if _, ok := s.op.dms[name]; !ok {
		mlimit, _ := strconv.ParseUint(in.Meta[metaMemLimit], 10, 64)
		dlimit, _ := strconv.ParseUint(in.Meta[metaDiskLimit], 10, 64)
		age, _ := strconv.ParseInt(in.Meta[metaExpire], 10, 64)
		s.op.dms[name] = s.op.NewDistMem(name, &DistMemOptions{
			MemLimit:   mlimit,
			DiskLimit:  dlimit,
			Expiration: age,
		})
	}

	reader, _ := s.op.dms[name].Reader(&readerOptions{LocalOnly: true})
	out := make(chan []byte)
	eg := new(errgroup.Group)
	eg.Go(func() error {
		for d := range out {
			err = hs.Send(&pb.Payload{Data: d})
			if err != nil {
				s.op.logger.Println("Send failed:", err)
			}
		}

		return nil
	})

	reader.Read(out)
	eg.Wait()

	if reader != nil {
		reader.Close()
	}

	return nil
}

func (s *service) DMemClose(ctx context.Context, in *pb.Payload) (*pb.Payload, error) {
	name := in.Meta[metaName]
	s.op.dms[name].Close()
	return &pb.Payload{}, nil
}
