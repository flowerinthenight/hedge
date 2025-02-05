package hedge

import (
	"context"
	"io"
	"strconv"

	pb "github.com/flowerinthenight/hedge-proto"
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

func (s *service) SoSWrite(hs pb.Hedge_SoSWriteServer) error {
	var err error
	ctx := hs.Context()
	var writer *Writer

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
		if _, ok := s.op.soss[name]; !ok {
			mlimit, _ := strconv.ParseUint(in.Meta[metaMemLimit], 10, 64)
			dlimit, _ := strconv.ParseUint(in.Meta[metaDiskLimit], 10, 64)
			age, _ := strconv.ParseInt(in.Meta[metaExpire], 10, 64)
			s.op.soss[name] = s.op.NewSoS(name, &SoSOptions{
				MemLimit:   mlimit,
				DiskLimit:  dlimit,
				Expiration: age,
			})
		}

		if writer == nil {
			writer, _ = s.op.soss[name].Writer(&writerOptions{
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

func (s *service) SoSRead(hs pb.Hedge_SoSReadServer) error {
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
	if _, ok := s.op.soss[name]; !ok {
		mlimit, _ := strconv.ParseUint(in.Meta[metaMemLimit], 10, 64)
		dlimit, _ := strconv.ParseUint(in.Meta[metaDiskLimit], 10, 64)
		age, _ := strconv.ParseInt(in.Meta[metaExpire], 10, 64)
		s.op.soss[name] = s.op.NewSoS(name, &SoSOptions{
			MemLimit:   mlimit,
			DiskLimit:  dlimit,
			Expiration: age,
		})
	}

	reader, _ := s.op.soss[name].Reader(&readerOptions{LocalOnly: true})
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

func (s *service) SoSClose(ctx context.Context, in *pb.Payload) (*pb.Payload, error) {
	name := in.Meta[metaName]
	s.op.soss[name].Close()
	return &pb.Payload{}, nil
}
