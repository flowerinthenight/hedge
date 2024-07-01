package hedge

import (
	"io"

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

			_ = in
		}
	})

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			hs.Send(&protov1.Payload{})
		}
	})

	return g.Wait()
}

func (s *service) DMemRead(hs protov1.Hedge_DMemReadServer) error {
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

			_ = in
		}
	})

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			hs.Send(&protov1.Payload{})
		}
	})

	return g.Wait()
}
