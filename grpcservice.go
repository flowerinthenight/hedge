package hedge

import (
	"io"
	"sync"

	protov1 "github.com/flowerinthenight/hedge/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type service struct {
	op *Op

	protov1.UnimplementedHedgeServer
}

func (s *service) Send(hs protov1.Hedge_SendServer) error {
	ctx := hs.Context()
	var w sync.WaitGroup
	w.Add(1)
	go func() {
		defer w.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			in, err := hs.Recv()
			if err == io.EOF {
				return
			}

			s.op.streamIn <- &StreamMessage{Payload: in}
		}
	}()

	w.Add(1)
	go func() {
		defer w.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			out := <-s.op.streamOut
			if out == nil {
				return
			}

			hs.Send(out.Payload)
		}
	}()

	w.Wait()
	return nil
}

func (s *service) Broadcast(hs protov1.Hedge_BroadcastServer) error {
	return status.Errorf(codes.Unimplemented, "method Broadcast not implemented")
}
