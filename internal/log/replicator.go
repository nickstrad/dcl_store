package log

import (
	"context"
	"sync"

	api "github.com/nickstrad/dkv_store/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.LogClient

	logger  *zap.Logger
	mu      sync.Mutex
	servers map[string]chan struct{}
	closed  bool
	close   chan struct{}
}

// Lazy initialization method to abstract away this from users
func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}

	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}

	if r.close == nil {
		r.close = make(chan struct{})
	}
}

func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.Dial(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to dial", addr)
		return
	}
	defer cc.Close()

	client := api.NewLogClient(cc)
	ctx := context.Background()
	stream, err := client.ReadStream(ctx, &api.ReadRequest{
		Offset: 0,
	},
	)

	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}

	records := make(chan *api.Record)
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to read", addr)
				return
			}
			records <- recv.Record
		}
	}()

	for {
		select {
		// if the replicator closes
		case <-r.close:
			return
			// if this server left serf
		case <-leave:
			return
		case record := <-records:
			_, err = r.LocalServer.Append(
				ctx,
				&api.AppendRequest{Record: record},
			)
			if err != nil {
				r.logError(err, "failed to append", "addr")
				return
			}
		}
	}
}

func (r *Replicator) logError(err error, msg string, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}

// Used as Handler interface method for when a server joins serf
func (r *Replicator) Join(name string, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}

	if _, ok := r.servers[name]; ok {
		//skip since we are already replicating
		return nil
	}

	// create 'leave' channel
	r.servers[name] = make(chan struct{})
	go r.replicate(addr, r.servers[name])

	return nil
}

// Used as Handler interface method for when a server leaves serf
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()
	if _, ok := r.servers[name]; !ok {
		return nil
	}

	// close channel used for streaming records
	close(r.servers[name])

	// remove server and channel from map
	delete(r.servers, name)
	return nil
}

func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}

	r.closed = true
	close(r.close)
	return nil
}
