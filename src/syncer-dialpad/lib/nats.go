package dialpad

import (
	"context"

	nts "github.com/nats-io/nats.go"
	js "github.com/nats-io/nats.go/jetstream"

	"github.com/BemiHQ/BemiDB/src/common"
)

const ()

type Nats struct {
	Config *Config
}

func NewNats(config *Config) *Nats {
	return &Nats{
		Config: config,
	}
}

func (nats *Nats) Stream(ctx context.Context) js.Stream {
	jetstream := nats.newJetstream()
	stream, err := jetstream.Stream(ctx, nats.Config.Nats.Stream)
	common.PanicIfError(nats.Config.CommonConfig, err)
	return stream
}

func (nats *Nats) newJetstream() js.JetStream {
	nc, err := nts.Connect(nats.Config.Nats.Url)
	common.PanicIfError(nats.Config.CommonConfig, err)
	jetstream, err := js.New(nc)
	common.PanicIfError(nats.Config.CommonConfig, err)
	return jetstream
}
