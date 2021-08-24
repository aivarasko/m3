// Copyright (c) 2021  Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package inprocess

import (
	"errors"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/cmd/services/m3dbnode/server"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/integration/resources"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	xconfig "github.com/m3db/m3/src/x/config"
)

type dbNode struct {
	cfg         config.Configuration
	interruptCh chan<- error
	shutdownCh  <-chan struct{}
}

// TODO(nate): think about how we can share this impl, but have an alternative
// constructor.
func NewDBNode(pathToCfg string) (resources.Node, error) {
	var cfg config.Configuration
	if err := xconfig.LoadFile(&cfg, pathToCfg, xconfig.Options{}); err != nil {
		return nil, err
	}

	interruptCh := make(chan error, cfg.Components())
	shutdownCh := make(chan struct{}, cfg.Components())
	go func() {
		server.RunComponents(server.Options{
			Configuration: cfg,
			InterruptCh:   interruptCh,
			ShutdownCh:    shutdownCh,
		})
	}()

	// TODO: setup TChannel client
	// TODO: figure out multinode setup (reps and instances)
	//    - may require a change in config

	return &dbNode{
		cfg:         cfg,
		interruptCh: interruptCh,
		shutdownCh:  shutdownCh,
	}, nil
}

func (d *dbNode) HostDetails(port int) (*admin.Host, error) {
	panic("implement me")
}

func (d *dbNode) Health() (*rpc.NodeHealthResult_, error) {
	panic("implement me")
}

func (d *dbNode) WaitForBootstrap() error {
	panic("implement me")
}

func (d *dbNode) WritePoint(req *rpc.WriteRequest) error {
	panic("implement me")
}

func (d *dbNode) WriteTaggedPoint(req *rpc.WriteTaggedRequest) error {
	panic("implement me")
}

func (d *dbNode) AggregateTiles(req *rpc.AggregateTilesRequest) (int64, error) {
	panic("implement me")
}

func (d *dbNode) Fetch(req *rpc.FetchRequest) (*rpc.FetchResult_, error) {
	panic("implement me")
}

func (d *dbNode) FetchTagged(req *rpc.FetchTaggedRequest) (*rpc.FetchTaggedResult_, error) {
	panic("implement me")
}

func (d *dbNode) Exec(commands ...string) (string, error) {
	panic("implement me")
}

func (d *dbNode) GoalStateExec(verifier resources.GoalStateVerifier, commands ...string) error {
	panic("implement me")
}

func (d *dbNode) Restart() error {
	panic("implement me")
}

func (d *dbNode) Close() error {
	for i := 0; i < d.cfg.Components(); i++ {
		select {
		case d.interruptCh <- errors.New("in-process node being shut down"):
			break
		case <-time.After(5 * time.Second):
			return errors.New("timeout sending interrupt. closing without graceful shutdown")
		}
	}

	for i := 0; i < d.cfg.Components(); i++ {
		select {
		case <-d.shutdownCh:
			break
		case <-time.After(1 * time.Minute):
			return errors.New("timeout waiting for shutdown notification. server closing may" +
				" not be completely graceful")
		}
	}

	return nil
}
