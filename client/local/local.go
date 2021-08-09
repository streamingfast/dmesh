// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package local

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/dfuse-io/dgrpc"
	"github.com/streamingfast/dmesh"
	"go.uber.org/zap"
)

type Local struct {
	allPeers     map[string]dmesh.Peer
	allPeersLock sync.RWMutex
}

// DNS example: `local://`
func New(dsnURL *url.URL) (*Local, error) {
	return &Local{
		allPeers: map[string]dmesh.Peer{},
	}, nil
}

func (l *Local) Start(ctx context.Context, watchServices []string) error {
	return nil
}

func (l *Local) Close() error {
	return nil
}

func (l *Local) Peers() (out []*dmesh.SearchPeer) {
	l.allPeersLock.RLock()
	defer l.allPeersLock.RUnlock()

	for _, peer := range l.allPeers {
		searchPeer, ok := peer.(*dmesh.SearchPeer)
		if ok {
			out = append(out, searchPeer)
		}
	}
	return
}

func (l *Local) PublishWithin(peer dmesh.Peer, timeout time.Duration) {
	go func() {
		time.Sleep(timeout)
		l.RegisterPeer(peer)
	}()
}

func (l *Local) PublishNow(peer dmesh.Peer) error {
	l.RegisterPeer(peer)
	return nil
}

func (l *Local) RegisterPeer(peer dmesh.Peer) {
	l.allPeersLock.Lock()
	defer l.allPeersLock.Unlock()

	existingPeer, found := l.allPeers[peer.Key()]
	if !found {
		if connectiblePeer, ok := peer.(dmesh.ConnectablePeer); ok {
			conn, err := dgrpc.NewInternalClient(connectiblePeer.Addr())
			if err != nil {
				zlog.Error("unable to track peer", zap.Error(err))
				return
			}
			connectiblePeer.SetConn(conn)
		} else {
			zlog.Info("peer is not connectible", zap.String("peer_key", peer.Key()))
		}
		l.allPeers[peer.Key()] = peer
	} else {
		// Merge the data into the existing
		// TODO: should we check if matches the connectable interface
		if mergeablePeer, ok := existingPeer.(dmesh.MergeablePeer); ok {
			mergeablePeer.Merge(peer)
			return
		}
		zlog.Info("peer is not mergeable", zap.String("peer_key", peer.Key()))
	}
}
