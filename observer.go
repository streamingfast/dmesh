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

package dmesh

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/namespace"
	"go.uber.org/zap"
)

const (
	EventUnkown = "unknown"
	EventSync   = "sync"
	EventUpdate = "update"
	EventDelete = "delete"
)

type PeerEvent struct {
	EventName string
	Peer      Peer
	PeerKey   string
}

func Observe(ctx context.Context, store *clientv3.Client, namespacePrefix, peerPrefix string) <-chan *PeerEvent {
	zlog.Debug("starting dmesh observer",
		zap.String("namespace", namespacePrefix),
		zap.String("peer_prefix", peerPrefix))

	namespacePrefix = fmt.Sprintf("/%s", namespacePrefix)

	storeKv := namespace.NewKV(store.KV, namespacePrefix)
	storeWatcher := namespace.NewWatcher(store.Watcher, namespacePrefix)

	keyPrefix := fmt.Sprintf("/%s/", peerPrefix)
	eventChan := make(chan *PeerEvent)

	go func() {
		zlog.Debug("syncing observing peers with server version prefix", zap.String("key_prefix", keyPrefix))
		resp, err := storeKv.Get(ctx, keyPrefix, clientv3.WithPrefix())
		if err != nil {
			// end this loop function cannot event sync with error, create a second channel and send an error?
			return
		}

		for _, kv := range resp.Kvs {
			peer, err := UnmarshalPeer(string(kv.Key), kv.Value)
			if err != nil {
				// skip peer
				continue
			}
			eventChan <- &PeerEvent{
				PeerKey:   string(kv.Key),
				EventName: EventSync,
				Peer:      peer,
			}
		}
		movingRevision := resp.Header.Revision
		for wresp := range storeWatcher.Watch(ctx, keyPrefix, clientv3.WithPrefix(), clientv3.WithRev(movingRevision)) {
			if wresp.Err() != nil {
				zlog.Error("failed to track event", zap.Error(wresp.Err()))
				time.Sleep(10 * time.Second)
				continue
			}
			for _, ev := range wresp.Events {
				switch ev.Type.String() {
				case "PUT", "DELETE":

					key := string(ev.Kv.Key)

					peer, err := UnmarshalPeer(key, ev.Kv.Value)
					if err != nil {
						// this happens when the peer get deleted
						peer = &SearchPeer{}
						peer.SetKey(key)
					}

					eventName := EventUnkown
					if ev.Type.String() == "PUT" {
						eventName = EventUpdate
					} else if ev.Type.String() == "DELETE" {
						eventName = EventDelete
					}
					eventChan <- &PeerEvent{
						PeerKey:   string(ev.Kv.Key),
						EventName: eventName,
						Peer:      peer,
					}
				default:
				}
				zlog.Debug("dmesh obeserved event",
					zap.String("type", ev.Type.String()),
					zap.String("getKey", string(ev.Kv.Key)),
					zap.String("value", string(ev.Kv.Value)),
					zap.String("value", ev.Type.String()))
			}
		}
		close(eventChan)
	}()
	return eventChan
}
