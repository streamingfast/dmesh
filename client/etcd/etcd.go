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

package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/dfuse-io/dgrpc"
	"github.com/dfuse-io/dmesh"
	"github.com/dfuse-io/dmesh/metrics"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/namespace"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"go.uber.org/zap"
)


// var DefaultDialer grpc.DialerFunc
// send list of peers and an interface

var logCount = 0
var DefaultPeerPublishInterval = 1 * time.Second
var DefaultGrantTTL = 5

type Etcd struct {
	ctx             context.Context
	client          *clientv3.Client
	initialRevision int64
	localIP         string

	leaseID         clientv3.LeaseID
	leaseTTL        int64
	leaseCancelFunc func()

	watchServices []string

	allPeers     map[string]dmesh.Peer
	allPeersLock sync.RWMutex

	regsiteredPeers map[string]*peerPublish
}

// DNS example: `etcd://etcd.dmesh:2379/eos-dev1`
// DNS example: `etcd://<etcd-host>:<etcd-port>/<etcd-namespace>`
func New(dsnURL *url.URL) (*Etcd, error) {
	addr, nspace, err := ParseDSNURL(dsnURL)
	if err != nil {
		return nil, fmt.Errorf("invalid dsn: %w", err)
	}

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to crate etcd client: %w", err)
	}

	zlog.Debug("setting up etcd client", zap.String("etcd_addr", addr), zap.String("etcd_namespace", nspace))
	etcdClient.KV = namespace.NewKV(etcdClient.KV, nspace)
	etcdClient.Watcher = namespace.NewWatcher(etcdClient.Watcher, nspace)
	etcdClient.Lease = namespace.NewLease(etcdClient.Lease, nspace)

	return &Etcd{
		client:          etcdClient,
		localIP:         dmesh.GetLocalIP(),
		allPeers:        map[string]dmesh.Peer{},
		regsiteredPeers: map[string]*peerPublish{},
	}, nil
}

func (d *Etcd) Start(ctx context.Context, watchServices []string) error {
	if ctx == nil {
		ctx = context.Background()
	}

	services, err := dmesh.ValidateServiceList(watchServices)
	if err != nil {
		return fmt.Errorf("invalid services watch list: %s", err)
	}
	d.ctx = ctx
	d.watchServices = services

	ttl := int64(5)
	if os.Getenv("DMESH_GRANT_TTL") != "" {
		ttl, _ = strconv.ParseInt(os.Getenv("DMESH_GRANT_TTL"), 10, 64)
	}

	d.leaseTTL = ttl

	if err := d.setupGrant(); err != nil {
		return err
	}

	if err := d.fetchCurrentState(); err != nil {
		return err
	}

	d.watchUpdates()

	return nil
}

func (d *Etcd) Close() error {
	// TODO: close
	return d.client.Close()
}


//TODO: find a better name
type peerPublish struct {
	publishWithinCh chan time.Duration
	publishInterval time.Duration
}

func (p *peerPublish) publishWithin(dur time.Duration) {
	p.publishWithinCh <- dur
}

func (d *Etcd) Peers() (out []*dmesh.SearchPeer) {
	d.allPeersLock.RLock()
	defer d.allPeersLock.RUnlock()

	for _, peer := range d.allPeers {
		searchPeer, ok := peer.(*dmesh.SearchPeer)
		if ok {
			out = append(out, searchPeer)
		}
	}
	return
}

// publishWithin 5 minutes
// durationToNext = 10 years
// I was waiting for 10 years
// interrupted:
//   you're asking for 5 minutes?
//   that's shorter than 10 years!
//   let's make the durationToNextPublish = 5 minutes
// PublishWithin(5 * time.Minute)
func (d *Etcd) PublishWithin(peer dmesh.Peer, timeout time.Duration) {
	pp := d.register(peer)
	if timeout == 0 {
		panic("no 0 value for PublishWithin()")
	}
	pp.publishWithin(timeout)
}

func (d *Etcd) PublishNow(peer dmesh.Peer) error {
	pp := d.register(peer)
	if d == nil {
		return fmt.Errorf("client not configured")
	}

	pp.publishWithin(0)

	peerKey := peer.Key()

	bytes, err := json.Marshal(peer)
	if err != nil {
		return fmt.Errorf("error marshaling peer %q: %s", peerKey, err)
	}

	_, err = d.client.Put(d.ctx, peerKey, string(bytes), clientv3.WithLease(d.leaseID))
	if err != nil && err == rpctypes.ErrLeaseNotFound {
		if err := d.setupGrant(); err != nil {
			return fmt.Errorf("setup grants under failed put: %s", err)
		}
		_, err = d.client.Put(d.ctx, peerKey, string(bytes), clientv3.WithLease(d.leaseID))
	}
	if err != nil {
		return err
	}

	if (logCount % 1000) == 0 {
		zlog.Debug("published peer", zap.String("peer_key", peerKey), zap.Reflect("peer", peer))
	}
	logCount++

	return nil
}

//--------------------------------------
// Dmesh Client ETCD specific functions

// Dmesh Client interace implementation
func (d *Etcd) register(peer dmesh.Peer) (pp *peerPublish) {

	pp, found := d.regsiteredPeers[peer.Key()]
	if !found {

		var err error
		interval := DefaultPeerPublishInterval
		if os.Getenv("DMESH_PUBLISH_INTERVAL") != "" {
			interval, err = time.ParseDuration(os.Getenv("DMESH_PUBLISH_INTERVAL"))
			if err != nil {
				zlog.Info("unabled to decode DMESH_PUBLISH_INTERVAL falling back to default", zap.String("DMESH_PUBLISH_INTERVAL", os.Getenv("DMESH_PUBLISH_INTERVAL")))
				interval = DefaultPeerPublishInterval
			}
		}
		pp = &peerPublish{
			publishWithinCh: make(chan time.Duration, 100),
			publishInterval: interval,
		}
		d.regsiteredPeers[peer.Key()] = pp
		go d.launchPeerPublishing(peer, pp)
	}

	return pp
}


// blocking call that periodically publishes the given peer
func (d *Etcd) launchPeerPublishing(peer dmesh.Peer, peerPublish *peerPublish) {
	zlog.Info("launching peer publishing", zap.Duration("polling_duration", peerPublish.publishInterval))
	nextPublishTime := time.Now().Add(peerPublish.publishInterval)
	for {
		lomgestWait := time.Until(nextPublishTime)
		zlog.Debug("publishing at the latest", zap.Duration("delay", lomgestWait))
		select {
		case <-time.After(lomgestWait):
			_ = d.PublishNow(peer)
			nextPublishTime = time.Now().Add(peerPublish.publishInterval)
		case within := <-peerPublish.publishWithinCh:
			if within == 0 {
				nextPublishTime = time.Now().Add(peerPublish.publishInterval)
			} else if within < time.Until(nextPublishTime) {
				nextPublishTime = time.Now().Add(within)
			}
		}
	}
}

func (d *Etcd) setupGrant() error {
	if d.leaseCancelFunc != nil {
		d.leaseCancelFunc()
	}

	ctx, cancel := context.WithCancel(context.Background())

	grant, err := d.client.Grant(ctx, d.leaseTTL)
	if err != nil {
		return fmt.Errorf("couldn't create grant: %s", err)
	}
	zlog.Info("grant creation",
		zap.Int64("id", int64(grant.ID)),
		zap.String("error", grant.Error),
		zap.Int64("ttl", grant.TTL))

	keepAliveChan, err := d.client.KeepAlive(ctx, grant.ID)
	if err != nil {
		return fmt.Errorf("setup keep alive: %s", err)
	}

	go d.showKeepAlives(grant.ID, keepAliveChan)

	d.leaseID = grant.ID
	d.leaseCancelFunc = cancel

	return nil
}

func (d *Etcd) fetchCurrentState() error {
	for _, servicePrefix := range d.watchServices {
		zlog.Debug("fetching current state", zap.String("prefix", servicePrefix))
		resp, err := d.client.Get(d.ctx, servicePrefix, clientv3.WithPrefix())
		if err != nil {
			return err
		}

		if resp.More {
			panic("whoops, haven't accounted for that.. it's probably the right time to crash the system anyway")
		}

		for _, kv := range resp.Kvs {
			err := d.trackPeer(kv.Key, kv.Value)
			if err != nil {
				return err
			}

		}
		d.initialRevision = resp.Header.Revision
	}

	return nil
}

func (d *Etcd) watchUpdates() {
	for _, servicePrefix := range d.watchServices {
		zlog.Debug("Watch Update running now on service", zap.String("service", servicePrefix))
		go func() {

			initRev := d.initialRevision
			zlog.Info("dmesh watching service", zap.String("service_prefix", servicePrefix), zap.Int64("initial_revision", initRev))

			// FIXME: We'd need another `for` loop, so `Watch()` is
			// restarted when it fails. Or does it need? Would it
			// simply retry and continue from where it left off?!

			for wresp := range d.client.Watch(d.ctx, servicePrefix, clientv3.WithPrefix(), clientv3.WithRev(initRev)) {
				if wresp.Err() != nil {
					zlog.Error("failed to track watching", zap.Error(wresp.Err()))
					// TODO: make this KILL the whole system?! What,s the failure condition of `Watch.Err()` ?
					time.Sleep(10 * time.Second)
					continue
				}

				d.applyUpdates(wresp.Events)
				initRev = wresp.Header.Revision
				zlog.Debug("dmesh peers updated", zap.Int("peer_count", len(d.allPeers)))
			}

		}()
	}
}

func (d *Etcd) applyUpdates(events []*clientv3.Event) {
	for _, ev := range events {
		switch ev.Type.String() {
		case "PUT":
			err := d.trackPeer(ev.Kv.Key, ev.Kv.Value)
			if err != nil {
				zlog.Error("unable to PUT event", zap.Error(err))
				continue
			}

		case "DELETE":
			d.deletePeer(ev.Kv.Key)
		default:
			zlog.Error("invalid etcd watch value", zap.String("value", ev.Type.String()))
			panic("invalid etcd watch value")
		}
		zlog.Debug("dmesh event handled",
			zap.String("type", ev.Type.String()),
			zap.String("getKey", string(ev.Kv.Key)),
			zap.String("value", string(ev.Kv.Value)),
			zap.String("value", ev.Type.String()))
	}
}

func (d *Etcd) showKeepAlives(grantID clientv3.LeaseID, keepAlive <-chan *clientv3.LeaseKeepAliveResponse) {
	for {
		resp, ok := <-keepAlive
		if !ok {
			zlog.Error("dmesh keep alive loop terminating", zap.Int64("grant_id", int64(grantID)))
			return
		}
		if resp != nil {
			zlog.Debug("keep alive ",
				zap.Int64("grant_id", int64(grantID)),
				zap.Int64("keep_alive_id", int64(resp.ID)),
				zap.Int64("keep_alive_ttl", resp.TTL))
		} else {
			zlog.Debug("keep alive resp is nil",
				zap.Int64("grant_id", int64(grantID)))
		}

		if resp.ID != grantID {
			zlog.Error("unexpected lease id in keep alive response",
				zap.Int64("grant_id", int64(grantID)),
				zap.Int64("keep_alive_id", int64(resp.ID)))
		}
		_ = resp
	}
}

func (d *Etcd) trackPeer(key, peerData []byte) error {

	peer, err := dmesh.UnmarshalPeer(string(key), peerData)
	if err != nil {
		return err
	}

	d.allPeersLock.Lock()
	defer d.allPeersLock.Unlock()

	existingPeer, found := d.allPeers[peer.Key()]
	if !found {
		if connectiblePeer, ok := peer.(dmesh.ConnectablePeer); ok {
			conn, err := dgrpc.NewInternalClient(connectiblePeer.Addr())
			if err != nil {
				return err
			}
			connectiblePeer.SetConn(conn)
		} else {
			zlog.Info("peer is not connectible", zap.String("peer_key", peer.Key()))
		}

		d.allPeers[peer.Key()] = peer
		return err
	} else {
		if mergeablePeer, ok := existingPeer.(dmesh.MergeablePeer); ok {
			mergeablePeer.Merge(peer)
		} else {
			zlog.Info("peer is not mergeable", zap.String("peer_key", peer.Key()))
		}
		return nil
	}
}

func (d *Etcd) deletePeer(key []byte) error {

	peerKey := string(key)
	zlog.Info("peer deleted", zap.String("peer_key", peerKey))

	metrics.DeletedPeerCountMetric.Inc()

	d.allPeersLock.Lock()
	defer d.allPeersLock.Unlock()

	peer, found := d.allPeers[peerKey]
	if found {
		if connectiblePeer, ok := peer.(dmesh.ConnectablePeer); ok {
			connectiblePeer.Close()
		}
		delete(d.allPeers, peerKey)
	}
	return nil
}
