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
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/dfuse-io/dgrpc"
	"google.golang.org/grpc"
)

var Hostname, _ = os.Hostname()

type MergeablePeer interface {
	// Merge copies from `in` any fields that could have moved, when
	// watching from etcd.
	Merge(in Peer)
}

type ConnectablePeer interface {
	Addr() string
	Conn() *grpc.ClientConn
	SetConn(conn *grpc.ClientConn)
	Close() error
	ResetConn() error
}

type Peer interface {
	SetKey(string) error
	Key() string
}

const (
	SearchServiceName  = "search"
	RelayerServiceName = "relayer"
)

type GenericPeer struct {
	sync.RWMutex

	grpcConn *grpc.ClientConn

	serviceVersion string // v2, v1
	serviceName    string // search, relayer
	addr           string // use this instead

	Ready bool       `json:"ready,omitempty"` // should be ready when service is live
	Host  string     `json:"host,omitempty"`  // search-liverouter-v2-1-59f98966b8-vzzsb
	Boot  *time.Time `json:"boot,omitempty"`
}

func newServiceData(serviceVersion, serviceName, listenAddr string, pollingDuration time.Duration) GenericPeer {
	now := time.Now()
	p := GenericPeer{
		serviceVersion: serviceVersion,
		serviceName:    serviceName,
		Host:           Hostname,
		Boot:           &now,
	}
	p.setLocalAddr(listenAddr)
	return p
}

// Peer interface
func (p *GenericPeer) Key() string {
	return fmt.Sprintf("/%s/%s/%s", p.serviceVersion, p.serviceName, p.addr)
}

func (p *GenericPeer) SetKey(key string) error {
	serviceVersion, serviceName, addr, err := spreadKey(key)
	if err != nil {
		return err
	}
	p.serviceVersion = serviceVersion
	p.serviceName = serviceName
	p.addr = addr
	return nil
}

// ConnectiblePeer interface
func (p *GenericPeer) Addr() string {
	return p.addr
}

func (p *GenericPeer) Conn() *grpc.ClientConn {
	p.RLock()
	defer p.RUnlock()

	if p.grpcConn == nil {
		conn, err := dgrpc.NewInternalClient(p.addr)
		if err != nil {
			panic("invalid address or error setting up gRPC ClientConn, should have been tested when creating the client first")
		}
		p.grpcConn = conn
	}

	return p.grpcConn
}

func (p *GenericPeer) SetConn(grpcConn *grpc.ClientConn) {
	p.grpcConn = grpcConn
}

func (p *GenericPeer) Close() error {
	p.Lock()
	defer p.Unlock()

	if p.grpcConn != nil {
		return p.grpcConn.Close()
	}

	return nil
}

func (p *GenericPeer) ResetConn() error {
	p.Lock()
	defer p.Unlock()

	err := p.grpcConn.Close()
	if err != nil {
		return err
	}

	conn, err := dgrpc.NewInternalClient(p.addr)
	if err != nil {
		return err
	}

	p.grpcConn = conn

	return nil
}

// helpers
func (p *GenericPeer) setLocalAddr(listenAddr string) {
	listenPort := listenAddr
	if strings.Contains(listenAddr, ":") {
		listenPort = strings.Split(listenAddr, ":")[1]
	}
	if strings.Contains(listenPort, ":") || strings.Contains(listenPort, ".") {
		panic(fmt.Sprintf("Invalid peer port number: %s", listenPort))
	}
	// TODO: validate port: no dots nor colons plz
	p.addr = getServiceAddr(listenPort)
}

func UnmarshalPeer(key string, peerData []byte) (Peer, error) {
	_, serviceName, _, err := spreadKey(key)
	if err != nil {
		return nil, err
	}

	var peer Peer
	switch serviceName {
	case SearchServiceName:
		peer = &SearchPeer{}
	case RelayerServiceName:
		peer = &RelayerPeer{}
	default:
		return nil, fmt.Errorf("unsupported dmesh service type: %s", serviceName)
	}

	err = json.Unmarshal(peerData, &peer)
	if err != nil {
		return nil, err
	}

	if peer == nil {
		return nil, fmt.Errorf("unable to decode peer: %s", string(peerData))
	}

	peer.SetKey(key)
	return peer, nil
}

func spreadKey(key string) (serviceVersion string, serviceName string, addr string, err error) {
	// /v2/search/10.1.138.75:9102
	fullKey := strings.TrimPrefix(string(key), "/")
	// ['v2', 'search', '10.1.138.75:9102']
	chunks := strings.Split(fullKey, "/")

	// sanity check
	if len(chunks) != 3 {
		return "", "", "", fmt.Errorf("unsupported dmesh peer getKey: %s", key)
	}
	return chunks[0], chunks[1], chunks[2], nil
}
