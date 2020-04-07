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

import "time"

const RelayerPeerType = "relayer"

type RelayerPeer struct {
	GenericPeer
	HeadBlockData

	HighBlockNum uint64 `json:"high_blk"`
}

const RelayerPeerPollingDuration = (10 * 365 * 24 * time.Hour)

func NewRelayerPeer(serviceVersion, listenAddr string, highBlockNum uint64) *RelayerPeer {
	return &RelayerPeer{
		GenericPeer:  newServiceData(serviceVersion, RelayerServiceName, listenAddr, RelayerPeerPollingDuration),
		HighBlockNum: highBlockNum,
	}
}

func (p *RelayerPeer) Merge(in Peer) {
	p.Lock()
	defer p.Unlock()

	other := in.(*RelayerPeer)
	p.Ready = other.Ready
	p.Boot = other.Boot
	p.IrrBlock = other.IrrBlock
	p.HeadBlock = other.HeadBlock
	p.HighBlockNum = other.HighBlockNum
}

func (p *RelayerPeer) Locked(f func()) Peer {
	p.Lock()
	defer p.Unlock()
	f()
	return p
}
