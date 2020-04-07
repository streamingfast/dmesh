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
	"time"
)

type SearchPeer struct {
	GenericPeer
	BlockRangeData

	ServesResolveForks bool `json:"resolveForks,omitempty"` // peer can resolve forks
	ServesReversible   bool `json:"reversible,omitempty"`   // peer serves the reversible segment of the chain, and is capable of navigating it
	HasMovingHead      bool `json:"headMoves,omitempty"`    // false for frozen ranges
	HasMovingTail      bool `json:"tailMoves,omitempty"`    // whether the process truncates its lower blocks

	publishPeerNow func(peer Peer) error

	ShardSize uint64 `json:"shardSize"`
	TierLevel uint32 `json:"tier"` // Frozen archives, first segments, are lower tiers (0, 1), and those at the tip are higher, more chances we roll out new ones at the tip of the chain.
}

func NewSearchForkResolverPeer(serviceVersion, listenAddr string, pollingDuration time.Duration) *SearchPeer {
	return &SearchPeer{
		GenericPeer:        newServiceData(serviceVersion, SearchServiceName, listenAddr, pollingDuration),
		ServesResolveForks: true,
	}
}

func NewSearchArchivePeer(serviceVersion, listenAddr string, hasMovingTail, hasMovingHead bool, shardSize uint64, tierLevel uint32, pollingDuration time.Duration) *SearchPeer {
	return &SearchPeer{
		GenericPeer:   newServiceData(serviceVersion, SearchServiceName, listenAddr, pollingDuration),
		HasMovingHead: hasMovingHead,
		HasMovingTail: hasMovingTail,
		ShardSize:     shardSize,
		TierLevel:     tierLevel,
	}
}

func NewSearchHeadPeer(serviceVersion, listenAddr string, shardSize uint64, tierLevel uint32, pollingDuration time.Duration) *SearchPeer {
	return &SearchPeer{
		GenericPeer:      newServiceData(serviceVersion, SearchServiceName, listenAddr, pollingDuration),
		HasMovingHead:    true,
		HasMovingTail:    true,
		ServesReversible: true,
		ShardSize:        shardSize,
		TierLevel:        tierLevel,
	}
}

func (p *SearchPeer) VirtualHead() uint64 {
	if p.ServesReversible {
		return p.HeadBlock
	}
	return p.IrrBlock
}

func (p *SearchPeer) Merge(in Peer) {
	p.Lock()
	defer p.Unlock()

	other := in.(*SearchPeer)
	p.Boot = other.Boot
	p.TailBlock = other.TailBlock
	p.TailBlockID = other.TailBlockID
	p.TailBlockTime = copyTimePointer(other.TailBlockTime)
	p.IrrBlock = other.IrrBlock
	p.IrrBlockID = other.IrrBlockID
	p.IrrBlockTime = copyTimePointer(other.IrrBlockTime)
	p.HeadBlock = other.HeadBlock
	p.HeadBlockID = other.HeadBlockID
	p.HeadBlockTime = copyTimePointer(other.HeadBlockTime)
	p.Ready = other.Ready
}

func (p *SearchPeer) Locked(f func()) Peer {
	p.Lock()
	defer p.Unlock()
	f()
	return p
}
