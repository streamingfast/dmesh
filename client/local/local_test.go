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
	"testing"

	"github.com/dfuse-io/dmesh"
	"github.com/stretchr/testify/assert"
)

func TestLocal_AllSearchPeers(t *testing.T) {
	tests := []struct {
		name              string
		peers             map[string]dmesh.Peer
		expectSearchPeers []*dmesh.SearchPeer
	}{
		{
			name: "golden road",
			peers: map[string]dmesh.Peer{
				"v1/search/123": &dmesh.SearchPeer{
					GenericPeer:      dmesh.NewTestGenericPeer("v1", "search", "123"),
					BlockRangeData:   dmesh.NewTestBlockRangeData(1, 2, 3),
					ServesReversible: true,
					ShardSize:        5000,
					TierLevel:        10,
				},
			},
			expectSearchPeers: []*dmesh.SearchPeer{
				{
					GenericPeer:      dmesh.NewTestGenericPeer("v1", "search", "123"),
					BlockRangeData:   dmesh.NewTestBlockRangeData(1, 2, 3),
					ServesReversible: true,
					ShardSize:        5000,
					TierLevel:        10,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			local := &Local{
				allPeers: test.peers,
			}

			assert.Equal(t, test.expectSearchPeers, local.Peers())
		})
	}
}

func TestLocal_RegisterPeer(t *testing.T) {
	tests := []struct {
		name        string
		peer        dmesh.Peer
		expectPeers map[string]dmesh.Peer
	}{
		{
			name: "golden road",
			peer: &dmesh.SearchPeer{
				GenericPeer:      dmesh.NewTestGenericPeer("v1", "search", "123"),
				BlockRangeData:   dmesh.NewTestBlockRangeData(1, 2, 3),
				ServesReversible: true,
				ShardSize:        5000,
				TierLevel:        10,
			},
			expectPeers: map[string]dmesh.Peer{
				"/v1/search/123": &dmesh.SearchPeer{
					GenericPeer:      dmesh.NewTestGenericPeer("v1", "search", "123"),
					BlockRangeData:   dmesh.NewTestBlockRangeData(1, 2, 3),
					ServesReversible: true,
					ShardSize:        5000,
					TierLevel:        10,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			local := &Local{
				allPeers: map[string]dmesh.Peer{},
			}
			local.RegisterPeer(test.peer)
			for key, peer := range local.allPeers {
				(peer.(dmesh.ConnectablePeer)).SetConn(nil)
				assert.Equal(t, test.expectPeers[key], peer)
			}
		})
	}
}

func TestLocal_PublishePeer(t *testing.T) {
	tests := []struct {
		name        string
		peers       map[string]dmesh.Peer
		peer        dmesh.Peer
		expectPeers map[string]dmesh.Peer
	}{
		{
			name: "golden road",
			peers: map[string]dmesh.Peer{
				"/v1/search/123": &dmesh.SearchPeer{
					GenericPeer:      dmesh.NewTestGenericPeer("v1", "search", "123"),
					BlockRangeData:   dmesh.NewTestBlockRangeData(1, 2, 3),
					ServesReversible: true,
					ShardSize:        5000,
					TierLevel:        10,
				},
			},
			peer: &dmesh.SearchPeer{
				GenericPeer:      dmesh.NewTestGenericPeer("v1", "search", "123"),
				BlockRangeData:   dmesh.NewTestBlockRangeData(4, 8, 10),
				ServesReversible: true,
				ShardSize:        5000,
				TierLevel:        10,
			},
			expectPeers: map[string]dmesh.Peer{
				"/v1/search/123": &dmesh.SearchPeer{
					GenericPeer:      dmesh.NewTestGenericPeer("v1", "search", "123"),
					BlockRangeData:   dmesh.NewTestBlockRangeData(4, 8, 10),
					ServesReversible: true,
					ShardSize:        5000,
					TierLevel:        10,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			local := &Local{
				allPeers: test.peers,
			}
			local.PublishNow(test.peer)
			for key, peer := range local.allPeers {
				(peer.(dmesh.ConnectablePeer)).SetConn(nil)
				assert.Equal(t, test.expectPeers[key], peer)
			}
		})
	}
}
