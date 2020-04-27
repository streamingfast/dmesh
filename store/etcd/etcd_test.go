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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/dfuse-io/dmesh"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEtcd_deletePeer(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		peers       map[string]dmesh.Peer
		expectPeers map[string]dmesh.Peer
		expectError bool
	}{
		{
			name: "peer is present to delete",
			key:  "/v2/search-liverouter-2/10.1.138.75:9000",
			peers: map[string]dmesh.Peer{
				"/v2/search-liverouter-2/10.1.138.75:9000": &dmesh.SearchPeer{
					GenericPeer: dmesh.NewTestGenericPeer("v2", "search-liverouter-2", "10.1.138.75:9000"),
				},
			},
			expectPeers: map[string]dmesh.Peer{},
		},
		{
			name: "peer is not present to delete",
			key:  "/v2/search-archive-1/192.168.1.1:9000",
			peers: map[string]dmesh.Peer{
				"/v2/search-liverouter-2/10.1.138.75:9000": &dmesh.SearchPeer{
					GenericPeer: dmesh.NewTestGenericPeer("v2", "search-liverouter-2", "10.1.138.75:9000"),
				},
			},
			expectPeers: map[string]dmesh.Peer{
				"/v2/search-liverouter-2/10.1.138.75:9000": &dmesh.SearchPeer{
					GenericPeer: dmesh.NewTestGenericPeer("v2", "search-liverouter-2", "10.1.138.75:9000"),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			etcd := &Etcd{
				allPeers: test.peers,
			}
			err := etcd.deletePeer([]byte(test.key))
			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, test.expectPeers, etcd.allPeers)
		})
	}
}

func TestEtcd_trackPeer(t *testing.T) {
	tests := []struct {
		name        string
		key         []byte
		peer        []byte
		peers       map[string]dmesh.Peer
		expectPeers map[string]dmesh.Peer
		expectError bool
	}{
		{
			name:  "new peer",
			key:   []byte("/v2/search/10.1.138.75:9000"),
			peer:  []byte("{\"boot\":\"2020-01-01T00:00:00Z\",\"tailBlockNum\":12,\"irrBlockNum\":19,\"headBlockNum\":34,\"shardSize\":50,\"tier\":50}"),
			peers: map[string]dmesh.Peer{},
			expectPeers: map[string]dmesh.Peer{
				"/v2/search/10.1.138.75:9000": &dmesh.SearchPeer{
					GenericPeer:    dmesh.NewTestGenericPeer("v2", "search", "10.1.138.75:9000"),
					BlockRangeData: dmesh.NewTestBlockRangeData(12, 19, 34),
					ShardSize:      50,
					TierLevel:      50,
				},
			},
		},
		{
			name:        "new peer with invalid data",
			key:         []byte("/v2/search/10.1.138.75:9000"),
			peer:        []byte("jhgf{\"boops2020-01-01T00:00:00Z\",\"tailBlockNum\":12,\"irrBlockNum\":19,\"headBlockNum\":34,\"shardSize\":50,\"tier\":50}"),
			peers:       map[string]dmesh.Peer{},
			expectError: true,
		},
		{
			name: "existing peer peer with invalid data",
			key:  []byte("/v2/search/10.1.138.75:9000"),
			peer: []byte("{\"boot\":\"2020-01-01T00:00:00Z\",\"tailBlockNum\":12,\"irrBlockNum\":23,\"headBlockNum\":45,\"shardSize\":50,\"tier\":50}"),
			peers: map[string]dmesh.Peer{
				"/v2/search/10.1.138.75:9000": &dmesh.SearchPeer{
					GenericPeer:    dmesh.NewTestGenericPeer("v2", "search", "10.1.138.75:9000"),
					BlockRangeData: dmesh.NewTestBlockRangeData(12, 19, 34),
					ShardSize:      50,
					TierLevel:      50,
				},
			},
			expectPeers: map[string]dmesh.Peer{
				"/v2/search/10.1.138.75:9000": &dmesh.SearchPeer{
					GenericPeer:    dmesh.NewTestGenericPeer("v2", "search", "10.1.138.75:9000"),
					BlockRangeData: dmesh.NewTestBlockRangeData(12, 23, 45),
					ShardSize:      50,
					TierLevel:      50,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			etcd := &Etcd{
				allPeers: test.peers,
			}
			err := etcd.trackPeer(test.key, test.peer)
			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				for key, peer := range etcd.allPeers {
					(peer.(dmesh.ConnectablePeer)).SetConn(nil)
					assert.Equal(t, test.expectPeers[key], peer)
				}

			}
		})
	}
}

func mustMarshalPeer(peer dmesh.Peer) []byte {
	bytes, err := json.Marshal(peer)
	if err != nil {
		panic("unable to marshal peer")
	}
	fmt.Println("", string(bytes))
	return bytes
}
