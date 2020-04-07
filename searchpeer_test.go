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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSearchPeerMarshal(t *testing.T) {
	tests := []struct {
		name      string
		peer      SearchPeer
		expectKey string
		expect    map[string]interface{}
	}{
		{
			name: "simple",
			peer: SearchPeer{
				GenericPeer:      newTestGenericPeer("v1", "search", "123"),
				BlockRangeData:   newTestBlockRangeData(1, 2, 3),
				ServesReversible: true,
				ShardSize:        5000,
				TierLevel:        10,
			},
			expectKey: "/v1/search/123",
			expect: map[string]interface{}{
				"boot":         "2020-01-01T00:00:00Z",
				"tailBlockNum": 1,
				"irrBlockNum":  2,
				"headBlockNum": 3,
				"reversible":   true,
				"shardSize":    5000,
				"tier":         10,
			},
		},
		{
			name: "full",
			peer: SearchPeer{
				GenericPeer: newTestGenericPeer("v1", "search", "123"),
				BlockRangeData: newTestBlockRangeDataFull(
					1, "00000001", time.Date(2015, 9, 18, 0, 0, 1, 0, time.UTC), // tail
					2, "00000002", time.Date(2015, 9, 18, 0, 0, 2, 0, time.UTC), // irr
					3, "00000003", time.Date(2015, 9, 18, 0, 0, 3, 0, time.UTC), // head
				),
				ServesReversible: true,
				ShardSize:        5000,
				TierLevel:        10,
			},
			expectKey: "/v1/search/123",
			expect: map[string]interface{}{
				"boot":          "2020-01-01T00:00:00Z",
				"tailBlockNum":  1,
				"tailBlockID":   "00000001",
				"tailBlockTime": "2015-09-18T00:00:01Z",
				"irrBlockNum":   2,
				"irrBlockID":    "00000002",
				"irrBlockTime":  "2015-09-18T00:00:02Z",
				"headBlockNum":  3,
				"headBlockID":   "00000003",
				"headBlockTime": "2015-09-18T00:00:03Z",
				"reversible":    true,
				"shardSize":     5000,
				"tier":          10,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := json.Marshal(test.peer)
			require.NoError(t, err)
			expected, _ := json.Marshal(test.expect)
			assert.JSONEq(t, string(expected), string(res))
			assert.Equal(t, test.expectKey, test.peer.Key())
		})
	}
}
