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

func NewTestGenericPeer(serviceVersion, serviceName, addr string) GenericPeer {
	return newTestGenericPeer(serviceVersion, serviceName, addr)
}

func NewTestReadyGenericPeer(serviceVersion, serviceName, addr string) GenericPeer {
	s := newTestGenericPeer(serviceVersion, serviceName, addr)
	s.Ready = true
	return s
}

func NewTestBlockRangeData(tail, irr, head uint64) BlockRangeData {
	return newTestBlockRangeData(tail, irr, head)
}

func newTestGenericPeer(serviceVersion, serviceName, addr string) GenericPeer {

	now, err := time.Parse(time.RFC3339, "2020-01-01T00:00:00Z")
	if err != nil {
		panic(err)
	}

	return GenericPeer{
		serviceName:    serviceName,
		serviceVersion: serviceVersion,
		addr:           addr,
		Boot:           &now,
	}
}

func newTestBlockRangeData(tail, irr, head uint64) BlockRangeData {
	return BlockRangeData{
		TailBlock: tail,
		HeadBlockData: HeadBlockData{
			IrrBlock:  irr,
			HeadBlock: head,
		},
	}
}

func newTestBlockRangeDataFull(
	tail uint64, tailID string, tailTime time.Time,
	irr uint64, irrID string, irrTime time.Time,
	head uint64, headID string, headTime time.Time,
) BlockRangeData {
	return BlockRangeData{
		TailBlock:     tail,
		TailBlockID:   tailID,
		TailBlockTime: &tailTime,
		HeadBlockData: HeadBlockData{
			IrrBlock:      irr,
			IrrBlockID:    irrID,
			IrrBlockTime:  &irrTime,
			HeadBlock:     head,
			HeadBlockID:   headID,
			HeadBlockTime: &headTime,
		},
	}
}
