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

type BlockRangeData struct {
	TailBlock     uint64     `json:"tailBlockNum,omitempty"`
	TailBlockID   string     `json:"tailBlockID,omitempty"`
	TailBlockTime *time.Time `json:"tailBlockTime,omitempty"`
	HeadBlockData
}

type HeadBlockData struct {
	IrrBlock      uint64     `json:"irrBlockNum,omitempty"`
	IrrBlockID    string     `json:"irrBlockID,omitempty"`
	IrrBlockTime  *time.Time `json:"irrBlockTime,omitempty"`
	HeadBlock     uint64     `json:"headBlockNum,omitempty"`
	HeadBlockID   string     `json:"headBlockID,omitempty"`
	HeadBlockTime *time.Time `json:"headBlockTime,omitempty"`
}

func (d BlockRangeData) Blocks() (tail, irr, head uint64) {
	return d.TailBlock, d.IrrBlock, d.HeadBlock
}

func (d BlockRangeData) HeadBlockPointers() (uint64, string, uint64, string, uint64, string) {
	return d.TailBlock, d.TailBlockID, d.IrrBlock, d.IrrBlockID, d.HeadBlock, d.HeadBlockID
}

func copyTimePointer(from *time.Time) *time.Time {
	if from == nil {
		return nil
	}
	to := time.Unix(0, from.UnixNano())
	return &to
}
