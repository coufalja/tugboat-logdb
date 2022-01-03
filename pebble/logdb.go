// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
Package pebble implements the persistent log storage used by Tugboat.
*/
package pebble

import (
	"github.com/coufalja/tugboat/logdb"
	"github.com/coufalja/tugboat/logger"
	pb "github.com/coufalja/tugboat/raftpb"
	"github.com/pkg/errors"
)

var plog = logger.GetLogger("logdb")

// IReusableKey is the interface for keys that can be reused. A reusable key is
// usually obtained by calling the GetKey() function of the IContext
// instance.
type IReusableKey interface {
	// SetEntryKey sets the key to be an entry key for the specified Raft node
	// with the specified entry index.
	SetEntryKey(clusterID uint64, nodeID uint64, index uint64)
	// SetStateKey sets the key to be an persistent state key suitable
	// for the specified Raft cluster node.
	SetStateKey(clusterID uint64, nodeID uint64)
	// SetMaxIndexKey sets the key to be the max possible index key for the
	// specified Raft cluster node.
	SetMaxIndexKey(clusterID uint64, nodeID uint64)
	// Key returns the underlying byte slice of the key.
	Key() []byte
	// Release releases the key instance so it can be reused in the future.
	Release()
}

// IContext is the per thread context used in the logdb module.
// IContext is expected to contain a list of reusable keys and byte
// slices that are owned per thread so they can be safely reused by the same
// thread when accessing ILogDB.
type IContext interface {
	// Destroy destroys the IContext instance.
	Destroy()
	// Reset resets the IContext instance, all previous returned keys and
	// buffers will be put back to the IContext instance and be ready to
	// be used for the next iteration.
	Reset()
	// GetKey returns a reusable key.
	GetKey() IReusableKey
	// GetValueBuffer returns a byte buffer with at least sz bytes in length.
	GetValueBuffer(sz uint64) []byte
	// GetWriteBatch returns a write batch or transaction instance.
	GetWriteBatch() interface{}
	// SetWriteBatch adds the write batch to the IContext instance.
	SetWriteBatch(wb interface{})
	// GetEntryBatch returns an entry batch instance.
	GetEntryBatch() pb.EntryBatch
	// GetLastEntryBatch returns an entry batch instance.
	GetLastEntryBatch() pb.EntryBatch
}

func Factory(config LogDBConfig) func(logdb.LogDBCallback, string, string) *ShardedDB {
	return func(callback logdb.LogDBCallback, nhPath string, walPath string) *ShardedDB {
		logDB, err := NewLogDB(config, callback, []string{nhPath}, []string{walPath}, false)
		if err != nil {
			panic(errors.WithStack(err))
		}
		return logDB
	}
}

// NewLogDB creates a Log DB instance based on provided configuration
// parameters. The underlying KV store used by the Log DB instance is created
// by the provided factory function.
func NewLogDB(config LogDBConfig, callback logdb.LogDBCallback, dirs []string, lldirs []string, check bool) (*ShardedDB, error) {
	checkDirs(config.Shards, dirs, lldirs)
	llDirRequired := len(lldirs) == 1
	if len(dirs) == 1 {
		for i := uint64(1); i < config.Shards; i++ {
			dirs = append(dirs, dirs[0])
			if llDirRequired {
				lldirs = append(lldirs, lldirs[0])
			}
		}
	}
	return OpenShardedDB(config, callback, dirs, lldirs, check)
}

func checkDirs(numOfShards uint64, dirs []string, lldirs []string) {
	if len(dirs) == 1 {
		if len(lldirs) != 0 && len(lldirs) != 1 {
			plog.Panicf("only 1 regular dir but %d low latency dirs", len(lldirs))
		}
	} else if len(dirs) > 1 {
		if uint64(len(dirs)) != numOfShards {
			plog.Panicf("%d regular dirs, but expect to have %d rdb instances",
				len(dirs), numOfShards)
		}
		if len(lldirs) > 0 {
			if len(dirs) != len(lldirs) {
				plog.Panicf("%v regular dirs, but %v low latency dirs", dirs, lldirs)
			}
		}
	} else {
		panic("no regular dir")
	}
}
