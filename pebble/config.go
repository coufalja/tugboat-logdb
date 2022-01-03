package pebble

import (
	"reflect"

	"github.com/lni/vfs"
)

const defaultLogDBShards uint64 = 16

// LogDBConfig is the configuration object for the LogDB storage engine. This
// config option is only for advanced users when tuning the balance of I/O
// performance and memory consumption.
//
// All KV* fields in LogDBConfig had their names derived from RocksDB options,
// please check RocksDB Tuning Guide wiki for more details.
//
// KVWriteBufferSize and KVMaxWriteBufferNumber are two parameters that directly
// affect the upper bound of memory size used by the built-in LogDB storage
// engine.
type LogDBConfig struct {
	FS                                 vfs.FS
	Shards                             uint64
	KVKeepLogFileNum                   uint64
	KVMaxBackgroundCompactions         uint64
	KVMaxBackgroundFlushes             uint64
	KVLRUCacheSize                     uint64
	KVWriteBufferSize                  uint64
	KVMaxWriteBufferNumber             uint64
	KVLevel0FileNumCompactionTrigger   uint64
	KVLevel0SlowdownWritesTrigger      uint64
	KVLevel0StopWritesTrigger          uint64
	KVMaxBytesForLevelBase             uint64
	KVMaxBytesForLevelMultiplier       uint64
	KVTargetFileSizeBase               uint64
	KVTargetFileSizeMultiplier         uint64
	KVLevelCompactionDynamicLevelBytes uint64
	KVRecycleLogFileNum                uint64
	KVNumOfLevels                      uint64
	KVBlockSize                        uint64
	SaveBufferSize                     uint64
	MaxSaveBufferSize                  uint64
}

// LogDBCallback is a callback function called by the LogDB.
type LogDBCallback func(busy bool)

// GetDefaultLogDBConfig returns the default configurations for the LogDB
// storage engine. The default LogDB configuration use up to 8GBytes memory.
func GetDefaultLogDBConfig() LogDBConfig {
	return GetLargeMemLogDBConfig()
}

// GetTinyMemLogDBConfig returns a LogDB config aimed for minimizing memory
// size. When using the returned config, LogDB takes up to 256MBytes memory.
func GetTinyMemLogDBConfig() LogDBConfig {
	cfg := getDefaultLogDBConfig()
	cfg.KVWriteBufferSize = 4 * 1024 * 1024
	cfg.KVMaxWriteBufferNumber = 4
	return cfg
}

// GetSmallMemLogDBConfig returns a LogDB config aimed to keep memory size at
// low level. When using the returned config, LogDB takes up to 1GBytes memory.
func GetSmallMemLogDBConfig() LogDBConfig {
	cfg := getDefaultLogDBConfig()
	cfg.KVWriteBufferSize = 16 * 1024 * 1024
	cfg.KVMaxWriteBufferNumber = 4
	return cfg
}

// GetMediumMemLogDBConfig returns a LogDB config aimed to keep memory size at
// medium level. When using the returned config, LogDB takes up to 4GBytes
// memory.
func GetMediumMemLogDBConfig() LogDBConfig {
	cfg := getDefaultLogDBConfig()
	cfg.KVWriteBufferSize = 64 * 1024 * 1024
	cfg.KVMaxWriteBufferNumber = 4
	return cfg
}

// GetLargeMemLogDBConfig returns a LogDB config aimed to keep memory size to be
// large for good I/O performance. It is the default setting used by the system.
// When using the returned config, LogDB takes up to 8GBytes memory.
func GetLargeMemLogDBConfig() LogDBConfig {
	return getDefaultLogDBConfig()
}

func getDefaultLogDBConfig() LogDBConfig {
	return LogDBConfig{
		FS:                                 vfs.Default,
		Shards:                             defaultLogDBShards,
		KVMaxBackgroundCompactions:         2,
		KVMaxBackgroundFlushes:             2,
		KVLRUCacheSize:                     0,
		KVKeepLogFileNum:                   16,
		KVWriteBufferSize:                  128 * 1024 * 1024,
		KVMaxWriteBufferNumber:             4,
		KVLevel0FileNumCompactionTrigger:   8,
		KVLevel0SlowdownWritesTrigger:      17,
		KVLevel0StopWritesTrigger:          24,
		KVMaxBytesForLevelBase:             4 * 1024 * 1024 * 1024,
		KVMaxBytesForLevelMultiplier:       2,
		KVTargetFileSizeBase:               16 * 1024 * 1024,
		KVTargetFileSizeMultiplier:         2,
		KVLevelCompactionDynamicLevelBytes: 0,
		KVRecycleLogFileNum:                0,
		KVNumOfLevels:                      7,
		KVBlockSize:                        32 * 1024,
		SaveBufferSize:                     32 * 1024,
		MaxSaveBufferSize:                  64 * 1024 * 1024,
	}
}

// MemorySizeMB returns the estimated upper bound memory size used by the LogDB
// storage engine. The returned value is in MBytes.
func (cfg *LogDBConfig) MemorySizeMB() uint64 {
	ss := cfg.KVWriteBufferSize * cfg.KVMaxWriteBufferNumber
	bs := ss * cfg.Shards
	return bs / (1024 * 1024)
}

// IsEmpty returns a boolean value indicating whether the LogDBConfig instance
// is empty.
func (cfg *LogDBConfig) IsEmpty() bool {
	return reflect.DeepEqual(cfg, &LogDBConfig{})
}
