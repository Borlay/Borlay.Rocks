using RocksDbSharp;
using System;

namespace Borlay.Rocks.Database
{
    public static class RocksOptionsExtensions
    {
        public static BlockBasedTableOptions SetDefaultOptions(this BlockBasedTableOptions options, bool wholeKeyFiltering, bool cacheAllIndexes, int blockSizeInKB = 16)
        {
            options = options.SetIndexType(BlockBasedTableIndexType.Hash)

                .SetNoBlockCache(false)
                .SetBlockSize((ulong)blockSizeInKB * SizeUnit.KB) //ssdDisc ? 16 * SizeUnit.KB : 64)  // 64
                .SetBlockCache(RocksInstance.Cache)

                //.SetBlockCacheCompressed(Cache.CreateLru(2 * SizeUnit.GB).Handle) // todo enable
                //.SetCacheIndexAndFilterBlocks(true)

                .SetPinL0FilterAndIndexBlocksInCache(true)
                .SetFilterPolicy(BloomFilterPolicy.Create(10, false))

                .SetWholeKeyFiltering(wholeKeyFiltering)

                .SetHashIndexAllowCollision(true)
                .SetFormatVersion(4)
            ;

            //if (wholeKeyFiltering)
            //    options.SetWholeKeyFiltering(false);
            //else
            //    options.SetWholeKeyFiltering(true);

            if (!cacheAllIndexes)
            {
                Native.Instance.rocksdb_block_based_options_set_cache_index_and_filter_blocks(options.Handle, true);
                Native.Instance.rocksdb_block_based_options_set_cache_index_and_filter_blocks_with_high_priority(options.Handle, true);
                Native.Instance.rocksdb_block_based_options_set_pin_top_level_index_and_filter(options.Handle, true);
            }
            else
                Native.Instance.rocksdb_block_based_options_set_cache_index_and_filter_blocks(options.Handle, false);

            return options;
        }

        public static ColumnFamilyOptions SetDefaultOptions(this ColumnFamilyOptions options, BlockBasedTableOptions tableOptions, int fixedPrefixLength, int baseSizeInMB = 256)
        {
            options = options
                .SetBlockBasedTableFactory(tableOptions)
                //.SetTargetFileSizeBase(32 * SizeUnit.MB)  // SetMaxBytesForLevelBase / 10
                .SetTargetFileSizeBase((ulong)baseSizeInMB * SizeUnit.MB)  // removed number to merge
                                                                    //.SetTargetFileSizeMultiplier() // default 1
                                                                    //.SetBlockBasedTableFactory(tableConfig)
                                                                    //.SetComparator()
                .SetWriteBufferSize((ulong)baseSizeInMB * SizeUnit.MB)
            .SetMaxWriteBufferNumber(5)
            .SetMinWriteBufferNumberToMerge(2)
            .SetLevel0FileNumCompactionTrigger(10)       // level 0 file count. total size 512 * 2 * 10 (buffer size * to merge * trigger)

            .SetLevelCompactionDynamicLevelBytes(true)

            .SetMaxBytesForLevelBase((ulong)baseSizeInMB * 10 * SizeUnit.MB) // same as level 0 size. // removed number to merge
                                                                      //.SetMaxBytesForLevelMultiplier() // default 10
                                                                      //.SetMemtablePrefixBloomSizeRatio()
            .SetPrefixExtractor(SliceTransform.CreateFixedPrefix((ulong)fixedPrefixLength))

            .SetMemtablePrefixBloomSizeRatio(1)
            .SetCompactionReadaheadSize(64 * SizeUnit.MB)

            //.SetCompression(Compression.Lz4)
            //.SetMinLevelToCompress(2)
            .SetCompressionPerLevel(new Compression[] { Compression.No, Compression.No, Compression.Lz4, Compression.Lz4, Compression.Lz4, Compression.Zstd, Compression.Zstd, Compression.Zstd }, 8)
            //.SetLevelCompactionDynamicLevelBytes(true)
            //.SetReportBgIoStats(true)

            ;



            return options;
        }

        public static DbOptions SetDefaultOptions(this DbOptions options, bool isSharded, bool directIO)
        {
            options = options.SetCreateMissingColumnFamilies(true)
                .SetAllowConcurrentMemtableWrite(false)
                .SetMaxOpenFiles(-1)
                .SetCreateIfMissing(true)
                .SetMaxBackgroundFlushes(isSharded ? 1 : 4)
                .SetMaxBackgroundCompactions(isSharded ? 1 : 8)

            .SetEnv(Env.CreateDefaultEnv().SetBackgroundThreads(4).Handle)
            .SetTableCacheNumShardbits(8)
            //.EnableStatistics()
                .SetUseDirectReads(directIO) // todo recheck
                .SetUseDirectIoForFlushAndCompaction(directIO) // todo recheck

            //.SetWriteBufferSize((ulong)512 * SizeUnit.MB)
            ; // todo recheck

            return options;
        }
    }
}
