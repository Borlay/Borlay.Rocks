using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Borlay.Rocks.Database
{
    public class DatabaseBuilder
    {
        private readonly string directory;
        private readonly ulong walTtlInSeconds;
        private readonly int shardCount;
        private readonly bool directIO;
        private readonly Recovery recovery;
        private readonly bool ssdDisc = true;

        private List<(string, ColumnFamilyOptions)> families = new List<(string, ColumnFamilyOptions)>();

        public DatabaseBuilder(string directory, ulong walTtlInSeconds, int shardCount, bool directIO, Recovery recovery, bool ssdDisc = true)
        {
            this.directory = directory;
            this.walTtlInSeconds = walTtlInSeconds;
            this.shardCount = shardCount;
            this.directIO= directIO;
            this.recovery = recovery;
            this.ssdDisc = ssdDisc;
        }

        public DatabaseBuilder AddColumnFamily(string name, bool optimizeForSequentialRead, bool cacheAllIndexes = false)
        {
            var tableOptions = new BlockBasedTableOptions().SetDefaultOptions(optimizeForSequentialRead ? false : true, cacheAllIndexes, ssdDisc ? 16 : 64);
            var familyOptions = new ColumnFamilyOptions().SetDefaultOptions(tableOptions, (ssdDisc ? 256 : 512));

            families.Add((name, familyOptions));

            return this;
        }

        public IDictionary<int, RocksInstance> CreateInstances()
        {
            var instances = Enumerable.Range(0, shardCount)
                .Select(i => RocksInstance.Create(directory, walTtlInSeconds, i, directIO, recovery, families.ToArray()))
                .ToDictionary(r => r.Index);

            return instances;
        }

        public RocksRepository CreateRepository()
        {
            var instances = CreateInstances();
            return new RocksRepository(instances);
        }
    }
}
