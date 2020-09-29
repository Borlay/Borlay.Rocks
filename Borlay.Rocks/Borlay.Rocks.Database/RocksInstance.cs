using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Borlay.Rocks.Database
{
    public class RocksInstance
    {
        public static Cache Cache = Cache.CreateLru(4 * SizeUnit.GB);

        public int Index { get; }

        public RocksDb Database { get; }

        //public ColumnFamilyHandle userFamily { get; }
        //public ColumnFamilyHandle channelFamily { get; }
        //public ColumnFamilyHandle channelMessageFamily { get; }
        //public ColumnFamilyHandle userChannelFamily { get; }

        public IDictionary<string, ColumnFamilyHandle> Families { get; }

        public RocksInstance(string directory, ulong walTtlInSeconds, int shardIndex, bool directIO, Recovery recovery, params (string, ColumnFamilyOptions)[] families)
        {
            var dataDirectory = directory.CreateDirectory("data");
            var walDirectory = directory.CreateDirectory("wal");

            //var tableOptions = new BlockBasedTableOptions().SetDefaultOptions();
            //var familyOptions = new ColumnFamilyOptions().SetDefaultOptions(tableOptions);

            var _families = new ColumnFamilies(null); //familyOptions);

            foreach (var family in families)
                _families.Add(family.Item1, family.Item2);

            //families.Add("users", familyOptions);
            //families.Add("channels", familyOptions);
            //families.Add("channel-messages", familyOptions);
            //families.Add("user-channels", familyOptions);


            var dbOptions = new DbOptions().SetDefaultOptions(true, directIO);
            dbOptions = dbOptions
                .SetWalRecoveryMode(recovery)
                .SetWalDir(walDirectory)
                .SetWALTtlSeconds(walTtlInSeconds);

            this.Database = RocksDb.Open(dbOptions, dataDirectory, _families);


            //userFamily = this.db.GetColumnFamily("users");
            //channelFamily = this.db.GetColumnFamily("channels");
            //channelMessageFamily = this.db.GetColumnFamily("channel-messages");
            //userChannelFamily = this.db.GetColumnFamily("user-channels");

            Families = families.ToDictionary(f => f.Item1, f => this.Database.GetColumnFamily(f.Item1));

            this.Index = shardIndex;
        }

        //public static RocksInstance Create(string direcotry, ulong walTtlInSeconds, int shardIndex)
        //{
        //    return new RocksInstance(Path.Combine(direcotry, $"{shardIndex}"), walTtlInSeconds, shardIndex);
        //}

        //public static RocksInstance Create(string direcotry, ulong walTtlInSeconds, int shardIndex)
        //{
        //    return new RocksInstance(Path.Combine(direcotry, $"{shardIndex}"), walTtlInSeconds, shardIndex);
        //}

        public static RocksInstance Create(string direcotry, ulong walTtlInSeconds, int shardIndex, bool directIO, Recovery recovery, params (string, ColumnFamilyOptions)[] families)
        {
            return new RocksInstance(Path.Combine(direcotry, $"{shardIndex}"), walTtlInSeconds, shardIndex, directIO, recovery, families);
        }

        //public static ColumnFamilyOptions Create(string columnFamily, bool sortedRecords, bool cacheAllIndexes = false, bool ssdDisc = true, ulong baseSizeInMB = 256)
        //{
        //    var tableOptions = new BlockBasedTableOptions().SetDefaultOptions(sortedRecords, cacheAllIndexes, ssdDisc);
        //    var familyOptions = new ColumnFamilyOptions().SetDefaultOptions(tableOptions, baseSizeInMB);
        //    return familyOptions;
        //}
    }
}
