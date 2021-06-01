using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using RocksDbSharp;
using System.Threading.Tasks;

namespace Borlay.Rocks.Database
{
    public class RocksRepository //: ISortedRepository
    {
        protected readonly IDictionary<int, RocksInstance> instances;

        //public RocksRepository(string directory, ulong walTtlInSeconds, int shards = 1)
        //{
        //    instances = Enumerable.Range(0, shards)
        //        .Select(i => RocksInstance.Create(directory, walTtlInSeconds, i))
        //        .ToDictionary(r => r.Index);
        //}

        public RocksRepository(IDictionary<int, RocksInstance> instances)
        {
            this.instances = instances ?? throw new ArgumentNullException(nameof(instances));
        }

        public virtual string GetStatistics(int shardIndex)
        {
            var st = this.instances[shardIndex].Database.GetProperty("rocksdb.stats");
            return st;
        }

        public virtual bool TryGetInstance(int shardIndex, out RocksInstance instance)
        {
            return instances.TryGetValue(shardIndex, out instance);
        }

        public virtual RocksInstance GetInstance(int shardIndex)
        {
            return instances[shardIndex];
        }

        public virtual RocksInstance GetInstance(Guid shardKey, out int shardIndex)
        {
            return instances.GetInstance(shardKey, out shardIndex);
        }

        public virtual async Task<RocksTransaction> WaitTransactionAsync(Guid shardKey)
        {
            var disposeAction = await AsyncLock.WaitAsync(shardKey);

            try
            {
                var instance = instances.GetInstance(shardKey, out var shardIndex);
                return new RocksTransaction(instance, shardIndex, disposeAction.Dispose);
            }
            catch
            {
                disposeAction.Dispose();
                throw;
            }
        }

        public virtual async Task<RocksTransactionCollection> WaitTransactionsAsync(params Guid[] shardKeys)
        {
            var disposeAction = await AsyncLock.WaitAsync(shardKeys);
            try
            {
                Dictionary<int, RocksTransaction> sharedTransactions = new Dictionary<int, RocksTransaction>();
                var transactions = shardKeys.ToDictionary(k => k, k =>
                {
                    var instance = instances.GetInstance(k, out var shardIndex);
                    if (sharedTransactions.TryGetValue(shardIndex, out var transaction))
                        return transaction;

                    transaction = new RocksTransaction(instance, shardIndex, null);
                    sharedTransactions[shardIndex] = transaction;
                    return transaction;
                });

                return new RocksTransactionCollection(transactions, disposeAction);
            }
            catch
            {
                disposeAction.Dispose();
                throw;
            }
        }

        ///// <summary>
        ///// Get Records in order. Shard index is calculated from parentId
        ///// </summary>
        ///// <typeparam name="T">Record type</typeparam>
        ///// <param name="parentId">Parent id and shard key of sorted records</param>
        ///// <param name="position"></param>
        ///// <param name="columnFamily"></param>
        ///// <returns></returns>
        //public virtual IEnumerable<T> GetRecords<T>(Guid parentId, long position, string columnFamily) where T: ISortableRecord
        //{
        //    var instance = instances.GetInstance(parentId, out var shardIndex);
        //    return instance.Database.GetRecords<T>(parentId, position, instance.Families[columnFamily], true);
        //}

        //public virtual IEnumerable<T> GetRecords<T>(Guid shardKey, Guid parentId, long position, string columnFamily) where T : ISortableRecord
        //{
        //    var instance = instances.GetInstance(shardKey, out var shardIndex);
        //    return instance.Database.GetRecords<T>(parentId, position, instance.Families[columnFamily], true);
        //}


        //public IEnumerable<PrivateChannelInfo> GetChannels(Guid userId, long position)
        //{
        //    var instance = instances.GetInstance(userId, out var shardIndex);
        //    return instance.db.GetRecords<PrivateChannelInfo>(userId, position, instance.userChannelFamily, true);
        //}

        //public IEnumerable<PrivateMessageInfo> GetMessages(Guid userId, Guid channelId, long position)
        //{
        //    var instance = instances.GetInstance(userId, out var shardIndex);
        //    return instance.db.GetRecords<PrivateMessageInfo>(channelId, position, instance.channelMessageFamily, true);
        //}

        //public IEnumerable<Guid> GetChannels()
        //{
        //    var readOptions = new ReadOptions();
        //    readOptions.SetPrefixSameAsStart(false);
        //    readOptions.SetTotalOrderSeek(false);
        //    var iterator = this.instances[0].db.NewIterator(this.instances[0].channelFamily, readOptions);

        //    try
        //    {
        //        iterator = iterator.SeekToFirst();
        //        while (iterator.Valid())
        //        {
        //            var value = iterator.Value();
        //            if (value == null || value.Length != 16) yield break;

        //            yield return new Guid(value);

        //            iterator = iterator.Next();
        //        }

        //    }
        //    finally
        //    {
        //        iterator.Dispose();
        //    }
        //}


    }
}
