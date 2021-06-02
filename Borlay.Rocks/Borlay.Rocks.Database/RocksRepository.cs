using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using RocksDbSharp;
using System.Threading.Tasks;

namespace Borlay.Rocks.Database
{
    public class RocksRepository
    {
        protected readonly IDictionary<int, RocksInstance> instances;

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

        public virtual RocksInstance GetInstance(Guid parentId, out int shardIndex)
        {
            return instances.GetInstance(parentId, out shardIndex);
        }

        public virtual RocksTransaction CreateTransaction(Guid parentId)
        {
            var instance = instances.GetInstance(parentId, out var shardIndex);
            return new RocksTransaction(instance, parentId, shardIndex, () => { });
        }

        public virtual async Task<RocksTransaction> WaitTransactionAsync(Guid parentId)
        {
            var disposeAction = await AsyncLock.WaitAsync(parentId);

            try
            {
                var instance = instances.GetInstance(parentId, out var shardIndex);
                return new RocksTransaction(instance, parentId, shardIndex, disposeAction.Dispose);
            }
            catch
            {
                disposeAction.Dispose();
                throw;
            }
        }

        public virtual async Task<RocksTransactionCollection> WaitTransactionsAsync(params Guid[] parentIds)
        {
            var disposeAction = await AsyncLock.WaitAsync(parentIds);
            try
            {
                var transactions = parentIds.ToDictionary(k => k, k =>
                {
                    var instance = instances.GetInstance(k, out var shardIndex);
                    var transaction = new RocksTransaction(instance, k, shardIndex, null);
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
    }
}
