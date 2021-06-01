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
    }
}
