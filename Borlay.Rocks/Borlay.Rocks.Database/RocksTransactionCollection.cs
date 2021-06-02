using System;
using System.Collections.Generic;
using System.Text;

namespace Borlay.Rocks.Database
{
    public class RocksTransactionCollection : IDisposable
    {
        private readonly IDisposable disposable;

        public Dictionary<Guid, RocksTransaction> Transactions { get; }

        public RocksTransactionCollection(Dictionary<Guid, RocksTransaction> transactions, IDisposable disposable)
        {
            this.disposable = disposable;
            this.Transactions = transactions ?? throw new ArgumentNullException(nameof(transactions));
        }

        public RocksTransaction this[Guid parentId] => Transactions[parentId];

        public void Commit(params Guid[] parentIds)
        {
            if (parentIds.Length == 0)
                throw new ArgumentException($"Cannot commit transactions because parent ids array is empty.");

            foreach (var parentId in parentIds)
                Transactions[parentId].Commit();
        }

        public void Dispose()
        {
            foreach (var transaction in Transactions)
            {
                try
                {
                    transaction.Value.Dispose();
                }
                catch(Exception e)
                {
                    Console.WriteLine(e.ToString());
                }
            }

            if(disposable != null)
            {
                try
                {
                    disposable.Dispose();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                }
            }
        }
    }
}
