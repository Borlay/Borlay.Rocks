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
