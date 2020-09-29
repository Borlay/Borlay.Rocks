using Borlay.Rocks.Database;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Borlay.Rocks.Tests
{
    [TestClass]
    public class RocksTests
    {
        //private readonly RocksRepository repository;

        public RocksTests()
        {
            //var builde = new DatabaseBuilder(@"C:\rocks\tests", 0, 1, true, RocksDbSharp.Recovery.AbsoluteConsistency, true);
            //repository = builde.Entity<TestEntity>(e => e.Id.ToByteArray(8), Order.Descending, false).CreateRepository();
        }

        [TestMethod]
        public async Task TestMethod1()
        {
            var builde = new DatabaseBuilder(@"C:\rocks\tests", 0, 1, true, RocksDbSharp.Recovery.TolerateCorruptedTailRecords, true);
            var repository = builde.Entity<TestEntity>(e => e.Id.ToByteArray(8), Order.Descending, false).CreateRepository();

            var parentId = Guid.NewGuid();

            var entity1 = new TestEntity()
            {
                Id = Guid.NewGuid(),
                Position = 0,
                Value = "Test 1",
            };

            var entity2 = new TestEntity()
            {
                Id = Guid.NewGuid(),
                Position = 0,
                Value = "Test 1",
            };

            using (var transaction = await repository.WaitTransactionAsync(parentId))
            {
                //transaction.Batch.wr

                transaction.SaveEntity(parentId, entity1);
                transaction.NextPosition();
                transaction.SaveEntity(parentId, entity2);

                transaction.Commit();
            }

            using (var transaction = await repository.WaitTransactionAsync(parentId))
            {
                var entities = transaction.GetEntities<TestEntity>(parentId, Order.Descending).ToArray();

                Assert.IsNotNull(entities);
                Assert.AreEqual(2, entities.Length);
                Assert.AreEqual(entity2.Id, entities[0].Id);
                Assert.AreEqual(entity1.Id, entities[1].Id);
            }
        }
    }

    public class TestEntity : IEntity
    {
        public long Position { get; set; }

        public Guid Id { get; set; }

        public string Value { get; set; }

        public Guid GetEntityId() => Id;
    }
}
