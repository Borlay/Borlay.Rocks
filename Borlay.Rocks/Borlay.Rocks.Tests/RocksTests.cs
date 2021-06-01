using Borlay.Rocks.Database;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Diagnostics;
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
            var builder = new DatabaseBuilder(@"C:\rocks\tests", 0, 1, true, RocksDbSharp.Recovery.TolerateCorruptedTailRecords, true);
            builder.Entity<TestEntity>(e => e.Id.ToByteArray(8), Order.Descending, out var descIndex);
            builder.HasIndex<TestEntity>("entity", e => e.Id.ToByteArray(), Order.None, false, out var noneOrderIndex);

            var repository = builder.CreateRepository();

            var watch = Stopwatch.StartNew();

            for (int i = 0; i < 10000; i++)
            {

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
                    transaction.SaveEntity(parentId, entity1);
                    transaction.NextPosition();
                    transaction.SaveEntity(parentId, entity2);

                    Assert.IsTrue(entity1.Position > 0);
                    Assert.IsTrue(entity2.Position > entity1.Position);

                    transaction.Commit();
                }

                using (var transaction = await repository.WaitTransactionAsync(parentId))
                {
                    var entities = transaction.GetEntities<TestEntity>(parentId, Order.Descending).ToArray();

                    Assert.IsNotNull(entities);
                    Assert.AreEqual(2, entities.Length);
                    Assert.AreEqual(entity2.Id, entities[0].Id);
                    Assert.AreEqual(entity1.Id, entities[1].Id);

                    transaction.TryGetEntity<TestEntity>(parentId, entity1, out var existingEntity);

                    Assert.IsNotNull(existingEntity);
                    Assert.AreEqual(entity1.Id, existingEntity.Id);
                    Assert.AreEqual(entity1.Position, existingEntity.Position);
                }
            }

            watch.Stop();
        }
    }

    public class TestEntity : IEntity, IPosition
    {
        public long Position { get; set; }

        public Guid Id { get; set; }

        public string Value { get; set; }

        public Guid GetEntityId() => Id;
    }
}
