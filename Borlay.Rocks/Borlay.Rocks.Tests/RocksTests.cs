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
            builder.Entity<TestEntity>(Order.Descending, out var descIndex);
            builder.HasIndex<TestEntity>("entity", Order.None, false, out var noneOrderIndex);

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
                    transaction.SaveEntity(entity1);
                    transaction.NextPosition();
                    transaction.SaveEntity(entity2);

                    Assert.IsTrue(entity1.Position > 0);
                    Assert.IsTrue(entity2.Position > entity1.Position);

                    transaction.Commit();
                }

                using (var transaction = await repository.WaitTransactionAsync(parentId))
                {
                    var entities = transaction.GetEntities<TestEntity>(Order.Descending).ToArray();

                    Assert.IsNotNull(entities);
                    Assert.AreEqual(2, entities.Length);
                    Assert.AreEqual(entity2.Id, entities[0].Id);
                    Assert.AreEqual(entity1.Id, entities[1].Id);

                    transaction.TryGetEntity<TestEntity>(entity1, out var existingEntity);

                    Assert.IsNotNull(existingEntity);
                    Assert.AreEqual(entity1.Id, existingEntity.Id);
                    Assert.AreEqual(entity1.Position, existingEntity.Position);
                }
            }

            watch.Stop();
        }

        [TestMethod]
        public async Task TestDescOrder()
        {
            var builder = new DatabaseBuilder(@"C:\rocks\tests\descorder", 0, 1, true, RocksDbSharp.Recovery.TolerateCorruptedTailRecords, true);
            builder.Entity<TestEntity>(Order.Descending, out var descIndex);
            //builder.HasIndex<TestEntity>("entity", Order.Descending, false, out var noneOrderIndex);

            var repository = builder.CreateRepository();

            var watch = Stopwatch.StartNew();

            var parentId = Guid.NewGuid();

            for (int i = 0; i < 30; i++)
            {
                var entity = new TestEntity()
                {
                    Id = Guid.NewGuid(),
                    Position = 0,
                    Value = $"Test {i}",
                };

                using (var transaction = await repository.WaitTransactionAsync(parentId))
                {
                    transaction.SaveEntity(entity);
                    transaction.Commit();
                }

                await Task.Delay(10);
            }

            using (var transaction = await repository.WaitTransactionAsync(parentId))
            {
                var entities1 = transaction.GetEntities<TestEntity>(0, Order.Descending).Take(10).ToArray();
                var minPos1 = entities1.Min(e => e.Position);
                var maxPos1 = entities1.Max(e => e.Position);

                Assert.AreEqual("Test 29", entities1.First().Value);
                Assert.AreEqual("Test 20", entities1.Last().Value);

                Assert.AreEqual(maxPos1, entities1.First().Position);

                var entities2 = transaction.GetEntities<TestEntity>(minPos1, Order.Descending).Take(10).ToArray();
                var minPos2 = entities2.Min(e => e.Position);
                var maxPos2 = entities2.Max(e => e.Position);

                Assert.AreEqual("Test 20", entities2.First().Value);
                Assert.AreEqual("Test 11", entities2.Last().Value);

                Assert.AreEqual(maxPos2, entities2.First().Position);
            }

            watch.Stop();
        }

        [TestMethod]
        public async Task TestAscOrder()
        {
            var builder = new DatabaseBuilder(@"C:\rocks\tests\ascorder", 0, 1, true, RocksDbSharp.Recovery.TolerateCorruptedTailRecords, true);
            builder.Entity<TestEntity>(Order.Ascending, out var descIndex);
            //builder.HasIndex<TestEntity>("entity", Order.Descending, false, out var noneOrderIndex);

            var repository = builder.CreateRepository();

            var watch = Stopwatch.StartNew();

            var parentId = Guid.NewGuid();

            for (int i = 0; i < 30; i++)
            {
                var entity = new TestEntity()
                {
                    Id = Guid.NewGuid(),
                    Position = 0,
                    Value = $"Test {i}",
                };

                using (var transaction = await repository.WaitTransactionAsync(parentId))
                {
                    transaction.SaveEntity(entity);
                    transaction.Commit();
                }

                await Task.Delay(10);
            }

            using (var transaction = await repository.WaitTransactionAsync(parentId))
            {
                var entities1 = transaction.GetEntities<TestEntity>(0, Order.Ascending).Take(10).ToArray();
                var minPos1 = entities1.Min(e => e.Position);
                var maxPos1 = entities1.Max(e => e.Position);

                Assert.AreEqual("Test 0", entities1.First().Value);
                Assert.AreEqual("Test 9", entities1.Last().Value);

                Assert.AreEqual(minPos1, entities1.First().Position);

                var entities2 = transaction.GetEntities<TestEntity>(maxPos1, Order.Ascending).Take(10).ToArray();
                var minPos2 = entities2.Min(e => e.Position);
                var maxPos2 = entities2.Max(e => e.Position);

                Assert.AreEqual("Test 9", entities2.First().Value);
                Assert.AreEqual("Test 18", entities2.Last().Value);

                Assert.AreEqual(minPos2, entities2.First().Position);
            }

            watch.Stop();
        }

        [TestMethod]
        public async Task TestRemove()
        {
            var builder = new DatabaseBuilder(@"C:\rocks\tests4", 0, 1, true, RocksDbSharp.Recovery.TolerateCorruptedTailRecords, true);
            builder.Entity<TestEntity>(Order.Descending, out var descIndex);
            //builder.HasIndex<TestEntity>("entity", Order.None, false, out var noneOrderIndex);

            var repository = builder.CreateRepository();

            var watch = Stopwatch.StartNew();

            var parentId = Guid.NewGuid();
            var entityId = Guid.NewGuid();

            for (int i = 0; i < 10000; i++)
            {
                var entity1 = new TestEntity()
                {
                    Id = entityId,
                    Position = 0,
                    Value = $"Test 1 {i}",
                };

                var entity2 = new TestEntity()
                {
                    Id = Guid.NewGuid(),
                    Position = 0,
                    Value = $"Test 2 {i}",
                };

                using (var transaction = await repository.WaitTransactionAsync(parentId))
                {
                    transaction.SaveEntity(entity1);
                    transaction.SetNextPosition(entity1);
                    transaction.SaveEntity(entity1);
                    transaction.SetNextPosition(entity1);
                    transaction.SaveEntity(entity1);
                    transaction.SetNextPosition(entity1);
                    transaction.SaveEntity(entity2);
                    transaction.SetNextPosition(entity2);

                    Assert.IsTrue(entity1.Position > 0);
                    Assert.IsTrue(entity2.Position > entity1.Position);

                    transaction.Commit();
                }
            }

            watch.Stop();

            using (var transaction = await repository.WaitTransactionAsync(parentId))
            {
                var deleteWatch = Stopwatch.StartNew();

                var entities1 = transaction.GetEntities<TestEntity>(Order.Descending).ToArray();

                deleteWatch.Stop();
                deleteWatch.Restart();

                var entities2 = transaction.GetEntities<TestEntity>(Order.Descending).ToArray();

                deleteWatch.Stop();
                deleteWatch.Restart();

                Assert.IsNotNull(entities1);
                Assert.IsNotNull(entities2);

                Assert.AreEqual(10001, entities1.Length);
                Assert.AreEqual(10001, entities2.Length);

                
            }
        }

        [TestMethod]
        public async Task TestDeleteEntity()
        {
            var builder = new DatabaseBuilder(@"C:\rocks\tests4", 0, 1, true, RocksDbSharp.Recovery.TolerateCorruptedTailRecords, true);
            builder.Entity<TestEntity>(Order.Descending, out var descIndex);
            //builder.HasIndex<TestEntity>("entity", Order.None, false, out var noneOrderIndex);

            var repository = builder.CreateRepository();

            var watch = Stopwatch.StartNew();

            var parentId = Guid.NewGuid();
            var entityId = Guid.NewGuid();

            for (int i = 0; i < 10000; i++)
            {
                var entity1 = new TestEntity()
                {
                    Id = entityId,
                    Position = 0,
                    Value = $"Test 1 {i}",
                };

                var entity2 = new TestEntity()
                {
                    Id = Guid.NewGuid(),
                    Position = 0,
                    Value = $"Test 2 {i}",
                };

                using (var transaction = await repository.WaitTransactionAsync(parentId))
                {
                    transaction.SaveEntity(entity1);
                    transaction.SetNextPosition(entity1);
                    transaction.SaveEntity(entity1);
                    transaction.SetNextPosition(entity1);
                    transaction.SaveEntity(entity1);
                    transaction.SetNextPosition(entity1);
                    transaction.SaveEntity(entity2);
                    transaction.SetNextPosition(entity2);

                    Assert.IsTrue(entity1.Position > 0);
                    Assert.IsTrue(entity2.Position > entity1.Position);

                    transaction.Commit();
                }
            }

            watch.Stop();

            using (var transaction = await repository.WaitTransactionAsync(parentId))
            {
                var deleteWatch = Stopwatch.StartNew();

                var entities1 = transaction.GetEntities<TestEntity>(Order.Descending).ToArray();

                deleteWatch.Stop();

                Assert.IsNotNull(entities1);
                Assert.AreEqual(10001, entities1.Length);

                var entityToDelete = entities1.Single(e => e.Id == entityId);

                transaction.TryGetEntity<TestEntity>(entityToDelete, Order.Descending, out var ent1);
                Assert.IsNotNull(ent1, "Entity should exist");

                transaction.DeleteEntity(entityToDelete, Order.Descending);
                transaction.Commit();

                var entities2 = transaction.GetEntities<TestEntity>(Order.Descending).ToArray();

                transaction.TryGetEntity<TestEntity>(entityToDelete, Order.Descending, out var ent2);
                Assert.IsNull(ent2, "Entity is not deleted");

                var deletedEntity = entities2.FirstOrDefault(e => e.Id == entityId);
                Assert.IsNull(deletedEntity, "Entity is not deleted");

                Assert.IsNotNull(entities2);
                Assert.AreEqual(10000, entities2.Length);
            }
        }


        [TestMethod]
        public async Task TestCache()
        {
            var builder = new DatabaseBuilder(@"C:\rocks\tests2", 0, 1, true, RocksDbSharp.Recovery.TolerateCorruptedTailRecords, true);
            builder.Entity<TestEntity>(Order.None, out var descIndex);
            builder.HasIndex<TestEntity>("entity", Order.Descending, false, out var noneOrderIndex);

            //descIndex.SetCache(100 * 1000);

            var repository = builder.CreateRepository();

            var watch = Stopwatch.StartNew();

            for (int i = 0; i < 10000; i++)
            {

                var parentId = Guid.NewGuid();

                var entity1 = new TestEntity()
                {
                    Id = Guid.NewGuid(),
                    Position = 0,
                    Value = $"Test 1 {i}",
                };

                var entity2 = new TestEntity()
                {
                    Id = Guid.NewGuid(),
                    Position = 0,
                    Value = $"Test 2 {i}",
                };

                using (var transaction = await repository.WaitTransactionAsync(parentId))
                {
                    transaction.SaveEntity(entity1);
                    transaction.NextPosition();
                    transaction.SaveEntity(entity2);

                    Assert.IsTrue(entity1.Position > 0);
                    Assert.IsTrue(entity2.Position > entity1.Position);

                    transaction.Commit();
                }

                using (var transaction = await repository.WaitTransactionAsync(parentId))
                {
                    //var entities = transaction.GetEntities<TestEntity>(Order.Descending).ToArray();

                    //Assert.IsNotNull(entities);
                    //Assert.AreEqual(2, entities.Length);
                    //Assert.AreEqual(entity2.Id, entities[0].Id);
                    //Assert.AreEqual(entity1.Id, entities[1].Id);

                    transaction.TryGetEntity<TestEntity>(entity1, out var existingEntity);

                    Assert.IsNotNull(existingEntity);
                    Assert.AreEqual(entity1.Id, existingEntity.Id);
                    Assert.AreEqual(entity1.Position, existingEntity.Position);
                    Assert.AreEqual($"Test 1 {i}", existingEntity.Value);
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
