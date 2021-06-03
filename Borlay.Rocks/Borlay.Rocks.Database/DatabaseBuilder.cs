using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
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

        public IDictionary<string, Entity> Entities = new Dictionary<string, Entity>();

        public DatabaseBuilder(string directory, ulong walTtlInSeconds, int shardCount, bool directIO, Recovery recovery, bool ssdDisc = true)
        {
            this.directory = directory;
            this.walTtlInSeconds = walTtlInSeconds;
            this.shardCount = shardCount;
            this.directIO = directIO;
            this.recovery = recovery;
            this.ssdDisc = ssdDisc;
        }

        public DatabaseBuilder Entity<T>(Order order, out EntityIndex<T> index, bool cacheAllIndexes = false, int parentIdLength = 16) where T: IEntity
        {
            HasIndex<T>("Primary", (e) => e.GetEntityId().ToByteArray(), order, true, out index, cacheAllIndexes, parentIdLength);
            return this;
        }

        public DatabaseBuilder Entity<T>(Func<T, byte[]> getIndex, Order order, out EntityIndex<T> index, bool cacheAllIndexes = false, int parentIdLength = 16)
        {
            HasIndex<T>("Primary", getIndex, order, true, out index, cacheAllIndexes, parentIdLength);
            return this;
        }

        public DatabaseBuilder HasIndex<T>(string name, Order order, bool hasValue, out EntityIndex<T> index, bool cacheAllIndexes = false, int parentIdLength = 16) where T : IEntity
        {
            return HasIndex<T>(name, (e) => e.GetEntityId().ToByteArray(), order, hasValue, out index, cacheAllIndexes, parentIdLength);
        }

        public DatabaseBuilder HasIndex<T>(string name, Func<T, byte[]> getIndex, Order order, bool hasValue, out EntityIndex<T> index, bool cacheAllIndexes = false, int parentIdLength = 16)
        {
            if(!Entities.TryGetValue(typeof(T).Name, out var entity))
            {
                entity = new Entity(typeof(T));
                Entities[typeof(T).Name] = entity;
            }

            name = $"{name}-{order}";
            var tableOptions = new BlockBasedTableOptions().SetDefaultOptions(order == Order.None, cacheAllIndexes, ssdDisc ? 16 : 64);
            var familyOptions = new ColumnFamilyOptions().SetDefaultOptions(tableOptions, parentIdLength, (ssdDisc ? 256 : 512));

            index = new EntityIndex<T>($"{typeof(T).Name}-{name}", parentIdLength, order, familyOptions, getIndex, hasValue); ;
            entity[name] = index;

            return this;
        }

        public DatabaseBuilder HasIndex<T, TEnum>(Func<T, TEnum, bool> matchEntity, Func<T, byte[]> getIndex, Order order, bool hasValue, bool cacheAllIndexes = false, int parentIdLength = 16) where TEnum : Enum
        {
            if (!Entities.TryGetValue(typeof(T).Name, out var entity))
            {
                entity = new Entity(typeof(T));
                Entities[typeof(T).Name] = entity;
            }

            foreach (var en in Enum.GetValues(typeof(TEnum)).Cast<TEnum>())
            {
                var _en = en;
                var name = $"{typeof(TEnum).Name}-{en.ToString()}-{order}";
                var tableOptions = new BlockBasedTableOptions().SetDefaultOptions(order == Order.None, cacheAllIndexes, ssdDisc ? 16 : 64);
                var familyOptions = new ColumnFamilyOptions().SetDefaultOptions(tableOptions, parentIdLength, (ssdDisc ? 256 : 512));

                var index = new EntityIndex<T>($"{typeof(T).Name}-{name}", parentIdLength, order, familyOptions, getIndex, hasValue);
                entity[name] = index;

                index.SetMatch<TEnum>(_en, (t) => matchEntity(t, _en));
            }

            return this;
        }

        public IDictionary<int, RocksInstance> CreateInstances()
        {
            var instances = Enumerable.Range(0, shardCount)
                .Select(i => new RocksInstance(Path.Combine(directory, $"{i}"), walTtlInSeconds, i, directIO, recovery, Entities))
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
