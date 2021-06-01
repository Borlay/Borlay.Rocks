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

        public DatabaseBuilder Entity<T>(Func<T, byte[]> getIndex, Order order, out EntityIndex<T> index, bool cacheAllIndexes = false, int parentIdLength = 16)
        {
            HasIndex<T>("Primary", getIndex, order, true, out index, cacheAllIndexes, parentIdLength);
            return this;
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

    public enum Order
    {
        None = 0,
        Ascending = 1,
        Descending,
    }

    public class Entity
    {
        public Type Type { get;  }

        private readonly IDictionary<string, EntityIndex> indexes = new Dictionary<string, EntityIndex>();

        public EntityIndex ValueIndex { get; private set; }

        public Entity(Type type)
        {
            this.Type = type;
        }

        public IEnumerable<KeyValuePair<string, EntityIndex>> Indexes => indexes;

        public EntityIndex this[string name]
        {
            set 
            {
                if (value.HasValue && !value.HasMatch)
                {
                    if (ValueIndex == null)
                        ValueIndex = value;
                    else if(ValueIndex.Order != Order.None && value.Order == Order.None)
                        ValueIndex = value;
                }

                indexes[name] = value;
            }
            get => indexes[name];
        }

    }

    public abstract class EntityIndex
    {
        public string ColumnFamilyName { get; protected set; }

        public ColumnFamilyOptions ColumnFamily { get; protected set; }

        public Order Order { get; protected set; }

        public bool HasValue { get; protected set; }

        public int PrefixLength { get; set; }

        public abstract bool HasMatch { get; }

        public bool AutoRemove { get; set; } = true;

        public abstract byte[] MakeKey(byte[] parentIndexBytes, object obj);

        public abstract bool MatchEntity(object obj);

        protected Enum matchEnum;

        public bool MatchEnum<TEnum>(TEnum _enum) where TEnum : Enum
        {
            if (matchEnum == null) return false;

            if(matchEnum is TEnum en)
                return en.CompareTo(_enum) == 0;

            return false;
        }
    }

    public class EntityIndex<T> : EntityIndex
    {
        private readonly Func<T, byte[]> getIndex;
        private Func<T, bool> matchEntity;
        

        public EntityIndex(string columnFamilyName, int prefixLength, Order order, ColumnFamilyOptions columnFamily, Func<T, byte[]> getIndex, bool hasValue)
        {
            this.ColumnFamilyName = columnFamilyName;
            this.Order = order;
            this.PrefixLength = prefixLength;
            this.getIndex = getIndex;
            this.ColumnFamily = columnFamily;
            this.HasValue = hasValue;
        }

        public override bool HasMatch => matchEntity != null;

        public void SetMatch<TEnum>(TEnum _enum, Func<T, bool> matchEntity) where TEnum : Enum
        {
            matchEnum = _enum;
            this.matchEntity = matchEntity;
        }

        public override byte[] MakeKey(byte[] parentIndexBytes, object obj)
        {
            var entity = (T)obj;
            var entityIndexBytes = getIndex?.Invoke(entity);
            var positionBytes = new byte[0];

            if (Order != Order.None)
            {
                if (entity is IPosition position)
                {
                    if(position.Position <= 0)
                        throw new ArgumentException($"Entity of type {typeof(T)} should have Position greater than 0 because it has Ordered index.");

                    positionBytes = Order == Order.Ascending ? position.Position.ToBytesByAscending() : position.Position.ToBytesByDescending();
                }
                else
                    throw new ArgumentException($"Entity of type {typeof(T)} should have IPosition interface because it has Ordered index.");
            }

            if(parentIndexBytes.Length > 16)
                Array.Resize(ref parentIndexBytes, 16);

            byte[] key = parentIndexBytes;

            if (positionBytes?.Length > 0)
            {
                key = key.Concat(positionBytes);

                if(entityIndexBytes.Length > 8)
                    Array.Resize(ref entityIndexBytes, 8);

            }
            else if(entityIndexBytes.Length > 16)
                Array.Resize(ref entityIndexBytes, 16);

            key = key.Concat(entityIndexBytes);
            return key;

        }

        public override bool MatchEntity(object obj)
        {
            return this.matchEntity?.Invoke((T)obj) ?? true;
        }
    }
}
