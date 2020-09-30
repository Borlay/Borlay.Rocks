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

        //private List<(string, ColumnFamilyOptions)> families = new List<(string, ColumnFamilyOptions)>();

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

        public DatabaseBuilder Entity<T>(Func<T, byte[]> getIndex, Order order, bool cacheAllIndexes = false, int parentIdLength = 16)
        {
            //var hasPosition = typeof(IPosition).GetTypeInfo().IsAssignableFrom(typeof(T));

            //var tableOptions = new BlockBasedTableOptions().SetDefaultOptions(hasPosition ? false : true, cacheAllIndexes, ssdDisc ? 16 : 64);
            //var familyOptions = new ColumnFamilyOptions().SetDefaultOptions(tableOptions, (ssdDisc ? 256 : 512));

            //var entity = new Entity(typeof(T), null);
            //var index = new EntityIndex<T>(typeof(T).Name, order, familyOptions, getIndex, true);

            //Entities[typeof(T).Name] = entity;

            HasIndex<T>("Primary", getIndex, order, true, cacheAllIndexes, parentIdLength);

            //families.Add((name, familyOptions));

            return this;
        }

        public DatabaseBuilder HasIndex<T>(string name, Func<T, byte[]> getIndex, Order order, bool hasValue, bool cacheAllIndexes = false, int parentIdLength = 16)
        {
            if(!Entities.TryGetValue(typeof(T).Name, out var entity))
            {
                entity = new Entity(typeof(T));
                Entities[typeof(T).Name] = entity;
            }

            name = $"{name}-{order}";
            var tableOptions = new BlockBasedTableOptions().SetDefaultOptions(order == Order.None, cacheAllIndexes, ssdDisc ? 16 : 64);
            var familyOptions = new ColumnFamilyOptions().SetDefaultOptions(tableOptions, parentIdLength, (ssdDisc ? 256 : 512));

            var index = new EntityIndex<T>($"{typeof(T).Name}-{name}", parentIdLength, order, familyOptions, getIndex, hasValue); ;
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


    //public class EntityBuilder<T>
    //{
    //    private EntityIndex<T> index;

    //    public virtual EntityBuilder<T> HasIndex(string name, Func<T, byte[]> getIndex, bool isUnique)
    //    {
    //        if (index != null)
    //            throw new ArgumentException("Entity can have only one index");

    //        index = new EntityIndex<T>(getIndex, isUnique);
    //        return this;
    //    }
    //}

    public enum Order
    {
        None = 0,
        Ascending = 1,
        Descending,
    }

    public class Entity
    {
        public Type Type { get;  }

        //public ColumnFamilyOptions ColumnFamily { get;  }

        //public EntityIndex Index { get; }

        private readonly IDictionary<string, EntityIndex> indexes = new Dictionary<string, EntityIndex>();

        //public string ColumnFamilyName { get; }

        public EntityIndex ValueIndex { get; private set; }

        public Entity(Type type)
        {
            this.Type = type;
            //this.Order = order;
            //this.ColumnFamily = columnFamily;
            //this.Index = index;
            //this.ColumnFamilyName = $"{type.Name}-{index.Name}";
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

        //public Entity(Type type, ColumnFamilyOptions columnFamily)
        //{
        //    this.Type = type;
        //    this.ColumnFamily = columnFamily;
        //    this.ColumnFamilyName = $"{type.Name}";
        //}
    }

    public abstract class EntityIndex
    {
        public string ColumnFamilyName { get; protected set; }

        //public bool IsUnique { get; protected set; }

        public ColumnFamilyOptions ColumnFamily { get; protected set; }

        public Order Order { get; protected set; }

        public bool HasValue { get; protected set; }

        public int PrefixLength { get; set; }

        public abstract bool HasMatch { get; }

        public abstract byte[] GetIndex(object obj);

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
        //public Func<T, byte[]> GetIndex { get; }

        private Func<T, bool> matchEntity;
        //private Func<Enum, bool> matchEnum;
        

        public EntityIndex(string columnFamilyName, int prefixLength, Order order, ColumnFamilyOptions columnFamily, Func<T, byte[]> getIndex, bool hasValue)
        {
            this.ColumnFamilyName = columnFamilyName;
            this.Order = order;
            this.PrefixLength = prefixLength;
            this.getIndex = getIndex;
            //this.IsUnique = isUnique;
            this.ColumnFamily = columnFamily;
            this.HasValue = hasValue;
        }

        public override bool HasMatch => matchEntity != null;

        public void SetMatch<TEnum>(TEnum _enum, Func<T, bool> matchEntity) where TEnum : Enum
        {
            matchEnum = _enum;
            //this.matchEnum = (e) =>
            //{
            //    if (e is TEnum _en) return _en.CompareTo(_enum) == 0;
            //    return false;
            //};
            this.matchEntity = matchEntity;
        }


        public override byte[] GetIndex(object obj)
        {
            return getIndex?.Invoke((T)obj);
        }

        public override bool MatchEntity(object obj)
        {
            return this.matchEntity?.Invoke((T)obj) ?? true;
        }
    }
}
