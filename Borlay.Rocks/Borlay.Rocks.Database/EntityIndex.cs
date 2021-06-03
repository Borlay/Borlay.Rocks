using Borlay.Arrays;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Borlay.Rocks.Database
{
    public abstract class EntityIndex
    {
        public Caches.Cache<ByteArray, byte[]> Cache { get; private set; } = null;

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

            if (matchEnum is TEnum en)
                return en.CompareTo(_enum) == 0;

            return false;
        }

        public void SetCache(int capasity)
        {
            SetCache(new Caches.Cache<ByteArray, byte[]>(capasity));
        }
        public void SetCache(Caches.Cache<ByteArray, byte[]> cache)
        {
            if (Order != Order.None)
                throw new ArgumentException($"Cache for order {Order} is not supported");

            this.Cache = cache;
        }

        public void TrySetValue(byte[] key, byte[] value)
        {
            Cache?.Set(new ByteArray(key.ToArray()), value);

        }
        public void TrySetValue(ByteArray key, byte[] value)
        {
            Cache?.Set(key, value);
        }

        public bool TryGetValue(byte[] key, out byte[] value)
        {
            value = null;
            if (Cache == null) return false;
            return Cache.TryGetValue(new ByteArray(key), out value);
        }

        public bool TryGetValue(ByteArray key, out byte[] value)
        {
            value = null;
            if (Cache == null) return false;
            return Cache.TryGetValue(key, out value);
        }

        public void RemoveValue(byte[] key)
        {
            RemoveValue(new ByteArray(key));
        }

        public void RemoveValue(ByteArray key)
        {
            if (Cache == null) return;
            Cache.Remove(key);
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

        public void SetMatch(Func<T, bool> matchEntity)
        {
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
                    if (position.Position <= 0)
                        throw new ArgumentException($"Entity of type {typeof(T)} should have Position greater than 0 because it has Ordered index.");

                    positionBytes = Order == Order.Ascending ? position.Position.ToBytesByAscending() : position.Position.ToBytesByDescending();
                }
                else
                    throw new ArgumentException($"Entity of type {typeof(T)} should have IPosition interface because it has Ordered index.");
            }

            if (parentIndexBytes.Length > 16)
                Array.Resize(ref parentIndexBytes, 16);

            byte[] key = parentIndexBytes;

            if (positionBytes?.Length > 0)
            {
                key = key.Concat(positionBytes);

                if (entityIndexBytes.Length > 8)
                    Array.Resize(ref entityIndexBytes, 8);

            }
            else if (entityIndexBytes.Length > 16)
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
