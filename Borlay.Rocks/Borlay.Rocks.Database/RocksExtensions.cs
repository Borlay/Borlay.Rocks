﻿using Newtonsoft.Json;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Borlay.Rocks.Database
{
    public static class RocksExtensions
    {
        //public static void WriteIndex<T>(this WriteBatch batch, Guid parentId, T record, ColumnFamilyHandle columnFamily) where T : ISortableRecord
        //{
        //    var descTimeBytes = record.Position.ToBytesByDescending();

        //    var recordKey = parentId.ToByteArray()
        //        .Concat(descTimeBytes)
        //        .Concat(record.Id.ToByteArray(8));

        //    var json = Newtonsoft.Json.JsonConvert.SerializeObject(record, new Newtonsoft.Json.JsonSerializerSettings() { NullValueHandling = NullValueHandling.Ignore });
        //    var bytes = Encoding.UTF8.GetBytes(json);

        //    batch.Put(recordKey.Concat(0), record.Id.ToByteArray(), columnFamily);
        //    batch.Put(recordKey.Concat(2), bytes, columnFamily);
        //}

        public static void Write(this WriteBatch batch, byte[] parentIndexBytes, byte[] valueIndexBytes, byte[] entityIndexBytes, long position, Order order, ColumnFamilyHandle columnFamily)
        {
            //var descTimeBytes = record.Position.ToBytesByDescending();

            //var recordKey = parentId.ToByteArray()
            //    .Concat(descTimeBytes)
            //    .Concat(record.Id.ToByteArray(8));

            var key = MakeIndex(parentIndexBytes, entityIndexBytes, position, order);

            //var json = Newtonsoft.Json.JsonConvert.SerializeObject(record, new Newtonsoft.Json.JsonSerializerSettings() { NullValueHandling = NullValueHandling.Ignore });
            //var bytes = Encoding.UTF8.GetBytes(json);

            batch.Put(key.Concat(0), entityIndexBytes);//record.Id.ToByteArray(), columnFamily);
            batch.Put(key.Concat(2), valueIndexBytes, columnFamily);
        }

        public static void Write<T>(this WriteBatch batch, byte[] parentIndexBytes, T record, byte[] indexBytes, long position, Order order, ColumnFamilyHandle columnFamily) where T : IEntity
        {
            //var descTimeBytes = record.Position.ToBytesByDescending();

            //var recordKey = parentId.ToByteArray()
            //    .Concat(descTimeBytes)
            //    .Concat(record.Id.ToByteArray(8));

            var key = MakeIndex(parentIndexBytes, indexBytes, position, order);

            var json = Newtonsoft.Json.JsonConvert.SerializeObject(record, new Newtonsoft.Json.JsonSerializerSettings() { NullValueHandling = NullValueHandling.Ignore });
            var bytes = Encoding.UTF8.GetBytes(json);

            batch.Put(key.Concat(0), record.GetEntityId().ToByteArray(), columnFamily);
            batch.Put(key.Concat(1), bytes, columnFamily);
        }

        public static byte[] MakeIndex(this byte[] parentIndexBytes, byte[] entityIndexBytes, long position, Order order)
        {
            byte[] timeBytes = null;

            if (order != Order.None)
            {
                if (position == 0)
                    throw new ArgumentException($"Position cannot be 0 for sortable entity");

                timeBytes = order == Order.Ascending ? position.ToBytesByAscending() : position.ToBytesByDescending();
            }

            return MakeIndex(parentIndexBytes, entityIndexBytes, timeBytes);
        }

        public static byte[] MakeIndex(this byte[] parentIndexBytes, byte[] entityIndexBytes, byte[] timeBytes)
        {
            if(entityIndexBytes?.Length > 0)
            {
                byte[] key = parentIndexBytes;

                if(timeBytes?.Length > 0)
                    key = key.Concat(timeBytes);

                key = key.Concat(entityIndexBytes);
                return key;
            }
            else
            {
                byte[] key = new byte[0];

                if (timeBytes?.Length > 0)
                    key = key.Concat(timeBytes);

                key = key.Concat(parentIndexBytes);
                return key;
            }
        }

        public static IEnumerable<T> GetEntities<T>(this RocksDb db, byte[] parentIndexBytes, long position, ColumnFamilyHandle columnFamily, ColumnFamilyHandle valueColumnFamily, bool clean = true) where T : IEntity
        {
            var records = new Dictionary<Guid, T>();
            List<(Guid, byte[])> toRemove = new List<(Guid, byte[])>();

            try
            {
                foreach (var recordJson in db.GetEntitiesBytes(parentIndexBytes, position, columnFamily, valueColumnFamily))
                {
                    if (records.ContainsKey(recordJson.Item1))
                        toRemove.Add((recordJson.Item1, recordJson.Item2));
                    else
                    {
                        var json = Encoding.UTF8.GetString(recordJson.Item4);
                        var record = Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json);
                        if(record is IPosition recPosition)
                            recPosition.Position = recordJson.Item3;

                        records[recordJson.Item1] = record;
                        yield return record;
                    }
                }
            }
            finally
            {
                if (clean)
                    db.Remove(toRemove, columnFamily);
            }
        }

        public static int Remove(this RocksDb db, List<(Guid, byte[])> toRemove, ColumnFamilyHandle columnFamily)
        {
            if (toRemove.Count == 0) return 0;
            var iterations = 0;

            using (WriteBatch batch = new WriteBatch())
            {
                var items = new List<(Guid, byte[])>();
                while (toRemove.Count > 0)
                {
                    items.Clear();
                    items.AddRange(toRemove.TakeWhile(i => i.Item1 == toRemove[0].Item1));
                    toRemove.RemoveRange(0, items.Count);
                    iterations++;

                    var firstKey = items[0].Item2;
                    var lastKey = items[items.Count - 1].Item2;

                    if (items.Count == 1)
                        batch.Delete(firstKey, columnFamily);
                    else
                    {
                        batch.DeleteRange(firstKey, (ulong)firstKey.Length, lastKey, (ulong)lastKey.Length, columnFamily);
                    }
                }

                db.Write(batch);
            }

            return iterations;
        }

        public static byte[] ToKey(this Guid parentId, Guid recordId, long position)
        {
            var positionBytes = position.ToBytesByDescending();
            var key = parentId.ToByteArray().Concat(positionBytes).Concat(recordId.ToByteArray(8));
            return key;
        }

        internal static IEnumerable<(Guid, byte[], long, byte[])> GetEntitiesBytes(this RocksDb db, byte[] parentIndexBytes, long position, ColumnFamilyHandle columnFamily, ColumnFamilyHandle valueColumnFamily)
        {
            var readOptions = new ReadOptions();
            readOptions.SetPrefixSameAsStart(true);
            readOptions.SetTotalOrderSeek(false);
            var iterator = db.NewIterator(columnFamily, readOptions);

            try
            {
                iterator = iterator.Seek(parentIndexBytes);
                if (position != 0)
                    iterator = iterator.Seek(parentIndexBytes.Concat(position.ToBytesByDescending()));

                while (iterator.Valid())
                {
                    var jsonBytes = iterator.GetEntityBytes(_key => db.Get(_key, valueColumnFamily), out var id, out var key, out position);
                    yield return (id, key, position, jsonBytes);
                }
            }
            finally
            {
                iterator.Dispose();
            }
        }

        public static RocksInstance GetInstance(this IDictionary<int, RocksInstance> instances, Guid userId, out int shardIndex)
        {
            shardIndex = Math.Abs(userId.GetHashCode()) % instances.Count;

            if (!instances.TryGetValue(shardIndex, out var instance))
                throw new KeyNotFoundException($"RocksDb instance of shard '{shardIndex}' for user '{userId}' not found.");

            return instance;
        }

        public static byte[] GetEntityBytes(this Iterator iterator, Func<byte[], byte[]> valueByKey, out Guid id, out byte[] key, out long position)
        {
            byte[] json = null;
            id = Guid.NewGuid();
            position = 0;
            key = null;

            byte[] lastKey = null;
            while (iterator.Valid())
            {
                var itKey = iterator.Key();

                var fieldKey = new byte[itKey.Length - 32];
                var field = Encoding.UTF8.GetString(itKey, 32, itKey.Length - 32);

                var baseKey = new byte[16];
                Array.Copy(itKey, 16, baseKey, 0, baseKey.Length);

                var descTimeBytes = new byte[8];
                Array.Copy(itKey, 16, descTimeBytes, 0, descTimeBytes.Length);

                if (lastKey == null)
                    lastKey = baseKey;
                else if (!lastKey.StartsWith(baseKey))
                    return json;

                if (position == 0)
                    position = descTimeBytes.ToLongByDescending();

                switch (itKey[itKey.Length - 1])
                {
                    case 0: id = new Guid(iterator.Value()); break;
                    case 1: json = iterator.Value(); break;
                    case 2: json = valueByKey(iterator.Value()); break;
                }

                Array.Resize(ref itKey, itKey.Length - 1);
                key = itKey;
                iterator = iterator.Next();
            }

            return json;
        }
    }
}