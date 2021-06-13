using Newtonsoft.Json;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Borlay.Rocks.Database
{
    public static class RocksExtensions
    {
        public static void Write(this WriteBatch batch, byte[] key, Guid recordId, long position, byte[] valueIndexBytes, ColumnFamilyHandle columnFamily)
        {
            batch.Put(key.Concat(0), recordId.ToByteArray(), columnFamily);
            batch.Put(key.Concat(2), valueIndexBytes, columnFamily);
            batch.Put(key.Concat(3), position.ToBytesByAscending(), columnFamily);
        }

        public static void Write(this WriteBatch batch, byte[] key, Guid recordId, byte[] bytes, long position,  ColumnFamilyHandle columnFamily)
        {
            batch.Put(key.Concat(0), recordId.ToByteArray(), columnFamily);
            batch.Put(key.Concat(1), bytes, columnFamily);
            batch.Put(key.Concat(3), position.ToBytesByAscending(), columnFamily);
        }

        public static IEnumerable<T> GetEntities<T>(this RocksDb db, byte[] parentIndexBytes, long position, ColumnFamilyHandle columnFamily, ColumnFamilyHandle valueColumnFamily, bool autoRemove = true) where T : IEntity
        {
            var records = new Dictionary<Guid, T>();

            List<(byte[] First, byte[] Last)> removeRanges = new List<(byte[] First, byte[] Last)>();
            byte[] firstToRemove = null;

            int count = 0;

            try
            {
                foreach (var recordJson in db.GetEntitiesBytes(parentIndexBytes, position, columnFamily, valueColumnFamily))
                {
                        count++;
                    if(count > 1000 * 1000)
                    {
                        if (autoRemove)
                        {
                            if (firstToRemove != null)
                            {
                                removeRanges.Add((firstToRemove, recordJson.Item2));
                                firstToRemove = null;
                            }
                        }

                        break;
                    }

                    if (records.ContainsKey(recordJson.Item1) && autoRemove)
                    {
                        if (firstToRemove == null)
                            firstToRemove = recordJson.Item2;
                    }
                    else
                    {
                        if (autoRemove)
                        {
                            if(firstToRemove != null)
                            {
                                removeRanges.Add((firstToRemove, recordJson.Item2));
                                firstToRemove = null;
                            }
                        }

                        var json = Encoding.UTF8.GetString(recordJson.Item4);
                        var record = Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json);
                        if (record is IPosition recPosition)
                            recPosition.Position = recordJson.Item3;

                        records[recordJson.Item1] = record;
                        yield return record;
                    }
                }
            }
            finally
            {
                if (removeRanges.Count > 0)
                    db.RemoveRange(columnFamily, removeRanges.ToArray());
            }
        }

        public static void DeleteEntities(this RocksDb db, byte[] parentIndexBytes, long position, ColumnFamilyHandle columnFamily)
        {
            byte[] firstToRemove = null;
            byte[] lastToRemove = null;

            foreach (var recordJson in db.GetEntitiesBytes(parentIndexBytes, position, _key => new byte[0], columnFamily))
            {
                if (firstToRemove == null)
                    firstToRemove = recordJson.Item2;

                lastToRemove = recordJson.Item2;
            }

            if(firstToRemove != null && lastToRemove != null)
            {
                db.RemoveRange(columnFamily, (firstToRemove, lastToRemove));
            }

            //var count = toRemove.Count;
            //db.RemoveRange(toRemove, columnFamily);
            //return count;
        }

        internal static void RemoveRange(this RocksDb db, ColumnFamilyHandle columnFamily, params (byte[] First, byte[] Last)[] removeRanges)
        {
            if (removeRanges.Count() == 0) return;

            using (WriteBatch batch = new WriteBatch())
            {
                foreach(var removable in removeRanges)
                {
                    batch.DeleteRange(removable.First, (ulong)removable.First.Length, removable.Last, (ulong)removable.Last.Length, columnFamily);
                }

                db.Write(batch);
            }
        }

        internal static int RemoveRange(this RocksDb db, List<(Guid, byte[])> toRemove, ColumnFamilyHandle columnFamily)
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

                    var removable = items.Where(i => i.Item2?.Length > 0).ToArray();
                    if (removable.Length == 0)
                        continue;

                    var firstKey = removable[0].Item2;
                    var lastKey = removable[removable.Length - 1].Item2;

                    if (removable.Length == 1)
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
            return db.GetEntitiesBytes(parentIndexBytes, position, _key => db.Get(_key, valueColumnFamily), columnFamily);
        }

        internal static IEnumerable<(Guid, byte[], long, byte[])> GetEntitiesBytes(this RocksDb db, byte[] parentIndexBytes, long position, Func<byte[], byte[]> valueByKey, ColumnFamilyHandle columnFamily)
        {
            var readOptions = new ReadOptions();
            readOptions.SetPrefixSameAsStart(true);
            readOptions.SetTotalOrderSeek(false);
            var iterator = db.NewIterator(columnFamily, readOptions);

            try
            {
                iterator = iterator.Seek(parentIndexBytes);
                if (position != 0)
                    iterator = iterator.Seek(parentIndexBytes.Concat(position.ToBytesByAscending()));

                while (iterator.Valid())
                {
                    var jsonBytes = iterator.GetEntityBytes(valueByKey, out var id, out var key, out position);
                    yield return (id, key, position, jsonBytes);
                }
            }
            finally
            {
                iterator.Dispose();
            }
        }

        public static RocksInstance GetInstance(this IDictionary<int, RocksInstance> instances, Guid shardKey, out int shardIndex)
        {
            shardIndex = Math.Abs(shardKey.GetHashCode()) % instances.Count;

            if (!instances.TryGetValue(shardIndex, out var instance))
                throw new KeyNotFoundException($"RocksDb instance of shard '{shardIndex}' for shard key '{shardKey}' not found.");

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

                var baseKey = new byte[16];
                Array.Copy(itKey, 16, baseKey, 0, baseKey.Length);

                if (lastKey == null)
                    lastKey = baseKey;
                else if (!lastKey.StartsWith(baseKey))
                    return json;

                var value = iterator.Value();

                switch (itKey[itKey.Length - 1])
                {
                    case 0: id = new Guid(value); break;
                    case 1: json = value; break;
                    case 2: json = valueByKey(value.Concat(1)); break;
                    case 3: position = value.ToLong(); break;
                }

                Array.Resize(ref itKey, itKey.Length - 1);
                key = itKey;
                iterator = iterator.Next();
            }

            return json;
        }
    }
}
