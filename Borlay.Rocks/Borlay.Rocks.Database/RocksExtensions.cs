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
        public static void Write<T>(this WriteBatch batch, Guid parentId, T record, ColumnFamilyHandle columnFamily) where T : ISortableRecord
        {
            var descTimeBytes = record.Position.ToBytesByDescending();

            var recordKey = parentId.ToByteArray()
                .Concat(descTimeBytes)
                .Concat(record.Id.ToByteArray(8));

            var json = Newtonsoft.Json.JsonConvert.SerializeObject(record, new Newtonsoft.Json.JsonSerializerSettings() { NullValueHandling = NullValueHandling.Ignore });
            var bytes = Encoding.UTF8.GetBytes(json);

            batch.Put(recordKey.Concat("id"), record.Id.ToByteArray(), columnFamily);
            batch.Put(recordKey.Concat("value"), bytes, columnFamily);
        }

        public static IEnumerable<T> GetRecords<T>(this RocksDb db, Guid parentId, long position, ColumnFamilyHandle columnFamily, bool clean = true) where T : ISortableRecord
        {
            var records = new Dictionary<Guid, T>();
            List<(Guid, byte[])> toRemove = new List<(Guid, byte[])>();

            try
            {
                foreach (var recordJson in db.GetRecordsJson(parentId, position, columnFamily))
                {
                    if (records.ContainsKey(recordJson.Item1))
                        toRemove.Add((recordJson.Item1, parentId.ToKey(recordJson.Item1, recordJson.Item2)));
                    else
                    {
                        var record = Newtonsoft.Json.JsonConvert.DeserializeObject<T>(recordJson.Item3);
                        record.Position = recordJson.Item2;
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

        internal static IEnumerable<(Guid, long, string)> GetRecordsJson(this RocksDb db, Guid userId, long position, ColumnFamilyHandle columnFamily)
        {
            var readOptions = new ReadOptions();
            readOptions.SetPrefixSameAsStart(true);
            readOptions.SetTotalOrderSeek(false);
            var iterator = db.NewIterator(columnFamily, readOptions);

            var prefix = userId.ToByteArray();
            try
            {
                iterator = iterator.Seek(prefix);
                if (position != 0)
                    iterator = iterator.Seek(prefix.Concat(position.ToBytesByDescending()));

                while (iterator.Valid())
                {
                    var json = iterator.GetRecordJson(out var id, out position);
                    yield return (id, position, json);
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

        public static string GetRecordJson(this Iterator iterator, out Guid id, out long position)
        {
            string json = null;
            id = Guid.NewGuid();
            position = 0;

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


                switch (field)
                {
                    case "id": id = new Guid(iterator.Value()); break;
                    case "value": json = iterator.StringValue(); break;
                }


                iterator = iterator.Next();
            }

            return json;
        }
    }
}
