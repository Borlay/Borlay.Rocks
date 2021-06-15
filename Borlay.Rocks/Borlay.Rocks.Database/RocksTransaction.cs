using Borlay.Arrays;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Borlay.Rocks.Database
{
    public class RocksTransaction : IDisposable
    {
        private readonly Action dispose;
        private readonly List<Action> commits = new List<Action>();

        public RocksInstance Instance { get; }

        public WriteBatch Batch { get; private set; }

        public long Position { get; protected set; }

        public int ShardIndex { get; protected set; }

        private readonly byte[] parentIndexBytes;

        public Guid ParentId { get; }

        public RocksTransaction(RocksInstance instance, Guid parentId, int shardIndex, Action dispose)
            : this(instance, parentId.ToByteArray(), shardIndex, dispose) 
        {
            this.ParentId = parentId;
        }

        private RocksTransaction(RocksInstance instance, byte[] parentIndexBytes, int shardIndex, Action dispose)
        {
            this.dispose = dispose;
            this.Instance = instance ?? throw new ArgumentNullException(nameof(instance));

            Batch = new WriteBatch();
            this.Position = DateTime.UtcNow.ToFileTime();

            this.ShardIndex = shardIndex;
            this.parentIndexBytes = parentIndexBytes ?? throw new ArgumentNullException(nameof(parentIndexBytes));
        }

        public long NextPosition()
        {
            return ++Position;
        }

        public long SetNextPosition<T>(T entity) where T: IPosition
        {
            entity.Position = NextPosition();
            return this.Position;
        }

        public void SetPosition(long position)
        {
            this.Position = position;
        }

        public byte[] SaveEntity<T>(T entity) where T : IEntity
        {
            return SaveEntity<T>(entity, e =>
            {
                var json = Newtonsoft.Json.JsonConvert.SerializeObject(entity, new Newtonsoft.Json.JsonSerializerSettings() { NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore });
                return Encoding.UTF8.GetBytes(json);
            });
        }

        public byte[] SaveEntity<T>(T entity, Func<T, byte[]> bodyProvider) where T: IEntity
        {
            if (!Instance.Entities.TryGetValue(typeof(T).Name, out var entityInfo))
                throw new ArgumentException($"Entity for type '{typeof(T).Name}' is not configured.");

            var valueIndex = entityInfo.ValueIndex;

            if (valueIndex == null)
                throw new Exception($"Entity should contain value index.");

            var pos = Position;

            if (entity is IPosition position)
            {
                if(position.Position <= 0)
                    position.Position = Position;

                pos = position.Position;
            }

            var valueKey = valueIndex.MakeKey(parentIndexBytes, entity);

            //var json = Newtonsoft.Json.JsonConvert.SerializeObject(entity, new Newtonsoft.Json.JsonSerializerSettings() { NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore });
            //var bytes = Encoding.UTF8.GetBytes(json);
            var bytes = bodyProvider(entity);

            foreach (var indexPair in entityInfo.Indexes)
            {
                var index = indexPair.Value;
                var key = index.MakeKey(parentIndexBytes, entity);
                var byteKey = new ByteArray(key.ToArray()); 
                var columnFamily = Instance.Families[index.ColumnFamilyName];

                if (!index.MatchEntity(entity))
                    continue;

                index.RemoveValue(byteKey);

                if (index.HasValue)
                    Batch.Write(key, entity.GetEntityId(), bytes, pos, columnFamily);
                else
                    Batch.Write(key, entity.GetEntityId(), pos, valueKey, columnFamily);

                commits.Add(() => index.TrySetValue(byteKey, bytes));
            }

            return bytes;
        }

        public bool ContainsEntity<T>(T entity)
        {
            return ContainsEntityAs<T, T>(entity, Order.None);
        }

        public bool ContainsEntity<T>(T entity, Order order)
        {
            return ContainsEntityAs<T, T>(entity, order);
        }

        public bool ContainsEntityAs<T, TRepository>(T entity)
        {
            return ContainsEntityAs<T, TRepository>(entity, Order.None);
        }

        public bool ContainsEntityAs<T, TRepository>(T entity, Order order)
        {
            if (!Instance.Entities.TryGetValue(typeof(TRepository).Name, out var entityInfo))
                throw new ArgumentException($"Entity for type '{typeof(TRepository).Name}' is not configured.");

            foreach (var index in entityInfo.Indexes)
            {
                if (index.Value.Order == order && index.Value.MatchEntity(entity))
                {
                    var columnFamily = Instance.Families[index.Value.ColumnFamilyName];
                    var key = index.Value.MakeKey(parentIndexBytes, entity);

                    var enitityBytes = Instance.Database.Get(key.Concat(0), columnFamily);
                    return enitityBytes?.Length > 0;
                }
            }

            return false;
        }

        public bool TryGetEntity<T>(T entity, out T existingEntity)
        {
            return TryGetEntityAs<T, T>(entity, Order.None, out existingEntity);
        }

        public bool TryGetEntity<T>(T entity, Order order, out T existingEntity)
        {
            return TryGetEntityAs<T, T>(entity, order, out existingEntity);
        }

        public bool TryGetEntityAs<T, TRepository>(T entity, out T existingEntity)
        {
            return TryGetEntityAs<T, TRepository>(entity, Order.None, out existingEntity);
        }

        public bool TryGetEntityAs<T, TRepository>(T entity, Order order, out T existingEntity)
        {
            if (!Instance.Entities.TryGetValue(typeof(TRepository).Name, out var entityInfo))
                throw new ArgumentException($"Entity for type '{typeof(TRepository).Name}' is not configured.");

            existingEntity = default(T);

            var valueColumnFamily = Instance.Families[entityInfo.ValueIndex.ColumnFamilyName];

            foreach (var index in entityInfo.Indexes)
            {
                if (index.Value.Order == order && index.Value.MatchEntity(entity))
                {
                    var columnFamily = Instance.Families[index.Value.ColumnFamilyName];
                    var key = index.Value.MakeKey(parentIndexBytes, entity);

                    if (!index.Value.TryGetValue(key, out var entityBytes))
                    {
                        if (index.Value.HasValue)
                        {
                            entityBytes = Instance.Database.Get(key.Concat(1), columnFamily);
                        }
                        else
                        {
                            var valueIndexBytes = Instance.Database.Get(key.Concat(2), columnFamily);
                            entityBytes = Instance.Database.Get(valueIndexBytes.Concat(1), valueColumnFamily);
                        }

                        index.Value.TrySetValue(key, entityBytes);
                    }

                    if(entityBytes?.Length > 0)
                    {
                        var json = Encoding.UTF8.GetString(entityBytes);
                        existingEntity = Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json);
                        return true;
                    }

                    return false;
                }
            }

            return false;
        }

        public IEnumerable<T> GetEntities<T>(DateTime position, Order order, string indexNamePrefix = null) where T : IEntity
        {
            return GetEntitiesAs<T, T>(position.ToFileTimeUtc(), order, indexNamePrefix);
        }

        public IEnumerable<T> GetEntities<T>(long position, Order order, string indexNamePrefix = null) where T : IEntity
        {
            return GetEntitiesAs<T, T>(position, order, indexNamePrefix);
        }

        public IEnumerable<T> GetEntities<T>(Order order, string indexNamePrefix = null) where T : IEntity
        {
            return GetEntitiesAs<T, T>(0, order, indexNamePrefix);
        }

        public IEnumerable<T> GetEntitiesAs<T, TRepository>(DateTime position, Order order, string indexNamePrefix = null) where T : IEntity
        {
            return GetEntitiesAs<T, TRepository>(position.ToFileTimeUtc(), order, indexNamePrefix);
        }

        public IEnumerable<T> GetEntitiesAs<T, TRepository>(Order order, string indexNamePrefix = null) where T : IEntity
        {
            return GetEntitiesAs<T, TRepository>(0, order, indexNamePrefix);
        }

        public IEnumerable<T> GetEntitiesAs<T, TRepository>(long position, Order order, string indexNamePrefix = null) where T : IEntity
        {
            if (!Instance.Entities.TryGetValue(typeof(TRepository).Name, out var entityInfo))
                throw new ArgumentException($"Entity for type '{typeof(TRepository).Name}' is not configured.");

            if (entityInfo.ValueIndex == null)
                throw new Exception($"Entity should contain value index.");

            var valueColumnFamily = Instance.Families[entityInfo.ValueIndex.ColumnFamilyName];

            if (position != 0 && order == Order.Descending)
                position = long.MaxValue - position;

            foreach (var indexKeyPair in entityInfo.Indexes)
            {
                if (indexNamePrefix != null && !indexKeyPair.Key.StartsWith(indexNamePrefix))
                    continue;

                var index = indexKeyPair.Value;
                var columnFamily = Instance.Families[index.ColumnFamilyName];

                if (index.Order == order && (!index.HasMatch || indexNamePrefix != null))
                    return Instance.Database.GetEntities<T>(parentIndexBytes, position, columnFamily, valueColumnFamily, index.AutoRemove);
            }

            return Enumerable.Empty<T>();
        }

        /// <summary>
        /// Delete entity from index that matches.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity"></param>
        /// <param name="order"></param>
        /// <param name="indexNamePrefix"></param>
        public void DeleteEntity<T>(T entity, Order order, string indexNamePrefix = null) where T : IEntity
        {
            DeleteEntityAs<T, T>(entity, order, indexNamePrefix);
        }


        /// <summary>
        /// Delete entity from index that matches.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity"></param>
        /// <param name="order"></param>
        /// <param name="indexNamePrefix"></param>
        public void DeleteEntityAs<T, TRepository>(T entity, Order order, string indexNamePrefix = null) where T : IEntity
        {
            if (!Instance.Entities.TryGetValue(typeof(TRepository).Name, out var entityInfo))
                throw new ArgumentException($"Entity for type '{typeof(TRepository).Name}' is not configured.");

            foreach (var index in entityInfo.Indexes)
            {
                if (index.Value.Order == order && index.Value.MatchEntity(entity))
                {
                    var columnFamily = Instance.Families[index.Value.ColumnFamilyName];
                    var key = index.Value.MakeKey(parentIndexBytes, entity);

                    index.Value.RemoveValue(key);
                    Batch.Delete(key.Concat(0), columnFamily);
                    Batch.Delete(key.Concat(1), columnFamily);
                    Batch.Delete(key.Concat(2), columnFamily);
                    Batch.Delete(key.Concat(3), columnFamily);
                }
            }
        }


        /// <summary>
        /// Delete entity from all indexes.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity"></param>
        /// <param name="order"></param>
        /// <param name="indexNamePrefix"></param>
        public void DeleteEntity<T>(T entity) where T : IEntity
        {
            DeleteEntityAs<T, T>(entity);
        }

        /// <summary>
        /// Delete entity from all indexes.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity"></param>
        /// <param name="order"></param>
        /// <param name="indexNamePrefix"></param>
        public void DeleteEntityAs<T, TRepository>(T entity) where T : IEntity
        {
            if (!Instance.Entities.TryGetValue(typeof(TRepository).Name, out var entityInfo))
                throw new ArgumentException($"Entity for type '{typeof(TRepository).Name}' is not configured.");

            foreach (var index in entityInfo.Indexes)
            {
                var columnFamily = Instance.Families[index.Value.ColumnFamilyName];
                var key = index.Value.MakeKey(parentIndexBytes, entity);

                index.Value.RemoveValue(key);
                Batch.Delete(key.Concat(0), columnFamily);
                Batch.Delete(key.Concat(1), columnFamily);
                Batch.Delete(key.Concat(2), columnFamily);
                Batch.Delete(key.Concat(3), columnFamily);
            }
        }

        public void DeleteEntities<T>(long position, Order order) where T : IEntity
        {
            if (!Instance.Entities.TryGetValue(typeof(T).Name, out var entityInfo))
                throw new ArgumentException($"Entity for type '{typeof(T).Name}' is not configured.");

            if (entityInfo.ValueIndex == null)
                throw new Exception($"Entity should contain value index.");

            foreach (var indexKeyPair in entityInfo.Indexes)
            {
                var index = indexKeyPair.Value;
                var columnFamily = Instance.Families[index.ColumnFamilyName];

                if (index.Order == order && !index.HasMatch)
                    Instance.Database.DeleteEntities(parentIndexBytes, position, columnFamily);
            }
        }

        

        //public IEnumerable<T> GetEntities<T, TEnum>(TEnum _enum, Guid parentId, DateTime position, Order order) where T : IEntity where TEnum : Enum
        //{
        //    return GetEntities<T, TEnum>(_enum, parentId.ToByteArray(), position.ToFileTimeUtc(), order);
        //}

        //public IEnumerable<T> GetEntities<T, TEnum>(TEnum _enum, Guid parentId, long position, Order order) where T : IEntity where TEnum : Enum
        //{
        //    return GetEntities<T, TEnum>(_enum, parentId.ToByteArray(), position, order);
        //}

        //public IEnumerable<T> GetEntities<T, TEnum>(TEnum _enum, byte[] parentIndexBytes, DateTime position, Order order) where T : IEntity where TEnum : Enum
        //{
        //    return GetEntities<T, TEnum>(_enum, parentIndexBytes, position.ToFileTimeUtc(), order);
        //}

        //public IEnumerable<T> GetEntities<T, TEnum>(TEnum _enum, byte[] parentIndexBytes, long position, Order order) where T : IEntity where TEnum : Enum
        //{
        //    if (!Instance.Entities.TryGetValue(typeof(T).Name, out var entityInfo))
        //        throw new ArgumentException($"Entity for type '{typeof(T).Name}' is not configured.");

        //    if (entityInfo.ValueIndex == null)
        //        throw new Exception($"Entity should contain value index.");

        //    var valueColumnFamily = Instance.Families[entityInfo.ValueIndex.ColumnFamilyName];

        //    foreach (var indexKeyPair in entityInfo.Indexes)
        //    {
        //        var index = indexKeyPair.Value;
        //        var columnFamily = Instance.Families[index.ColumnFamilyName];

        //        if (index.Order == order && index.HasMatch && index.MatchEnum(_enum))
        //            return Instance.Database.GetEntities<T>(parentIndexBytes, position, columnFamily, valueColumnFamily);
        //    }

        //    return Enumerable.Empty<T>();
        //}

        public virtual void Commit()
        {
            Instance.Database.Write(Batch);

            foreach (var commit in commits)
                commit.Invoke();

            commits.Clear();
            Batch.Dispose();
            Batch = new WriteBatch();
        }

        public virtual void Dispose()
        {
            commits.Clear();

            try
            {
                Batch.Dispose();
            }
            catch(Exception e)
            {
                Console.WriteLine(e.ToString());
            }
            finally
            {
                dispose?.Invoke();
            }
        }
    }
}
