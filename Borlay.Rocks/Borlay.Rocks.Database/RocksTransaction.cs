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

        public RocksInstance Instance { get; }

        public WriteBatch Batch { get; private set; }

        public long Position { get; protected set; }

        public int ShardIndex { get; protected set; }
        public RocksTransaction(RocksInstance instance, int shardIndex, Action dispose)
        {
            this.dispose = dispose;
            this.Instance = instance ?? throw new ArgumentNullException(nameof(instance));

            Batch = new WriteBatch();
            this.Position = DateTime.Now.ToFileTimeUtc();
            this.ShardIndex = shardIndex;
        }

        public void NextPosition()
        {
            Position++;
        }

        public void SetPosition(int position)
        {
            this.Position = position;
        }

        public void SaveEntity<T>(Guid parentId, T entity) where T : IEntity
        {
            SaveEntity(parentId.ToByteArray(), entity);
        }

        public void SaveEntity<T>(byte[] parentIndexBytes, T entity) where T: IEntity
        {
            if (!Instance.Entities.TryGetValue(typeof(T).Name, out var entityInfo))
                throw new ArgumentException($"Entity for type '{typeof(T).Name}' is not configured.");

            var valueIndex = entityInfo.ValueIndex;

            if (valueIndex == null)
                throw new Exception($"Entity should contain value index.");


            if(entity is IPosition position && position.Position <= 0)
                position.Position = Position;

            var valueKey = valueIndex.MakeKey(parentIndexBytes, entity);

            foreach (var index in entityInfo.Indexes)
            {
                var key = index.Value.MakeKey(parentIndexBytes, entity);
                var columnFamily = Instance.Families[index.Value.ColumnFamilyName];

                if (!index.Value.MatchEntity(entity))
                    continue;

                if (index.Value.HasValue)
                    Batch.Write<T>(key, entity, columnFamily);
                else
                    Batch.Write(key, entity.GetEntityId(), valueKey, columnFamily);
            }
        }

        public bool ContainsEntity<T>(Guid parentId, T entity)
        {
            return ContainsEntity<T>(parentId.ToByteArray(), entity, Order.None);
        }

        public bool ContainsEntity<T>(byte[] parentIndexBytes, T entity)
        {
            return ContainsEntity<T>(parentIndexBytes, entity, Order.None);
        }

        public bool ContainsEntity<T>(Guid parentId, T entity, Order order)
        {
            return ContainsEntity<T>(parentId.ToByteArray(), entity, order);
        }

        public bool ContainsEntity<T>(byte[] parentIndexBytes, T entity, Order order)
        {
            if (!Instance.Entities.TryGetValue(typeof(T).Name, out var entityInfo))
                throw new ArgumentException($"Entity for type '{typeof(T).Name}' is not configured.");

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

        public bool TryGetEntity<T>(Guid parentId, T entity, out T existingEntity)
        {
            return TryGetEntity<T>(parentId.ToByteArray(), entity, Order.None, out existingEntity);
        }

        public bool TryGetEntity<T>(Guid parentId, T entity, Order order, out T existingEntity)
        {
            return TryGetEntity<T>(parentId.ToByteArray(), entity, order, out existingEntity);
        }

        public bool TryGetEntity<T>(byte[] parentIndexBytes, T entity, out T existingEntity)
        {
            return TryGetEntity<T>(parentIndexBytes, entity, Order.None, out existingEntity);
        }

        public bool TryGetEntity<T>(byte[] parentIndexBytes, T entity, Order order, out T existingEntity)
        {
            if (!Instance.Entities.TryGetValue(typeof(T).Name, out var entityInfo))
                throw new ArgumentException($"Entity for type '{typeof(T).Name}' is not configured.");

            existingEntity = default(T);

            var valueColumnFamily = Instance.Families[entityInfo.ValueIndex.ColumnFamilyName];

            foreach (var index in entityInfo.Indexes)
            {
                if (index.Value.Order == order && index.Value.MatchEntity(entity))
                {
                    var columnFamily = Instance.Families[index.Value.ColumnFamilyName];
                    var key = index.Value.MakeKey(parentIndexBytes, entity);

                    byte[] entityBytes = null;

                    if (index.Value.HasValue)
                        entityBytes = Instance.Database.Get(key.Concat(1), columnFamily);
                    else
                    {
                        var valueIndexBytes = Instance.Database.Get(key.Concat(2), columnFamily);
                        entityBytes = Instance.Database.Get(valueIndexBytes.Concat(1), valueColumnFamily);
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

        public IEnumerable<T> GetEntities<T>(Guid parentId, DateTime position, Order order) where T : IEntity
        {
            return GetEntities<T>(parentId.ToByteArray(), position.ToFileTimeUtc(), order);
        }

        public IEnumerable<T> GetEntities<T>(Guid parentId, long position, Order order) where T : IEntity
        {
            return GetEntities<T>(parentId.ToByteArray(), position, order);
        }

        public IEnumerable<T> GetEntities<T>(Guid parentId, Order order) where T : IEntity
        {
            return GetEntities<T>(parentId.ToByteArray(), 0, order);
        }

        public IEnumerable<T> GetEntities<T>(byte[] parentIndexBytes, Order order) where T : IEntity
        {
            return GetEntities<T>(parentIndexBytes, 0, order);
        }

        public IEnumerable<T> GetEntities<T>(byte[] parentIndexBytes, long position, Order order) where T : IEntity
        {
            if (!Instance.Entities.TryGetValue(typeof(T).Name, out var entityInfo))
                throw new ArgumentException($"Entity for type '{typeof(T).Name}' is not configured.");

            if (entityInfo.ValueIndex == null)
                throw new Exception($"Entity should contain value index.");

            var valueColumnFamily = Instance.Families[entityInfo.ValueIndex.ColumnFamilyName];

            foreach (var indexKeyPair in entityInfo.Indexes)
            {
                var index = indexKeyPair.Value;
                var columnFamily = Instance.Families[index.ColumnFamilyName];

                if (index.Order == order && !index.HasMatch)
                    return Instance.Database.GetEntities<T>(parentIndexBytes, position, columnFamily, valueColumnFamily, index.AutoRemove);
            }

            return Enumerable.Empty<T>();
        }

        public void DeleteEntities<T>(byte[] parentIndexBytes, long position, Order order) where T : IEntity
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

        public IEnumerable<T> GetEntities<T, TEnum>(TEnum _enum, Guid parentId, DateTime position, Order order) where T : IEntity where TEnum : Enum
        {
            return GetEntities<T, TEnum>(_enum, parentId.ToByteArray(), position.ToFileTimeUtc(), order);
        }

        public IEnumerable<T> GetEntities<T, TEnum>(TEnum _enum, Guid parentId, long position, Order order) where T : IEntity where TEnum : Enum
        {
            return GetEntities<T, TEnum>(_enum, parentId.ToByteArray(), position, order);
        }

        public IEnumerable<T> GetEntities<T, TEnum>(TEnum _enum, byte[] parentIndexBytes, DateTime position, Order order) where T : IEntity where TEnum : Enum
        {
            return GetEntities<T, TEnum>(_enum, parentIndexBytes, position.ToFileTimeUtc(), order);
        }

        public IEnumerable<T> GetEntities<T, TEnum>(TEnum _enum, byte[] parentIndexBytes, long position, Order order) where T : IEntity where TEnum : Enum
        {
            if (!Instance.Entities.TryGetValue(typeof(T).Name, out var entityInfo))
                throw new ArgumentException($"Entity for type '{typeof(T).Name}' is not configured.");

            if (entityInfo.ValueIndex == null)
                throw new Exception($"Entity should contain value index.");

            var valueColumnFamily = Instance.Families[entityInfo.ValueIndex.ColumnFamilyName];

            foreach (var indexKeyPair in entityInfo.Indexes)
            {
                var index = indexKeyPair.Value;
                var columnFamily = Instance.Families[index.ColumnFamilyName];

                if (index.Order == order && index.HasMatch && index.MatchEnum(_enum))
                    return Instance.Database.GetEntities<T>(parentIndexBytes, position, columnFamily, valueColumnFamily);
            }

            return Enumerable.Empty<T>();
        }

        public virtual void Commit()
        {
            Instance.Database.Write(Batch);
            Batch.Dispose();
            Batch = new WriteBatch();
        }

        public virtual void Dispose()
        {
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
