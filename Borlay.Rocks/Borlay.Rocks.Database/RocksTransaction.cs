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

        public WriteBatch Batch { get; }

        public long Position { get; protected set; }

        public RocksTransaction(RocksInstance instance, Action dispose)
        {
            this.dispose = dispose;
            this.Instance = instance ?? throw new ArgumentNullException(nameof(instance));

            Batch = new WriteBatch();
            this.Position = DateTime.Now.ToFileTimeUtc();
        }

        public void NextPosition()
        {
            Position++;
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

            var position = Position;

            if (entity is IPosition entityPosition)
            {
                if (entityPosition.Position > 0)
                    position = entityPosition.Position;
                else
                    entityPosition.Position = position;
            }

            var entityIndexBytes = valueIndex.GetIndex(entity);
            var valueIndexBytes = parentIndexBytes.MakeIndex(entityIndexBytes, position, valueIndex.Order);

            foreach (var index in entityInfo.Indexes)
            {
                var eIndexBytes = index.Value.GetIndex(entity);
                var columnFamily = Instance.Families[index.Value.ColumnFamilyName];

                if (!index.Value.MatchEntity(entity))
                    continue;

                if (index.Value.HasValue)
                    Batch.Write<T>(parentIndexBytes, entity, eIndexBytes, position, index.Value.Order, columnFamily);
                else
                    Batch.Write(parentIndexBytes, valueIndexBytes, eIndexBytes, position, index.Value.Order, columnFamily);
            }
        }

        public bool ContainsEntity<T>(Guid parentId, T entity)
        {
            return ContainsEntity<T>(parentId.ToByteArray(), entity, 0, Order.None);
        }

        public bool ContainsEntity<T>(byte[] parentIndexBytes, T entity)
        {
            return ContainsEntity<T>(parentIndexBytes, entity, 0, Order.None);
        }

        public bool ContainsEntity<T>(Guid parentId, T entity, long position, Order order)
        {
            return ContainsEntity<T>(parentId.ToByteArray(), entity, position, order);
        }

        public bool ContainsEntity<T>(Guid parentId, T entity, DateTime position, Order order)
        {
            return ContainsEntity<T>(parentId.ToByteArray(), entity, position.ToFileTimeUtc(), order);
        }

        public bool ContainsEntity<T>(byte[] parentIndexBytes, T entity, DateTime position, Order order)
        {
            return ContainsEntity<T>(parentIndexBytes, entity, position.ToFileTimeUtc(), order);
        }

        public bool ContainsEntity<T>(byte[] parentIndexBytes, T entity, long position, Order order)
        {
            if (!Instance.Entities.TryGetValue(typeof(T).Name, out var entityInfo))
                throw new ArgumentException($"Entity for type '{typeof(T).Name}' is not configured.");

            foreach (var index in entityInfo.Indexes)
            {
                var eIndexBytes = index.Value.GetIndex(entity);
                var columnFamily = Instance.Families[index.Value.ColumnFamilyName];

                if (index.Value.Order == order && index.Value.MatchEntity(entity))
                {
                    var indexBytes = parentIndexBytes.MakeIndex(eIndexBytes, position, order);

                    var enitityBytes = Instance.Database.Get(indexBytes.Concat(0), columnFamily);
                    return enitityBytes?.Length > 0;
                }
            }

            return false;
        }

        public bool TryGetEntity<T>(byte[] parentIndexBytes, T entity, long position, Order order, out T existingEntity)
        {
            if (!Instance.Entities.TryGetValue(typeof(T).Name, out var entityInfo))
                throw new ArgumentException($"Entity for type '{typeof(T).Name}' is not configured.");

            existingEntity = default(T);

            foreach (var indexKeyPair in entityInfo.Indexes)
            {
                var index = indexKeyPair.Value;
                var eIndexBytes = index.GetIndex(entity);
                var columnFamily = Instance.Families[index.ColumnFamilyName];

                if (index.Order == order && index.MatchEntity(entity))
                {
                    var indexBytes = parentIndexBytes.MakeIndex(eIndexBytes, position, order);

                    byte[] entityBytes = null;

                    if (index.HasValue)
                        entityBytes = Instance.Database.Get(indexBytes.Concat(1), columnFamily);
                    else
                    {
                        var valueIndexBytes = Instance.Database.Get(indexBytes.Concat(2), columnFamily);
                        entityBytes = Instance.Database.Get(valueIndexBytes.Concat(1), columnFamily);
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
                    return Instance.Database.GetEntities<T>(parentIndexBytes, position, columnFamily, valueColumnFamily);
            }

            return Enumerable.Empty<T>();
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


        private bool commited = false;
        public virtual void Commit()
        {
            if (commited)
                throw new ObjectDisposedException("Transaction cannot be committed twice");

            commited = true;
            Instance.Database.Write(Batch);
        }

        public virtual void Dispose()
        {
            try
            {
                Batch.Dispose();
            }
            finally
            {
                dispose?.Invoke();
            }
        }
    }
}
