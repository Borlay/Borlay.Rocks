using System;
using System.Collections.Generic;
using System.Text;

namespace Borlay.Rocks.Database
{
    public class Entity
    {
        public Type Type { get; }

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
                    else if (ValueIndex.Order != Order.None && value.Order == Order.None)
                        ValueIndex = value;
                }

                indexes[name] = value;
            }
            get => indexes[name];
        }

    }
}
