using System;
using System.Collections.Generic;
using System.Text;

namespace Borlay.Rocks
{
    public interface ISortedRepository
    {
        IEnumerable<T> GetRecords<T>(Guid parentId, long position, string columnFamily) where T : ISortableRecord;
        IEnumerable<T> GetRecords<T>(Guid shardKey, Guid parentId, long position, string columnFamily) where T : ISortableRecord;
    }
}
