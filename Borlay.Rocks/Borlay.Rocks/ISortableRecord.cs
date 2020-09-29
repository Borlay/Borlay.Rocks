using System;
using System.Collections.Generic;
using System.Text;

namespace Borlay.Rocks
{
    public interface ISortableRecord : IRecord
    {
        long Position { get; set; }
    }
}
